#!/usr/bin/env python3
# Copyright 2026 Snowflake Inc.
# SPDX-License-Identifier: Apache-2.0

import types

from diff_environments import diff_environments
from diff_live import _nifi_diff_has_changes, diff_nifi_root_parameter_context, diff_runtime
from format_change_plan import format_change_plan, format_nifi_section
from translate_live_diff import translate


def _parameter(name, value=None, sensitive=False, refs=None):
    return types.SimpleNamespace(
        name=name,
        value=value,
        sensitive=sensitive,
        referenced_assets=list(refs or []),
    )


def _parameter_entity(parameter):
    return types.SimpleNamespace(parameter=parameter)


def _asset(asset_id, name, missing_content=False):
    return types.SimpleNamespace(id=asset_id, name=name, missing_content=missing_content)


def _asset_entity(asset):
    return types.SimpleNamespace(asset=asset)


def _provider_cfg(provider_id="pp-1"):
    return types.SimpleNamespace(id=provider_id)


def _parameter_context(context_id, name, parameters=None, inherited=None, provider_cfg=None):
    return types.SimpleNamespace(
        id=context_id,
        component=types.SimpleNamespace(
            id=context_id,
            name=name,
            parameters=[_parameter_entity(parameter) for parameter in (parameters or [])],
            inherited_parameter_contexts=list(inherited or []),
            parameter_provider_configuration=provider_cfg,
        ),
        revision=types.SimpleNamespace(version=0),
    )


def _context_ref(context_id, name, provider_cfg=None):
    return types.SimpleNamespace(
        id=context_id,
        component=types.SimpleNamespace(
            id=context_id,
            name=name,
            parameter_provider_configuration=provider_cfg,
        ),
    )


def _process_group(group_id, context_ref=None):
    return types.SimpleNamespace(
        id=group_id,
        component=types.SimpleNamespace(id=group_id, name="root", parameter_context=context_ref),
    )


def _install_root_parameter_context_api(
    fake_nipyapi,
    *,
    root_pg=None,
    parameter_context=None,
    assets=None,
    list_contexts=None,
    process_group_error=None,
    context_error=None,
    assets_error=None,
):
    def get_process_group(**kwargs):
        if process_group_error is not None:
            raise process_group_error
        assert kwargs["id"] == "root"
        return root_pg

    def get_parameter_context(**kwargs):
        if context_error is not None:
            raise context_error
        assert parameter_context is not None
        return parameter_context

    def get_assets1(**kwargs):
        if assets_error is not None:
            raise assets_error
        return types.SimpleNamespace(assets=[_asset_entity(asset) for asset in (assets or [])])

    def get_parameter_contexts(**kwargs):
        return types.SimpleNamespace(parameter_contexts=list(list_contexts or []))

    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_process_group"] = get_process_group
    fake_nipyapi.api_methods["ParameterContextsApi"]["get_parameter_context"] = get_parameter_context
    fake_nipyapi.api_methods["ParameterContextsApi"]["get_assets1"] = get_assets1
    fake_nipyapi.api_methods["FlowApi"]["get_parameter_contexts"] = get_parameter_contexts


def _describe_module_stubs():
    return {
        "manage_flows": types.SimpleNamespace(configure_nifi=lambda *args, **kwargs: None, list_process_groups=lambda parent_id=None: []),
        "manage_controller_services": types.SimpleNamespace(list_controller_services=lambda: [], list_root_pg_controller_services=lambda: []),
        "manage_service_bindings": types.SimpleNamespace(build_root_pg_service_uuid_to_name_map=lambda *_args, **_kwargs: {}, describe_flow_service_bindings=lambda *_args, **_kwargs: []),
        "setup_registry_client": types.SimpleNamespace(list_registry_clients=lambda: []),
        "manage_parameter_providers": types.SimpleNamespace(),
    }


def test_describe_nifi_state_reports_root_parameter_context_without_secret_leakage(fake_nipyapi, import_cd_module):
    fake_nipyapi.api_methods["FlowApi"]["get_controller_services_from_controller"] = lambda **kwargs: types.SimpleNamespace(controller_services=[])
    fake_nipyapi.api_methods["FlowApi"]["get_controller_services_from_group"] = lambda **kwargs: types.SimpleNamespace(controller_services=[])
    fake_nipyapi.api_methods["FlowApi"]["get_parameter_providers"] = lambda **kwargs: types.SimpleNamespace(parameter_providers=[])
    provider_ref = _context_ref("pc-provider", "Provider POSTGRES", provider_cfg=_provider_cfg())
    parameter_context = _parameter_context(
        "pc-root",
        "Root Parameter Context",
        parameters=[
            _parameter("Database Driver", refs=[types.SimpleNamespace(id="asset-1", name="driver.jar")]),
            _parameter("Shared Query Timeout", value="30 sec"),
            _parameter("POSTGRES_PWD", value=None, sensitive=True),
        ],
        inherited=[provider_ref],
    )
    _install_root_parameter_context_api(
        fake_nipyapi,
        root_pg=_process_group("root", _context_ref("pc-root", "Root Parameter Context")),
        parameter_context=parameter_context,
        assets=[_asset("asset-1", "driver.jar")],
        list_contexts=[provider_ref, _context_ref("pc-root", "Root Parameter Context")],
    )
    module = import_cd_module("describe_nifi_state", _describe_module_stubs())

    state = module.describe_nifi_state(
        "https://example.invalid/nifi-api",
        pat="token",
        desired_root_parameter_context={
            "provided_parameter_contexts": ".*POSTGRES",
            "parameters": {
                "Shared Query Timeout": "30 sec",
                "POSTGRES_PWD": "${{ secrets.POSTGRES_PWD }}",
            },
            "assets": [{"name": "driver.jar", "url": "https://secret.invalid/driver.jar", "parameter": "Database Driver"}],
        },
    )

    root_state = state["root_parameter_context"]
    assert root_state["context"] == {"present": True, "id": "pc-root", "name": "Root Parameter Context"}
    assert root_state["inherited"] == ["Provider POSTGRES"]
    assert root_state["provider_context_names"] == ["Provider POSTGRES"]
    assert root_state["parameters"] == {
        "Database Driver": None,
        "Shared Query Timeout": "30 sec",
        "POSTGRES_PWD": "<sensitive>",
    }
    assert root_state["entries"] == [{
        "name": "driver.jar",
        "parameter": "Database Driver",
        "status": "matched",
        "asset_id": "asset-1",
        "missing_content": False,
        "messages": [],
    }]
    assert root_state["blocked"] is False
    assert root_state["messages"] == []
    assert "secret.invalid" not in str(root_state)
    assert "token" not in str(root_state)


def test_describe_nifi_state_marks_missing_root_context(fake_nipyapi, import_cd_module):
    fake_nipyapi.api_methods["FlowApi"]["get_controller_services_from_controller"] = lambda **kwargs: types.SimpleNamespace(controller_services=[])
    fake_nipyapi.api_methods["FlowApi"]["get_controller_services_from_group"] = lambda **kwargs: types.SimpleNamespace(controller_services=[])
    fake_nipyapi.api_methods["FlowApi"]["get_parameter_providers"] = lambda **kwargs: types.SimpleNamespace(parameter_providers=[])
    provider_ref = _context_ref("pc-provider", "Provider POSTGRES", provider_cfg=_provider_cfg())
    _install_root_parameter_context_api(
        fake_nipyapi,
        root_pg=_process_group("root", None),
        parameter_context=None,
        assets=[],
        list_contexts=[provider_ref],
    )
    module = import_cd_module("describe_nifi_state", _describe_module_stubs())

    state = module.describe_nifi_state(
        "https://example.invalid/nifi-api",
        pat="token",
        desired_root_parameter_context={
            "provided_parameter_contexts": ".*POSTGRES",
            "parameters": {"Shared Query Timeout": "30 sec"},
            "assets": [{"name": "driver.jar", "url": "https://example.invalid/driver.jar", "parameter": "Database Driver"}],
        },
    )

    root_state = state["root_parameter_context"]
    assert root_state["context"] == {"present": False, "id": None, "name": None}
    assert root_state["provider_context_names"] == ["Provider POSTGRES"]
    assert root_state["inherited"] == []
    assert root_state["parameters"] == {}
    assert root_state["entries"][0]["status"] == "missing_context"
    assert root_state["blocked"] is False


def test_describe_nifi_state_blocks_root_parameter_context_read_and_sanitizes_error(fake_nipyapi, import_cd_module):
    class _LeakyError(Exception):
        status = 403
        reason = "Forbidden"
        body = "HTTP response body: password=secret"

    fake_nipyapi.api_methods["FlowApi"]["get_controller_services_from_controller"] = lambda **kwargs: types.SimpleNamespace(controller_services=[])
    fake_nipyapi.api_methods["FlowApi"]["get_controller_services_from_group"] = lambda **kwargs: types.SimpleNamespace(controller_services=[])
    fake_nipyapi.api_methods["FlowApi"]["get_parameter_providers"] = lambda **kwargs: types.SimpleNamespace(parameter_providers=[])
    provider_ref = _context_ref("pc-provider", "Provider POSTGRES", provider_cfg=_provider_cfg())
    _install_root_parameter_context_api(
        fake_nipyapi,
        root_pg=_process_group("root", _context_ref("pc-root", "Root Parameter Context")),
        parameter_context=_parameter_context("pc-root", "Root Parameter Context", inherited=[provider_ref]),
        assets_error=_LeakyError(),
        list_contexts=[provider_ref],
    )
    module = import_cd_module("describe_nifi_state", _describe_module_stubs())

    state = module.describe_nifi_state(
        "https://example.invalid/nifi-api",
        pat="token",
        desired_root_parameter_context={
            "assets": [{"name": "driver.jar", "url": "https://example.invalid/driver.jar", "parameter": "Database Driver"}],
        },
    )

    root_state = state["root_parameter_context"]
    assert root_state["blocked"] is True
    assert root_state["messages"] == ["Failed to inspect root parameter-context assets: HTTP 403 Forbidden"]
    assert root_state["entries"][0]["status"] == "blocked"
    assert "secret" not in str(root_state)
    assert "example.invalid/driver.jar" not in str(root_state)


def test_diff_root_parameter_context_covers_inheritance_parameters_assets_and_runtime_changes():
    desired_root_parameter_context = {
        "provided_parameter_contexts": "Base|Provider POSTGRES",
        "parameters": {
            "Shared Query Timeout": "30 sec",
            "Static": "same",
            "Sensitive": "${{ secrets.SENSITIVE }}",
            "New": "created",
        },
        "assets": [
            {"name": "driver.jar", "parameter": "Database Driver"},
            {"name": "stable.jar", "parameter": "Stable Driver"},
            {"name": "bad.jar", "parameter": "Bad Driver"},
        ],
    }
    live_root_parameter_context = {
        "context": {"present": True, "id": "pc-root", "name": "Root Parameter Context"},
        "inherited": ["Base"],
        "provider_context_names": ["Base", "Provider POSTGRES"],
        "parameters": {
            "Shared Query Timeout": "15 sec",
            "Static": "same",
            "Sensitive": "<sensitive>",
        },
        "entries": [
            {"name": "driver.jar", "parameter": "Database Driver", "status": "missing_asset", "asset_id": None, "missing_content": None, "messages": ["missing"]},
            {"name": "stable.jar", "parameter": "Stable Driver", "status": "matched", "asset_id": "asset-2", "missing_content": False, "messages": []},
            {"name": "bad.jar", "parameter": "Bad Driver", "status": "literal_parameter", "asset_id": None, "missing_content": None, "messages": ["literal"]},
        ],
        "blocked": False,
        "messages": [],
    }

    root_diff = diff_nifi_root_parameter_context(live_root_parameter_context, desired_root_parameter_context)
    assert root_diff["inheritance"] == {
        "missing": ["Provider POSTGRES"],
        "unchanged": ["Base"],
        "unmatched_pattern": False,
    }
    assert root_diff["parameters"]["changes"] == {
        "Shared Query Timeout": {"live": "15 sec", "desired": "30 sec"},
        "New": {"live": None, "desired": "created"},
    }
    assert root_diff["parameters"]["unchanged"] == ["Static"]
    assert root_diff["created"][0]["status"] == "missing_asset"
    assert root_diff["unchanged"] == [{
        "name": "stable.jar",
        "parameter": "Stable Driver",
        "asset_id": "asset-2",
        "missing_content": False,
        "messages": [],
    }]
    assert root_diff["health"][0]["status"] == "literal_parameter"

    runtime_diff = diff_runtime(
        {
            "status": "ACTIVE",
            "network_rules": [],
            "connectors": [],
            "nifi": {
                "controller_services": [],
                "root_pg_controller_services": [],
                "parameter_providers": [],
                "flow_registries": [],
                "flows": [],
                "parameters": {},
                "service_bindings": {"created": [], "modified": [], "deleted": [], "unchanged": [], "health": [], "blocked": []},
                "root_parameter_context": live_root_parameter_context,
            },
        },
        {"name": "MY_RUNTIME", "root_parameter_context": desired_root_parameter_context},
    )
    assert _nifi_diff_has_changes(runtime_diff["nifi"]) is True
    assert runtime_diff["nifi"]["root_parameter_context"]["inheritance"]["missing"] == ["Provider POSTGRES"]


def test_translate_and_render_include_root_parameter_context_changes_without_secret_or_url_leakage():
    nifi_diff = {
        "controller_services": {"created": [], "modified": [], "deleted": [], "unchanged": []},
        "root_pg_controller_services": {"created": [], "modified": [], "deleted": [], "unchanged": []},
        "parameter_providers": {"created": [], "modified": [], "deleted": [], "unchanged": []},
        "flow_registries": {"created": [], "modified": [], "deleted": [], "unchanged": []},
        "flows": {"created": [], "modified": [], "deleted": [], "unchanged": []},
        "parameters": {},
        "service_bindings": {"created": [], "modified": [], "deleted": [], "unchanged": [], "health": [], "blocked": []},
        "root_parameter_context": {
            "created": [{"name": "driver.jar", "parameter": "Database Driver", "status": "missing_asset", "messages": ["missing asset"]}],
            "modified": [{"name": "driver-b.jar", "parameter": "Driver B", "status": "missing_content", "messages": ["content missing"]}],
            "deleted": [{"name": "old-driver.jar", "parameter": "Old Driver"}],
            "unchanged": [],
            "health": [{"name": "bad-driver.jar", "parameter": "Bad Driver", "status": "literal_parameter", "messages": ["literal | bad `value` <tag>"]}],
            "blocked": [{"name": "blocked-driver.jar", "parameter": "Blocked Driver", "status": "blocked", "messages": ["HTTP 403 Forbidden"]}],
            "context": {"present": True, "id": "pc-root", "name": "Root Parameter Context"},
            "inheritance": {"missing": ["Provider POSTGRES"], "unchanged": [], "unmatched_pattern": False},
            "parameters": {
                "changes": {
                    "Shared Query Timeout": {"live": "15 sec", "desired": "30 sec"},
                    "POSTGRES_PWD": {"live": None, "desired": "${{ secrets.POSTGRES_PWD }}"},
                },
                "unchanged": [],
            },
        },
    }
    live_diff = {
        "account": {"name": "example"},
        "deployments": {
            "unchanged": [{
                "name": "UNCHANGED_DEPLOYMENT",
                "status": "ACTIVE",
                "runtimes": {"unchanged": [{
                    "name": "UNCHANGED_RUNTIME",
                    "status": "ACTIVE",
                    "nifi": {
                        "controller_services": {"created": [], "modified": [], "deleted": [], "unchanged": []},
                        "root_pg_controller_services": {"created": [], "modified": [], "deleted": [], "unchanged": []},
                        "parameter_providers": {"created": [], "modified": [], "deleted": [], "unchanged": []},
                        "flow_registries": {"created": [], "modified": [], "deleted": [], "unchanged": []},
                        "flows": {"created": [], "modified": [], "deleted": [], "unchanged": []},
                        "parameters": {},
                        "service_bindings": {"created": [], "modified": [], "deleted": [], "unchanged": [], "health": [], "blocked": []},
                        "root_parameter_context": {
                            "created": [],
                            "modified": [],
                            "deleted": [],
                            "unchanged": [{"name": "stable-driver.jar", "parameter": "Stable Driver"}],
                            "health": [],
                            "blocked": [],
                            "inheritance": {"missing": [], "unchanged": ["Provider POSTGRES"], "unmatched_pattern": False},
                            "parameters": {"changes": {}, "unchanged": ["Shared Query Timeout"]},
                        },
                    },
                }], "to_create": [], "to_modify": [], "to_delete": [], "url_managed": []},
            }],
            "to_create": [],
            "to_delete": [],
            "to_modify": [{
                "name": "MY_DEPLOYMENT",
                "dep_changes": {},
                "runtimes": {
                    "unchanged": [],
                    "to_create": [],
                    "to_delete": [],
                    "url_managed": [],
                    "to_modify": [{
                        "name": "MY_RUNTIME",
                        "live": {"status": "ACTIVE"},
                        "desired": {"name": "MY_RUNTIME", "root_parameter_context": {"assets": [{"name": "driver.jar", "parameter": "Database Driver"}] }},
                        "diff": {
                            "changed_fields": {},
                            "network_rule_changes": {"created": [], "modified": [], "deleted": []},
                            "connector_changes": {"created": [], "modified": [], "deleted": []},
                            "nifi": nifi_diff,
                        },
                    }],
                },
            }],
        },
    }

    translated = translate(live_diff)
    runtime_mod = translated["deployments"]["modified"][0]["runtime_changes"]["modified"][0]
    assert runtime_mod["root_parameter_context_changes"]["created"][0]["status"] == "missing_asset"
    assert runtime_mod["root_parameter_context_changes"]["inheritance"]["missing"] == ["Provider POSTGRES"]
    assert runtime_mod["root_parameter_context_changes"]["parameters"]["changes"]["POSTGRES_PWD"]["desired"] == "${{ secrets.POSTGRES_PWD }}"

    rendered = format_nifi_section(nifi_diff)
    assert any("Root Parameter Context" in line for line in rendered)
    assert any("Inherited contexts to add" in line for line in rendered)
    assert any("(secret)" in line for line in rendered)
    assert not any("${{ secrets.POSTGRES_PWD }}" in line for line in rendered)
    assert not any("https://" in line for line in rendered)
    assert any("Root parameter context asset health `bad-driver.jar` / `Bad Driver`" in line for line in rendered)
    assert any("literal \\| bad \\`value\\` &lt;tag&gt;" in line for line in rendered)

    plan = format_change_plan(live_diff)
    assert "1 root parameter-context asset(s)" in plan
    assert "1 root parameter(s)" in plan
    assert "1 inherited context(s)" in plan


def test_diff_environments_emits_root_parameter_context_changes(tmp_path):
    old_path = tmp_path / "old.yaml"
    new_path = tmp_path / "new.yaml"
    old_path.write_text(
        """
account:
  name: local
deployments:
  - name: DEPLOYMENT
    runtimes:
      - name: RUNTIME
        root_parameter_context:
          provided_parameter_contexts: ".*POSTGRES"
          parameters:
            Shared Query Timeout: 15 sec
          assets:
            - name: stable-driver.jar
              url: https://example.invalid/stable-driver.jar
              parameter: Stable Driver
""",
        encoding="utf-8",
    )
    new_path.write_text(
        """
account:
  name: local
deployments:
  - name: DEPLOYMENT
    runtimes:
      - name: RUNTIME
        root_parameter_context:
          provided_parameter_contexts: "Base|.*POSTGRES"
          parameters:
            Shared Query Timeout: 30 sec
          assets:
            - name: stable-driver.jar
              url: https://example.invalid/stable-driver.jar
              parameter: Stable Driver
            - name: new-driver.jar
              url: https://example.invalid/new-driver.jar
              parameter: New Driver
""",
        encoding="utf-8",
    )

    changes = diff_environments(str(old_path), str(new_path))
    runtime_mod = changes["deployments"]["modified"][0]["runtime_changes"]["modified"][0]

    assert "root_parameter_context" not in runtime_mod["changed_fields"]
    assert runtime_mod["root_parameter_context_changes"] == {
        "changed": True,
        "old": {
            "provided_parameter_contexts": ".*POSTGRES",
            "parameters": {"Shared Query Timeout": "15 sec"},
            "assets": [{"name": "stable-driver.jar", "url": "https://example.invalid/stable-driver.jar", "parameter": "Stable Driver"}],
        },
        "new": {
            "provided_parameter_contexts": "Base|.*POSTGRES",
            "parameters": {"Shared Query Timeout": "30 sec"},
            "assets": [
                {"name": "stable-driver.jar", "url": "https://example.invalid/stable-driver.jar", "parameter": "Stable Driver"},
                {"name": "new-driver.jar", "url": "https://example.invalid/new-driver.jar", "parameter": "New Driver"},
            ],
        },
    }


def test_diff_environments_emits_unchanged_root_parameter_context_as_dict(tmp_path):
    old_path = tmp_path / "old.yaml"
    new_path = tmp_path / "new.yaml"
    config = """
account:
  name: local
deployments:
  - name: DEPLOYMENT
    runtimes:
      - name: RUNTIME
        comment: old
        root_parameter_context:
          parameters:
            Shared Query Timeout: 15 sec
"""
    old_path.write_text(config, encoding="utf-8")
    new_path.write_text(config.replace("comment: old", "comment: new"), encoding="utf-8")

    changes = diff_environments(str(old_path), str(new_path))
    runtime_mod = changes["deployments"]["modified"][0]["runtime_changes"]["modified"][0]

    assert runtime_mod["root_parameter_context_changes"] == {
        "changed": False,
        "old": {"parameters": {"Shared Query Timeout": "15 sec"}},
        "new": {"parameters": {"Shared Query Timeout": "15 sec"}},
    }


def test_describe_live_state_passes_desired_root_parameter_context_in_all_paths(tmp_path, import_cd_module, monkeypatch):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
account:
  name: local
deployments:
  - name: LOCAL_DEPLOYMENT
    deployment_type: SNOWFLAKE
    runtimes:
      - name: LOCAL_NIFI
        database: ""
        schema: ""
        url: https://localhost:8443
        flows: []
        root_parameter_context:
          parameters:
            Url Driver: present
  - name: SOM_DEPLOYMENT
    deployment_type: SNOWFLAKE
    runtimes:
      - name: SOM_RUNTIME
        database: DB
        schema: SCHEMA
        flows: []
        root_parameter_context:
          assets:
            - name: som-driver.jar
              url: https://example.invalid/som-driver.jar
              parameter: Som Driver
""",
        encoding="utf-8",
    )
    describe_calls = []
    stubs = {
        "describe_nifi_state": types.SimpleNamespace(
            describe_nifi_state=lambda *args, **kwargs: describe_calls.append(kwargs.get("desired_root_parameter_context")) or {"flows": [], "flow_registries": [], "root_parameter_context": {"entries": []}}
        ),
        "manage_connectors": types.SimpleNamespace(get_connector_config=lambda *args, **kwargs: {}),
        "manage_flows": types.SimpleNamespace(configure_nifi=lambda *args, **kwargs: None),
    }
    module = import_cd_module("describe_live_state", stubs)
    module.load_config = lambda _path: {
        "account": {"name": "local"},
        "deployments": [
            {
                "name": "LOCAL_DEPLOYMENT",
                "deployment_type": "SNOWFLAKE",
                "runtimes": [{
                    "name": "LOCAL_NIFI",
                    "database": "",
                    "schema": "",
                    "url": "https://localhost:8443",
                    "flows": [],
                    "root_parameter_context": {"parameters": {"Url Driver": "present"}},
                }],
            },
            {
                "name": "SOM_DEPLOYMENT",
                "deployment_type": "SNOWFLAKE",
                "runtimes": [{
                    "name": "SOM_RUNTIME",
                    "database": "DB",
                    "schema": "SCHEMA",
                    "flows": [],
                    "root_parameter_context": {
                        "assets": [{"name": "som-driver.jar", "url": "https://example.invalid/som-driver.jar", "parameter": "Som Driver"}],
                    },
                }],
            },
        ],
    }
    module.list_deployments = lambda conn: [{"name": "SOM_DEPLOYMENT", "status": "ACTIVE", "type": "SNOWFLAKE"}]
    module.list_runtimes = lambda conn: [{"name": "SOM_RUNTIME", "deployment": "SOM_DEPLOYMENT", "database_name": "DB", "schema_name": "SCHEMA", "status": "ACTIVE"}]
    module.list_connectors = lambda conn: []
    module.describe_runtime = lambda *args, **kwargs: {"server_url": "https://runtime.example.invalid/nifi"}
    module.get_network_rules_for_runtime = lambda *args, **kwargs: []
    module._resolve_nifi_auth = lambda rt_cfg: None
    monkeypatch.setenv("NIFI_RUNTIME_PAT", "token")

    module.build_live_state(str(config_path), conn={"account_url": "x", "pat": "y", "user": "z", "role": "r"})

    assert describe_calls == [
        {"parameters": {"Url Driver": "present"}},
        {"assets": [{"name": "som-driver.jar", "url": "https://example.invalid/som-driver.jar", "parameter": "Som Driver"}]},
    ]