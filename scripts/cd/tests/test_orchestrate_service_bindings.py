#!/usr/bin/env python3
# Copyright 2026 Snowflake Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import types

import pytest


def _orchestrate_stubs(call_order, flow_pg_lookup=None):
    manage_deployment = types.SimpleNamespace(
        create_deployment=lambda *args, **kwargs: None,
        alter_deployment=lambda *args, **kwargs: None,
        delete_deployment=lambda *args, **kwargs: None,
        describe_deployment=lambda *args, **kwargs: None,
        snow_sql=lambda *args, **kwargs: [],
    )
    manage_eai = types.SimpleNamespace(
        create_runtime_eai=lambda *args, **kwargs: None,
        delete_runtime_eai=lambda *args, **kwargs: None,
        eai_name_for_runtime=lambda name: f"EAI_{name}",
        namespaced_nr_name=lambda runtime_name, rule_name: f"{runtime_name}_{rule_name}",
        create_network_rule=lambda *args, **kwargs: None,
        alter_network_rule=lambda *args, **kwargs: None,
        drop_network_rule=lambda *args, **kwargs: None,
        drop_eai=lambda *args, **kwargs: None,
    )
    manage_runtime = types.SimpleNamespace(
        create_runtime=lambda *args, **kwargs: None,
        alter_runtime=lambda *args, **kwargs: None,
        delete_runtime=lambda *args, **kwargs: None,
        describe_runtime=lambda *args, **kwargs: None,
        suspend_runtime=lambda *args, **kwargs: None,
        resume_runtime=lambda *args, **kwargs: None,
    )
    manage_parameters = types.SimpleNamespace(
        resolve_value=lambda value: value,
        reconcile_flow_parameters=lambda *args, **kwargs: call_order.append("parameters"),
        add_inherited_parameter_contexts=lambda *args, **kwargs: call_order.append("inherit"),
        apply_parameter_overrides=lambda *args, **kwargs: call_order.append("overrides"),
    )
    setup_registry_client = types.SimpleNamespace(
        setup=lambda *args, **kwargs: None,
        find_registry_client=lambda *args, **kwargs: None,
        delete_registry_client=lambda *args, **kwargs: None,
    )
    manage_flows = types.SimpleNamespace(
        reconcile_flows=lambda *args, **kwargs: call_order.append("reconcile_flows"),
        delete_flows=lambda *args, **kwargs: None,
        find_flow_pg_by_name=flow_pg_lookup or (lambda name: types.SimpleNamespace(id=f"pg-{name}", component=types.SimpleNamespace(name=name))),
        configure_nifi=lambda *args, **kwargs: None,
        start_flow=lambda pg_id, pg_name="": call_order.append(f"start:{pg_name}:{pg_id}"),
        stop_flow=lambda pg_id, pg_name="": call_order.append(f"stop:{pg_name}:{pg_id}"),
    )
    manage_assets = types.SimpleNamespace(
        reconcile_flow_assets=lambda *args, **kwargs: call_order.append("assets"),
    )
    manage_controller_services = types.SimpleNamespace(
        reconcile_controller_services=lambda *args, **kwargs: None,
        delete_controller_services=lambda *args, **kwargs: None,
        reconcile_root_pg_controller_services=lambda *args, **kwargs: None,
        delete_root_pg_controller_services=lambda *args, **kwargs: None,
    )
    manage_parameter_providers = types.SimpleNamespace(
        reconcile_parameter_providers=lambda *args, **kwargs: [],
        delete_parameter_providers=lambda *args, **kwargs: None,
        fetch_auto_provisioned_provider=lambda *args, **kwargs: [],
    )
    manage_connectors = types.SimpleNamespace(
        create_connector=lambda *args, **kwargs: None,
        connector_exists=lambda *args, **kwargs: False,
        describe_connector=lambda *args, **kwargs: None,
        apply_connector_config=lambda *args, **kwargs: None,
        get_connector_config=lambda *args, **kwargs: None,
        put_connector_config=lambda *args, **kwargs: None,
        upload_connector_asset=lambda *args, **kwargs: None,
        download_asset=lambda *args, **kwargs: None,
        start_connector=lambda *args, **kwargs: None,
        stop_connector=lambda *args, **kwargs: None,
        delete_connector=lambda *args, **kwargs: None,
        get_connector_config_uri=lambda *args, **kwargs: None,
        add_live_version=lambda *args, **kwargs: None,
        commit_connector=lambda *args, **kwargs: None,
        wait_for_connector=lambda *args, **kwargs: None,
    )
    return {
        "manage_deployment": manage_deployment,
        "manage_eai": manage_eai,
        "manage_runtime": manage_runtime,
        "manage_parameters": manage_parameters,
        "setup_registry_client": setup_registry_client,
        "manage_flows": manage_flows,
        "manage_assets": manage_assets,
        "manage_controller_services": manage_controller_services,
        "manage_parameter_providers": manage_parameter_providers,
        "manage_connectors": manage_connectors,
    }


def test_reconcile_flows_applies_bindings_after_assets_and_before_start(fake_nipyapi, import_cd_module):
    call_order = []
    stubs = _orchestrate_stubs(call_order)
    stubs["manage_service_bindings"] = types.SimpleNamespace(
        reconcile_service_bindings=lambda flow_spec, *args, **kwargs: call_order.append(f"bindings:{flow_spec['name']}"),
    )

    module = import_cd_module(
        "orchestrate",
        stubs,
    )

    module._reconcile_flows(
        {
            "flow_registries": [{"name": "nifihub"}],
            "root_pg_controller_services": [{"name": "SharedJsonReader"}],
            "flows": [{
                "name": "Example Flow",
                "registry": "nifihub",
                "bucket": "examples",
                "flow": "hello-world",
                "version": "latest",
                "start": True,
                "assets": [{"name": "driver.jar", "url": "https://example.invalid/driver.jar", "parameter": "Driver"}],
                "parameters": {"Run Schedule": "1 min"},
                "parameter_overrides": {"Env": "dev"},
                "service_bindings": [{"target": "Parse Records", "properties": {"Record Reader": "SharedJsonReader"}}],
            }],
        },
        runtime_url="https://example.invalid/nifi-api",
        provider_context_names=["shared"],
    )

    assert call_order == [
        "reconcile_flows",
        "assets",
        "parameters",
        "overrides",
        "bindings:Example Flow",
        "start:Example Flow:pg-Example Flow",
    ]


def test_reconcile_flows_rebinds_after_version_update_using_current_ids_before_start(fake_nipyapi, import_cd_module):
    call_order = []
    runtime_state = {
        "flow_pg_id": "pg-before",
        "target_processor_id": "proc-before",
        "root_service_id": "root-cs-before",
        "processor_properties": {},
    }
    discovery_calls = []
    update_calls = []

    def find_flow_pg_by_name(name):
        return types.SimpleNamespace(id=runtime_state["flow_pg_id"], component=types.SimpleNamespace(name=name))

    def current_processor():
        return types.SimpleNamespace(
            id=runtime_state["target_processor_id"],
            component=types.SimpleNamespace(
                id=runtime_state["target_processor_id"],
                name="Parse Records",
                parent_group_id=runtime_state["flow_pg_id"],
                type="org.apache.nifi.processors.standard.UpdateRecord",
                validation_status="VALID",
                validation_errors=[],
                config=types.SimpleNamespace(
                    properties=dict(runtime_state["processor_properties"]),
                    descriptors={"Record Reader": types.SimpleNamespace(
                        identifies_controller_service="org.apache.nifi.controller.ControllerService",
                        sensitive=False,
                        required=False,
                        dynamic=False,
                    )},
                ),
            ),
            revision=types.SimpleNamespace(version=0),
            status=types.SimpleNamespace(run_status="STOPPED"),
        )

    def current_root_service():
        return types.SimpleNamespace(
            id=runtime_state["root_service_id"],
            component=types.SimpleNamespace(
                id=runtime_state["root_service_id"],
                name="SharedJsonReader",
                parent_group_id="root",
                state="ENABLED",
                properties={},
                descriptors={},
            ),
        )

    stubs = _orchestrate_stubs(call_order, flow_pg_lookup=find_flow_pg_by_name)

    def reconcile_flows(group, *args, **kwargs):
        call_order.append("reconcile_flows")
        runtime_state["flow_pg_id"] = "pg-after"
        runtime_state["target_processor_id"] = "proc-after"
        runtime_state["root_service_id"] = "root-cs-after"

    stubs["manage_flows"].reconcile_flows = reconcile_flows
    manage_service_bindings = import_cd_module(
        "manage_service_bindings",
        {
            "manage_flows": types.SimpleNamespace(
                configure_nifi=lambda *args, **kwargs: None,
                list_process_groups=lambda parent_id=None: [find_flow_pg_by_name("Example Flow")],
                start_flow=lambda *args, **kwargs: None,
            ),
            "manage_controller_services": types.SimpleNamespace(
                list_root_pg_controller_services=lambda: [current_root_service()],
            ),
        },
    )
    stubs["manage_service_bindings"] = manage_service_bindings

    def get_processors(**kwargs):
        discovery_calls.append(kwargs["id"])
        call_order.append(f"discover:{kwargs['id']}")
        return types.SimpleNamespace(processors=[current_processor()])

    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_processors"] = get_processors
    fake_nipyapi.api_methods["FlowApi"]["get_controller_services_from_group"] = lambda **kwargs: types.SimpleNamespace(controller_services=[])
    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_process_group"] = lambda **kwargs: types.SimpleNamespace(
        id=kwargs["id"],
        component=types.SimpleNamespace(name="Example Flow", parent_group_id="root"),
        running_count=0,
        stopped_count=1,
    )
    fake_nipyapi.api_methods["ProcessorsApi"]["get_processor"] = lambda **kwargs: current_processor()
    def update_processor(**kwargs):
        properties = kwargs["body"].component.config.properties
        call_order.append(f"update:{properties['Record Reader']}")
        update_calls.append(properties)
        runtime_state["processor_properties"].update(properties)
        return current_processor()

    fake_nipyapi.api_methods["ProcessorsApi"]["update_processor"] = update_processor
    fake_nipyapi.canvas.schedule_process_group = lambda pg_id, running: None

    module = import_cd_module("orchestrate", stubs)

    runtime_spec = {
        "flow_registries": [{"name": "nifihub"}],
        "root_pg_controller_services": [{"name": "SharedJsonReader"}],
        "flows": [{
            "name": "Example Flow",
            "registry": "nifihub",
            "bucket": "examples",
            "flow": "hello-world",
            "version": "latest",
            "start": True,
            "service_bindings": [{"target": "Parse Records", "properties": {"Record Reader": "SharedJsonReader"}}],
        }],
    }

    module._reconcile_flows(
        runtime_spec,
        runtime_url="https://example.invalid/nifi-api",
        provider_context_names=[],
    )

    assert discovery_calls == ["pg-after"]
    assert update_calls == [{"Record Reader": "root-cs-after"}]
    assert call_order == [
        "reconcile_flows",
        "discover:pg-after",
        "update:root-cs-after",
        "start:Example Flow:pg-after",
    ]


def test_apply_runtime_modification_rejects_blocked_live_binding_state(fake_nipyapi, import_cd_module):
    stubs = _orchestrate_stubs([])
    stubs["manage_deployment"].deployment_exists = lambda *args, **kwargs: True
    stubs["manage_runtime"].runtime_exists = lambda *args, **kwargs: True
    stubs["manage_service_bindings"] = types.SimpleNamespace(reconcile_service_bindings=lambda *args, **kwargs: None)

    module = import_cd_module(
        "orchestrate",
        stubs,
    )

    mod = {
        "new": {"name": "MY_RUNTIME", "database": "OPENFLOW", "schema": "OPENFLOW", "url": "https://example.invalid"},
        "service_binding_changes": {
            "blocked": [{"flow": "Example Flow", "target": "Lookup", "messages": ["HTTP 403 Forbidden"]}],
        },
        "flow_changes": {},
        "parameter_provider_changes": {},
        "controller_service_changes": {},
        "root_pg_controller_service_changes": {},
        "flow_registry_changes": {},
        "connector_changes": {},
        "network_rule_changes": {"created": [], "modified": [], "deleted": []},
        "changed_fields": {},
    }

    with pytest.raises(RuntimeError, match="Service binding live-state read failed; refusing to apply runtime 'MY_RUNTIME': Example Flow / Lookup: HTTP 403 Forbidden"):
        module.apply_runtime_modification(mod, {"account": "example"})


class _LeakyDeleteError(Exception):
    def __str__(self):
        return "HTTP response headers: Set-Cookie: secret\nHTTP response body: sensitive"


def test_apply_deployment_modifications_sanitizes_delete_failures(fake_nipyapi, import_cd_module, capsys):
    stubs = _orchestrate_stubs([])
    stubs["manage_service_bindings"] = types.SimpleNamespace(reconcile_service_bindings=lambda *args, **kwargs: None)
    module = import_cd_module("orchestrate", stubs)
    module.runtime_exists = lambda *args, **kwargs: True
    module._delete_connectors = lambda *args, **kwargs: (_ for _ in ()).throw(_LeakyDeleteError())

    errors = []
    module.apply_deployment_modifications(
        [{
            "name": "DEPLOYMENT",
            "runtime_changes": {
                "created": [],
                "modified": [],
                "deleted": [{"name": "RUNTIME", "database": "DB", "schema": "SCHEMA", "url": "https://example.invalid", "flows": []}],
            },
        }],
        {"account": "example"},
        errors,
    )

    out = capsys.readouterr().out
    assert errors == ["Runtime RUNTIME delete failed: _LeakyDeleteError"]
    assert "Set-Cookie" not in out
    assert "sensitive" not in out
    assert "traceback" not in out.lower()


def test_apply_deployment_deletes_sanitizes_delete_failures_and_aggregates_errors(fake_nipyapi, import_cd_module, capsys):
    stubs = _orchestrate_stubs([])
    stubs["manage_service_bindings"] = types.SimpleNamespace(reconcile_service_bindings=lambda *args, **kwargs: None)
    module = import_cd_module("orchestrate", stubs)

    def failing_delete_flows(flows, rt, runtime_url):
        raise _LeakyDeleteError()

    module._delete_flows = failing_delete_flows
    module._delete_connectors = lambda *args, **kwargs: None
    module.delete_runtime = lambda *args, **kwargs: None
    module.delete_runtime_eai = lambda *args, **kwargs: None

    errors = []
    module.apply_deployment_deletes(
        [{
            "name": "DEPLOYMENT",
            "runtimes_to_delete": [
                {"name": "RUNTIME_ONE", "database": "DB", "schema": "SCHEMA", "url": "https://example.invalid", "flows": [{"name": "Flow One"}]},
                {"name": "RUNTIME_TWO", "database": "DB", "schema": "SCHEMA", "url": "https://example.invalid", "flows": [{"name": "Flow Two"}]},
            ],
        }],
        {"account": "example"},
        errors,
    )

    out = capsys.readouterr().out
    assert errors == [
        "Runtime RUNTIME_ONE delete failed: _LeakyDeleteError",
        "Runtime RUNTIME_TWO delete failed: _LeakyDeleteError",
    ]
    assert "Set-Cookie" not in out
    assert "sensitive" not in out
    assert "traceback" not in out.lower()