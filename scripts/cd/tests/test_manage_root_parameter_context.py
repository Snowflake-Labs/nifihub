#!/usr/bin/env python3
# Copyright 2026 Snowflake Inc.
# SPDX-License-Identifier: Apache-2.0

import types
from collections import defaultdict

import pytest


def _parameter(name, value=None, sensitive=False, refs=None, description=None):
    return types.SimpleNamespace(
        name=name,
        value=value,
        sensitive=sensitive,
        description=description,
        referenced_assets=list(refs or []),
    )


def _parameter_entity(parameter):
    return types.SimpleNamespace(parameter=parameter)


def _asset_ref(asset_id, name):
    return types.SimpleNamespace(id=asset_id, name=name)


def _asset(asset_id, name, missing_content=False):
    return types.SimpleNamespace(id=asset_id, name=name, missing_content=missing_content)


def _asset_entity(asset):
    return types.SimpleNamespace(asset=asset)


def _parameter_context(context_id, name, parameters=None, inherited=None, revision_version=0):
    return types.SimpleNamespace(
        id=context_id,
        component=types.SimpleNamespace(
            id=context_id,
            name=name,
            parameters=[_parameter_entity(parameter) for parameter in (parameters or [])],
            inherited_parameter_contexts=list(inherited or []),
        ),
        revision=types.SimpleNamespace(version=revision_version),
    )


def _parameter_context_ref(context_id, name):
    return types.SimpleNamespace(id=context_id, component=types.SimpleNamespace(id=context_id, name=name))


def _process_group(group_id, name="root", context_ref=None, revision_version=0):
    return types.SimpleNamespace(
        id=group_id,
        component=types.SimpleNamespace(id=group_id, name=name, parameter_context=context_ref),
        revision=types.SimpleNamespace(version=revision_version),
    )


class _RevisionConflict(Exception):
    status = 409
    reason = "Conflict"
    body = "Revision version conflict"


class _Harness:
    def __init__(self):
        self.root_pg = _process_group("root")
        self.process_groups = {"root": self.root_pg}
        self.parameter_contexts = {}
        self.assets_by_context = defaultdict(dict)
        self.created_parameter_contexts = []
        self.update_process_group_calls = []
        self.create_asset_calls = []
        self.submit_bodies = []
        self.deleted_update_requests = []
        self.update_sequences = []
        self.get_update_calls = []
        self.attach_conflicts_remaining = 0
        self.next_asset_id = 1
        self.events = []

    def install(self, fake_nipyapi):
        fake_nipyapi.api_methods["FlowApi"]["get_parameter_contexts"] = self.get_parameter_contexts
        fake_nipyapi.api_methods["ProcessGroupsApi"]["get_process_group"] = self.get_process_group
        fake_nipyapi.api_methods["ProcessGroupsApi"]["update_process_group"] = self.update_process_group
        fake_nipyapi.api_methods["ParameterContextsApi"]["get_parameter_context"] = self.get_parameter_context
        fake_nipyapi.api_methods["ParameterContextsApi"]["create_parameter_context"] = self.create_parameter_context
        fake_nipyapi.api_methods["ParameterContextsApi"]["submit_parameter_context_update"] = self.submit_parameter_context_update
        fake_nipyapi.api_methods["ParameterContextsApi"]["get_parameter_context_update"] = self.get_parameter_context_update
        fake_nipyapi.api_methods["ParameterContextsApi"]["delete_update_request"] = self.delete_update_request
        fake_nipyapi.api_methods["ParameterContextsApi"]["get_assets1"] = self.get_assets1
        fake_nipyapi.api_methods["ParameterContextsApi"]["create_asset1"] = self.create_asset1

    def get_parameter_contexts(self, **_kwargs):
        return types.SimpleNamespace(parameter_contexts=list(self.parameter_contexts.values()))

    def get_process_group(self, **kwargs):
        return self.process_groups[kwargs["id"]]

    def update_process_group(self, **kwargs):
        self.events.append("attach-context")
        self.update_process_group_calls.append(kwargs)
        if self.attach_conflicts_remaining > 0:
            self.attach_conflicts_remaining -= 1
            raise _RevisionConflict()
        current = self.process_groups[kwargs["id"]]
        ref = kwargs["body"].component.parameter_context
        updated = _process_group(
            kwargs["id"],
            name=current.component.name,
            context_ref=_parameter_context_ref(ref.id, ref.component.name),
            revision_version=current.revision.version + 1,
        )
        self.process_groups[kwargs["id"]] = updated
        self.root_pg = updated
        return updated

    def get_parameter_context(self, **kwargs):
        return self.parameter_contexts[kwargs["id"]]

    def create_parameter_context(self, **kwargs):
        body = kwargs["body"]
        context_id = f"pc-{len(self.parameter_contexts) + 1}"
        context = _parameter_context(context_id, body.component.name, revision_version=0)
        self.parameter_contexts[context_id] = context
        self.created_parameter_contexts.append(body.component.name)
        self.events.append(f"create-context:{body.component.name}")
        return context

    def submit_parameter_context_update(self, **kwargs):
        context_id = kwargs["context_id"]
        body = kwargs["body"]
        request_id = f"req-{len(self.submit_bodies) + 1}"
        self.events.append(f"submit:{request_id}")
        self.submit_bodies.append((context_id, body))
        sequence = self.update_sequences.pop(0) if self.update_sequences else [types.SimpleNamespace(complete=True, failure_reason=None)]
        last = sequence[-1]
        if last.complete and not last.failure_reason:
            self._apply_parameter_context_update(context_id, body)
        self.get_update_calls.append([])
        self.get_update_calls[-1].append(request_id)
        self._current_sequences = getattr(self, "_current_sequences", {})
        self._current_sequences[request_id] = list(sequence)
        return types.SimpleNamespace(request=types.SimpleNamespace(request_id=request_id))

    def get_parameter_context_update(self, **kwargs):
        request_id = kwargs["request_id"]
        self.get_update_calls[-1].append(request_id)
        sequence = self._current_sequences[request_id]
        if len(sequence) > 1:
            status = sequence.pop(0)
        else:
            status = sequence[0]
        return types.SimpleNamespace(request=status)

    def delete_update_request(self, **kwargs):
        self.deleted_update_requests.append((kwargs["context_id"], kwargs["request_id"]))

    def get_assets1(self, **kwargs):
        return types.SimpleNamespace(
            assets=[_asset_entity(asset) for asset in self.assets_by_context[kwargs["context_id"]].values()]
        )

    def create_asset1(self, **kwargs):
        asset = _asset(f"asset-{self.next_asset_id}", kwargs["filename"], missing_content=False)
        self.next_asset_id += 1
        self.assets_by_context[kwargs["context_id"]][asset.name] = asset
        self.create_asset_calls.append(kwargs)
        self.events.append(f"create-asset:{kwargs['filename']}")
        return types.SimpleNamespace(asset=asset)

    def _apply_parameter_context_update(self, context_id, body):
        current = self.parameter_contexts[context_id]
        parameters_by_name = {entity.parameter.name: self._clone_parameter(entity.parameter) for entity in current.component.parameters}
        parameter_order = [entity.parameter.name for entity in current.component.parameters]
        for entity in body.component.parameters:
            parameter = entity.parameter
            name = parameter.name
            if self._is_delete_parameter_dto(parameter):
                parameters_by_name.pop(name, None)
                parameter_order = [existing_name for existing_name in parameter_order if existing_name != name]
                continue
            merged = parameters_by_name.get(name, _parameter(name))
            if hasattr(parameter, "sensitive"):
                merged.sensitive = getattr(parameter, "sensitive", False)
            if hasattr(parameter, "description"):
                merged.description = getattr(parameter, "description", None)
            if hasattr(parameter, "referenced_assets"):
                merged.referenced_assets = [
                    _asset_ref(ref.id, ref.name)
                    for ref in (getattr(parameter, "referenced_assets", None) or [])
                ]
            if getattr(parameter, "value_removed", False):
                merged.value = None
            elif hasattr(parameter, "value"):
                merged.value = getattr(parameter, "value", None)
            parameters_by_name[name] = merged
            if name not in parameter_order:
                parameter_order.append(name)
        updated = _parameter_context(
            context_id,
            body.component.name,
            parameters=[parameters_by_name[name] for name in parameter_order if name in parameters_by_name],
            inherited=list(body.component.inherited_parameter_contexts or current.component.inherited_parameter_contexts or []),
            revision_version=current.revision.version + 1,
        )
        self.parameter_contexts[context_id] = updated

    @staticmethod
    def _clone_parameter(parameter):
        return _parameter(
            parameter.name,
            value=getattr(parameter, "value", None),
            sensitive=getattr(parameter, "sensitive", False),
            refs=[_asset_ref(ref.id, ref.name) for ref in (getattr(parameter, "referenced_assets", None) or [])],
            description=getattr(parameter, "description", None),
        )

    @staticmethod
    def _is_delete_parameter_dto(parameter):
        return (
            hasattr(parameter, "name")
            and hasattr(parameter, "value")
            and getattr(parameter, "value", None) is None
            and not getattr(parameter, "value_removed", False)
            and not hasattr(parameter, "sensitive")
            and not hasattr(parameter, "description")
            and not hasattr(parameter, "referenced_assets")
        )


def _load_assets_module(import_cd_module):
    return import_cd_module(
        "manage_assets",
        {"manage_flows": types.SimpleNamespace(configure_nifi=lambda *args, **kwargs: None)},
    )


def _load_root_module(import_cd_module):
    assets_module = _load_assets_module(import_cd_module)
    parameters_module = import_cd_module(
        "manage_parameters",
        {"manage_flows": types.SimpleNamespace(configure_nifi=lambda *args, **kwargs: None)},
    )
    root_module = import_cd_module(
        "manage_root_parameter_context",
        {
            "manage_flows": types.SimpleNamespace(configure_nifi=lambda *args, **kwargs: None),
            "manage_assets": assets_module,
            "manage_parameters": parameters_module,
        },
    )
    return root_module, assets_module


def _asset_spec(name="driver.jar", url="https://example.invalid/driver.jar", parameter="Database Driver"):
    return {"name": name, "url": url, "parameter": parameter}


def test_reconcile_root_parameter_context_creates_context_parameter_and_binding_preserving_state(fake_nipyapi, import_cd_module, monkeypatch):
    harness = _Harness()
    harness.update_sequences = [
        [types.SimpleNamespace(complete=True, failure_reason=None)],
        [types.SimpleNamespace(complete=True, failure_reason=None)],
    ]
    harness.install(fake_nipyapi)
    module, assets_module = _load_root_module(import_cd_module)
    monkeypatch.setattr(assets_module, "_download_file", lambda url: b"driver-bytes")

    module.reconcile_root_parameter_context(
        {"assets": [_asset_spec()]},
        runtime_url="https://example.invalid/nifi-api",
        nifi_pat="token",
    )

    assert harness.created_parameter_contexts == ["Root Parameter Context"]
    context_id = harness.root_pg.component.parameter_context.id
    context = harness.parameter_contexts[context_id]
    direct_parameters = {entity.parameter.name: entity.parameter for entity in context.component.parameters}
    assert direct_parameters["Database Driver"].sensitive is False
    assert direct_parameters["Database Driver"].value is None
    assert [(ref.id, ref.name) for ref in direct_parameters["Database Driver"].referenced_assets] == [("asset-1", "driver.jar")]
    assert context.component.inherited_parameter_contexts == []
    assert len(harness.submit_bodies) == 2
    assert harness.create_asset_calls[0]["filename"] == "driver.jar"
    assert harness.deleted_update_requests == [(context_id, "req-1"), (context_id, "req-2")]


def test_reconcile_root_parameter_context_uses_existing_direct_context_and_skips_healthy_bound_asset(fake_nipyapi, import_cd_module, monkeypatch):
    harness = _Harness()
    context = _parameter_context(
        "pc-1",
        "Already Attached",
        parameters=[_parameter("Database Driver", refs=[_asset_ref("asset-1", "driver.jar")])],
    )
    harness.parameter_contexts[context.id] = context
    harness.root_pg = _process_group("root", context_ref=_parameter_context_ref(context.id, context.component.name))
    harness.process_groups["root"] = harness.root_pg
    harness.assets_by_context[context.id]["driver.jar"] = _asset("asset-1", "driver.jar", missing_content=False)
    harness.install(fake_nipyapi)
    module, assets_module = _load_root_module(import_cd_module)
    monkeypatch.setattr(assets_module, "_download_file", lambda _url: pytest.fail("download should not run"))

    module.reconcile_root_parameter_context(
        {"assets": [_asset_spec()]},
        runtime_url="https://example.invalid/nifi-api",
        nifi_pat="token",
    )

    assert harness.created_parameter_contexts == []
    assert harness.update_process_group_calls == []
    assert harness.create_asset_calls == []
    assert harness.submit_bodies == []


def test_reconcile_root_parameter_context_reuses_exact_name_context_and_retries_one_revision_conflict(fake_nipyapi, import_cd_module, monkeypatch):
    harness = _Harness()
    context = _parameter_context("pc-1", "Root Parameter Context", parameters=[_parameter("Database Driver", refs=[])])
    harness.parameter_contexts[context.id] = context
    harness.assets_by_context[context.id]["driver.jar"] = _asset("asset-1", "driver.jar", missing_content=False)
    harness.attach_conflicts_remaining = 1
    harness.update_sequences = [[types.SimpleNamespace(complete=True, failure_reason=None)]]
    harness.install(fake_nipyapi)
    module, assets_module = _load_root_module(import_cd_module)
    monkeypatch.setattr(assets_module, "_download_file", lambda _url: pytest.fail("download should not run"))

    module.reconcile_root_parameter_context(
        {"assets": [_asset_spec()]},
        runtime_url="https://example.invalid/nifi-api",
        nifi_pat="token",
    )

    assert harness.created_parameter_contexts == []
    assert len(harness.update_process_group_calls) == 2
    parameter = {entity.parameter.name: entity.parameter for entity in harness.parameter_contexts["pc-1"].component.parameters}["Database Driver"]
    assert [(ref.id, ref.name) for ref in parameter.referenced_assets] == [("asset-1", "driver.jar")]


@pytest.mark.parametrize(
    ("parameter", "pattern"),
    [
        (_parameter("Database Driver", sensitive=True), "is sensitive"),
        (_parameter("Database Driver", value="/tmp/driver.jar"), "has a literal value"),
        (_parameter("Database Driver", refs=[_asset_ref("a1", "x"), _asset_ref("a2", "y")]), "multiple assets"),
    ],
)
def test_reconcile_root_parameter_context_rejects_incompatible_asset_parameter_before_download(fake_nipyapi, import_cd_module, monkeypatch, parameter, pattern):
    harness = _Harness()
    context = _parameter_context("pc-1", "Attached", parameters=[parameter])
    harness.parameter_contexts[context.id] = context
    harness.root_pg = _process_group("root", context_ref=_parameter_context_ref(context.id, context.component.name))
    harness.process_groups["root"] = harness.root_pg
    harness.install(fake_nipyapi)
    module, assets_module = _load_root_module(import_cd_module)
    monkeypatch.setattr(assets_module, "_download_file", lambda _url: pytest.fail("download should not run"))

    with pytest.raises(RuntimeError, match=pattern):
        module.reconcile_root_parameter_context(
            {"assets": [_asset_spec()]},
            runtime_url="https://example.invalid/nifi-api",
            nifi_pat="token",
        )

    assert harness.create_asset_calls == []
    assert harness.submit_bodies == []


@pytest.mark.parametrize(
    "spec, pattern",
    [
        ({"assets": [_asset_spec("driver.jar", "https://example.invalid/a", "P1"), _asset_spec("driver.jar", "https://example.invalid/b", "P2")]}, "Duplicate asset names"),
        ({"assets": [_asset_spec("driver-a.jar", "https://example.invalid/a", "P1"), _asset_spec("driver-b.jar", "https://example.invalid/b", "P1")]}, "Multiple assets declared for the same parameter"),
        ({"parameters": {"P1": "value"}, "assets": [_asset_spec("driver.jar", "https://example.invalid/a", "P1")]}, "cannot also be asset targets"),
    ],
)
def test_reconcile_root_parameter_context_rejects_duplicate_or_overlapping_declarations(fake_nipyapi, import_cd_module, spec, pattern):
    harness = _Harness()
    harness.install(fake_nipyapi)
    module, _assets_module = _load_root_module(import_cd_module)

    with pytest.raises(RuntimeError, match=pattern):
        module.reconcile_root_parameter_context(
            spec,
            runtime_url="https://example.invalid/nifi-api",
            nifi_pat="token",
        )

    assert harness.created_parameter_contexts == []
    assert harness.create_asset_calls == []


def test_reconcile_root_parameter_context_reuploads_missing_content_with_legacy_asset_methods(fake_nipyapi, import_cd_module, monkeypatch):
    harness = _Harness()
    context = _parameter_context("pc-1", "Attached", parameters=[_parameter("Database Driver")])
    harness.parameter_contexts[context.id] = context
    harness.root_pg = _process_group("root", context_ref=_parameter_context_ref(context.id, context.component.name))
    harness.process_groups["root"] = harness.root_pg
    harness.assets_by_context[context.id]["driver.jar"] = _asset("stale-asset", "driver.jar", missing_content=True)
    harness.update_sequences = [[types.SimpleNamespace(complete=True, failure_reason=None)]]
    harness.install(fake_nipyapi)
    fake_nipyapi.api_methods["ParameterContextsApi"].pop("get_assets1", None)
    fake_nipyapi.api_methods["ParameterContextsApi"]["get_assets"] = harness.get_assets1
    fake_nipyapi.api_methods["ParameterContextsApi"].pop("create_asset1", None)
    fake_nipyapi.api_methods["ParameterContextsApi"]["create_asset"] = harness.create_asset1
    module, assets_module = _load_root_module(import_cd_module)
    monkeypatch.setattr(assets_module, "_download_file", lambda _url: b"replacement")

    module.reconcile_root_parameter_context(
        {"assets": [_asset_spec()]},
        runtime_url="https://example.invalid/nifi-api",
        nifi_pat="token",
    )

    parameter = {entity.parameter.name: entity.parameter for entity in harness.parameter_contexts[context.id].component.parameters}["Database Driver"]
    assert harness.create_asset_calls[0]["body"] == b"replacement"
    assert [(ref.id, ref.name) for ref in parameter.referenced_assets] == [("asset-1", "driver.jar")]


def test_reconcile_root_parameter_context_timeout_cleans_update_request(fake_nipyapi, import_cd_module, monkeypatch):
    harness = _Harness()
    context = _parameter_context("pc-1", "Attached", parameters=[_parameter("Database Driver")])
    harness.parameter_contexts[context.id] = context
    harness.root_pg = _process_group("root", context_ref=_parameter_context_ref(context.id, context.component.name))
    harness.process_groups["root"] = harness.root_pg
    harness.assets_by_context[context.id]["driver.jar"] = _asset("asset-1", "driver.jar", missing_content=False)
    harness.update_sequences = [[types.SimpleNamespace(complete=False, failure_reason=None)]]
    harness.install(fake_nipyapi)
    module, assets_module = _load_root_module(import_cd_module)
    monkeypatch.setattr(assets_module.time, "sleep", lambda _seconds: None)
    now = iter([0, 61])
    monkeypatch.setattr(assets_module.time, "time", lambda: next(now))

    with pytest.raises(RuntimeError, match="timed out"):
        module.reconcile_root_parameter_context(
            {"assets": [_asset_spec()]},
            runtime_url="https://example.invalid/nifi-api",
            nifi_pat="token",
        )

    assert harness.deleted_update_requests == [(context.id, "req-1")]


def test_reconcile_root_parameter_context_failure_cleans_update_request(fake_nipyapi, import_cd_module):
    harness = _Harness()
    context = _parameter_context("pc-1", "Attached", parameters=[_parameter("Database Driver")])
    harness.parameter_contexts[context.id] = context
    harness.root_pg = _process_group("root", context_ref=_parameter_context_ref(context.id, context.component.name))
    harness.process_groups["root"] = harness.root_pg
    harness.assets_by_context[context.id]["driver.jar"] = _asset("asset-1", "driver.jar", missing_content=False)
    harness.update_sequences = [[types.SimpleNamespace(complete=True, failure_reason="boom")]]
    harness.install(fake_nipyapi)
    module, _assets_module = _load_root_module(import_cd_module)

    with pytest.raises(RuntimeError, match="boom"):
        module.reconcile_root_parameter_context(
            {"assets": [_asset_spec()]},
            runtime_url="https://example.invalid/nifi-api",
            nifi_pat="token",
        )

    assert harness.deleted_update_requests == [(context.id, "req-1")]


def test_reconcile_root_parameter_context_adds_inheritance_additively_and_preserves_existing(fake_nipyapi, import_cd_module):
    harness = _Harness()
    base = _parameter_context_ref("pc-base", "Base")
    inherited_existing = _parameter_context("pc-base", "Base")
    inherited_new = _parameter_context("pc-provider", "Provider POSTGRES")
    root_context = _parameter_context("pc-root", "Root Parameter Context", inherited=[base])
    harness.parameter_contexts[inherited_existing.id] = inherited_existing
    harness.parameter_contexts[inherited_new.id] = inherited_new
    harness.parameter_contexts[root_context.id] = root_context
    harness.root_pg = _process_group("root", context_ref=_parameter_context_ref(root_context.id, root_context.component.name))
    harness.process_groups["root"] = harness.root_pg
    harness.install(fake_nipyapi)
    module, _assets_module = _load_root_module(import_cd_module)

    module.reconcile_root_parameter_context(
        {"provided_parameter_contexts": ".*POSTGRES|Base"},
        runtime_url="https://example.invalid/nifi-api",
        nifi_pat="token",
        provider_context_names=["Base", "Provider POSTGRES"],
    )

    inherited = harness.parameter_contexts[root_context.id].component.inherited_parameter_contexts
    assert [(ctx.id, ctx.component.name) for ctx in inherited] == [
        ("pc-base", "Base"),
        ("pc-provider", "Provider POSTGRES"),
    ]
    assert len(harness.submit_bodies) == 1


def test_reconcile_root_parameter_context_logs_no_inheritance_match_and_continues(fake_nipyapi, import_cd_module, capsys):
    harness = _Harness()
    root_context = _parameter_context("pc-root", "Root Parameter Context")
    harness.parameter_contexts[root_context.id] = root_context
    harness.root_pg = _process_group("root", context_ref=_parameter_context_ref(root_context.id, root_context.component.name))
    harness.process_groups["root"] = harness.root_pg
    harness.install(fake_nipyapi)
    module, _assets_module = _load_root_module(import_cd_module)

    module.reconcile_root_parameter_context(
        {"provided_parameter_contexts": "NO_MATCH.*"},
        runtime_url="https://example.invalid/nifi-api",
        nifi_pat="token",
        provider_context_names=["Provider POSTGRES"],
    )

    assert harness.submit_bodies == []
    assert "No provider contexts matched pattern 'NO_MATCH.*'" in capsys.readouterr().out


def test_reconcile_root_parameter_context_creates_direct_parameter(fake_nipyapi, import_cd_module):
    harness = _Harness()
    root_context = _parameter_context("pc-root", "Root Parameter Context")
    harness.parameter_contexts[root_context.id] = root_context
    harness.root_pg = _process_group("root", context_ref=_parameter_context_ref(root_context.id, root_context.component.name))
    harness.process_groups["root"] = harness.root_pg
    harness.install(fake_nipyapi)
    module, _assets_module = _load_root_module(import_cd_module)

    module.reconcile_root_parameter_context(
        {"parameters": {"Shared Query Timeout": "30 sec"}},
        runtime_url="https://example.invalid/nifi-api",
        nifi_pat="token",
    )

    parameter = {entity.parameter.name: entity.parameter for entity in harness.parameter_contexts[root_context.id].component.parameters}["Shared Query Timeout"]
    assert parameter.value == "30 sec"
    assert len(harness.submit_bodies) == 1


def test_reconcile_root_parameter_context_updates_and_clears_direct_parameters_in_single_payload(fake_nipyapi, import_cd_module):
    harness = _Harness()
    root_context = _parameter_context(
        "pc-root",
        "Root Parameter Context",
        parameters=[
            _parameter("Keep", value="same", description="kept"),
            _parameter("Change", value="old"),
            _parameter("Clear", value="old-clear"),
        ],
    )
    harness.parameter_contexts[root_context.id] = root_context
    harness.root_pg = _process_group("root", context_ref=_parameter_context_ref(root_context.id, root_context.component.name))
    harness.process_groups["root"] = harness.root_pg
    harness.install(fake_nipyapi)
    module, _assets_module = _load_root_module(import_cd_module)

    module.reconcile_root_parameter_context(
        {"parameters": {"Keep": "same", "Change": "new", "Clear": None, "New": "created"}},
        runtime_url="https://example.invalid/nifi-api",
        nifi_pat="token",
    )

    assert len(harness.submit_bodies) == 1
    payload = harness.submit_bodies[0][1]
    payload_parameters = {entity.parameter.name: entity.parameter for entity in payload.component.parameters}
    assert sorted(payload_parameters) == ["Change", "Clear", "New"]
    assert payload_parameters["Change"].value == "new"
    assert payload_parameters["Clear"].value is None
    assert payload_parameters["Clear"].value_removed is True
    final_parameters = {entity.parameter.name: entity.parameter for entity in harness.parameter_contexts[root_context.id].component.parameters}
    assert final_parameters["Keep"].value == "same"
    assert final_parameters["Keep"].description == "kept"
    assert final_parameters["Change"].value == "new"
    assert final_parameters["Clear"].value is None
    assert final_parameters["New"].value == "created"


def test_reconcile_root_parameter_context_skips_idempotent_parameters(fake_nipyapi, import_cd_module):
    harness = _Harness()
    root_context = _parameter_context("pc-root", "Root Parameter Context", parameters=[_parameter("Shared Query Timeout", value="30 sec")])
    harness.parameter_contexts[root_context.id] = root_context
    harness.root_pg = _process_group("root", context_ref=_parameter_context_ref(root_context.id, root_context.component.name))
    harness.process_groups["root"] = harness.root_pg
    harness.install(fake_nipyapi)
    module, _assets_module = _load_root_module(import_cd_module)

    module.reconcile_root_parameter_context(
        {"parameters": {"Shared Query Timeout": "30 sec"}},
        runtime_url="https://example.invalid/nifi-api",
        nifi_pat="token",
    )

    assert harness.submit_bodies == []


def test_reconcile_root_parameter_context_resolves_github_vars(fake_nipyapi, import_cd_module, monkeypatch):
    harness = _Harness()
    root_context = _parameter_context("pc-root", "Root Parameter Context")
    harness.parameter_contexts[root_context.id] = root_context
    harness.root_pg = _process_group("root", context_ref=_parameter_context_ref(root_context.id, root_context.component.name))
    harness.process_groups["root"] = harness.root_pg
    harness.install(fake_nipyapi)
    module, _assets_module = _load_root_module(import_cd_module)
    monkeypatch.setenv("GH_VARS_JSON", '{"QUERY_TIMEOUT":"45 sec"}')

    module.reconcile_root_parameter_context(
        {"parameters": {"Shared Query Timeout": "${{ vars.QUERY_TIMEOUT }}"}},
        runtime_url="https://example.invalid/nifi-api",
        nifi_pat="token",
    )

    parameter = {entity.parameter.name: entity.parameter for entity in harness.parameter_contexts[root_context.id].component.parameters}["Shared Query Timeout"]
    assert parameter.value == "45 sec"


def test_reconcile_root_parameter_context_rejects_github_secret_refs_in_direct_parameters(fake_nipyapi, import_cd_module):
    harness = _Harness()
    root_context = _parameter_context("pc-root", "Root Parameter Context")
    harness.parameter_contexts[root_context.id] = root_context
    harness.root_pg = _process_group("root", context_ref=_parameter_context_ref(root_context.id, root_context.component.name))
    harness.process_groups["root"] = harness.root_pg
    harness.install(fake_nipyapi)
    module, _assets_module = _load_root_module(import_cd_module)

    with pytest.raises(
        RuntimeError,
        match="root_parameter_context.parameters are stored as non-sensitive NiFi parameters; use a provided parameter context for secrets",
    ):
        module.reconcile_root_parameter_context(
            {"parameters": {"Shared Query Timeout": "${{ secrets.QUERY_TIMEOUT }}"}},
            runtime_url="https://example.invalid/nifi-api",
            nifi_pat="token",
        )

    assert harness.submit_bodies == []


@pytest.mark.parametrize(
    ("parameter", "pattern"),
    [
        (_parameter("Shared Query Timeout", value="30 sec", sensitive=True), "is sensitive and cannot be managed directly"),
        (_parameter("Shared Query Timeout", value=None, refs=[_asset_ref("asset-1", "driver.jar")]), "references assets and cannot be managed directly"),
    ],
)
def test_reconcile_root_parameter_context_rejects_sensitive_or_asset_bound_parameter_before_write(fake_nipyapi, import_cd_module, parameter, pattern):
    harness = _Harness()
    root_context = _parameter_context("pc-root", "Root Parameter Context", parameters=[parameter])
    harness.parameter_contexts[root_context.id] = root_context
    harness.root_pg = _process_group("root", context_ref=_parameter_context_ref(root_context.id, root_context.component.name))
    harness.process_groups["root"] = harness.root_pg
    harness.install(fake_nipyapi)
    module, _assets_module = _load_root_module(import_cd_module)

    with pytest.raises(RuntimeError, match=pattern):
        module.reconcile_root_parameter_context(
            {"parameters": {"Shared Query Timeout": "45 sec"}},
            runtime_url="https://example.invalid/nifi-api",
            nifi_pat="token",
        )

    assert harness.submit_bodies == []


def test_reconcile_root_parameter_context_orders_inherit_then_parameters_then_assets(fake_nipyapi, import_cd_module, monkeypatch):
    harness = _Harness()
    provider = _parameter_context("pc-provider", "Provider POSTGRES")
    root_context = _parameter_context(
        "pc-root",
        "Root Parameter Context",
        parameters=[
            _parameter("Shared Query Timeout", value="15 sec"),
            _parameter("Database Driver"),
        ],
    )
    harness.parameter_contexts[provider.id] = provider
    harness.parameter_contexts[root_context.id] = root_context
    harness.root_pg = _process_group("root", context_ref=_parameter_context_ref(root_context.id, root_context.component.name))
    harness.process_groups["root"] = harness.root_pg
    harness.install(fake_nipyapi)
    module, assets_module = _load_root_module(import_cd_module)
    monkeypatch.setattr(assets_module, "_download_file", lambda _url: b"driver-bytes")

    module.reconcile_root_parameter_context(
        {
            "provided_parameter_contexts": ".*POSTGRES",
            "parameters": {"Shared Query Timeout": "30 sec"},
            "assets": [_asset_spec()],
        },
        runtime_url="https://example.invalid/nifi-api",
        nifi_pat="token",
        provider_context_names=["Provider POSTGRES"],
    )

    assert [event for event in harness.events if event.startswith("submit:") or event.startswith("create-asset:")] == [
        "submit:req-1",
        "submit:req-2",
        "create-asset:driver.jar",
        "submit:req-3",
    ]
    first_body = harness.submit_bodies[0][1]
    second_body = harness.submit_bodies[1][1]
    third_body = harness.submit_bodies[2][1]
    assert [ctx.component.name for ctx in first_body.component.inherited_parameter_contexts] == ["Provider POSTGRES"]
    second_parameters = {entity.parameter.name: entity.parameter for entity in second_body.component.parameters}
    assert second_parameters["Shared Query Timeout"].value == "30 sec"
    third_parameters = {entity.parameter.name: entity.parameter for entity in third_body.component.parameters}
    assert [(ref.id, ref.name) for ref in third_parameters["Database Driver"].referenced_assets] == [("asset-1", "driver.jar")]


@pytest.mark.parametrize(
    ("spec", "provider_context_names", "assets", "expected_payload_names"),
    [
        ({"parameters": {"Shared Query Timeout": "30 sec"}}, None, None, ["Shared Query Timeout"]),
        ({"provided_parameter_contexts": ".*POSTGRES"}, ["Provider POSTGRES"], None, []),
        ({"assets": [_asset_spec()]}, None, {"driver.jar": _asset("asset-1", "driver.jar", missing_content=False)}, ["Database Driver"]),
    ],
)
def test_root_parameter_context_updates_do_not_resend_masked_sensitive_parameters(
    fake_nipyapi,
    import_cd_module,
    monkeypatch,
    spec,
    provider_context_names,
    assets,
    expected_payload_names,
):
    harness = _Harness()
    root_context = _parameter_context(
        "pc-root",
        "Root Parameter Context",
        parameters=[
            _parameter("Database Driver"),
            _parameter("Hidden Secret", value="********", sensitive=True),
        ],
    )
    harness.parameter_contexts[root_context.id] = root_context
    harness.root_pg = _process_group("root", context_ref=_parameter_context_ref(root_context.id, root_context.component.name))
    harness.process_groups["root"] = harness.root_pg
    if assets:
        harness.assets_by_context[root_context.id].update(assets)
    if provider_context_names:
        harness.parameter_contexts["pc-provider"] = _parameter_context("pc-provider", "Provider POSTGRES")
    harness.install(fake_nipyapi)
    module, assets_module = _load_root_module(import_cd_module)
    monkeypatch.setattr(assets_module, "_download_file", lambda _url: pytest.fail("download should not run"))

    module.reconcile_root_parameter_context(
        spec,
        runtime_url="https://example.invalid/nifi-api",
        nifi_pat="token",
        provider_context_names=provider_context_names,
    )

    assert len(harness.submit_bodies) == 1
    payload = harness.submit_bodies[0][1]
    payload_names = [entity.parameter.name for entity in payload.component.parameters]
    assert payload_names == expected_payload_names
    assert "Hidden Secret" not in payload_names
    final_parameters = {entity.parameter.name: entity.parameter for entity in harness.parameter_contexts[root_context.id].component.parameters}
    assert final_parameters["Hidden Secret"].value == "********"


def test_reconcile_root_parameter_context_absent_spec_is_noop(fake_nipyapi, import_cd_module):
    harness = _Harness()
    harness.install(fake_nipyapi)
    module, _assets_module = _load_root_module(import_cd_module)

    module.reconcile_root_parameter_context({}, runtime_url="https://example.invalid/nifi-api", nifi_pat="token")
    module.reconcile_root_parameter_context(None, runtime_url="https://example.invalid/nifi-api", nifi_pat="token")

    assert harness.created_parameter_contexts == []
    assert harness.update_process_group_calls == []
    assert harness.submit_bodies == []


def test_download_file_rejects_non_http_urls_without_opening(import_cd_module, monkeypatch):
    module = _load_assets_module(import_cd_module)
    monkeypatch.setattr(module.urllib.request, "urlopen", lambda *args, **kwargs: pytest.fail("urlopen should not run"))

    with pytest.raises(RuntimeError, match="must use http or https"):
        module._download_file("file:///etc/passwd")


def test_download_file_enforces_timeout_and_size_limit_without_logging_url(import_cd_module, monkeypatch, capsys):
    module = _load_assets_module(import_cd_module)

    class Response:
        headers = {"Content-Length": "3"}

        def geturl(self):
            return "https://example.invalid/path/driver.jar"

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self, size):
            assert size == module.ASSET_DOWNLOAD_MAX_BYTES + 1
            return b"jar"

    calls = []
    monkeypatch.setattr(
        module.urllib.request,
        "urlopen",
        lambda request, timeout, context: calls.append((request.full_url, timeout, context)) or Response(),
    )

    assert module._download_file("https://user:secret@example.invalid/path/driver.jar?token=hidden") == b"jar"
    assert calls[0][:2] == ("https://user:secret@example.invalid/path/driver.jar?token=hidden", 60)
    assert calls[0][2].verify_mode == module.ssl.CERT_REQUIRED
    output = capsys.readouterr().out
    assert "driver.jar" in output
    assert "secret" not in output
    assert "token" not in output


def test_download_file_rejects_oversized_content_length(import_cd_module, monkeypatch):
    module = _load_assets_module(import_cd_module)

    class Response:
        headers = {"Content-Length": str(module.ASSET_DOWNLOAD_MAX_BYTES + 1)}

        def geturl(self):
            return "https://example.invalid/driver.jar"

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(module.urllib.request, "urlopen", lambda *args, **kwargs: Response())

    with pytest.raises(RuntimeError, match="exceeds"):
        module._download_file("https://example.invalid/driver.jar")


def test_download_file_rejects_redirect_to_non_http_scheme(import_cd_module, monkeypatch):
    module = _load_assets_module(import_cd_module)

    class Response:
        headers = {}

        def geturl(self):
            return "ftp://example.invalid/driver.jar"

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(module.urllib.request, "urlopen", lambda *args, **kwargs: Response())

    with pytest.raises(RuntimeError, match="redirect URL must use http or https"):
        module._download_file("https://example.invalid/driver.jar")


def test_reconcile_flow_assets_still_targets_parameter_owner_context(fake_nipyapi, import_cd_module, monkeypatch):
    harness = _Harness()
    inherited_context = _parameter_context("pc-shared", "Shared", parameters=[_parameter("Database Driver")])
    direct_context = _parameter_context("pc-flow", "Flow", inherited=[_parameter_context_ref("pc-shared", "Shared")])
    harness.parameter_contexts[inherited_context.id] = inherited_context
    harness.parameter_contexts[direct_context.id] = direct_context
    harness.process_groups["flow-pg"] = _process_group(
        "flow-pg",
        name="Example Flow",
        context_ref=_parameter_context_ref(direct_context.id, direct_context.component.name),
    )
    harness.update_sequences = [[types.SimpleNamespace(complete=True, failure_reason=None)]]
    harness.install(fake_nipyapi)
    module = _load_assets_module(import_cd_module)
    monkeypatch.setattr(module, "_download_file", lambda _url: b"flow-driver")

    module.reconcile_flow_assets(
        "flow-pg",
        [_asset_spec()],
        pg_name="Example Flow",
    )

    assert harness.create_asset_calls[0]["context_id"] == "pc-shared"
    parameter = {entity.parameter.name: entity.parameter for entity in harness.parameter_contexts["pc-shared"].component.parameters}["Database Driver"]
    assert [(ref.id, ref.name) for ref in parameter.referenced_assets] == [("asset-1", "driver.jar")]