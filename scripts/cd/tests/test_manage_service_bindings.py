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


def _entity(name=None, entity_id=None, component_kwargs=None, revision_version=0, status_kwargs=None):
    component = types.SimpleNamespace(name=name, id=entity_id, **(component_kwargs or {}))
    entity = types.SimpleNamespace(id=entity_id, component=component, revision=types.SimpleNamespace(version=revision_version))
    if status_kwargs is not None:
        entity.status = types.SimpleNamespace(**status_kwargs)
    return entity


def _processor_entity(
    name,
    entity_id,
    parent_group_id,
    properties=None,
    descriptors=None,
    validation_status="VALID",
    validation_errors=None,
    run_status="STOPPED",
    revision_version=0,
):
    return _entity(
        name=name,
        entity_id=entity_id,
        component_kwargs={
            "parent_group_id": parent_group_id,
            "type": "org.apache.nifi.processors.standard.UpdateRecord",
            "validation_status": validation_status,
            "validation_errors": validation_errors or [],
            "config": types.SimpleNamespace(
                properties=dict(properties or {}),
                descriptors=dict(descriptors or {}),
            ),
        },
        revision_version=revision_version,
        status_kwargs={"run_status": run_status},
    )


def _controller_service_entity(
    name,
    entity_id,
    parent_group_id,
    properties=None,
    descriptors=None,
    validation_status="VALID",
    validation_errors=None,
    state="DISABLED",
    revision_version=0,
):
    return _entity(
        name=name,
        entity_id=entity_id,
        component_kwargs={
            "parent_group_id": parent_group_id,
            "type": "org.apache.nifi.lookup.RecordSetWriterLookup",
            "validation_status": validation_status,
            "validation_errors": validation_errors or [],
            "properties": dict(properties or {}),
            "descriptors": dict(descriptors or {}),
            "state": state,
        },
        revision_version=revision_version,
    )


def _descriptor(identifies=True, sensitive=False, required=False, dynamic=False):
    return types.SimpleNamespace(
        identifies_controller_service="org.apache.nifi.controller.ControllerService" if identifies else "",
        sensitive=sensitive,
        required=required,
        dynamic=dynamic,
    )


def _pg_entity(name, group_id, parent_group_id, running_count=0, stopped_count=1):
    return types.SimpleNamespace(
        id=group_id,
        component=types.SimpleNamespace(name=name, parent_group_id=parent_group_id),
        running_count=running_count,
        stopped_count=stopped_count,
    )


def _flow_pg(name="Example Flow", running=False):
    return _pg_entity(name, "flow-pg", "root", running_count=1 if running else 0, stopped_count=0 if running else 1)


def _manage_flows_module(flow_pgs=None, start_calls=None, start_error=None):
    flow_pgs = flow_pgs or [_flow_pg()]

    def start_flow(pg_id, pg_name=""):
        if start_calls is not None:
            start_calls.append((pg_id, pg_name))
        if start_error is not None:
            raise start_error

    return types.SimpleNamespace(
        configure_nifi=lambda *args, **kwargs: None,
        list_process_groups=lambda parent_id=None: flow_pgs,
        start_flow=start_flow,
    )


def _import_manage_service_bindings(import_cd_module, manage_flows, manage_controller_services):
    return import_cd_module(
        "manage_service_bindings",
        {
            "manage_flows": manage_flows,
            "manage_controller_services": manage_controller_services,
        },
    )


class _ApiException(Exception):
    def __init__(self, status, reason, headers=None, body=None):
        super().__init__(f"HTTP response headers: {headers}\nHTTP response body: {body}")
        self.status = status
        self.reason = reason
        self.headers = headers
        self.body = body


@pytest.mark.parametrize(
    ("api_name", "method_name"),
    [
        ("ProcessorsApi", "totally_unknown_method"),
        ("ControllerServicesApi", "totally_unknown_method"),
        ("ProcessGroupsApi", "totally_unknown_method"),
    ],
)
def test_fake_api_rejects_unexpected_method_names(fake_nipyapi, api_name, method_name):
    api = getattr(fake_nipyapi.nifi, api_name)()
    with pytest.raises(AssertionError, match=fr"Unexpected fake {api_name} method: {method_name}"):
        getattr(api, method_name)


def test_real_method_names_cover_supported_run_status_calls(real_nipyapi_method_names):
    processor_methods = real_nipyapi_method_names.get("ProcessorsApi", {})
    controller_service_methods = real_nipyapi_method_names.get("ControllerServicesApi", {})
    if processor_methods:
        assert "update_run_status5" in processor_methods
        assert "body" in processor_methods["update_run_status5"].parameters
        assert "id" in processor_methods["update_run_status5"].parameters
        if "update_run_status4" in processor_methods:
            assert "body" in processor_methods["update_run_status4"].parameters
            assert "id" in processor_methods["update_run_status4"].parameters
    if controller_service_methods:
        assert "update_run_status2" in controller_service_methods
        assert "body" in controller_service_methods["update_run_status2"].parameters
        assert "id" in controller_service_methods["update_run_status2"].parameters
        if "update_run_status1" in controller_service_methods:
            assert "body" in controller_service_methods["update_run_status1"].parameters
            assert "id" in controller_service_methods["update_run_status1"].parameters


def test_reconcile_processor_binding_quiesces_once_and_updates_uuid(fake_nipyapi, import_cd_module):
    quiesce_calls = []
    runtime_state = {
        "processor": _processor_entity(
            name="Parse Records",
            entity_id="proc-1",
            parent_group_id="flow-pg",
            properties={},
            descriptors={"Record Reader": _descriptor()},
        ),
        "root_services": [_controller_service_entity("CSV Reader", "root-cs-1", "root")],
    }

    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_processors"] = lambda **kwargs: types.SimpleNamespace(processors=[runtime_state["processor"]])
    fake_nipyapi.api_methods["FlowApi"]["get_controller_services_from_group"] = lambda **kwargs: types.SimpleNamespace(controller_services=[])
    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_process_group"] = lambda **kwargs: _flow_pg()
    fake_nipyapi.api_methods["ProcessorsApi"]["get_processor"] = lambda **kwargs: runtime_state["processor"]

    update_calls = []

    def update_processor(**kwargs):
        update_calls.append(kwargs["body"].component.config.properties)
        runtime_state["processor"] = _processor_entity(
            name="Parse Records",
            entity_id="proc-1",
            parent_group_id="flow-pg",
            properties={"Record Reader": "root-cs-1"},
            descriptors={"Record Reader": _descriptor()},
            revision_version=1,
        )
        return runtime_state["processor"]

    fake_nipyapi.api_methods["ProcessorsApi"]["update_processor"] = update_processor
    fake_nipyapi.canvas.schedule_process_group = lambda pg_id, running: quiesce_calls.append((pg_id, running))

    module = _import_manage_service_bindings(
        import_cd_module,
        _manage_flows_module(),
        types.SimpleNamespace(list_root_pg_controller_services=lambda: runtime_state["root_services"]),
    )

    changed = module.reconcile_service_bindings(
        {
            "name": "Example Flow",
            "start": True,
            "service_bindings": [{"target": "Parse Records", "properties": {"Record Reader": "CSV Reader"}}],
        },
        [{"name": "CSV Reader", "type": "org.apache.nifi.json.JsonTreeReader"}],
        runtime_url="https://example.invalid/nifi-api",
        nifi_pat="token",
    )

    assert changed is True
    assert quiesce_calls == [("flow-pg", False)]
    assert update_calls == [{"Record Reader": "root-cs-1"}]


def test_running_processor_uses_update_run_status5_when_available(fake_nipyapi, import_cd_module):
    calls = []
    runtime_state = {
        "processor": _processor_entity(
            name="Parse Records",
            entity_id="proc-1",
            parent_group_id="flow-pg",
            properties={},
            descriptors={"Record Reader": _descriptor()},
            run_status="RUNNING",
        )
    }

    def get_processor(**kwargs):
        return runtime_state["processor"]

    def update_run_status5(**kwargs):
        calls.append("update_run_status5")
        runtime_state["processor"] = _processor_entity(
            name="Parse Records",
            entity_id="proc-1",
            parent_group_id="flow-pg",
            properties={},
            descriptors={"Record Reader": _descriptor()},
            run_status="STOPPED",
            revision_version=1,
        )
        return runtime_state["processor"]

    def update_processor(**kwargs):
        runtime_state["processor"] = _processor_entity(
            name="Parse Records",
            entity_id="proc-1",
            parent_group_id="flow-pg",
            properties={"Record Reader": "root-cs-1"},
            descriptors={"Record Reader": _descriptor()},
            run_status="STOPPED",
            revision_version=2,
        )
        return runtime_state["processor"]

    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_processors"] = lambda **kwargs: types.SimpleNamespace(processors=[runtime_state["processor"]])
    fake_nipyapi.api_methods["FlowApi"]["get_controller_services_from_group"] = lambda **kwargs: types.SimpleNamespace(controller_services=[])
    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_process_group"] = lambda **kwargs: _flow_pg()
    fake_nipyapi.api_methods["ProcessorsApi"]["get_processor"] = get_processor
    fake_nipyapi.api_methods["ProcessorsApi"]["update_run_status5"] = update_run_status5
    fake_nipyapi.api_methods["ProcessorsApi"]["update_processor"] = update_processor

    module = _import_manage_service_bindings(
        import_cd_module,
        _manage_flows_module(),
        types.SimpleNamespace(list_root_pg_controller_services=lambda: [_controller_service_entity("CSV Reader", "root-cs-1", "root")]),
    )

    module.reconcile_service_bindings(
        {
            "name": "Example Flow",
            "start": True,
            "service_bindings": [{"target": "Parse Records", "properties": {"Record Reader": "CSV Reader"}}],
        },
        [{"name": "CSV Reader", "type": "org.apache.nifi.json.JsonTreeReader"}],
        runtime_url="https://example.invalid/nifi-api",
        nifi_pat="token",
    )

    assert calls == ["update_run_status5"]


def test_running_processor_falls_back_to_update_run_status4(fake_nipyapi, import_cd_module):
    calls = []
    runtime_state = {
        "processor": _processor_entity(
            name="Parse Records",
            entity_id="proc-1",
            parent_group_id="flow-pg",
            properties={},
            descriptors={"Record Reader": _descriptor()},
            run_status="RUNNING",
        )
    }

    def get_processor(**kwargs):
        return runtime_state["processor"]

    def update_run_status4(**kwargs):
        calls.append("update_run_status4")
        runtime_state["processor"] = _processor_entity(
            name="Parse Records",
            entity_id="proc-1",
            parent_group_id="flow-pg",
            properties={},
            descriptors={"Record Reader": _descriptor()},
            run_status="STOPPED",
            revision_version=1,
        )
        return runtime_state["processor"]

    def update_processor(**kwargs):
        runtime_state["processor"] = _processor_entity(
            name="Parse Records",
            entity_id="proc-1",
            parent_group_id="flow-pg",
            properties={"Record Reader": "root-cs-1"},
            descriptors={"Record Reader": _descriptor()},
            run_status="STOPPED",
            revision_version=2,
        )
        return runtime_state["processor"]

    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_processors"] = lambda **kwargs: types.SimpleNamespace(processors=[runtime_state["processor"]])
    fake_nipyapi.api_methods["FlowApi"]["get_controller_services_from_group"] = lambda **kwargs: types.SimpleNamespace(controller_services=[])
    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_process_group"] = lambda **kwargs: _flow_pg()
    fake_nipyapi.api_methods["ProcessorsApi"]["get_processor"] = get_processor
    fake_nipyapi.api_methods["ProcessorsApi"]["update_run_status4"] = update_run_status4
    fake_nipyapi.api_methods["ProcessorsApi"]["update_processor"] = update_processor

    module = _import_manage_service_bindings(
        import_cd_module,
        _manage_flows_module(),
        types.SimpleNamespace(list_root_pg_controller_services=lambda: [_controller_service_entity("CSV Reader", "root-cs-1", "root")]),
    )

    module.reconcile_service_bindings(
        {
            "name": "Example Flow",
            "start": True,
            "service_bindings": [{"target": "Parse Records", "properties": {"Record Reader": "CSV Reader"}}],
        },
        [{"name": "CSV Reader", "type": "org.apache.nifi.json.JsonTreeReader"}],
        runtime_url="https://example.invalid/nifi-api",
        nifi_pat="token",
    )

    assert calls == ["update_run_status4"]


def test_controller_service_target_success_uses_supported_run_status_method(fake_nipyapi, import_cd_module):
    calls = []
    runtime_state = {
        "service": _controller_service_entity(
            name="Lookup",
            entity_id="cs-1",
            parent_group_id="flow-pg",
            properties={"Record Reader": "root-cs-old"},
            descriptors={"Record Reader": _descriptor()},
            state="ENABLED",
        )
    }

    def get_controller_service(**kwargs):
        return runtime_state["service"]

    def update_run_status2(**kwargs):
        calls.append("update_run_status2")
        runtime_state["service"] = _controller_service_entity(
            name="Lookup",
            entity_id="cs-1",
            parent_group_id="flow-pg",
            properties={"Record Reader": "root-cs-old"},
            descriptors={"Record Reader": _descriptor()},
            state="DISABLED",
            revision_version=1,
        )
        return runtime_state["service"]

    def update_controller_service(**kwargs):
        calls.append("update_controller_service")
        runtime_state["service"] = _controller_service_entity(
            name="Lookup",
            entity_id="cs-1",
            parent_group_id="flow-pg",
            properties={"Record Reader": "root-cs-1"},
            descriptors={"Record Reader": _descriptor()},
            state="DISABLED",
            revision_version=2,
        )
        return runtime_state["service"]

    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_processors"] = lambda **kwargs: types.SimpleNamespace(processors=[])
    fake_nipyapi.api_methods["FlowApi"]["get_controller_services_from_group"] = lambda **kwargs: types.SimpleNamespace(controller_services=[runtime_state["service"]])
    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_process_group"] = lambda **kwargs: _flow_pg()
    fake_nipyapi.api_methods["ControllerServicesApi"]["get_controller_service"] = get_controller_service
    fake_nipyapi.api_methods["ControllerServicesApi"]["update_run_status2"] = update_run_status2
    fake_nipyapi.api_methods["ControllerServicesApi"]["update_controller_service"] = update_controller_service

    module = _import_manage_service_bindings(
        import_cd_module,
        _manage_flows_module(),
        types.SimpleNamespace(list_root_pg_controller_services=lambda: [_controller_service_entity("CSV Reader", "root-cs-1", "root")]),
    )

    changed = module.reconcile_service_bindings(
        {
            "name": "Example Flow",
            "start": False,
            "service_bindings": [{"target": "Lookup", "properties": {"Record Reader": "CSV Reader"}}],
        },
        [{"name": "CSV Reader", "type": "org.apache.nifi.json.JsonTreeReader"}],
        runtime_url="https://example.invalid/nifi-api",
        nifi_pat="token",
    )

    assert changed is True
    assert calls == ["update_run_status2", "update_controller_service"]


def test_controller_service_target_falls_back_to_update_run_status1(fake_nipyapi, import_cd_module):
    calls = []
    runtime_state = {
        "service": _controller_service_entity(
            name="Lookup",
            entity_id="cs-1",
            parent_group_id="flow-pg",
            properties={"Record Reader": "root-cs-old"},
            descriptors={"Record Reader": _descriptor()},
            state="ENABLED",
        )
    }

    def get_controller_service(**kwargs):
        return runtime_state["service"]

    def update_run_status1(**kwargs):
        calls.append("update_run_status1")
        runtime_state["service"] = _controller_service_entity(
            name="Lookup",
            entity_id="cs-1",
            parent_group_id="flow-pg",
            properties={"Record Reader": "root-cs-old"},
            descriptors={"Record Reader": _descriptor()},
            state="DISABLED",
            revision_version=1,
        )
        return runtime_state["service"]

    def update_controller_service(**kwargs):
        calls.append("update_controller_service")
        runtime_state["service"] = _controller_service_entity(
            name="Lookup",
            entity_id="cs-1",
            parent_group_id="flow-pg",
            properties={"Record Reader": "root-cs-1"},
            descriptors={"Record Reader": _descriptor()},
            state="DISABLED",
            revision_version=2,
        )
        return runtime_state["service"]

    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_processors"] = lambda **kwargs: types.SimpleNamespace(processors=[])
    fake_nipyapi.api_methods["FlowApi"]["get_controller_services_from_group"] = lambda **kwargs: types.SimpleNamespace(controller_services=[runtime_state["service"]])
    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_process_group"] = lambda **kwargs: _flow_pg()
    fake_nipyapi.api_methods["ControllerServicesApi"]["get_controller_service"] = get_controller_service
    fake_nipyapi.api_methods["ControllerServicesApi"]["update_run_status1"] = update_run_status1
    fake_nipyapi.api_methods["ControllerServicesApi"]["update_controller_service"] = update_controller_service

    module = _import_manage_service_bindings(
        import_cd_module,
        _manage_flows_module(),
        types.SimpleNamespace(list_root_pg_controller_services=lambda: [_controller_service_entity("CSV Reader", "root-cs-1", "root")]),
    )

    changed = module.reconcile_service_bindings(
        {
            "name": "Example Flow",
            "start": False,
            "service_bindings": [{"target": "Lookup", "properties": {"Record Reader": "CSV Reader"}}],
        },
        [{"name": "CSV Reader", "type": "org.apache.nifi.json.JsonTreeReader"}],
        runtime_url="https://example.invalid/nifi-api",
        nifi_pat="token",
    )

    assert changed is True
    assert calls == ["update_run_status1", "update_controller_service"]


def test_dynamic_probe_success_updates_binding(fake_nipyapi, import_cd_module):
    state = {"phase": "initial"}
    payloads = []

    def current_processor():
        if state["phase"] == "initial":
            return _processor_entity("Lookup", "proc-1", "flow-pg", properties={}, descriptors={})
        return _processor_entity(
            "Lookup",
            "proc-1",
            "flow-pg",
            properties={"csv": "root-cs-1"},
            descriptors={"csv": _descriptor(dynamic=True)},
            revision_version=1,
        )

    def update_processor(**kwargs):
        payloads.append(kwargs["body"].component.config.properties)
        state["phase"] = "probed"
        return current_processor()

    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_processors"] = lambda **kwargs: types.SimpleNamespace(processors=[current_processor()])
    fake_nipyapi.api_methods["FlowApi"]["get_controller_services_from_group"] = lambda **kwargs: types.SimpleNamespace(controller_services=[])
    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_process_group"] = lambda **kwargs: _flow_pg()
    fake_nipyapi.api_methods["ProcessorsApi"]["get_processor"] = lambda **kwargs: current_processor()
    fake_nipyapi.api_methods["ProcessorsApi"]["update_processor"] = update_processor

    module = _import_manage_service_bindings(
        import_cd_module,
        _manage_flows_module(),
        types.SimpleNamespace(list_root_pg_controller_services=lambda: [_controller_service_entity("CSV Reader", "root-cs-1", "root")]),
    )

    changed = module.reconcile_service_bindings(
        {
            "name": "Example Flow",
            "start": True,
            "service_bindings": [{"target": "Lookup", "properties": {"csv": "CSV Reader"}}],
        },
        [{"name": "CSV Reader", "type": "org.apache.nifi.json.JsonTreeReader"}],
        runtime_url="https://example.invalid/nifi-api",
        nifi_pat="token",
    )

    assert changed is True
    assert payloads == [{"csv": "root-cs-1"}]


def test_dynamic_probe_failure_rolls_back(fake_nipyapi, import_cd_module):
    state = {"phase": "initial"}
    update_payloads = []

    def current_processor():
        if state["phase"] == "initial":
            return _processor_entity("Lookup", "proc-1", "flow-pg", properties={}, descriptors={})
        if state["phase"] == "probed":
            return _processor_entity(
                "Lookup",
                "proc-1",
                "flow-pg",
                properties={"csv": "root-cs-1"},
                descriptors={"csv": _descriptor(identifies=False, dynamic=True)},
                revision_version=1,
            )
        return _processor_entity("Lookup", "proc-1", "flow-pg", properties={}, descriptors={}, revision_version=2)

    def update_processor(**kwargs):
        update_payloads.append(kwargs["body"].component.config.properties)
        state["phase"] = "rolled_back" if kwargs["body"].component.config.properties["csv"] is None else "probed"
        return current_processor()

    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_processors"] = lambda **kwargs: types.SimpleNamespace(processors=[current_processor()])
    fake_nipyapi.api_methods["FlowApi"]["get_controller_services_from_group"] = lambda **kwargs: types.SimpleNamespace(controller_services=[])
    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_process_group"] = lambda **kwargs: _flow_pg()
    fake_nipyapi.api_methods["ProcessorsApi"]["get_processor"] = lambda **kwargs: current_processor()
    fake_nipyapi.api_methods["ProcessorsApi"]["update_processor"] = update_processor

    module = _import_manage_service_bindings(
        import_cd_module,
        _manage_flows_module(),
        types.SimpleNamespace(list_root_pg_controller_services=lambda: [_controller_service_entity("CSV Reader", "root-cs-1", "root")]),
    )

    with pytest.raises(RuntimeError, match="original values and run state were restored"):
        module.reconcile_service_bindings(
            {
                "name": "Example Flow",
                "start": True,
                "service_bindings": [{"target": "Lookup", "properties": {"csv": "CSV Reader"}}],
            },
            [{"name": "CSV Reader", "type": "org.apache.nifi.json.JsonTreeReader"}],
            runtime_url="https://example.invalid/nifi-api",
            nifi_pat="token",
        )

    assert update_payloads == [{"csv": "root-cs-1"}, {"csv": None}]


def test_required_property_removal_fails_before_quiesce(fake_nipyapi, import_cd_module):
    quiesce_calls = []
    lookup_service = _controller_service_entity(
        name="Lookup",
        entity_id="cs-1",
        parent_group_id="flow-pg",
        properties={"Record Reader": "root-cs-1"},
        descriptors={"Record Reader": _descriptor(required=True)},
    )

    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_processors"] = lambda **kwargs: types.SimpleNamespace(processors=[])
    fake_nipyapi.api_methods["FlowApi"]["get_controller_services_from_group"] = lambda **kwargs: types.SimpleNamespace(controller_services=[lookup_service])
    fake_nipyapi.api_methods["ControllerServicesApi"]["get_controller_service"] = lambda **kwargs: lookup_service
    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_process_group"] = lambda **kwargs: _flow_pg()
    fake_nipyapi.canvas.schedule_process_group = lambda pg_id, running: quiesce_calls.append((pg_id, running))

    module = _import_manage_service_bindings(
        import_cd_module,
        _manage_flows_module(),
        types.SimpleNamespace(list_root_pg_controller_services=lambda: [_controller_service_entity("CSV Reader", "root-cs-1", "root")]),
    )

    with pytest.raises(RuntimeError, match="required and cannot be removed"):
        module.reconcile_service_bindings(
            {
                "name": "Example Flow",
                "start": False,
                "service_bindings": [{"target": "Lookup", "properties": {"Record Reader": None}}],
            },
            [{"name": "CSV Reader", "type": "org.apache.nifi.json.JsonTreeReader"}],
            runtime_url="https://example.invalid/nifi-api",
            nifi_pat="token",
        )

    assert quiesce_calls == []


def test_unknown_root_pg_service_name_fails_before_mutation(fake_nipyapi, import_cd_module):
    processor = _processor_entity("Parse Records", "proc-1", "flow-pg", descriptors={"Record Reader": _descriptor()})
    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_processors"] = lambda **kwargs: types.SimpleNamespace(processors=[processor])
    fake_nipyapi.api_methods["FlowApi"]["get_controller_services_from_group"] = lambda **kwargs: types.SimpleNamespace(controller_services=[])
    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_process_group"] = lambda **kwargs: _flow_pg()
    fake_nipyapi.api_methods["ProcessorsApi"]["get_processor"] = lambda **kwargs: processor

    module = _import_manage_service_bindings(
        import_cd_module,
        _manage_flows_module(),
        types.SimpleNamespace(list_root_pg_controller_services=lambda: [_controller_service_entity("CSV Reader", "root-cs-1", "root")]),
    )

    with pytest.raises(RuntimeError, match="references unknown root PG controller service 'Missing Reader'"):
        module.reconcile_service_bindings(
            {
                "name": "Example Flow",
                "start": True,
                "service_bindings": [{"target": "Parse Records", "properties": {"Record Reader": "Missing Reader"}}],
            },
            [{"name": "CSV Reader", "type": "org.apache.nifi.json.JsonTreeReader"}],
            runtime_url="https://example.invalid/nifi-api",
            nifi_pat="token",
        )


def test_non_controller_service_descriptor_is_rejected(fake_nipyapi, import_cd_module):
    processor = _processor_entity("Lookup", "proc-1", "flow-pg", descriptors={"csv": _descriptor(identifies=False)})
    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_processors"] = lambda **kwargs: types.SimpleNamespace(processors=[processor])
    fake_nipyapi.api_methods["FlowApi"]["get_controller_services_from_group"] = lambda **kwargs: types.SimpleNamespace(controller_services=[])
    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_process_group"] = lambda **kwargs: _flow_pg()
    fake_nipyapi.api_methods["ProcessorsApi"]["get_processor"] = lambda **kwargs: processor

    module = _import_manage_service_bindings(
        import_cd_module,
        _manage_flows_module(),
        types.SimpleNamespace(list_root_pg_controller_services=lambda: [_controller_service_entity("CSV Reader", "root-cs-1", "root")]),
    )

    with pytest.raises(RuntimeError, match="does not identify a controller service"):
        module.reconcile_service_bindings(
            {
                "name": "Example Flow",
                "start": False,
                "service_bindings": [{"target": "Lookup", "properties": {"csv": "CSV Reader"}}],
            },
            [{"name": "CSV Reader", "type": "org.apache.nifi.json.JsonTreeReader"}],
            runtime_url="https://example.invalid/nifi-api",
            nifi_pat="token",
        )


def test_sensitive_descriptor_is_rejected(fake_nipyapi, import_cd_module):
    processor = _processor_entity("Lookup", "proc-1", "flow-pg", descriptors={"csv": _descriptor(sensitive=True)})
    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_processors"] = lambda **kwargs: types.SimpleNamespace(processors=[processor])
    fake_nipyapi.api_methods["FlowApi"]["get_controller_services_from_group"] = lambda **kwargs: types.SimpleNamespace(controller_services=[])
    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_process_group"] = lambda **kwargs: _flow_pg()
    fake_nipyapi.api_methods["ProcessorsApi"]["get_processor"] = lambda **kwargs: processor

    module = _import_manage_service_bindings(
        import_cd_module,
        _manage_flows_module(),
        types.SimpleNamespace(list_root_pg_controller_services=lambda: [_controller_service_entity("CSV Reader", "root-cs-1", "root")]),
    )

    with pytest.raises(RuntimeError, match="is sensitive and cannot be managed via service_bindings"):
        module.reconcile_service_bindings(
            {
                "name": "Example Flow",
                "start": False,
                "service_bindings": [{"target": "Lookup", "properties": {"csv": "CSV Reader"}}],
            },
            [{"name": "CSV Reader", "type": "org.apache.nifi.json.JsonTreeReader"}],
            runtime_url="https://example.invalid/nifi-api",
            nifi_pat="token",
        )


def test_ambiguous_target_is_rejected_with_paths(fake_nipyapi, import_cd_module):
    proc = _processor_entity("Lookup", "proc-1", "child-a", descriptors={"csv": _descriptor()})
    cs = _controller_service_entity("Lookup", "cs-1", "child-b", descriptors={"csv": _descriptor()})
    group_map = {
        "child-a": _pg_entity("Nested A", "child-a", "flow-pg"),
        "child-b": _pg_entity("Nested B", "child-b", "flow-pg"),
        "flow-pg": _flow_pg(),
    }
    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_processors"] = lambda **kwargs: types.SimpleNamespace(processors=[proc])
    fake_nipyapi.api_methods["FlowApi"]["get_controller_services_from_group"] = lambda **kwargs: types.SimpleNamespace(controller_services=[cs])
    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_process_group"] = lambda **kwargs: group_map[kwargs["id"]]
    fake_nipyapi.api_methods["ProcessorsApi"]["get_processor"] = lambda **kwargs: proc

    module = _import_manage_service_bindings(
        import_cd_module,
        _manage_flows_module(),
        types.SimpleNamespace(list_root_pg_controller_services=lambda: [_controller_service_entity("CSV Reader", "root-cs-1", "root")]),
    )

    with pytest.raises(RuntimeError, match=r"Service binding target 'Lookup' is ambiguous: processor at Example Flow/Nested A, controller_service at Example Flow/Nested B"):
        module.reconcile_service_bindings(
            {
                "name": "Example Flow",
                "start": True,
                "service_bindings": [{"target": "Lookup", "properties": {"csv": "CSV Reader"}}],
            },
            [{"name": "CSV Reader", "type": "org.apache.nifi.json.JsonTreeReader"}],
            runtime_url="https://example.invalid/nifi-api",
            nifi_pat="token",
        )


def test_discover_targets_excludes_ancestor_groups(fake_nipyapi, import_cd_module):
    descendant = _controller_service_entity("Lookup", "cs-1", "flow-pg", descriptors={"csv": _descriptor()})
    ancestor = _controller_service_entity("Lookup", "cs-root", "root", descriptors={"csv": _descriptor()})

    def get_controller_services_from_group(**kwargs):
        assert kwargs["include_ancestor_groups"] is False
        assert kwargs["include_descendant_groups"] is True
        services = [descendant]
        if kwargs["include_ancestor_groups"]:
            services.append(ancestor)
        return types.SimpleNamespace(controller_services=services)

    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_processors"] = lambda **kwargs: types.SimpleNamespace(processors=[])
    fake_nipyapi.api_methods["FlowApi"]["get_controller_services_from_group"] = get_controller_services_from_group
    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_process_group"] = lambda **kwargs: _flow_pg()

    module = _import_manage_service_bindings(
        import_cd_module,
        _manage_flows_module(),
        types.SimpleNamespace(list_root_pg_controller_services=lambda: []),
    )

    candidates = module.discover_service_binding_targets("flow-pg", "Example Flow")

    assert [candidate["id"] for candidate in candidates] == ["cs-1"]


def test_flow_process_group_resolution_is_case_insensitive_and_rejects_duplicates(fake_nipyapi, import_cd_module):
    module = _import_manage_service_bindings(
        import_cd_module,
        _manage_flows_module(flow_pgs=[_flow_pg("Example Flow"), _flow_pg("example flow")]),
        types.SimpleNamespace(list_root_pg_controller_services=lambda: []),
    )

    with pytest.raises(RuntimeError, match="Flow process group 'EXAMPLE FLOW' is ambiguous"):
        module.resolve_flow_process_group("EXAMPLE FLOW")


def test_validation_timeout_fails_and_restores_original_values(fake_nipyapi, import_cd_module, monkeypatch):
    now = {"value": 0}
    payloads = []
    state = {"updated": False}

    def fake_time():
        return now["value"]

    def fake_sleep(seconds):
        now["value"] += seconds

    def current_processor():
        if state["updated"]:
            return _processor_entity(
                "Lookup",
                "proc-1",
                "flow-pg",
                properties={"csv": "root-cs-1"},
                descriptors={"csv": _descriptor(dynamic=True)},
                validation_status="",
                validation_errors=[],
                revision_version=1,
            )
        return _processor_entity("Lookup", "proc-1", "flow-pg", properties={}, descriptors={})

    def update_processor(**kwargs):
        payloads.append(kwargs["body"].component.config.properties)
        state["updated"] = kwargs["body"].component.config.properties["csv"] is not None
        return current_processor()

    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_processors"] = lambda **kwargs: types.SimpleNamespace(processors=[current_processor()])
    fake_nipyapi.api_methods["FlowApi"]["get_controller_services_from_group"] = lambda **kwargs: types.SimpleNamespace(controller_services=[])
    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_process_group"] = lambda **kwargs: _flow_pg()
    fake_nipyapi.api_methods["ProcessorsApi"]["get_processor"] = lambda **kwargs: current_processor()
    fake_nipyapi.api_methods["ProcessorsApi"]["update_processor"] = update_processor

    module = _import_manage_service_bindings(
        import_cd_module,
        _manage_flows_module(),
        types.SimpleNamespace(list_root_pg_controller_services=lambda: [_controller_service_entity("CSV Reader", "root-cs-1", "root")]),
    )
    monkeypatch.setattr(module.time, "time", fake_time)
    monkeypatch.setattr(module.time, "sleep", fake_sleep)

    with pytest.raises(RuntimeError, match="did not reach VALID within 30s"):
        module.reconcile_service_bindings(
            {
                "name": "Example Flow",
                "start": True,
                "service_bindings": [{"target": "Lookup", "properties": {"csv": "CSV Reader"}}],
            },
            [{"name": "CSV Reader", "type": "org.apache.nifi.json.JsonTreeReader"}],
            runtime_url="https://example.invalid/nifi-api",
            nifi_pat="token",
        )

    assert payloads == [{"csv": "root-cs-1"}, {"csv": None}]


def test_explicit_null_post_write_refusal_restores_original_values(fake_nipyapi, import_cd_module):
    payloads = []
    state = {"removed": False}

    def current_processor():
        if state["removed"]:
            return _processor_entity(
                "Lookup",
                "proc-1",
                "flow-pg",
                properties={"Record Reader": "root-cs-1"},
                descriptors={"Record Reader": _descriptor(dynamic=True)},
                revision_version=1,
            )
        return _processor_entity(
            "Lookup",
            "proc-1",
            "flow-pg",
            properties={"Record Reader": "root-cs-1"},
            descriptors={"Record Reader": _descriptor(dynamic=True)},
        )

    def update_processor(**kwargs):
        payloads.append(kwargs["body"].component.config.properties)
        requested = kwargs["body"].component.config.properties["Record Reader"]
        if requested is None:
            state["removed"] = True
        else:
            state["removed"] = False
        return current_processor()

    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_processors"] = lambda **kwargs: types.SimpleNamespace(processors=[current_processor()])
    fake_nipyapi.api_methods["FlowApi"]["get_controller_services_from_group"] = lambda **kwargs: types.SimpleNamespace(controller_services=[])
    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_process_group"] = lambda **kwargs: _flow_pg()
    fake_nipyapi.api_methods["ProcessorsApi"]["get_processor"] = lambda **kwargs: current_processor()
    fake_nipyapi.api_methods["ProcessorsApi"]["update_processor"] = update_processor

    module = _import_manage_service_bindings(
        import_cd_module,
        _manage_flows_module(),
        types.SimpleNamespace(list_root_pg_controller_services=lambda: [_controller_service_entity("CSV Reader", "root-cs-1", "root")]),
    )

    with pytest.raises(RuntimeError, match="property 'Record Reader' could not be removed"):
        module.reconcile_service_bindings(
            {
                "name": "Example Flow",
                "start": False,
                "service_bindings": [{"target": "Lookup", "properties": {"Record Reader": None}}],
            },
            [{"name": "CSV Reader", "type": "org.apache.nifi.json.JsonTreeReader"}],
            runtime_url="https://example.invalid/nifi-api",
            nifi_pat="token",
        )

    assert payloads == [{"Record Reader": None}, {"Record Reader": "root-cs-1"}]


def test_rollback_and_flow_restoration_failures_are_reported(fake_nipyapi, import_cd_module):
    start_calls = []
    state = {"phase": "running"}

    def current_processor():
        if state["phase"] == "running":
            return _processor_entity(
                "Lookup",
                "proc-1",
                "flow-pg",
                properties={},
                descriptors={},
                run_status="RUNNING",
            )
        if state["phase"] == "stopped":
            return _processor_entity(
                "Lookup",
                "proc-1",
                "flow-pg",
                properties={},
                descriptors={},
                run_status="STOPPED",
                revision_version=1,
            )
        return _processor_entity(
            "Lookup",
            "proc-1",
            "flow-pg",
            properties={"csv": "root-cs-1"},
            descriptors={"csv": _descriptor(identifies=False, dynamic=True)},
            run_status="STOPPED",
            revision_version=2,
        )

    def update_run_status4(**kwargs):
        state["phase"] = "stopped"
        return current_processor()

    def update_processor(**kwargs):
        if kwargs["body"].component.config.properties["csv"] is None:
            raise RuntimeError("rollback boom")
        state["phase"] = "bad-write"
        return current_processor()

    def schedule_process_group(pg_id, running):
        if not running:
            state["phase"] = "stopped"

    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_processors"] = lambda **kwargs: types.SimpleNamespace(processors=[current_processor()])
    fake_nipyapi.api_methods["FlowApi"]["get_controller_services_from_group"] = lambda **kwargs: types.SimpleNamespace(controller_services=[])
    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_process_group"] = lambda **kwargs: _flow_pg(running=state["phase"] == "running") if kwargs["id"] == "flow-pg" else _flow_pg(running=True)
    fake_nipyapi.api_methods["ProcessorsApi"]["get_processor"] = lambda **kwargs: current_processor()
    fake_nipyapi.api_methods["ProcessorsApi"]["update_run_status4"] = update_run_status4
    fake_nipyapi.api_methods["ProcessorsApi"]["update_processor"] = update_processor
    fake_nipyapi.canvas.schedule_process_group = schedule_process_group

    module = _import_manage_service_bindings(
        import_cd_module,
        _manage_flows_module(start_calls=start_calls, start_error=RuntimeError("start boom"), flow_pgs=[_flow_pg(running=True)]),
        types.SimpleNamespace(list_root_pg_controller_services=lambda: [_controller_service_entity("CSV Reader", "root-cs-1", "root")]),
    )

    with pytest.raises(RuntimeError, match=r"does not identify a controller service.*current entry restore failed: rollback boom.*flow restoration failed: start boom"):
        module.reconcile_service_bindings(
            {
                "name": "Example Flow",
                "start": True,
                "service_bindings": [{"target": "Lookup", "properties": {"csv": "CSV Reader"}}],
            },
            [{"name": "CSV Reader", "type": "org.apache.nifi.json.JsonTreeReader"}],
            runtime_url="https://example.invalid/nifi-api",
            nifi_pat="token",
        )

    assert start_calls == [("flow-pg", "Example Flow")]


def test_out_of_band_repair_then_noop(fake_nipyapi, import_cd_module):
    quiesce_calls = []
    runtime_state = {
        "processor": _processor_entity(
            "Parse Records",
            "proc-1",
            "flow-pg",
            properties={"Record Reader": "root-cs-old"},
            descriptors={"Record Reader": _descriptor()},
        )
    }

    def update_processor(**kwargs):
        runtime_state["processor"] = _processor_entity(
            "Parse Records",
            "proc-1",
            "flow-pg",
            properties={"Record Reader": "root-cs-1"},
            descriptors={"Record Reader": _descriptor()},
            revision_version=1,
        )
        return runtime_state["processor"]

    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_processors"] = lambda **kwargs: types.SimpleNamespace(processors=[runtime_state["processor"]])
    fake_nipyapi.api_methods["FlowApi"]["get_controller_services_from_group"] = lambda **kwargs: types.SimpleNamespace(controller_services=[])
    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_process_group"] = lambda **kwargs: _flow_pg()
    fake_nipyapi.api_methods["ProcessorsApi"]["get_processor"] = lambda **kwargs: runtime_state["processor"]
    fake_nipyapi.api_methods["ProcessorsApi"]["update_processor"] = update_processor
    fake_nipyapi.canvas.schedule_process_group = lambda pg_id, running: quiesce_calls.append((pg_id, running))

    module = _import_manage_service_bindings(
        import_cd_module,
        _manage_flows_module(),
        types.SimpleNamespace(list_root_pg_controller_services=lambda: [_controller_service_entity("CSV Reader", "root-cs-1", "root")]),
    )

    changed = module.reconcile_service_bindings(
        {
            "name": "Example Flow",
            "start": True,
            "service_bindings": [{"target": "Parse Records", "properties": {"Record Reader": "CSV Reader"}}],
        },
        [{"name": "CSV Reader", "type": "org.apache.nifi.json.JsonTreeReader"}],
        runtime_url="https://example.invalid/nifi-api",
        nifi_pat="token",
    )
    unchanged = module.reconcile_service_bindings(
        {
            "name": "Example Flow",
            "start": True,
            "service_bindings": [{"target": "Parse Records", "properties": {"Record Reader": "CSV Reader"}}],
        },
        [{"name": "CSV Reader", "type": "org.apache.nifi.json.JsonTreeReader"}],
        runtime_url="https://example.invalid/nifi-api",
        nifi_pat="token",
    )

    assert changed is True
    assert unchanged is False
    assert quiesce_calls == [("flow-pg", False)]


def test_reconcile_is_noop_when_binding_matches_live_state(fake_nipyapi, import_cd_module):
    processor = _processor_entity(
        name="Parse Records",
        entity_id="proc-1",
        parent_group_id="flow-pg",
        properties={"Record Reader": "root-cs-1"},
        descriptors={"Record Reader": _descriptor()},
    )

    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_processors"] = lambda **kwargs: types.SimpleNamespace(processors=[processor])
    fake_nipyapi.api_methods["FlowApi"]["get_controller_services_from_group"] = lambda **kwargs: types.SimpleNamespace(controller_services=[])
    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_process_group"] = lambda **kwargs: _flow_pg()
    fake_nipyapi.api_methods["ProcessorsApi"]["get_processor"] = lambda **kwargs: processor

    module = _import_manage_service_bindings(
        import_cd_module,
        _manage_flows_module(),
        types.SimpleNamespace(list_root_pg_controller_services=lambda: [_controller_service_entity("CSV Reader", "root-cs-1", "root")]),
    )

    changed = module.reconcile_service_bindings(
        {
            "name": "Example Flow",
            "start": True,
            "service_bindings": [{"target": "Parse Records", "properties": {"Record Reader": "CSV Reader"}}],
        },
        [{"name": "CSV Reader", "type": "org.apache.nifi.json.JsonTreeReader"}],
        runtime_url="https://example.invalid/nifi-api",
        nifi_pat="token",
    )

    assert changed is False


def test_describe_flow_service_bindings_hides_unmanaged_uuid(fake_nipyapi, import_cd_module):
    lookup_service = _controller_service_entity(
        name="Lookup",
        entity_id="cs-1",
        parent_group_id="flow-pg",
        properties={"Record Reader": "4f9b8f63-702a-4f8e-bc4d-cbd8b1f67cbb"},
        descriptors={"Record Reader": _descriptor()},
        validation_status="INVALID",
        validation_errors=["Controller service is missing"],
    )
    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_processors"] = lambda **kwargs: types.SimpleNamespace(processors=[])
    fake_nipyapi.api_methods["FlowApi"]["get_controller_services_from_group"] = lambda **kwargs: types.SimpleNamespace(controller_services=[lookup_service])
    fake_nipyapi.api_methods["ControllerServicesApi"]["get_controller_service"] = lambda **kwargs: lookup_service
    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_process_group"] = lambda **kwargs: _flow_pg()

    module = _import_manage_service_bindings(
        import_cd_module,
        _manage_flows_module(),
        types.SimpleNamespace(list_root_pg_controller_services=lambda: []),
    )

    described = module.describe_flow_service_bindings(
        "flow-pg",
        "Example Flow",
        [{"target": "Lookup", "properties": {"Record Reader": "CSV Reader"}}],
        {},
    )

    assert described[0]["validation_status"] == "INVALID"
    assert described[0]["properties"]["Record Reader"]["warning"] == "Unmanaged controller service reference"
    assert "4f9b8f63" not in str(described)


def test_describe_flow_service_bindings_fails_closed_and_sanitizes_api_errors(fake_nipyapi, import_cd_module):
    lookup_service = _controller_service_entity(
        name="Lookup",
        entity_id="cs-1",
        parent_group_id="flow-pg",
        properties={"Record Reader": "root-cs-1"},
        descriptors={"Record Reader": _descriptor()},
    )
    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_processors"] = lambda **kwargs: types.SimpleNamespace(processors=[])
    fake_nipyapi.api_methods["FlowApi"]["get_controller_services_from_group"] = lambda **kwargs: types.SimpleNamespace(controller_services=[lookup_service])
    fake_nipyapi.api_methods["ControllerServicesApi"]["get_controller_service"] = lambda **kwargs: (_ for _ in ()).throw(
        _ApiException(403, "Forbidden", headers={"Set-Cookie": "secret"}, body="stack trace")
    )
    fake_nipyapi.api_methods["ProcessGroupsApi"]["get_process_group"] = lambda **kwargs: _flow_pg()

    module = _import_manage_service_bindings(
        import_cd_module,
        _manage_flows_module(),
        types.SimpleNamespace(list_root_pg_controller_services=lambda: []),
    )

    described = module.describe_flow_service_bindings(
        "flow-pg",
        "Example Flow",
        [{"target": "Lookup", "properties": {"Record Reader": "CSV Reader"}}],
        {},
    )

    assert described[0]["state"] == "unknown"
    assert described[0]["issues"] == ["HTTP 403 Forbidden"]
    assert "Set-Cookie" not in str(described)
    assert "stack trace" not in str(described)