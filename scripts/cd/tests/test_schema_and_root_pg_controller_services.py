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

import json
import os
import types

import pytest
from jsonschema import Draft7Validator


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
SCHEMA_PATH = os.path.join(REPO_ROOT, "environments", "schema.json")


def _base_config():
    return {
        "account": {"name": "example", "github_environment": "example"},
        "deployments": [{
            "name": "MY_DEPLOYMENT",
            "deployment_type": "SNOWFLAKE",
            "runtimes": [{
                "name": "MY_RUNTIME",
                "database": "OPENFLOW",
                "schema": "OPENFLOW",
                "flows": [{
                    "name": "Example Flow",
                    "bucket": "examples",
                    "flow": "hello-world",
                    "version": "latest",
                }],
            }],
        }],
    }


def _validate(config):
    with open(SCHEMA_PATH) as handle:
        schema = json.load(handle)
    validator = Draft7Validator(schema)
    return [error.message for error in validator.iter_errors(config)]


def _controller_service(name, state="DISABLED", properties=None, revision_version=0, entity_id="cs-1"):
    return types.SimpleNamespace(
        id=entity_id,
        component=types.SimpleNamespace(name=name, state=state, properties=dict(properties or {})),
        revision=types.SimpleNamespace(version=revision_version),
    )


def test_existing_flow_shape_remains_valid():
    assert _validate(_base_config()) == []


def test_service_bindings_require_explicit_start():
    config = _base_config()
    config["deployments"][0]["runtimes"][0]["flows"][0]["service_bindings"] = [{
        "target": "Parse Records",
        "properties": {"Record Reader": "Shared Reader"},
    }]
    errors = _validate(config)
    assert any("'start' is a required property" in message for message in errors)


def test_service_bindings_allow_optional_bundle_and_null_removal():
    config = _base_config()
    runtime = config["deployments"][0]["runtimes"][0]
    runtime["root_pg_controller_services"] = [{
        "name": "Shared Reader",
        "type": "org.apache.nifi.json.JsonTreeReader",
        "bundle": {
            "group": "org.apache.nifi",
            "artifact": "nifi-record-serialization-services-nar",
            "version": "2.11.0",
        },
    }]
    flow = runtime["flows"][0]
    flow["start"] = True
    flow["service_bindings"] = [{
        "target": "Parse Records",
        "properties": {
            "Record Reader": "Shared Reader",
            "Obsolete Reader": None,
        },
    }]
    assert _validate(config) == []


def test_service_bindings_reject_non_string_values():
    config = _base_config()
    flow = config["deployments"][0]["runtimes"][0]["flows"][0]
    flow["start"] = True
    flow["service_bindings"] = [{
        "target": "Parse Records",
        "properties": {"Record Reader": ["bad"]},
    }]
    errors = _validate(config)
    assert any("is not valid under any of the given schemas" in message or "is not of type 'string', 'null'" in message for message in errors)


def test_service_bindings_reject_empty_string_service_name():
    config = _base_config()
    flow = config["deployments"][0]["runtimes"][0]["flows"][0]
    flow["start"] = True
    flow["service_bindings"] = [{
        "target": "Parse Records",
        "properties": {"Record Reader": ""},
    }]
    errors = _validate(config)
    assert any("should be non-empty" in message or "is not valid under any of the given schemas" in message for message in errors)


def test_controller_service_bundle_must_be_complete():
    config = _base_config()
    runtime = config["deployments"][0]["runtimes"][0]
    runtime["root_pg_controller_services"] = [{
        "name": "Shared Reader",
        "type": "org.apache.nifi.json.JsonTreeReader",
        "bundle": {"group": "org.apache.nifi", "artifact": "nifi-record-serialization-services-nar"},
    }]
    errors = _validate(config)
    assert any("'version' is a required property" in message for message in errors)


def test_root_pg_controller_service_creation_includes_bundle(fake_nipyapi, import_cd_module):
    captured = {}

    def create_controller_service1(**kwargs):
        captured["body"] = kwargs["body"]
        captured["id"] = kwargs["id"]
        return types.SimpleNamespace(id="root-cs-id")

    fake_nipyapi.api_methods["ProcessGroupsApi"]["create_controller_service1"] = create_controller_service1
    manage_flows = types.SimpleNamespace(configure_nifi=lambda *args, **kwargs: None)
    module = import_cd_module("manage_controller_services", {"manage_flows": manage_flows})

    module._create_root_pg({
        "name": "Shared Reader",
        "type": "org.apache.nifi.json.JsonTreeReader",
        "bundle": {
            "group": "org.apache.nifi",
            "artifact": "nifi-record-serialization-services-nar",
            "version": "2.11.0",
        },
        "properties": {"Schema Access Strategy": "inherit-record-schema"},
    })

    bundle = captured["body"].component.bundle
    assert bundle.group == "org.apache.nifi"
    assert bundle.artifact == "nifi-record-serialization-services-nar"
    assert bundle.version == "2.11.0"
    assert captured["id"] == "root"


def test_root_pg_controller_service_creation_supports_legacy_process_group_method(fake_nipyapi, import_cd_module):
    captured = {}

    def create_controller_service(**kwargs):
        captured["body"] = kwargs["body"]
        captured["id"] = kwargs["id"]
        return types.SimpleNamespace(id="root-cs-id")

    fake_nipyapi.api_methods["ProcessGroupsApi"]["create_controller_service"] = create_controller_service
    manage_flows = types.SimpleNamespace(configure_nifi=lambda *args, **kwargs: None)
    module = import_cd_module("manage_controller_services", {"manage_flows": manage_flows})

    module._create_root_pg({
        "name": "Shared Reader",
        "type": "org.apache.nifi.json.JsonTreeReader",
        "properties": {},
    })

    assert captured["id"] == "root"
    assert captured["body"].component.name == "Shared Reader"


def test_root_pg_controller_service_creation_omits_bundle_when_not_declared(fake_nipyapi, import_cd_module):
    captured = {}

    def create_controller_service1(**kwargs):
        captured["body"] = kwargs["body"]
        return types.SimpleNamespace(id="root-cs-id")

    fake_nipyapi.api_methods["ProcessGroupsApi"]["create_controller_service1"] = create_controller_service1
    manage_flows = types.SimpleNamespace(configure_nifi=lambda *args, **kwargs: None)
    module = import_cd_module("manage_controller_services", {"manage_flows": manage_flows})

    module._create_root_pg({
        "name": "Shared Reader",
        "type": "org.apache.nifi.json.JsonTreeReader",
        "properties": {},
    })

    assert getattr(captured["body"].component, "bundle", None) is None


def test_root_pg_controller_service_creation_fails_clearly_when_no_supported_method_exists(fake_nipyapi, import_cd_module):
    manage_flows = types.SimpleNamespace(configure_nifi=lambda *args, **kwargs: None)
    module = import_cd_module("manage_controller_services", {"manage_flows": manage_flows})

    with pytest.raises(
        AttributeError,
        match=r"ProcessGroupsApi does not provide a supported root PG controller service creation method \(create_controller_service1, create_controller_service\)",
    ):
        module._create_root_pg({
            "name": "Shared Reader",
            "type": "org.apache.nifi.json.JsonTreeReader",
            "properties": {},
        })


def test_set_state_prefers_update_run_status2_when_both_methods_exist(fake_nipyapi, import_cd_module):
    calls = []
    runtime_state = {"service": _controller_service("Shared Reader", state="DISABLED")}

    def update_run_status2(**kwargs):
        calls.append(f"update_run_status2:{kwargs['body'].state}")
        runtime_state["service"] = _controller_service("Shared Reader", state=kwargs["body"].state, revision_version=1)
        return runtime_state["service"]

    def update_run_status1(**kwargs):
        calls.append(f"update_run_status1:{kwargs['body'].state}")
        runtime_state["service"] = _controller_service("Shared Reader", state=kwargs["body"].state, revision_version=1)
        return runtime_state["service"]

    fake_nipyapi.api_methods["FlowApi"]["get_controller_services_from_controller"] = lambda **kwargs: types.SimpleNamespace(
        controller_services=[runtime_state["service"]]
    )
    fake_nipyapi.api_methods["ControllerServicesApi"]["update_run_status2"] = update_run_status2
    fake_nipyapi.api_methods["ControllerServicesApi"]["update_run_status1"] = update_run_status1
    manage_flows = types.SimpleNamespace(configure_nifi=lambda *args, **kwargs: None)
    module = import_cd_module("manage_controller_services", {"manage_flows": manage_flows})

    updated = module._set_state(runtime_state["service"], "ENABLED")

    assert calls == ["update_run_status2:ENABLED"]
    assert updated.component.state == "ENABLED"


def test_set_state_falls_back_to_update_run_status1(fake_nipyapi, import_cd_module):
    calls = []
    runtime_state = {"service": _controller_service("Shared Reader", state="DISABLED")}

    def update_run_status1(**kwargs):
        calls.append(kwargs["body"].state)
        runtime_state["service"] = _controller_service("Shared Reader", state=kwargs["body"].state, revision_version=1)
        return runtime_state["service"]

    fake_nipyapi.api_methods["FlowApi"]["get_controller_services_from_controller"] = lambda **kwargs: types.SimpleNamespace(
        controller_services=[runtime_state["service"]]
    )
    fake_nipyapi.api_methods["ControllerServicesApi"]["update_run_status1"] = update_run_status1
    manage_flows = types.SimpleNamespace(configure_nifi=lambda *args, **kwargs: None)
    module = import_cd_module("manage_controller_services", {"manage_flows": manage_flows})

    updated = module._set_state(runtime_state["service"], "ENABLED")

    assert calls == ["ENABLED"]
    assert updated.component.state == "ENABLED"


def test_set_state_fails_clearly_when_no_supported_run_status_method_exists(fake_nipyapi, import_cd_module):
    fake_nipyapi.api_methods["FlowApi"]["get_controller_services_from_controller"] = lambda **kwargs: types.SimpleNamespace(
        controller_services=[_controller_service("Shared Reader", state="DISABLED")]
    )
    manage_flows = types.SimpleNamespace(configure_nifi=lambda *args, **kwargs: None)
    module = import_cd_module("manage_controller_services", {"manage_flows": manage_flows})

    with pytest.raises(
        AttributeError,
        match=r"ControllerServicesApi does not provide a supported controller service run-status update method \(update_run_status2, update_run_status1\)",
    ):
        module._set_state(_controller_service("Shared Reader", state="DISABLED"), "ENABLED")


def test_reconcile_root_pg_controller_services_create_then_enable_uses_current_run_status_method(fake_nipyapi, import_cd_module):
    calls = []
    runtime_state = {"service": None}

    def get_controller_services_from_group(**kwargs):
        services = [] if runtime_state["service"] is None else [runtime_state["service"]]
        return types.SimpleNamespace(controller_services=services)

    def create_controller_service1(**kwargs):
        calls.append("create_controller_service1")
        runtime_state["service"] = _controller_service("Shared Reader", state="DISABLED", entity_id="root-cs-id")
        return runtime_state["service"]

    def update_run_status2(**kwargs):
        calls.append(f"update_run_status2:{kwargs['body'].state}")
        runtime_state["service"] = _controller_service(
            "Shared Reader",
            state=kwargs["body"].state,
            entity_id="root-cs-id",
            revision_version=1,
        )
        return runtime_state["service"]

    fake_nipyapi.api_methods["FlowApi"]["get_controller_services_from_group"] = get_controller_services_from_group
    fake_nipyapi.api_methods["ProcessGroupsApi"]["create_controller_service1"] = create_controller_service1
    fake_nipyapi.api_methods["ControllerServicesApi"]["update_run_status2"] = update_run_status2
    manage_flows = types.SimpleNamespace(configure_nifi=lambda *args, **kwargs: None)
    module = import_cd_module("manage_controller_services", {"manage_flows": manage_flows})

    module.reconcile_root_pg_controller_services(
        [{"name": "Shared Reader", "type": "org.apache.nifi.json.JsonTreeReader", "properties": {}}],
        runtime_url="https://example.invalid/nifi-api",
        nifi_pat="token",
    )

    assert calls == ["create_controller_service1", "update_run_status2:ENABLED"]
    assert runtime_state["service"].component.state == "ENABLED"


def test_reconcile_controller_services_existing_service_disable_update_enable_uses_legacy_run_status_method(fake_nipyapi, import_cd_module):
    calls = []
    runtime_state = {
        "service": _controller_service("Lookup", state="ENABLED", properties={"Record Reader": "old"}),
    }

    def update_run_status1(**kwargs):
        calls.append(f"update_run_status1:{kwargs['body'].state}")
        runtime_state["service"] = _controller_service(
            "Lookup",
            state=kwargs["body"].state,
            properties=runtime_state["service"].component.properties,
            revision_version=runtime_state["service"].revision.version + 1,
        )
        return runtime_state["service"]

    def update_controller_service(**kwargs):
        calls.append("update_controller_service")
        runtime_state["service"] = _controller_service(
            "Lookup",
            state="DISABLED",
            properties=kwargs["body"].component.properties,
            revision_version=runtime_state["service"].revision.version + 1,
        )
        return runtime_state["service"]

    fake_nipyapi.api_methods["FlowApi"]["get_controller_services_from_controller"] = lambda **kwargs: types.SimpleNamespace(
        controller_services=[runtime_state["service"]]
    )
    fake_nipyapi.api_methods["ControllerServicesApi"]["update_run_status1"] = update_run_status1
    fake_nipyapi.api_methods["ControllerServicesApi"]["update_controller_service"] = update_controller_service
    manage_flows = types.SimpleNamespace(configure_nifi=lambda *args, **kwargs: None)
    module = import_cd_module("manage_controller_services", {"manage_flows": manage_flows})

    module.reconcile_controller_services(
        [{"name": "Lookup", "type": "org.apache.nifi.lookup.RecordSetWriterLookup", "properties": {"Record Reader": "new"}}],
        runtime_url="https://example.invalid/nifi-api",
        nifi_pat="token",
    )

    assert calls == ["update_run_status1:DISABLED", "update_controller_service", "update_run_status1:ENABLED"]
    assert runtime_state["service"].component.properties == {"Record Reader": "new"}
    assert runtime_state["service"].component.state == "ENABLED"


def test_reconcile_controller_services_existing_service_disable_update_enable_uses_current_run_status_method(fake_nipyapi, import_cd_module):
    calls = []
    runtime_state = {
        "service": _controller_service("Lookup", state="ENABLED", properties={"Record Reader": "old"}),
    }

    def update_run_status2(**kwargs):
        calls.append(f"update_run_status2:{kwargs['body'].state}")
        runtime_state["service"] = _controller_service(
            "Lookup",
            state=kwargs["body"].state,
            properties=runtime_state["service"].component.properties,
            revision_version=runtime_state["service"].revision.version + 1,
        )
        return runtime_state["service"]

    def update_controller_service(**kwargs):
        calls.append("update_controller_service")
        runtime_state["service"] = _controller_service(
            "Lookup",
            state="DISABLED",
            properties=kwargs["body"].component.properties,
            revision_version=runtime_state["service"].revision.version + 1,
        )
        return runtime_state["service"]

    fake_nipyapi.api_methods["FlowApi"]["get_controller_services_from_controller"] = lambda **kwargs: types.SimpleNamespace(
        controller_services=[runtime_state["service"]]
    )
    fake_nipyapi.api_methods["ControllerServicesApi"]["update_run_status2"] = update_run_status2
    fake_nipyapi.api_methods["ControllerServicesApi"]["update_controller_service"] = update_controller_service
    manage_flows = types.SimpleNamespace(configure_nifi=lambda *args, **kwargs: None)
    module = import_cd_module("manage_controller_services", {"manage_flows": manage_flows})

    module.reconcile_controller_services(
        [{"name": "Lookup", "type": "org.apache.nifi.lookup.RecordSetWriterLookup", "properties": {"Record Reader": "new"}}],
        runtime_url="https://example.invalid/nifi-api",
        nifi_pat="token",
    )

    assert calls == ["update_run_status2:DISABLED", "update_controller_service", "update_run_status2:ENABLED"]
    assert runtime_state["service"].component.properties == {"Record Reader": "new"}
    assert runtime_state["service"].component.state == "ENABLED"


def test_real_process_group_controller_service_creation_surface_supports_body_and_id(real_nipyapi_method_names):
    process_group_methods = real_nipyapi_method_names.get("ProcessGroupsApi", {})
    supported = {
        name: signature
        for name, signature in process_group_methods.items()
        if name in {"create_controller_service1", "create_controller_service"}
    }
    if not process_group_methods:
        return
    assert supported
    for signature in supported.values():
        assert "body" in signature.parameters
        assert "id" in signature.parameters


def test_real_manage_controller_services_surface_supports_expected_methods(real_nipyapi_method_names):
    if not real_nipyapi_method_names:
        return

    flow_methods = real_nipyapi_method_names.get("FlowApi", {})
    assert "get_controller_services_from_controller" in flow_methods
    assert "get_controller_services_from_group" in flow_methods
    assert "id" in flow_methods["get_controller_services_from_group"].parameters

    controller_api_methods = real_nipyapi_method_names.get("ControllerApi", {})
    assert "create_controller_service" in controller_api_methods
    assert "body" in controller_api_methods["create_controller_service"].parameters

    controller_service_methods = real_nipyapi_method_names.get("ControllerServicesApi", {})
    assert "get_controller_service" in controller_service_methods
    assert "update_controller_service" in controller_service_methods
    assert "remove_controller_service" in controller_service_methods
    assert "update_run_status2" in controller_service_methods
    assert "id" in controller_service_methods["get_controller_service"].parameters
    assert "id" in controller_service_methods["remove_controller_service"].parameters
    assert "body" in controller_service_methods["update_controller_service"].parameters
    assert "id" in controller_service_methods["update_controller_service"].parameters
    assert "body" in controller_service_methods["update_run_status2"].parameters
    assert "id" in controller_service_methods["update_run_status2"].parameters
    if "update_run_status1" in controller_service_methods:
        assert "body" in controller_service_methods["update_run_status1"].parameters
        assert "id" in controller_service_methods["update_run_status1"].parameters