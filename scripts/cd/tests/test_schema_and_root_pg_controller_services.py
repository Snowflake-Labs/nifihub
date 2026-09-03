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

    def create_controller_service(**kwargs):
        captured["body"] = kwargs["body"]
        return types.SimpleNamespace(id="root-cs-id")

    fake_nipyapi.api_methods["ProcessGroupsApi"]["create_controller_service"] = create_controller_service
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


def test_root_pg_controller_service_creation_omits_bundle_when_not_declared(fake_nipyapi, import_cd_module):
    captured = {}

    def create_controller_service(**kwargs):
        captured["body"] = kwargs["body"]
        return types.SimpleNamespace(id="root-cs-id")

    fake_nipyapi.api_methods["ProcessGroupsApi"]["create_controller_service"] = create_controller_service
    manage_flows = types.SimpleNamespace(configure_nifi=lambda *args, **kwargs: None)
    module = import_cd_module("manage_controller_services", {"manage_flows": manage_flows})

    module._create_root_pg({
        "name": "Shared Reader",
        "type": "org.apache.nifi.json.JsonTreeReader",
        "properties": {},
    })

    assert getattr(captured["body"].component, "bundle", None) is None