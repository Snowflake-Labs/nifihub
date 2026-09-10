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

from diff_live import diff_nifi_state
from format_change_plan import format_nifi_section
from translate_live_diff import translate


def test_diff_reports_binding_changes_and_health_without_uuid_leakage():
    live_nifi = {
        "controller_services": [],
        "root_pg_controller_services": [],
        "parameter_providers": [],
        "flow_registries": [],
        "flows": [{
            "name": "Example Flow",
            "registry": "nifihub",
            "bucket": "examples",
            "flow": "hello-world",
            "version": "1",
            "running": True,
            "service_bindings": [{
                "target": "Parse Records",
                "state": "known",
                "target_kind": "processor",
                "process_group_path": "Example Flow",
                "validation_status": "INVALID",
                "validation_errors": ["Controller service missing"],
                "properties": {
                    "Record Reader": {"configured": True, "service": None, "warning": "Unmanaged controller service reference"},
                    "Obsolete Reader": {"configured": True, "service": "Legacy Reader"},
                },
                "issues": [],
            }],
        }],
        "parameters": {},
    }
    desired_runtime = {
        "flows": [{
            "name": "Example Flow",
            "registry": "nifihub",
            "bucket": "examples",
            "flow": "hello-world",
            "version": "1",
            "start": True,
            "service_bindings": [{
                "target": "Parse Records",
                "properties": {
                    "Record Reader": "Shared Reader",
                    "Obsolete Reader": None,
                },
            }],
        }],
    }

    diff = diff_nifi_state(live_nifi, desired_runtime)
    bindings = diff["service_bindings"]

    assert bindings["modified"][0]["action"] == "repair"
    assert bindings["deleted"][0]["action"] == "remove"
    assert bindings["health"][0]["status"] == "INVALID"
    assert "4f9b8f63" not in str(bindings)


def test_diff_separates_health_from_start_and_blocks_unknown_live_reads():
    live_nifi = {
        "controller_services": [],
        "root_pg_controller_services": [],
        "parameter_providers": [],
        "flow_registries": [],
        "flows": [{
            "name": "Example Flow",
            "registry": "nifihub",
            "bucket": "examples",
            "flow": "hello-world",
            "version": "1",
            "running": False,
            "service_bindings": [{
                "target": "Reader | Lookup",
                "state": "unknown",
                "target_kind": "processor",
                "process_group_path": "Example Flow",
                "issues": ["HTTP 403 Forbidden"],
                "properties": {},
            }],
        }],
        "parameters": {},
    }
    desired_runtime = {
        "flows": [{
            "name": "Example Flow",
            "registry": "nifihub",
            "bucket": "examples",
            "flow": "hello-world",
            "version": "1",
            "start": True,
            "service_bindings": [{
                "target": "Reader | Lookup",
                "properties": {"Record `Reader`": "Shared|Reader"},
            }],
        }],
    }

    diff = diff_nifi_state(live_nifi, desired_runtime)

    assert diff["flows"]["modified"][0]["changes"]["start"] == {"live": False, "desired": True}
    assert diff["service_bindings"]["blocked"] == [{
        "flow": "Example Flow",
        "target": "Reader | Lookup",
        "target_kind": "processor",
        "process_group_path": "Example Flow",
        "status": "UNKNOWN",
        "messages": ["HTTP 403 Forbidden"],
    }]


def test_translate_and_render_include_service_binding_changes():
    live_diff = {
        "account": {"name": "example"},
        "deployments": {
            "unchanged": [],
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
                        "desired": {"name": "MY_RUNTIME"},
                        "diff": {
                            "changed_fields": {},
                            "network_rule_changes": {"created": [], "modified": [], "deleted": []},
                            "connector_changes": {"created": [], "modified": [], "deleted": []},
                            "nifi": {
                                "controller_services": {"created": [], "modified": [], "deleted": [], "unchanged": []},
                                "root_pg_controller_services": {"created": [], "modified": [], "deleted": [], "unchanged": []},
                                "parameter_providers": {"created": [], "modified": [], "deleted": [], "unchanged": []},
                                "flow_registries": {"created": [], "modified": [], "deleted": [], "unchanged": []},
                                "flows": {"created": [], "modified": [{
                                    "name": "Example Flow",
                                    "changes": {"start": {"live": False, "desired": True}},
                                    "live": {"running": False},
                                    "desired": {"start": True},
                                }], "deleted": [], "unchanged": []},
                                "parameters": {},
                                "service_bindings": {
                                    "created": [],
                                    "modified": [{
                                        "flow": "Example Flow",
                                        "target": "Parse Records",
                                        "target_kind": "processor",
                                        "process_group_path": "Example Flow",
                                        "property": "Record Reader",
                                        "live": "Legacy Reader",
                                        "desired": "Shared Reader",
                                        "action": "change",
                                    }],
                                    "deleted": [],
                                    "unchanged": [],
                                    "health": [{
                                        "flow": "Example Flow",
                                        "target": "Parse Records",
                                        "status": "INVALID",
                                        "messages": ["Controller service missing"],
                                    }],
                                    "blocked": [{
                                        "flow": "Example Flow",
                                        "target": "Reader | Lookup",
                                        "status": "UNKNOWN",
                                        "messages": ["HTTP 403 Forbidden", "bad | markdown `raw` <tag>"],
                                    }],
                                },
                            },
                        },
                    }],
                },
            }],
        },
    }

    translated = translate(live_diff)
    runtime_mod = translated["deployments"]["modified"][0]["runtime_changes"]["modified"][0]
    assert runtime_mod["service_binding_changes"]["modified"][0]["desired"] == "Shared Reader"
    assert runtime_mod["service_binding_changes"]["blocked"][0]["messages"][0] == "HTTP 403 Forbidden"

    rendered = format_nifi_section(live_diff["deployments"]["to_modify"][0]["runtimes"]["to_modify"][0]["diff"]["nifi"])
    assert any("Service Bindings" in line for line in rendered)
    assert any("Shared Reader" in line for line in rendered)
    assert any("start: False→True" in line for line in rendered)
    assert any("Binding health `Example Flow` / `Parse Records`" in line for line in rendered)
    assert any("Binding state unknown `Example Flow` / `Reader \\| Lookup`" in line for line in rendered)
    assert any("bad \\| markdown \\`raw\\` &lt;tag&gt;" in line for line in rendered)