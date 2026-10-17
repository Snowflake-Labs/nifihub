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

#!/usr/bin/env python3
"""Translate a live NiFi diff to the resource changes consumed by the orchestrator."""
import json
import sys


def _translate_nifi_changes(nifi_diff):
    if not nifi_diff or nifi_diff.get("error"):
        return {
            "flow_changes": {"created": [], "modified": [], "deleted": []},
            "controller_service_changes": {"created": [], "modified": [], "deleted": []},
            "root_pg_controller_service_changes": {"created": [], "modified": [], "deleted": []},
            "parameter_provider_changes": {"created": [], "modified": [], "deleted": []},
            "root_parameter_context_changes": {
                "created": [], "modified": [], "deleted": [], "health": [], "blocked": [], "unchanged": [],
                "inheritance": {"missing": [], "unchanged": [], "unmatched_pattern": False},
                "parameters": {"changes": {}, "unchanged": []},
                "context": {},
            },
            "service_binding_changes": {"created": [], "modified": [], "deleted": [], "health": [], "blocked": []},
            "flow_registries_changed": False,
        }

    cs_diff = nifi_diff.get("controller_services", {})
    root_pg_cs_diff = nifi_diff.get("root_pg_controller_services", {})
    pp_diff = nifi_diff.get("parameter_providers", {})
    reg_diff = nifi_diff.get("flow_registries", {})
    flow_diff = nifi_diff.get("flows", {})

    flow_registries_changed = bool(
        reg_diff.get("created") or reg_diff.get("modified") or reg_diff.get("deleted")
    )

    flow_changes = {
        "created": flow_diff.get("created", []),
        "modified": [{"old": m.get("live", {}), "new": m.get("desired", {})}
                     for m in flow_diff.get("modified", [])],
        "deleted": flow_diff.get("deleted", []),
    }

    cs_changes = {
        "created": cs_diff.get("created", []),
        "modified": [{"old": m, "new": m} for m in cs_diff.get("modified", [])],
        "deleted": cs_diff.get("deleted", []),
    }

    root_pg_cs_changes = {
        "created": root_pg_cs_diff.get("created", []),
        "modified": [{"old": m, "new": m} for m in root_pg_cs_diff.get("modified", [])],
        "deleted": root_pg_cs_diff.get("deleted", []),
    }

    pp_changes = {
        "created": pp_diff.get("created", []),
        "modified": [{"old": m, "new": m} for m in pp_diff.get("modified", [])],
        "deleted": pp_diff.get("deleted", []),
    }

    service_binding_diff = nifi_diff.get("service_bindings", {})
    service_binding_changes = {
        "created": service_binding_diff.get("created", []),
        "modified": service_binding_diff.get("modified", []),
        "deleted": service_binding_diff.get("deleted", []),
        "health": service_binding_diff.get("health", []),
        "blocked": service_binding_diff.get("blocked", []),
    }
    root_parameter_context_diff = nifi_diff.get("root_parameter_context", {})
    root_parameter_context_changes = {
        "created": root_parameter_context_diff.get("created", []),
        "modified": root_parameter_context_diff.get("modified", []),
        "deleted": root_parameter_context_diff.get("deleted", []),
        "health": root_parameter_context_diff.get("health", []),
        "blocked": root_parameter_context_diff.get("blocked", []),
        "unchanged": root_parameter_context_diff.get("unchanged", []),
        "root_process_group_id": root_parameter_context_diff.get("root_process_group_id"),
        "context": root_parameter_context_diff.get("context", {}),
        "inheritance": root_parameter_context_diff.get("inheritance", {"missing": [], "unchanged": [], "unmatched_pattern": False}),
        "parameters": root_parameter_context_diff.get("parameters", {"changes": {}, "unchanged": []}),
    }

    return {
        "flow_changes": flow_changes,
        "controller_service_changes": cs_changes,
        "root_pg_controller_service_changes": root_pg_cs_changes,
        "parameter_provider_changes": pp_changes,
        "root_parameter_context_changes": root_parameter_context_changes,
        "service_binding_changes": service_binding_changes,
        "flow_registries_changed": flow_registries_changed,
        "flow_registry_changes": {
            "created": reg_diff.get("created", []),
            "deleted": reg_diff.get("deleted", []),
        },
    }


def translate(live_diff):
    changes = {"runtimes": {"modified": []}}
    runtimes = live_diff.get("runtimes", {})
    for runtime in runtimes.get("to_modify", []) + runtimes.get("unchanged", []):
        nifi = runtime.get("diff", {}).get("nifi", {})
        if nifi.get("error"):
            raise RuntimeError(f"Cannot read NiFi runtime '{runtime['name']}'; refusing to apply changes.")
        if nifi.get("skipped"):
            continue
        changes["runtimes"]["modified"].append({
            "name": runtime["name"],
            "new": runtime["desired"],
            **_translate_nifi_changes(nifi),
        })
    return changes


def main():
    if len(sys.argv) < 2:
        print("Usage: translate_live_diff.py <live-diff.json>", file=sys.stderr)
        sys.exit(1)

    with open(sys.argv[1]) as f:
        live_diff = json.load(f)

    result = translate(live_diff)
    json.dump(result, sys.stdout, indent=2)
    print()


if __name__ == "__main__":
    main()
