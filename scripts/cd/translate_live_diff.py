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
"""Translate diff_live.py output into the changes.json format that orchestrate.py expects."""
import json
import sys


def _remap_changed_fields(fields):
    return {k: {"old": v["live"], "new": v["desired"]} for k, v in (fields or {}).items()}


def _translate_nr_modifications(nr_mods):
    result = []
    for nr_mod in (nr_mods or []):
        result.append({"old": nr_mod.get("old", {}), "new": nr_mod.get("new", {})})
    return result


def _translate_connector_changes(conn_diff):
    if not conn_diff:
        return {"created": [], "modified": [], "deleted": []}
    return {
        "created": conn_diff.get("created", []),
        "modified": [{"old": m.get("live", {}), "new": m.get("desired", {})}
                     for m in conn_diff.get("modified", [])],
        "deleted": conn_diff.get("deleted", []),
    }


def _translate_nifi_changes(nifi_diff):
    if not nifi_diff or nifi_diff.get("error"):
        return {
            "flow_changes": {"created": [], "modified": [], "deleted": []},
            "controller_service_changes": {"created": [], "modified": [], "deleted": []},
            "parameter_provider_changes": {"created": [], "modified": [], "deleted": []},
            "flow_registries_changed": False,
        }

    cs_diff = nifi_diff.get("controller_services", {})
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

    pp_changes = {
        "created": pp_diff.get("created", []),
        "modified": [{"old": m, "new": m} for m in pp_diff.get("modified", [])],
        "deleted": pp_diff.get("deleted", []),
    }

    return {
        "flow_changes": flow_changes,
        "controller_service_changes": cs_changes,
        "parameter_provider_changes": pp_changes,
        "flow_registries_changed": flow_registries_changed,
        "flow_registry_changes": {
            "created": reg_diff.get("created", []),
            "deleted": reg_diff.get("deleted", []),
        },
    }


def _translate_runtime_mod(rt_mod):
    desired = rt_mod["desired"]
    live = rt_mod.get("live", {})
    diff = rt_mod.get("diff", {})

    changed_fields = _remap_changed_fields(diff.get("changed_fields", {}))

    nr_changes = diff.get("network_rule_changes", {})
    network_rule_changes = {
        "created": nr_changes.get("created", []),
        "modified": _translate_nr_modifications(nr_changes.get("modified", [])),
        "deleted": nr_changes.get("deleted", []),
    }

    connector_changes = _translate_connector_changes(diff.get("connector_changes"))
    nifi_changes = _translate_nifi_changes(diff.get("nifi"))

    return {
        "name": desired["name"],
        "old": desired,
        "new": desired,
        "changed_fields": changed_fields,
        "network_rule_changes": network_rule_changes,
        "connector_changes": connector_changes,
        **nifi_changes,
    }


def translate(live_diff):
    changes = {
        "account": live_diff.get("account", {}),
        "deployments": {"created": [], "modified": [], "deleted": []},
    }

    deps = live_diff.get("deployments", {})

    for dep in deps.get("to_create", []):
        entry = dict(dep)
        entry["runtimes_to_create"] = entry.pop("runtimes", [])
        changes["deployments"]["created"].append(entry)

    for dep in deps.get("to_modify", []):
        changed_fields = _remap_changed_fields(dep.get("dep_changes", {}))

        rt_info = dep.get("runtimes", {})
        # url_managed runtimes are always reconciled (no SOM state to diff — NiFi
        # content is applied on every run via apply_runtime_create non-SOM path).
        url_managed = rt_info.get("url_managed", [])
        runtime_changes = {
            "created": rt_info.get("to_create", []) + url_managed,
            "modified": [_translate_runtime_mod(m) for m in rt_info.get("to_modify", [])],
            "deleted": rt_info.get("to_delete", []),
        }

        changes["deployments"]["modified"].append({
            "name": dep["name"],
            "changed_fields": changed_fields,
            "runtime_changes": runtime_changes,
        })

    for dep in deps.get("unchanged", []):
        rt_info = dep.get("runtimes", {})
        url_managed = rt_info.get("url_managed", [])
        has_rt_changes = (
            rt_info.get("to_create")
            or rt_info.get("to_modify")
            or rt_info.get("to_delete")
            or url_managed
        )
        if has_rt_changes:
            runtime_changes = {
                "created": rt_info.get("to_create", []) + url_managed,
                "modified": [_translate_runtime_mod(m) for m in rt_info.get("to_modify", [])],
                "deleted": rt_info.get("to_delete", []),
            }
            changes["deployments"]["modified"].append({
                "name": dep["name"],
                "changed_fields": {},
                "runtime_changes": runtime_changes,
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