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
import json
import sys

import yaml


def load_yaml(path):
    if not path or path == "":
        return {"deployments": []}
    try:
        with open(path) as f:
            data = yaml.safe_load(f)
        return data if data else {"deployments": []}
    except FileNotFoundError:
        return {"deployments": []}


def index_by_name(items):
    return {item["name"]: item for item in (items or [])}


def diff_lists(old_items, new_items):
    old_idx = index_by_name(old_items)
    new_idx = index_by_name(new_items)
    created = [v for k, v in new_idx.items() if k not in old_idx]
    deleted = [v for k, v in old_idx.items() if k not in new_idx]
    modified = []
    for name in old_idx:
        if name in new_idx and old_idx[name] != new_idx[name]:
            modified.append({"old": old_idx[name], "new": new_idx[name]})
    return created, modified, deleted


def diff_runtimes(old_runtimes, new_runtimes):
    created, modified, deleted = diff_lists(old_runtimes, new_runtimes)
    results = {"created": created, "modified": [], "deleted": deleted}
    for mod in modified:
        old_rt = mod["old"]
        new_rt = mod["new"]
        changed_fields = {}
        for key in set(list(old_rt.keys()) + list(new_rt.keys())):
            if key in ("flows", "network_rules", "flow_registries", "controller_services", "root_pg_controller_services", "parameter_providers", "connectors"):
                continue
            if old_rt.get(key) != new_rt.get(key):
                changed_fields[key] = {"old": old_rt.get(key), "new": new_rt.get(key)}

        old_flows = old_rt.get("flows", [])
        new_flows = new_rt.get("flows", [])
        flow_changes = diff_flows(old_flows, new_flows)

        old_nr = old_rt.get("network_rules", [])
        new_nr = new_rt.get("network_rules", [])
        nr_created, nr_modified, nr_deleted = diff_lists(old_nr, new_nr)

        old_frs = old_rt.get("flow_registries", [])
        new_frs = new_rt.get("flow_registries", [])
        flow_registries_changed = old_frs != new_frs

        old_cs = old_rt.get("controller_services", [])
        new_cs = new_rt.get("controller_services", [])
        controller_service_changes = diff_controller_services(old_cs, new_cs)

        old_root_pg_cs = old_rt.get("root_pg_controller_services", [])
        new_root_pg_cs = new_rt.get("root_pg_controller_services", [])
        root_pg_controller_service_changes = diff_controller_services(old_root_pg_cs, new_root_pg_cs)

        old_pp = old_rt.get("parameter_providers", [])
        new_pp = new_rt.get("parameter_providers", [])
        parameter_provider_changes = diff_controller_services(old_pp, new_pp)

        old_conn = old_rt.get("connectors", [])
        new_conn = new_rt.get("connectors", [])
        connector_changes = diff_controller_services(old_conn, new_conn)

        results["modified"].append({
            "name": new_rt["name"],
            "old": old_rt,
            "new": new_rt,
            "changed_fields": changed_fields,
            "flow_changes": flow_changes,
            "controller_service_changes": controller_service_changes,
            "root_pg_controller_service_changes": root_pg_controller_service_changes,
            "parameter_provider_changes": parameter_provider_changes,
            "connector_changes": connector_changes,
            "network_rule_changes": {
                "created": nr_created,
                "modified": [{"old": m["old"], "new": m["new"]} for m in nr_modified],
                "deleted": nr_deleted,
            },
            "flow_registries_changed": flow_registries_changed,
        })
    return results


def diff_controller_services(old_services, new_services):
    old_idx = {s["name"]: s for s in (old_services or [])}
    new_idx = {s["name"]: s for s in (new_services or [])}
    created = [v for k, v in new_idx.items() if k not in old_idx]
    deleted = [v for k, v in old_idx.items() if k not in new_idx]
    modified = []
    for name in old_idx:
        if name in new_idx and old_idx[name] != new_idx[name]:
            modified.append({"old": old_idx[name], "new": new_idx[name]})
    return {"created": created, "modified": modified, "deleted": deleted}


def diff_flows(old_flows, new_flows):
    def flow_key(f):
        return f["name"]

    old_idx = {flow_key(f): f for f in (old_flows or [])}
    new_idx = {flow_key(f): f for f in (new_flows or [])}
    created = [v for k, v in new_idx.items() if k not in old_idx]
    deleted = [v for k, v in old_idx.items() if k not in new_idx]
    modified = []
    for key in old_idx:
        if key in new_idx and old_idx[key] != new_idx[key]:
            modified.append({"old": old_idx[key], "new": new_idx[key]})
    return {"created": created, "modified": modified, "deleted": deleted}


def diff_environments(old_path, new_path):
    old_cfg = load_yaml(old_path)
    new_cfg = load_yaml(new_path)

    old_deployments = old_cfg.get("deployments", [])
    new_deployments = new_cfg.get("deployments", [])

    dep_created, dep_modified, dep_deleted = diff_lists(old_deployments, new_deployments)

    changes = {
        "account": new_cfg.get("account", old_cfg.get("account", {})),
        "deployments": {
            "created": [],
            "modified": [],
            "deleted": [],
        },
    }

    for dep in dep_created:
        entry = {**dep, "runtimes_to_create": dep.get("runtimes", [])}
        entry.pop("runtimes", None)
        changes["deployments"]["created"].append(entry)

    for dep in dep_deleted:
        entry = {**dep, "runtimes_to_delete": dep.get("runtimes", [])}
        entry.pop("runtimes", None)
        changes["deployments"]["deleted"].append(entry)

    for mod in dep_modified:
        old_dep = mod["old"]
        new_dep = mod["new"]

        changed_fields = {}
        for key in ("display_name", "comment"):
            if old_dep.get(key) != new_dep.get(key):
                changed_fields[key] = {"old": old_dep.get(key), "new": new_dep.get(key)}

        runtime_changes = diff_runtimes(
            old_dep.get("runtimes", []),
            new_dep.get("runtimes", []),
        )

        changes["deployments"]["modified"].append({
            "name": new_dep["name"],
            "changed_fields": changed_fields,
            "runtime_changes": runtime_changes,
        })

    return changes


def main():
    if len(sys.argv) < 3:
        print("Usage: diff_environments.py <old.yaml> <new.yaml>", file=sys.stderr)
        sys.exit(1)

    old_path = sys.argv[1]
    new_path = sys.argv[2]
    changes = diff_environments(old_path, new_path)
    json.dump(changes, sys.stdout, indent=2)


if __name__ == "__main__":
    main()