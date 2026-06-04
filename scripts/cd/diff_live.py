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

import re

import yaml

from manage_parameters import resolve_value


_SECRET_RE = re.compile(r'^\$\{\{\s*secrets\.')
_UUID_RE = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE)

_AUTO_PROVISIONED_CS = {"OPENFLOW - SNOWFLAKE CONNECTION SERVICE"}
_AUTO_PROVISIONED_PP = {"OPENFLOW - SNOWFLAKE PARAMETER PROVIDER"}
_AUTO_PROVISIONED_REG = {"CONNECTORFLOWREGISTRYCLIENT"}

def _norm(val):
    if val is None:
        return ""
    if isinstance(val, str):
        return val.strip()
    return val


def _index_by_name(items):
    return {item["name"].upper(): item for item in (items or [])}


def _is_secret_ref(val):
    if not val or not isinstance(val, str):
        return False
    return bool(_SECRET_RE.match(val))


def diff_network_rules(live_rules, desired_rules):
    live_idx = _index_by_name(live_rules)
    desired_idx = _index_by_name(desired_rules)

    created = [v for k, v in desired_idx.items() if k not in live_idx]
    deleted = [v for k, v in live_idx.items() if k not in desired_idx]
    modified = []

    for name_upper, desired_nr in desired_idx.items():
        if name_upper not in live_idx:
            continue
        live_nr = live_idx[name_upper]
        live_vals = sorted(live_nr.get("values", []))
        desired_vals = sorted(desired_nr.get("values", []))
        if live_vals != desired_vals:
            modified.append({"old": live_nr, "new": desired_nr})

    return {"created": created, "modified": modified, "deleted": deleted}


def diff_nifi_controller_services(live_cs, desired_cs):
    live_idx = {cs["name"].upper(): cs for cs in (live_cs or [])}
    desired_idx = {cs["name"].upper(): cs for cs in (desired_cs or [])}

    created = [v for k, v in desired_idx.items() if k not in live_idx]
    deleted = [v for k, v in live_idx.items() if k not in desired_idx and k not in _AUTO_PROVISIONED_CS]
    modified = []
    unchanged = []

    for name_upper, desired in desired_idx.items():
        if name_upper not in live_idx:
            continue
        live = live_idx[name_upper]
        changes = {}
        if _norm(live.get("type", "")).lower() != _norm(desired.get("type", "")).lower():
            changes["type"] = {"live": live.get("type"), "desired": desired.get("type")}
        live_props = live.get("properties", {})
        desired_props = desired.get("properties", {})
        for k, v in desired_props.items():
            if _is_secret_ref(v):
                continue
            live_v = live_props.get(k, "")
            if _UUID_RE.match(str(live_v)):
                continue
            if _norm(live_v) != _norm(v):
                changes[f"property:{k}"] = {"live": live_v, "desired": v}
        if changes:
            modified.append({"name": desired["name"], "changes": changes})
        else:
            unchanged.append(desired["name"])

    return {"created": created, "modified": modified, "deleted": deleted, "unchanged": unchanged}


def diff_nifi_parameter_providers(live_pp, desired_pp):
    live_idx = {pp["name"].upper(): pp for pp in (live_pp or [])}
    desired_idx = {pp["name"].upper(): pp for pp in (desired_pp or [])}

    created = [v for k, v in desired_idx.items() if k not in live_idx]
    deleted = [v for k, v in live_idx.items() if k not in desired_idx and k not in _AUTO_PROVISIONED_PP]
    modified = []
    unchanged = []

    for name_upper, desired in desired_idx.items():
        if name_upper not in live_idx:
            continue
        live = live_idx[name_upper]
        changes = {}
        if _norm(live.get("type", "")).lower() != _norm(desired.get("type", "")).lower():
            changes["type"] = {"live": live.get("type"), "desired": desired.get("type")}
        live_props = live.get("properties", {})
        desired_props = desired.get("properties", {})
        for k, v in desired_props.items():
            if _is_secret_ref(v):
                continue
            live_v = live_props.get(k, "")
            if _UUID_RE.match(str(live_v)):
                continue
            if _norm(live_v) != _norm(v):
                changes[f"property:{k}"] = {"live": live_v, "desired": v}
        if changes:
            modified.append({"name": desired["name"], "changes": changes})
        else:
            unchanged.append(desired["name"])

    return {"created": created, "modified": modified, "deleted": deleted, "unchanged": unchanged}


def diff_nifi_registries(live_regs, desired_regs):
    live_idx = {r["name"].upper(): r for r in (live_regs or [])}
    desired_idx = {r["name"].upper(): r for r in (desired_regs or [])}

    created = [v for k, v in desired_idx.items() if k not in live_idx]
    deleted = [v for k, v in live_idx.items() if k not in desired_idx and k not in _AUTO_PROVISIONED_REG]
    modified = []
    unchanged = []

    for name_upper, desired in desired_idx.items():
        if name_upper not in live_idx:
            continue
        live = live_idx[name_upper]
        changes = {}
        live_props = live.get("properties", {})
        desired_props = desired.get("properties", {})
        for k, v in desired_props.items():
            if _is_secret_ref(v):
                continue
            if k.lower() in ("personal access token", "password", "secret"):
                continue
            live_v = live_props.get(k, "")
            if _norm(live_v) != _norm(v):
                changes[f"property:{k}"] = {"live": live_v, "desired": v}
        if changes:
            modified.append({"name": desired["name"], "changes": changes})
        else:
            unchanged.append(desired["name"])

    return {"created": created, "modified": modified, "deleted": deleted, "unchanged": unchanged}


def diff_nifi_flows(live_flows, desired_flows):
    live_idx = {f["name"].upper(): f for f in (live_flows or [])}
    desired_idx = {f["name"].upper(): f for f in (desired_flows or [])}

    created = [v for k, v in desired_idx.items() if k not in live_idx]
    deleted = [v for k, v in live_idx.items() if k not in desired_idx]
    modified = []
    unchanged = []

    for name_upper, desired in desired_idx.items():
        if name_upper not in live_idx:
            continue
        live = live_idx[name_upper]
        changes = {}
        if _norm(live.get("registry", "")) != _norm(desired.get("registry", "")):
            changes["registry"] = {"live": live.get("registry"), "desired": desired.get("registry")}
        if _norm(live.get("bucket", "")) != _norm(desired.get("bucket", "")):
            changes["bucket"] = {"live": live.get("bucket"), "desired": desired.get("bucket")}
        if _norm(live.get("flow", "")) != _norm(desired.get("flow", "")):
            changes["flow"] = {"live": live.get("flow"), "desired": desired.get("flow")}
        desired_version = desired.get("version", "")
        if desired_version and desired_version != "latest":
            if _norm(live.get("version", "")) != _norm(desired_version):
                changes["version"] = {"live": live.get("version"), "desired": desired_version}
        live_running = live.get("running", False)
        desired_start = desired.get("start", False)
        if live_running != desired_start:
            changes["start"] = {"live": live_running, "desired": desired_start}
        if changes:
            modified.append({"name": desired["name"], "changes": changes, "live": live, "desired": desired})
        else:
            unchanged.append({"name": desired["name"], "version": live.get("version", "?")})

    return {"created": created, "modified": modified, "deleted": deleted, "unchanged": unchanged}


def diff_nifi_parameters(live_params, desired_params):
    changes = {}
    unchanged = []

    for key, desired_val in (desired_params or {}).items():
        if _is_secret_ref(desired_val):
            continue
        live_val = (live_params or {}).get(key)
        if live_val == "<sensitive>":
            continue
        if _norm(live_val) != _norm(desired_val):
            changes[key] = {"live": live_val, "desired": desired_val}
        else:
            unchanged.append(key)

    return {"changes": changes, "unchanged": unchanged}


def diff_nifi_state(live_nifi, desired_rt):
    if not live_nifi or live_nifi.get("error"):
        return {"error": live_nifi.get("error") if live_nifi else "No NiFi state available"}

    cs_diff = diff_nifi_controller_services(
        live_nifi.get("controller_services", []),
        desired_rt.get("controller_services", [])
    )
    pp_diff = diff_nifi_parameter_providers(
        live_nifi.get("parameter_providers", []),
        desired_rt.get("parameter_providers", [])
    )
    reg_diff = diff_nifi_registries(
        live_nifi.get("flow_registries", []),
        desired_rt.get("flow_registries", [])
    )
    flow_diff = diff_nifi_flows(
        live_nifi.get("flows", []),
        desired_rt.get("flows", [])
    )

    param_diffs = {}
    live_params = live_nifi.get("parameters", {})
    for flow_cfg in desired_rt.get("flows", []):
        flow_name = flow_cfg["name"]
        desired_params = flow_cfg.get("parameters", {})
        if desired_params:
            live_flow_params = live_params.get(flow_name, {})
            param_diffs[flow_name] = diff_nifi_parameters(live_flow_params, desired_params)

    return {
        "controller_services": cs_diff,
        "parameter_providers": pp_diff,
        "flow_registries": reg_diff,
        "flows": flow_diff,
        "parameters": param_diffs,
    }


def diff_connectors(live_connectors, desired_connectors):
    live_idx = _index_by_name(live_connectors)
    desired_idx = _index_by_name(desired_connectors)

    created = [v for k, v in desired_idx.items() if k not in live_idx]
    deleted = [v for k, v in live_idx.items() if k not in desired_idx]
    modified = []

    for name_upper, desired_c in desired_idx.items():
        if name_upper not in live_idx:
            continue
        live_c = live_idx[name_upper]
        changes = {}
        for field in ("definition", "display_name", "comment"):
            live_val = _norm(live_c.get(field, ""))
            desired_val = _norm(desired_c.get(field, ""))
            if live_val.upper() != desired_val.upper() and desired_val:
                changes[field] = {"live": live_val, "desired": desired_val}
        live_running = (live_c.get("status", "").upper() == "RUNNING")
        desired_start = desired_c.get("start", False)
        if live_running != desired_start:
            changes["start"] = {"live": live_running, "desired": desired_start}

        # Compare connector parameters
        live_params = live_c.get("parameters", {})
        desired_params = desired_c.get("parameters", {})
        param_changes = {}
        for param_name, desired_val in (desired_params or {}).items():
            if not desired_val and desired_val != 0:
                continue
            try:
                resolved = resolve_value(desired_val)
            except Exception:
                resolved = desired_val
            live_val = live_params.get(param_name, "")
            if _norm(live_val) != _norm(resolved):
                param_changes[param_name] = {"live": live_val, "desired": desired_val}
        if param_changes:
            changes["parameters"] = param_changes

        if changes:
            modified.append({"name": desired_c["name"], "changes": changes, "live": live_c, "desired": desired_c})

    return {"created": created, "modified": modified, "deleted": deleted}


def diff_runtime(live_rt, desired_rt):
    changed_fields = {}
    comparable = ["node_type", "min_nodes", "max_nodes", "execute_as_role", "display_name", "comment"]

    for field in comparable:
        live_val = _norm(live_rt.get(field, ""))
        desired_val = _norm(desired_rt.get(field, ""))
        if isinstance(live_val, str) and isinstance(desired_val, str):
            if live_val.upper() != desired_val.upper() and desired_val:
                changed_fields[field] = {"live": live_val, "desired": desired_val}
        elif live_val != desired_val and desired_val != "":
            changed_fields[field] = {"live": live_val, "desired": desired_val}

    live_suspended = (live_rt.get("status", "").upper() == "SUSPENDED")
    desired_suspend = desired_rt.get("suspend", False)
    if live_suspended != desired_suspend:
        changed_fields["suspend"] = {"live": live_suspended, "desired": desired_suspend}

    nr_changes = diff_network_rules(
        live_rt.get("network_rules", []),
        desired_rt.get("network_rules", [])
    )

    connector_changes = diff_connectors(
        live_rt.get("connectors", []),
        desired_rt.get("connectors", [])
    )

    if desired_rt.get("reconcile") is False:
        nifi_diff = {"skipped": True}
    else:
        nifi_diff = diff_nifi_state(
            live_rt.get("nifi"),
            desired_rt
        )

    return {
        "changed_fields": changed_fields,
        "network_rule_changes": nr_changes,
        "connector_changes": connector_changes,
        "nifi": nifi_diff,
    }


def diff_live(live_state, config_path):
    with open(config_path) as f:
        config = yaml.safe_load(f) or {}

    result = {
        "account": config.get("account", {}),
        "deployments": {
            "unchanged": [],
            "to_create": [],
            "to_modify": [],
            "to_delete": [],
            "url_managed": [],
        },
    }

    live_deployments = _index_by_name(live_state.get("deployments", []))

    for dep_cfg in config.get("deployments", []):
        dep_name = dep_cfg["name"]
        dep_name_upper = dep_name.upper()

        live_dep = live_deployments.get(dep_name_upper)

        if not live_dep:
            result["deployments"]["to_create"].append({
                "name": dep_name,
                "deployment_type": dep_cfg.get("deployment_type", "SNOWFLAKE"),
                "display_name": dep_cfg.get("display_name", ""),
                "comment": dep_cfg.get("comment", ""),
                "runtimes": dep_cfg.get("runtimes", []),
            })
            continue

        dep_changes = {}
        for field in ("display_name", "comment"):
            live_val = _norm(live_dep.get(field, ""))
            desired_val = _norm(dep_cfg.get(field, ""))
            if live_val != desired_val and desired_val:
                dep_changes[field] = {"live": live_val, "desired": desired_val}

        live_runtimes = _index_by_name(live_dep.get("runtimes", []))
        desired_runtimes = dep_cfg.get("runtimes", [])

        rt_results = {
            "unchanged": [],
            "to_create": [],
            "to_modify": [],
            "to_delete": [],
            "url_managed": [],
        }

        desired_rt_names = set()
        for rt_cfg in desired_runtimes:
            rt_name_upper = rt_cfg["name"].upper()
            desired_rt_names.add(rt_name_upper)

            if rt_cfg.get("url"):
                rt_results["url_managed"].append(rt_cfg)
                continue

            live_rt = live_runtimes.get(rt_name_upper)

            if not live_rt:
                rt_results["to_create"].append(rt_cfg)
                continue

            rt_diff = diff_runtime(live_rt, rt_cfg)
            nifi_diff = rt_diff.get("nifi", {})
            nifi_has_changes = False
            if nifi_diff and not nifi_diff.get("error"):
                for section in ("controller_services", "parameter_providers", "flow_registries", "flows"):
                    sd = nifi_diff.get(section, {})
                    if sd.get("created") or sd.get("modified") or sd.get("deleted"):
                        nifi_has_changes = True
                        break
                if not nifi_has_changes:
                    for flow_name, pdiff in nifi_diff.get("parameters", {}).items():
                        if pdiff.get("changes"):
                            nifi_has_changes = True
                            break

            has_changes = (
                rt_diff["changed_fields"]
                or rt_diff["network_rule_changes"]["created"]
                or rt_diff["network_rule_changes"]["modified"]
                or rt_diff["network_rule_changes"]["deleted"]
                or rt_diff["connector_changes"]["created"]
                or rt_diff["connector_changes"]["modified"]
                or rt_diff["connector_changes"]["deleted"]
                or nifi_has_changes
            )

            if has_changes:
                rt_results["to_modify"].append({
                    "name": rt_cfg["name"],
                    "live": live_rt,
                    "desired": rt_cfg,
                    "diff": rt_diff,
                })
            else:
                rt_results["unchanged"].append({
                    "name": rt_cfg["name"],
                    "status": live_rt.get("status", "UNKNOWN"),
                    "nifi": rt_diff.get("nifi", {}),
                })

        for rt_name_upper, live_rt in live_runtimes.items():
            if rt_name_upper not in desired_rt_names:
                rt_results["to_delete"].append(live_rt)

        has_dep_changes = (
            dep_changes
            or rt_results["to_create"]
            or rt_results["to_modify"]
            or rt_results["to_delete"]
        )

        if has_dep_changes:
            result["deployments"]["to_modify"].append({
                "name": dep_name,
                "live_status": live_dep.get("status", "UNKNOWN"),
                "dep_changes": dep_changes,
                "runtimes": rt_results,
            })
        else:
            result["deployments"]["unchanged"].append({
                "name": dep_name,
                "status": live_dep.get("status", "UNKNOWN"),
                "runtimes": rt_results,
            })

    return result


def main():
    if len(sys.argv) < 3:
        print("Usage: diff_live.py <live-state.json> <config.yaml>", file=sys.stderr)
        sys.exit(1)

    live_state_path = sys.argv[1]
    config_path = sys.argv[2]

    with open(live_state_path) as f:
        live_state = json.load(f)

    result = diff_live(live_state, config_path)
    json.dump(result, sys.stdout, indent=2)
    print()


if __name__ == "__main__":
    main()