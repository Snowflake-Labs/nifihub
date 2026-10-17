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
import re
import sys

from nifi_config import load_config

_SECRET_RE = re.compile(r'^\$\{\{\s*secrets\.')
_UUID_RE = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE)


def _norm(val):
    if val is None:
        return ""
    if isinstance(val, str):
        return val.strip()
    return val


def _is_secret_ref(val):
    if not val or not isinstance(val, str):
        return False
    return bool(_SECRET_RE.match(val))


def diff_nifi_controller_services(live_cs, desired_cs):
    live_idx = {cs["name"].upper(): cs for cs in (live_cs or [])}
    desired_idx = {cs["name"].upper(): cs for cs in (desired_cs or [])}

    created = [v for k, v in desired_idx.items() if k not in live_idx]
    deleted = [v for k, v in live_idx.items() if k not in desired_idx]
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


def diff_nifi_root_pg_controller_services(live_cs, desired_cs):
    """Diff root process group-scoped controller services (no auto-provisioned exclusions)."""
    live_idx = {cs["name"].upper(): cs for cs in (live_cs or [])}
    desired_idx = {cs["name"].upper(): cs for cs in (desired_cs or [])}

    created = [v for k, v in desired_idx.items() if k not in live_idx]
    deleted = [v for k, v in live_idx.items() if k not in desired_idx]
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
    deleted = [v for k, v in live_idx.items() if k not in desired_idx]
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
    deleted = [v for k, v in live_idx.items() if k not in desired_idx]
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


def diff_nifi_service_bindings(live_flows, desired_flows):
    live_idx = {f["name"].upper(): f for f in (live_flows or [])}
    created = []
    modified = []
    deleted = []
    unchanged = []
    health = []
    blocked = []

    for desired_flow in desired_flows or []:
        desired_bindings = desired_flow.get("service_bindings", [])
        if not desired_bindings:
            continue

        live_flow = live_idx.get(desired_flow["name"].upper(), {})
        live_bindings = {
            binding.get("target"): binding
            for binding in live_flow.get("service_bindings", [])
        }

        for desired_binding in desired_bindings:
            live_binding = live_bindings.get(desired_binding["target"], {})
            target_kind = live_binding.get("target_kind", "unknown")
            process_group_path = live_binding.get("process_group_path", desired_flow["name"])
            validation_status = live_binding.get("validation_status")
            validation_errors = list(live_binding.get("validation_errors", []))
            issues = list(live_binding.get("issues", []))
            live_properties = live_binding.get("properties", {})
            live_state = live_binding.get("state", "known")

            if live_state != "known":
                blocked.append({
                    "flow": desired_flow["name"],
                    "target": desired_binding["target"],
                    "target_kind": target_kind,
                    "process_group_path": process_group_path,
                    "status": validation_status or "UNKNOWN",
                    "messages": issues or ["Live binding state could not be read safely"],
                })
                continue

            health_messages = []
            if issues:
                health_messages.extend(issues)
            if validation_status and str(validation_status).upper() != "VALID":
                health_messages.extend(validation_errors or [validation_status])
            for property_name, property_state in live_properties.items():
                warning = property_state.get("warning")
                if warning:
                    health_messages.append(f"{property_name}: {warning}")
            if health_messages:
                health.append({
                    "flow": desired_flow["name"],
                    "target": desired_binding["target"],
                    "status": validation_status or "UNKNOWN",
                    "messages": health_messages,
                })

            for property_name, desired_service in desired_binding.get("properties", {}).items():
                property_state = live_properties.get(property_name, {})
                live_service = property_state.get("service")
                warning = property_state.get("warning")
                base_entry = {
                    "flow": desired_flow["name"],
                    "target": desired_binding["target"],
                    "target_kind": target_kind,
                    "process_group_path": process_group_path,
                    "property": property_name,
                }
                if desired_service is None:
                    if property_state.get("configured"):
                        deleted.append({
                            **base_entry,
                            "live": live_service,
                            "desired": None,
                            "action": "remove",
                        })
                    else:
                        unchanged.append({**base_entry, "desired": None})
                    continue
                if live_service == desired_service and not warning:
                    unchanged.append({**base_entry, "desired": desired_service})
                    continue
                action = "create"
                bucket = created
                if warning:
                    action = "repair"
                    bucket = modified
                elif live_service is not None and live_service != desired_service:
                    action = "change"
                    bucket = modified
                bucket.append({
                    **base_entry,
                    "live": live_service,
                    "desired": desired_service,
                    "action": action,
                })

    return {
        "created": created,
        "modified": modified,
        "deleted": deleted,
        "unchanged": unchanged,
        "health": health,
        "blocked": blocked,
    }


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


def diff_nifi_root_parameter_context(live_root_parameter_context, desired_root_parameter_context):
    created = []
    modified = []
    deleted = []
    unchanged = []
    health = []
    blocked = []

    desired_root_parameter_context = desired_root_parameter_context or {}
    live_root_parameter_context = live_root_parameter_context or {}

    live_context = live_root_parameter_context.get("context", {})
    live_inherited = list(live_root_parameter_context.get("inherited", []) or [])
    live_parameters = dict(live_root_parameter_context.get("parameters", {}) or {})
    provider_context_names = list(live_root_parameter_context.get("provider_context_names", []) or [])
    desired_assets = list(desired_root_parameter_context.get("assets", []) or [])

    inheritance_pattern = desired_root_parameter_context.get("provided_parameter_contexts")
    matched_inheritance = []
    if inheritance_pattern:
        matched_inheritance = [
            name for name in provider_context_names
            if re.fullmatch(inheritance_pattern, name)
        ]
    inherited_set = set(live_inherited)
    inheritance_missing = [name for name in matched_inheritance if name not in inherited_set]
    inheritance_unchanged = [name for name in matched_inheritance if name in inherited_set]

    parameter_diff = diff_nifi_parameters(
        live_parameters,
        desired_root_parameter_context.get("parameters", {}),
    )

    if live_root_parameter_context.get("blocked"):
        for entry in live_root_parameter_context.get("entries", []):
            blocked.append({
                "name": entry.get("name"),
                "parameter": entry.get("parameter"),
                "status": entry.get("status", "blocked"),
                "messages": list(entry.get("messages", [])),
            })
        if not blocked:
            blocked.append({
                "status": "blocked",
                "messages": list(live_root_parameter_context.get("messages", [])) or ["Live root parameter context state could not be read safely"],
            })

    desired_by_name = {
        asset["name"]: asset
        for asset in desired_assets
        if asset.get("name")
    }
    live_entries = {
        entry.get("name", ""): entry
        for entry in live_root_parameter_context.get("entries", [])
        if entry.get("name")
    }

    for asset_name, desired_asset in desired_by_name.items():
        live_entry = live_entries.get(asset_name, {})
        base_entry = {
            "name": desired_asset.get("name"),
            "parameter": desired_asset.get("parameter"),
            "asset_id": live_entry.get("asset_id"),
            "missing_content": live_entry.get("missing_content"),
            "messages": list(live_entry.get("messages", [])),
        }
        status = live_entry.get("status")
        if status == "matched":
            unchanged.append(base_entry)
        elif status in ("missing_context", "missing_parameter", "missing_asset", "missing_reference"):
            created.append({**base_entry, "action": "create", "status": status})
        elif status in ("wrong_reference", "missing_content"):
            modified.append({**base_entry, "action": "repair", "status": status})
        elif status in ("sensitive_parameter", "literal_parameter", "multiple_references"):
            health.append({**base_entry, "status": status})
        elif status == "blocked":
            blocked.append({**base_entry, "status": status})
        else:
            created.append({**base_entry, "action": "create", "status": status or "missing_state"})

    return {
        "created": created,
        "modified": modified,
        "deleted": deleted,
        "unchanged": unchanged,
        "health": health,
        "blocked": blocked,
        "root_process_group_id": live_root_parameter_context.get("root_process_group_id"),
        "context": live_context,
        "inheritance": {
            "missing": inheritance_missing,
            "unchanged": inheritance_unchanged,
            "unmatched_pattern": bool(inheritance_pattern) and not matched_inheritance,
        },
        "parameters": parameter_diff,
    }


def diff_nifi_state(live_nifi, desired_rt):
    if not live_nifi or live_nifi.get("error"):
        return {"error": live_nifi.get("error") if live_nifi else "No NiFi state available"}

    cs_diff = diff_nifi_controller_services(
        live_nifi.get("controller_services", []),
        desired_rt.get("controller_services", [])
    ) if "controller_services" in desired_rt else {}
    root_pg_cs_diff = diff_nifi_root_pg_controller_services(
        live_nifi.get("root_pg_controller_services", []),
        desired_rt.get("root_pg_controller_services", [])
    ) if "root_pg_controller_services" in desired_rt else {}
    pp_diff = diff_nifi_parameter_providers(
        live_nifi.get("parameter_providers", []),
        desired_rt.get("parameter_providers", [])
    ) if "parameter_providers" in desired_rt else {}
    reg_diff = diff_nifi_registries(
        live_nifi.get("flow_registries", []),
        desired_rt.get("flow_registries", [])
    ) if "flow_registries" in desired_rt else {}
    flow_diff = diff_nifi_flows(
        live_nifi.get("flows", []),
        desired_rt.get("flows", [])
    ) if "flows" in desired_rt else {}
    service_binding_diff = diff_nifi_service_bindings(
        live_nifi.get("flows", []),
        desired_rt.get("flows", [])
    ) if "flows" in desired_rt else {}
    root_parameter_context_diff = diff_nifi_root_parameter_context(
        live_nifi.get("root_parameter_context", {}),
        desired_rt.get("root_parameter_context", {}),
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
        "root_pg_controller_services": root_pg_cs_diff,
        "parameter_providers": pp_diff,
        "flow_registries": reg_diff,
        "flows": flow_diff,
        "parameters": param_diffs,
        "service_bindings": service_binding_diff,
        "root_parameter_context": root_parameter_context_diff,
    }


def _nifi_diff_has_changes(nifi_diff):
    if not nifi_diff or nifi_diff.get("error") or nifi_diff.get("skipped"):
        return False
    for section in (
        "controller_services",
        "root_pg_controller_services",
        "parameter_providers",
        "flow_registries",
        "flows",
        "service_bindings",
        "root_parameter_context",
    ):
        section_diff = nifi_diff.get(section, {})
        if section_diff.get("created") or section_diff.get("modified") or section_diff.get("deleted") or section_diff.get("health") or section_diff.get("blocked"):
            return True
    root_parameter_context_diff = nifi_diff.get("root_parameter_context", {})
    if root_parameter_context_diff.get("inheritance", {}).get("missing"):
        return True
    if root_parameter_context_diff.get("parameters", {}).get("changes"):
        return True
    for pdiff in nifi_diff.get("parameters", {}).values():
        if pdiff.get("changes"):
            return True
    return False


def diff_runtime(live_rt, desired_rt):
    if desired_rt.get("reconcile") is False:
        return {"nifi": {"skipped": True}}
    return {"nifi": diff_nifi_state(live_rt.get("nifi"), desired_rt)}


def diff_live(live_state, config_path):
    config = load_config(config_path)
    live_by_name = {rt["name"].upper(): rt for rt in live_state.get("runtimes", [])}
    result = {"name": config.get("name", ""), "runtimes": {"unchanged": [], "to_modify": []}}
    for runtime in config["runtimes"]:
        live = live_by_name.get(runtime["name"].upper(), {})
        diff = diff_runtime(live, runtime)
        entry = {"name": runtime["name"], "desired": runtime, "diff": diff}
        section = "to_modify" if _nifi_diff_has_changes(diff["nifi"]) or diff["nifi"].get("error") else "unchanged"
        result["runtimes"][section].append(entry)
    return result


def main():
    if len(sys.argv) != 3:
        sys.exit("Usage: diff_live.py <live-state.json> <config.yaml>")
    with open(sys.argv[1]) as handle:
        live_state = json.load(handle)
    json.dump(diff_live(live_state, sys.argv[2]), sys.stdout, indent=2)
    print()


if __name__ == "__main__":
    main()
