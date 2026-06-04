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


def _status_emoji(status):
    status = (status or "").upper()
    if status == "ACTIVE":
        return ":white_check_mark:"
    if status == "SUSPENDED":
        return ":zzz:"
    if status == "RUNNING":
        return ":white_check_mark:"
    if status in ("STOPPED", "CREATED"):
        return ":stop_sign:"
    return ":grey_question:"


def format_deployment_section(dep, mode):
    lines = []
    name = dep["name"]

    if mode == "unchanged":
        status = dep.get("status", "UNKNOWN")
        lines.append(f"### Deployment: `{name}`")
        lines.append(f"{_status_emoji(status)} Exists ({status}) — no deployment-level changes\n")
        rt_info = dep.get("runtimes", {})
        lines.extend(format_runtimes_section(rt_info))
    elif mode == "to_create":
        lines.append(f"### Deployment: `{name}` :new:")
        lines.append(f"Will be **created** as `{dep.get('deployment_type', 'SNOWFLAKE')}`")
        if dep.get("display_name"):
            lines.append(f"- Display Name: {dep['display_name']}")
        if dep.get("comment"):
            lines.append(f"- Comment: {dep['comment']}")
        runtimes = dep.get("runtimes", [])
        if runtimes:
            lines.append(f"\n**{len(runtimes)} runtime(s)** will be created:")
            for rt in runtimes:
                lines.append(f"- `{rt['name']}` ({rt.get('node_type', '?')}, {rt.get('min_nodes', '?')}-{rt.get('max_nodes', '?')} nodes)")
        lines.append("")
    elif mode == "to_modify":
        live_status = dep.get("live_status", "UNKNOWN")
        lines.append(f"### Deployment: `{name}`")
        lines.append(f"{_status_emoji(live_status)} Exists ({live_status})\n")
        dep_changes = dep.get("dep_changes", {})
        if dep_changes:
            lines.append("**Deployment-level changes:**\n")
            lines.append("| Property | Live | Desired |")
            lines.append("|----------|------|---------|")
            for field, vals in dep_changes.items():
                lines.append(f"| {field} | {vals['live']} | {vals['desired']} |")
            lines.append("")
        rt_info = dep.get("runtimes", {})
        lines.extend(format_runtimes_section(rt_info))

    return lines


def format_runtimes_section(rt_info):
    lines = []

    for rt in rt_info.get("unchanged", []):
        lines.append(f"#### Runtime: `{rt['name']}`")
        lines.append(f"{_status_emoji(rt.get('status'))} No changes needed ({rt.get('status', 'UNKNOWN')})\n")
        nifi = rt.get("nifi", {})
        if nifi and nifi.get("skipped"):
            lines.append("> :fast_forward: NiFi reconciliation skipped (`reconcile: false`)\n")
        elif nifi and not nifi.get("error"):
            lines.extend(format_nifi_section(nifi, summary_only=True))
        elif nifi and nifi.get("error"):
            lines.append(f"> :warning: NiFi API: {nifi['error']}\n")

    for rt in rt_info.get("to_create", []):
        lines.append(f"#### Runtime: `{rt['name']}` :new:")
        lines.append("Will be **created** with:\n")
        props = []
        if rt.get("node_type"):
            props.append(f"node_type: {rt['node_type']}")
        if rt.get("min_nodes"):
            props.append(f"min_nodes: {rt['min_nodes']}")
        if rt.get("max_nodes"):
            props.append(f"max_nodes: {rt['max_nodes']}")
        if rt.get("execute_as_role"):
            props.append(f"execute_as_role: {rt['execute_as_role']}")
        if props:
            lines.append(f"- {', '.join(props)}")
        nr = rt.get("network_rules", [])
        if nr:
            lines.append(f"- Network rules: {', '.join(r['name'] for r in nr)}")
        connectors = rt.get("connectors", [])
        if connectors:
            lines.append(f"- Connectors: {', '.join(c['name'] for c in connectors)}")
        if rt.get("suspend"):
            lines.append("- :zzz: Will be created **suspended**")
        lines.append("")

    for rt in rt_info.get("to_modify", []):
        name = rt["name"]
        live = rt.get("live", {})
        diff = rt.get("diff", {})
        lines.append(f"#### Runtime: `{name}`")
        lines.append(f"{_status_emoji(live.get('status'))} Exists ({live.get('status', 'UNKNOWN')}) — changes needed:\n")

        changed_fields = diff.get("changed_fields", {})
        if changed_fields:
            lines.append("| Property | Live | Desired |")
            lines.append("|----------|------|---------|")
            for field, vals in changed_fields.items():
                lines.append(f"| {field} | `{vals['live']}` | `{vals['desired']}` |")
            lines.append("")

        nr_changes = diff.get("network_rule_changes", {})
        if nr_changes.get("created"):
            lines.append("**Network rules to add:**")
            for nr in nr_changes["created"]:
                lines.append(f"- :new: `{nr['name']}`: {', '.join(nr.get('values', []))}")
            lines.append("")
        if nr_changes.get("modified"):
            lines.append("**Network rules to modify:**")
            for nr_mod in nr_changes["modified"]:
                old_vals = ', '.join(nr_mod['old'].get('values', []))
                new_vals = ', '.join(nr_mod['new'].get('values', []))
                lines.append(f"- :pencil2: `{nr_mod['new']['name']}`: {old_vals} → {new_vals}")
            lines.append("")
        if nr_changes.get("deleted"):
            lines.append("**Network rules to remove:**")
            for nr in nr_changes["deleted"]:
                lines.append(f"- :x: `{nr['name']}`")
            lines.append("")

        conn_changes = diff.get("connector_changes", {})
        if conn_changes.get("created"):
            lines.append("**Connectors to create:**")
            for c in conn_changes["created"]:
                lines.append(f"- :new: `{c['name']}` (definition: {c.get('definition', '?')})")
            lines.append("")
        if conn_changes.get("modified"):
            lines.append("**Connectors to modify:**")
            for c_mod in conn_changes["modified"]:
                name = c_mod["name"]
                changes = c_mod.get("changes", {})
                field_changes = {k: v for k, v in changes.items() if k != "parameters"}
                param_changes = changes.get("parameters", {})
                parts = [f"{k}: `{v['desired']}`" for k, v in field_changes.items()]
                if param_changes:
                    parts.append(f"{len(param_changes)} parameter(s) changed")
                lines.append(f"- :pencil2: `{name}`: {', '.join(parts) if parts else 'changes detected'}")
                if param_changes:
                    lines.append("  | Parameter | Desired |")
                    lines.append("  |---|---|")
                    for p_name, p_vals in param_changes.items():
                        lines.append(f"  | `{p_name}` | `{p_vals['desired']}` |")
            lines.append("")
        if conn_changes.get("deleted"):
            lines.append("**Connectors to delete:**")
            for c in conn_changes["deleted"]:
                lines.append(f"- :wastebasket: `{c['name']}` (currently {c.get('status', '?')})")
            lines.append("")

        nifi_diff = diff.get("nifi", {})
        if nifi_diff and nifi_diff.get("skipped"):
            lines.append("> :fast_forward: NiFi reconciliation skipped (`reconcile: false`)\n")
        elif nifi_diff and not nifi_diff.get("error"):
            lines.extend(format_nifi_section(nifi_diff, summary_only=False))
        elif nifi_diff and nifi_diff.get("error"):
            lines.append(f"> :warning: NiFi API: {nifi_diff['error']}\n")

    for rt in rt_info.get("to_delete", []):
        lines.append(f"#### Runtime: `{rt['name']}` :wastebasket:")
        lines.append(f"Exists in environment ({rt.get('status', '?')}) but **not in YAML** — will be deleted\n")

    for rt in rt_info.get("url_managed", []):
        lines.append(f"#### Runtime: `{rt['name']}` :link:")
        lines.append(f"URL-managed (`{rt.get('url', '')}`) — NiFi API diff available in Phase 2\n")

    return lines


def format_nifi_section(nifi_diff, summary_only=False):
    lines = []

    if not nifi_diff or nifi_diff.get("error") or nifi_diff.get("skipped"):
        return lines

    cs_diff = nifi_diff.get("controller_services", {})
    pp_diff = nifi_diff.get("parameter_providers", {})
    reg_diff = nifi_diff.get("flow_registries", {})
    flow_diff = nifi_diff.get("flows", {})
    param_diffs = nifi_diff.get("parameters", {})

    has_nifi_changes = any(
        d.get("created") or d.get("modified") or d.get("deleted")
        for d in [cs_diff, pp_diff, reg_diff, flow_diff]
    ) or any(p.get("changes") for p in param_diffs.values())

    if summary_only and not has_nifi_changes:
        cs_count = len(cs_diff.get("unchanged", []))
        pp_count = len(pp_diff.get("unchanged", []))
        reg_count = len(reg_diff.get("unchanged", []))
        flow_count = len(flow_diff.get("unchanged", []))
        parts = []
        if cs_count:
            parts.append(f"{cs_count} controller service(s)")
        if pp_count:
            parts.append(f"{pp_count} parameter provider(s)")
        if reg_count:
            parts.append(f"{reg_count} registry client(s)")
        if flow_count:
            parts.append(f"{flow_count} flow(s)")
        if parts:
            lines.append(f"> NiFi: {', '.join(parts)} — all matching YAML\n")
        return lines

    lines.append("\n**NiFi Resources:**\n")

    if cs_diff.get("created") or cs_diff.get("modified") or cs_diff.get("deleted") or cs_diff.get("unchanged"):
        lines.append("| Controller Service | Type | Status |")
        lines.append("|---|---|---|")
        for name in cs_diff.get("unchanged", []):
            lines.append(f"| {name} | — | :white_check_mark: no changes |")
        for cs in cs_diff.get("created", []):
            lines.append(f"| {cs['name']} | {cs.get('type', '?')} | :new: to create |")
        for cs_mod in cs_diff.get("modified", []):
            changes_str = ", ".join(cs_mod.get("changes", {}).keys())
            lines.append(f"| {cs_mod['name']} | — | :pencil2: {changes_str} |")
        for cs in cs_diff.get("deleted", []):
            lines.append(f"| {cs['name']} | {cs.get('type', '?')} | :wastebasket: to remove |")
        lines.append("")

    if pp_diff.get("created") or pp_diff.get("modified") or pp_diff.get("deleted") or pp_diff.get("unchanged"):
        lines.append("| Parameter Provider | Type | Status |")
        lines.append("|---|---|---|")
        for name in pp_diff.get("unchanged", []):
            lines.append(f"| {name} | — | :white_check_mark: no changes |")
        for pp in pp_diff.get("created", []):
            lines.append(f"| {pp['name']} | {pp.get('type', '?')} | :new: to create |")
        for pp_mod in pp_diff.get("modified", []):
            changes_str = ", ".join(pp_mod.get("changes", {}).keys())
            lines.append(f"| {pp_mod['name']} | — | :pencil2: {changes_str} |")
        for pp in pp_diff.get("deleted", []):
            lines.append(f"| {pp['name']} | {pp.get('type', '?')} | :wastebasket: to remove |")
        lines.append("")

    if reg_diff.get("created") or reg_diff.get("modified") or reg_diff.get("deleted") or reg_diff.get("unchanged"):
        lines.append("| Flow Registry Client | Status |")
        lines.append("|---|---|")
        for name in reg_diff.get("unchanged", []):
            lines.append(f"| {name} | :white_check_mark: no changes |")
        for r in reg_diff.get("created", []):
            lines.append(f"| {r['name']} | :new: to create |")
        for r_mod in reg_diff.get("modified", []):
            changes_str = ", ".join(r_mod.get("changes", {}).keys())
            lines.append(f"| {r_mod['name']} | :pencil2: {changes_str} |")
        for r in reg_diff.get("deleted", []):
            lines.append(f"| {r['name']} | :wastebasket: to remove |")
        lines.append("")

    if flow_diff.get("created") or flow_diff.get("modified") or flow_diff.get("deleted") or flow_diff.get("unchanged"):
        lines.append("| Flow | Registry/Bucket/Flow | Version | State | Action |")
        lines.append("|---|---|---|---|---|")
        for f in flow_diff.get("unchanged", []):
            lines.append(f"| {f['name']} | — | {f.get('version', '?')} | — | :white_check_mark: |")
        for f in flow_diff.get("created", []):
            lines.append(f"| {f['name']} | {f.get('registry', '?')}/{f.get('bucket', '?')}/{f.get('flow', '?')} | {f.get('version', 'latest')} | — | :new: |")
        for f_mod in flow_diff.get("modified", []):
            changes_str = ", ".join(f"{k}: {v['live']}→{v['desired']}" for k, v in f_mod.get("changes", {}).items())
            lines.append(f"| {f_mod['name']} | — | — | — | :pencil2: {changes_str} |")
        for f in flow_diff.get("deleted", []):
            lines.append(f"| {f['name']} | — | — | — | :wastebasket: |")
        lines.append("")

    for flow_name, pdiff in param_diffs.items():
        changes = pdiff.get("changes", {})
        if changes:
            lines.append(f"**Parameters for `{flow_name}`:**\n")
            lines.append("| Parameter | Desired |")
            lines.append("|---|---|")
            for param_name, vals in changes.items():
                desired_v = vals.get("desired", "—") or "—"
                lines.append(f"| {param_name} | `{desired_v}` |")
            lines.append("")

    return lines


def format_change_plan(diff_result):
    lines = []
    lines.append("## :arrows_counterclockwise: Environment Change Plan\n")
    lines.append(f"**Account:** `{diff_result.get('account', {}).get('name', '?')}`\n")

    deployments = diff_result.get("deployments", {})

    has_any_changes = (
        deployments.get("to_create")
        or deployments.get("to_modify")
        or deployments.get("to_delete")
    )

    if not has_any_changes and not deployments.get("unchanged"):
        lines.append(":warning: No deployments found in live state or YAML.\n")
        return "\n".join(lines)

    for dep in deployments.get("unchanged", []):
        lines.extend(format_deployment_section(dep, "unchanged"))

    for dep in deployments.get("to_create", []):
        lines.extend(format_deployment_section(dep, "to_create"))

    for dep in deployments.get("to_modify", []):
        lines.extend(format_deployment_section(dep, "to_modify"))

    for dep in deployments.get("to_delete", []):
        lines.append(f"### Deployment: `{dep['name']}` :wastebasket:")
        lines.append(f"Exists in environment but **not in YAML** — will be deleted\n")

    lines.append("---")
    if has_any_changes:
        lines.append("> :rocket: This plan will be applied when the PR is merged to main.")
    else:
        lines.append("> :white_check_mark: No changes needed — environment matches YAML.")
    lines.append("")
    lines.append("> *Diff computed against live Snowflake environment. Sensitive parameter values (secrets) are not compared.*")

    return "\n".join(lines)


def main():
    if len(sys.argv) < 2:
        print("Usage: format_change_plan.py <diff-result.json>", file=sys.stderr)
        sys.exit(1)

    with open(sys.argv[1]) as f:
        diff_result = json.load(f)

    print(format_change_plan(diff_result))


if __name__ == "__main__":
    main()