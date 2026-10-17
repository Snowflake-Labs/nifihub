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
from html import escape as _html_escape


def _md_text(value):
    text = "—" if value is None else str(value)
    text = _html_escape(text, quote=False)
    return text.replace("|", "\\|").replace("`", "\\`").replace("\n", " ")


def _md_code(value):
    return f"`{_md_text(value)}`"


def _render_desired_parameter_value(value):
    if isinstance(value, str) and value.startswith("${{"):
        return "(secret)"
    return _md_code(value if value not in (None, "") else "—")


def format_nifi_section(nifi_diff, summary_only=False):
    lines = []

    if not nifi_diff or nifi_diff.get("error") or nifi_diff.get("skipped"):
        return lines

    cs_diff = nifi_diff.get("controller_services", {})
    rpcs_diff = nifi_diff.get("root_pg_controller_services", {})
    pp_diff = nifi_diff.get("parameter_providers", {})
    reg_diff = nifi_diff.get("flow_registries", {})
    flow_diff = nifi_diff.get("flows", {})
    param_diffs = nifi_diff.get("parameters", {})
    binding_diff = nifi_diff.get("service_bindings", {})
    root_parameter_context_diff = nifi_diff.get("root_parameter_context", {})

    has_nifi_changes = any(
        d.get("created") or d.get("modified") or d.get("deleted") or d.get("health") or d.get("blocked")
        for d in [cs_diff, rpcs_diff, pp_diff, reg_diff, flow_diff, binding_diff, root_parameter_context_diff]
    ) or any(p.get("changes") for p in param_diffs.values())
    has_nifi_changes = has_nifi_changes or bool(root_parameter_context_diff.get("inheritance", {}).get("missing")) or bool(root_parameter_context_diff.get("parameters", {}).get("changes"))

    if summary_only and not has_nifi_changes:
        cs_count = len(cs_diff.get("unchanged", []))
        rpcs_count = len(rpcs_diff.get("unchanged", []))
        pp_count = len(pp_diff.get("unchanged", []))
        reg_count = len(reg_diff.get("unchanged", []))
        flow_count = len(flow_diff.get("unchanged", []))
        parts = []
        if cs_count:
            parts.append(f"{cs_count} controller service(s)")
        if rpcs_count:
            parts.append(f"{rpcs_count} root-PG controller service(s)")
        if pp_count:
            parts.append(f"{pp_count} parameter provider(s)")
        if reg_count:
            parts.append(f"{reg_count} registry client(s)")
        if flow_count:
            parts.append(f"{flow_count} flow(s)")
        if binding_diff.get("unchanged"):
            parts.append(f"{len(binding_diff.get('unchanged', []))} service binding(s)")
        if root_parameter_context_diff.get("unchanged"):
            parts.append(f"{len(root_parameter_context_diff.get('unchanged', []))} root parameter-context asset(s)")
        root_parameter_changes = root_parameter_context_diff.get("parameters", {}).get("unchanged", [])
        if root_parameter_changes:
            parts.append(f"{len(root_parameter_changes)} root parameter(s)")
        root_inheritance = root_parameter_context_diff.get("inheritance", {}).get("unchanged", [])
        if root_inheritance:
            parts.append(f"{len(root_inheritance)} inherited context(s)")
        if parts:
            lines.append(f"> NiFi: {', '.join(parts)} — all matching YAML\n")
        return lines

    lines.append("\n**NiFi Resources:**\n")

    if cs_diff.get("created") or cs_diff.get("modified") or cs_diff.get("deleted") or cs_diff.get("unchanged"):
        lines.append("| Controller Service | Type | Status |")
        lines.append("|---|---|---|")
        for name in cs_diff.get("unchanged", []):
            lines.append(f"| {_md_text(name)} | — | :white_check_mark: no changes |")
        for cs in cs_diff.get("created", []):
            lines.append(f"| {_md_text(cs['name'])} | {_md_text(cs.get('type', '?'))} | :new: to create |")
        for cs_mod in cs_diff.get("modified", []):
            changes_str = ", ".join(cs_mod.get("changes", {}).keys())
            lines.append(f"| {_md_text(cs_mod['name'])} | — | :pencil2: {_md_text(changes_str)} |")
        for cs in cs_diff.get("deleted", []):
            lines.append(f"| {_md_text(cs['name'])} | {_md_text(cs.get('type', '?'))} | :wastebasket: to remove |")
        lines.append("")

    if rpcs_diff.get("created") or rpcs_diff.get("modified") or rpcs_diff.get("deleted") or rpcs_diff.get("unchanged"):
        lines.append("| Root PG Controller Service | Type | Status |")
        lines.append("|---|---|---|")
        for name in rpcs_diff.get("unchanged", []):
            lines.append(f"| {_md_text(name)} | — | :white_check_mark: no changes |")
        for cs in rpcs_diff.get("created", []):
            lines.append(f"| {_md_text(cs['name'])} | {_md_text(cs.get('type', '?'))} | :new: to create |")
        for cs_mod in rpcs_diff.get("modified", []):
            changes_str = ", ".join(cs_mod.get("changes", {}).keys())
            lines.append(f"| {_md_text(cs_mod['name'])} | — | :pencil2: {_md_text(changes_str)} |")
        for cs in rpcs_diff.get("deleted", []):
            lines.append(f"| {_md_text(cs['name'])} | {_md_text(cs.get('type', '?'))} | :wastebasket: to remove |")
        lines.append("")

    if pp_diff.get("created") or pp_diff.get("modified") or pp_diff.get("deleted") or pp_diff.get("unchanged"):
        lines.append("| Parameter Provider | Type | Status |")
        lines.append("|---|---|---|")
        for name in pp_diff.get("unchanged", []):
            lines.append(f"| {_md_text(name)} | — | :white_check_mark: no changes |")
        for pp in pp_diff.get("created", []):
            lines.append(f"| {_md_text(pp['name'])} | {_md_text(pp.get('type', '?'))} | :new: to create |")
        for pp_mod in pp_diff.get("modified", []):
            changes_str = ", ".join(pp_mod.get("changes", {}).keys())
            lines.append(f"| {_md_text(pp_mod['name'])} | — | :pencil2: {_md_text(changes_str)} |")
        for pp in pp_diff.get("deleted", []):
            lines.append(f"| {_md_text(pp['name'])} | {_md_text(pp.get('type', '?'))} | :wastebasket: to remove |")
        lines.append("")

    if reg_diff.get("created") or reg_diff.get("modified") or reg_diff.get("deleted") or reg_diff.get("unchanged"):
        lines.append("| Flow Registry Client | Status |")
        lines.append("|---|---|")
        for name in reg_diff.get("unchanged", []):
            lines.append(f"| {_md_text(name)} | :white_check_mark: no changes |")
        for r in reg_diff.get("created", []):
            lines.append(f"| {_md_text(r['name'])} | :new: to create |")
        for r_mod in reg_diff.get("modified", []):
            changes_str = ", ".join(r_mod.get("changes", {}).keys())
            lines.append(f"| {_md_text(r_mod['name'])} | :pencil2: {_md_text(changes_str)} |")
        for r in reg_diff.get("deleted", []):
            lines.append(f"| {_md_text(r['name'])} | :wastebasket: to remove |")
        lines.append("")

    if flow_diff.get("created") or flow_diff.get("modified") or flow_diff.get("deleted") or flow_diff.get("unchanged"):
        lines.append("| Flow | Registry/Bucket/Flow | Version | State | Action |")
        lines.append("|---|---|---|---|---|")
        for f in flow_diff.get("unchanged", []):
            lines.append(f"| {_md_text(f['name'])} | — | {_md_text(f.get('version', '?'))} | — | :white_check_mark: |")
        for f in flow_diff.get("created", []):
            lines.append(f"| {_md_text(f['name'])} | {_md_text(f.get('registry', '?'))}/{_md_text(f.get('bucket', '?'))}/{_md_text(f.get('flow', '?'))} | {_md_text(f.get('version', 'latest'))} | — | :new: |")
        for f_mod in flow_diff.get("modified", []):
            changes_str = ", ".join(f"{k}: {v['live']}→{v['desired']}" for k, v in f_mod.get("changes", {}).items())
            lines.append(f"| {_md_text(f_mod['name'])} | — | — | — | :pencil2: {_md_text(changes_str)} |")
        for f in flow_diff.get("deleted", []):
            lines.append(f"| {_md_text(f['name'])} | — | — | — | :wastebasket: |")
        lines.append("")

    if binding_diff.get("created") or binding_diff.get("modified") or binding_diff.get("deleted") or binding_diff.get("health") or binding_diff.get("blocked") or binding_diff.get("unchanged"):
        lines.append("| Service Bindings | Property | Action |")
        lines.append("|---|---|---|")
        for binding in binding_diff.get("unchanged", []):
            lines.append(f"| {_md_text(binding['flow'])} / {_md_text(binding['target'])} | {_md_text(binding['property'])} | :white_check_mark: no changes |")
        for binding in binding_diff.get("created", []):
            lines.append(f"| {_md_text(binding['flow'])} / {_md_text(binding['target'])} | {_md_text(binding['property'])} | :new: {_md_text(binding['desired'])} |")
        for binding in binding_diff.get("modified", []):
            action = binding.get("action", "change")
            lines.append(f"| {_md_text(binding['flow'])} / {_md_text(binding['target'])} | {_md_text(binding['property'])} | :pencil2: {_md_text(action)} {_md_text(binding.get('live', '—'))} -> {_md_text(binding.get('desired', '—'))} |")
        for binding in binding_diff.get("deleted", []):
            lines.append(f"| {_md_text(binding['flow'])} / {_md_text(binding['target'])} | {_md_text(binding['property'])} | :wastebasket: remove {_md_text(binding.get('live', '—'))} |")
        lines.append("")
        for issue in binding_diff.get("health", []):
            messages = "; ".join(_md_text(message) for message in issue.get("messages", []))
            lines.append(f"> :warning: Binding health {_md_code(issue['flow'])} / {_md_code(issue['target'])} ({_md_text(issue.get('status', 'UNKNOWN'))}): {messages}")
        for issue in binding_diff.get("blocked", []):
            messages = "; ".join(_md_text(message) for message in issue.get("messages", []))
            lines.append(f"> :no_entry: Binding state unknown {_md_code(issue['flow'])} / {_md_code(issue['target'])} ({_md_text(issue.get('status', 'UNKNOWN'))}): {messages}")
        if binding_diff.get("health") or binding_diff.get("blocked"):
            lines.append("")

    root_inheritance = root_parameter_context_diff.get("inheritance", {})
    root_parameter_diff = root_parameter_context_diff.get("parameters", {})
    if root_inheritance.get("missing") or root_parameter_diff.get("changes") or root_parameter_context_diff.get("created") or root_parameter_context_diff.get("modified") or root_parameter_context_diff.get("deleted") or root_parameter_context_diff.get("health") or root_parameter_context_diff.get("blocked") or root_parameter_context_diff.get("unchanged"):
        lines.append("**Root Parameter Context:**")
        if root_inheritance.get("missing"):
            lines.append("- Inherited contexts to add: " + ", ".join(_md_code(name) for name in root_inheritance.get("missing", [])))
        if root_inheritance.get("unmatched_pattern"):
            lines.append("- Inherited context pattern matched no provider contexts")
        if root_parameter_diff.get("changes"):
            lines.append("| Root Parameter | Desired |")
            lines.append("|---|---|")
            for param_name, vals in root_parameter_diff.get("changes", {}).items():
                lines.append(f"| {_md_text(param_name)} | {_render_desired_parameter_value(vals.get('desired'))} |")
            lines.append("")
        lines.append("| Root Parameter Context Asset | Parameter | Status |")
        lines.append("|---|---|---|")
        for asset in root_parameter_context_diff.get("unchanged", []):
            lines.append(f"| {_md_text(asset['name'])} | {_md_text(asset['parameter'])} | :white_check_mark: no changes |")
        for asset in root_parameter_context_diff.get("created", []):
            lines.append(f"| {_md_text(asset['name'])} | {_md_text(asset['parameter'])} | :new: {_md_text(asset.get('status', 'create'))} |")
        for asset in root_parameter_context_diff.get("modified", []):
            lines.append(f"| {_md_text(asset['name'])} | {_md_text(asset['parameter'])} | :pencil2: {_md_text(asset.get('status', 'repair'))} |")
        for asset in root_parameter_context_diff.get("deleted", []):
            lines.append(f"| {_md_text(asset['name'])} | {_md_text(asset['parameter'])} | :information_source: declaration removed; no runtime cleanup |")
        lines.append("")
        for issue in root_parameter_context_diff.get("health", []):
            messages = "; ".join(_md_text(message) for message in issue.get("messages", []))
            lines.append(f"> :warning: Root parameter context asset health {_md_code(issue['name'])} / {_md_code(issue['parameter'])} ({_md_text(issue.get('status', 'UNKNOWN'))}): {messages}")
        for issue in root_parameter_context_diff.get("blocked", []):
            messages = "; ".join(_md_text(message) for message in issue.get("messages", []))
            labels = []
            if issue.get("parameter"):
                labels.append(_md_code(issue["parameter"]))
            if issue.get("name"):
                labels.append(_md_code(issue["name"]))
            target = " / ".join(labels) if labels else "root parameter context"
            lines.append(f"> :no_entry: Root parameter context state unknown {target} ({_md_text(issue.get('status', 'UNKNOWN'))}): {messages}")
        if root_parameter_context_diff.get("health") or root_parameter_context_diff.get("blocked"):
            lines.append("")

    for flow_name, pdiff in param_diffs.items():
        changes = pdiff.get("changes", {})
        if changes:
            lines.append(f"**Parameters for `{flow_name}`:**\n")
            lines.append("| Parameter | Desired |")
            lines.append("|---|---|")
            for param_name, vals in changes.items():
                desired_v = vals.get("desired", "—") or "—"
                lines.append(f"| {_md_text(param_name)} | {_md_code(desired_v)} |")
            lines.append("")

    return lines


def format_change_plan(diff_result):
    lines = ["## NiFi Environment Change Plan\n", f"**Environment:** {_md_code(diff_result.get('name', ''))}\n"]
    runtimes = diff_result.get("runtimes", {})
    for section in ("to_modify", "unchanged"):
        for runtime in runtimes.get(section, []):
            lines.append(f"### NiFi: {_md_code(runtime['name'])}\n")
            nifi = runtime.get("diff", {}).get("nifi", {})
            if nifi.get("error"):
                lines.append("> Cannot read NiFi state; changes are blocked.\n")
            elif nifi.get("skipped"):
                lines.append("> Reconciliation skipped (`reconcile: false`).\n")
            else:
                lines.extend(format_nifi_section(nifi, summary_only=section == "unchanged"))
    lines.append("\nSensitive values are not compared; declared resources are reconciled on each apply.")
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
