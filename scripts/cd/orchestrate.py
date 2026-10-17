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
"""Reconcile resources on existing Apache NiFi instances."""
import argparse
import json
import os
import sys
from collections import defaultdict

from manage_parameters import resolve_value
from setup_registry_client import setup as setup_registry, find_registry_client, delete_registry_client
from manage_flows import reconcile_flows, delete_flows, find_flow_pg_by_name, configure_nifi, start_flow, stop_flow
from manage_parameters import reconcile_flow_parameters, add_inherited_parameter_contexts, apply_parameter_overrides
from manage_assets import reconcile_flow_assets
from manage_controller_services import (
    reconcile_controller_services, delete_controller_services,
    reconcile_root_pg_controller_services, delete_root_pg_controller_services,
)
from manage_root_parameter_context import reconcile_root_parameter_context
from manage_parameter_providers import reconcile_parameter_providers, delete_parameter_providers
from manage_service_bindings import reconcile_service_bindings
from safe_exceptions import format_safe_exception
from nifi_config import get_nifi_auth, load_config


def _get_nifi_pat():
    return os.environ.get("NIFI_TOKEN", "")


def _get_nifi_auth(runtime):
    return get_nifi_auth(runtime)


def _runtime_url(runtime):
    return runtime["url"]


def _reconcile_parameter_providers(runtime, runtime_url):
    providers = runtime.get("parameter_providers", [])
    if not providers:
        return []
    return reconcile_parameter_providers(providers, runtime_url, _get_nifi_pat(), nifi_auth=_get_nifi_auth(runtime)) or []


def _reconcile_controller_services(rt, runtime_url):
    """Reconcile all desired controller services on the runtime."""
    services = rt.get("controller_services", [])
    if not services:
        return
    nifi_pat = _get_nifi_pat()
    reconcile_controller_services(services, runtime_url, nifi_pat, nifi_auth=_get_nifi_auth(rt))


def _delete_controller_services(services, runtime_url, nifi_auth=None):
    """Delete controller services explicitly removed from config."""
    if not services:
        return
    nifi_pat = _get_nifi_pat()
    delete_controller_services(services, runtime_url, nifi_pat, nifi_auth=nifi_auth)


def _reconcile_root_pg_controller_services(rt, runtime_url):
    """Reconcile all desired root process group-scoped controller services."""
    services = rt.get("root_pg_controller_services", [])
    if not services:
        return
    nifi_pat = _get_nifi_pat()
    reconcile_root_pg_controller_services(services, runtime_url, nifi_pat, nifi_auth=_get_nifi_auth(rt))


def _format_blocked_entries(entries):
    parts = []
    for entry in entries:
        labels = []
        if entry.get("parameter"):
            labels.append(str(entry["parameter"]))
        if entry.get("name"):
            labels.append(str(entry["name"]))
        if not labels:
            labels.append("<unknown>")
        messages = [str(message) for message in entry.get("messages", [])] or ["blocked"]
        parts.append(f"{' -> '.join(labels)}: {'; '.join(messages)}")
    return "; ".join(parts)


def _reconcile_root_parameter_context(rt, runtime_url, provider_context_names):
    spec = rt.get("root_parameter_context")
    if not spec:
        return
    nifi_pat = _get_nifi_pat()
    reconcile_root_parameter_context(
        spec,
        runtime_url,
        nifi_pat,
        nifi_auth=_get_nifi_auth(rt),
        provider_context_names=provider_context_names,
    )


def _delete_root_pg_controller_services(services, runtime_url, nifi_auth=None):
    """Delete root PG controller services explicitly removed from config."""
    if not services:
        return
    nifi_pat = _get_nifi_pat()
    delete_root_pg_controller_services(services, runtime_url, nifi_pat, nifi_auth=nifi_auth)


def _delete_parameter_providers(providers, runtime_url, nifi_auth=None):
    """Delete parameter providers explicitly removed from config."""
    if not providers:
        return
    nifi_pat = _get_nifi_pat()
    delete_parameter_providers(providers, runtime_url, nifi_pat, nifi_auth=nifi_auth)


def _setup_flow_registries(rt, runtime_url):
    """Provision all Flow Registry Clients declared on a runtime."""
    nifi_pat = _get_nifi_pat()
    nifi_auth = _get_nifi_auth(rt)
    for rc in rt.get("flow_registries", []):
        properties = {k: resolve_value(v) for k, v in rc.get("properties", {}).items()}
        setup_registry(
            rc["name"], properties, runtime_url, nifi_pat,
            type_override=rc.get("type"),
            nifi_auth=nifi_auth,
        )


def _delete_flow_registries(to_delete, runtime_url, rt):
    """Delete Flow Registry Clients that were removed from config."""
    if not to_delete:
        return
    nifi_pat = _get_nifi_pat()
    nifi_auth = _get_nifi_auth(rt)
    configure_nifi(runtime_url, pat=nifi_pat, nifi_auth=nifi_auth)
    for rc in to_delete:
        existing = find_registry_client(rc["name"])
        if existing:
            delete_registry_client(existing)
        else:
            print(f"[registry] '{rc['name']}' not found, skipping delete")


def _default_registry(rt):
    """Return the name of the first Flow Registry Client, or None."""
    registries = rt.get("flow_registries", [])
    return registries[0]["name"] if registries else None


def _reconcile_flows(rt, runtime_url, provider_context_names=None):
    """Checkout all flows, apply parameters, and start flows that have start:true."""
    flows = rt.get("flows", [])
    if not flows:
        return
    nifi_pat = _get_nifi_pat()
    nifi_auth = _get_nifi_auth(rt)
    default_reg = _default_registry(rt)
    groups = defaultdict(list)
    for f in flows:
        groups[f.get("registry", default_reg)].append(f)
    for registry_name, group in groups.items():
        reconcile_flows(group, registry_name, runtime_url, nifi_pat, nifi_auth=nifi_auth)

    if provider_context_names:
        import re
        configure_nifi(runtime_url, pat=nifi_pat, nifi_auth=nifi_auth)
        for flow_spec in flows:
            pattern = flow_spec.get("provided_parameter_contexts")
            if not pattern:
                continue
            pg = find_flow_pg_by_name(flow_spec["name"])
            if pg:
                filtered = [n for n in provider_context_names if re.fullmatch(pattern, n)]
                if filtered:
                    add_inherited_parameter_contexts(pg.id, filtered, pg_name=flow_spec["name"])
            else:
                print(f"[params] PG '{flow_spec['name']}' not found — skipping inheritance")

    flows_with_assets = [f for f in flows if f.get("assets")]
    if flows_with_assets:
        configure_nifi(runtime_url, pat=nifi_pat, nifi_auth=nifi_auth)
        for flow_spec in flows_with_assets:
            pg = find_flow_pg_by_name(flow_spec["name"])
            if pg:
                reconcile_flow_assets(pg.id, flow_spec["assets"], pg_name=flow_spec["name"])
            else:
                print(f"[assets] PG '{flow_spec['name']}' not found — skipping assets")

    flows_with_params = [f for f in flows if f.get("parameters")]
    if flows_with_params:
        configure_nifi(runtime_url, pat=nifi_pat, nifi_auth=nifi_auth)
        for flow_spec in flows_with_params:
            pg = find_flow_pg_by_name(flow_spec["name"])
            if pg:
                reconcile_flow_parameters(pg.id, flow_spec["parameters"], pg_name=flow_spec["name"])
            else:
                print(f"[params] PG '{flow_spec['name']}' not found — skipping parameters")

    flows_with_overrides = [f for f in flows if f.get("parameter_overrides")]
    if flows_with_overrides:
        configure_nifi(runtime_url, pat=nifi_pat, nifi_auth=nifi_auth)
        for flow_spec in flows_with_overrides:
            pg = find_flow_pg_by_name(flow_spec["name"])
            if pg:
                apply_parameter_overrides(pg.id, flow_spec["parameter_overrides"], pg_name=flow_spec["name"])
            else:
                print(f"[params] PG '{flow_spec['name']}' not found — skipping overrides")

    flows_with_bindings = [f for f in flows if f.get("service_bindings")]
    if flows_with_bindings:
        configure_nifi(runtime_url, pat=nifi_pat, nifi_auth=nifi_auth)
        for flow_spec in flows_with_bindings:
            reconcile_service_bindings(
                flow_spec,
                rt.get("root_pg_controller_services", []),
                runtime_url,
                nifi_pat,
                nifi_auth=nifi_auth,
            )

    flows_to_start = [f for f in flows if f.get("start")]
    if flows_to_start:
        configure_nifi(runtime_url, pat=nifi_pat, nifi_auth=nifi_auth)
        for flow_spec in flows_to_start:
            pg = find_flow_pg_by_name(flow_spec["name"])
            if pg:
                start_flow(pg.id, flow_spec["name"])
            else:
                print(f"[flow] PG '{flow_spec['name']}' not found — skipping start")

    flows_to_stop = [f for f in flows if "start" in f and not f["start"]]
    if flows_to_stop:
        configure_nifi(runtime_url, pat=nifi_pat, nifi_auth=nifi_auth)
        for flow_spec in flows_to_stop:
            pg = find_flow_pg_by_name(flow_spec["name"])
            if pg:
                stop_flow(pg.id, flow_spec["name"])
            else:
                print(f"[flow] PG '{flow_spec['name']}' not found — skipping stop")


def _delete_flows(flows, rt, runtime_url):
    """Delete process groups for flows explicitly removed from config."""
    if not flows:
        return
    nifi_pat = _get_nifi_pat()
    default_reg = _default_registry(rt)
    groups = defaultdict(list)
    for f in flows:
        groups[f.get("registry", default_reg)].append(f)
    for registry_name, group in groups.items():
        delete_flows(group, registry_name, runtime_url, nifi_pat, nifi_auth=_get_nifi_auth(rt))


def apply_runtime_modification(mod):
    rt = mod["new"]
    if rt.get("reconcile") is False:
        return
    runtime_url = _runtime_url(rt)
    blocked_bindings = mod.get("service_binding_changes", {}).get("blocked", [])
    if blocked_bindings:
        details = "; ".join(
            f"{entry['flow']} / {entry['target']}: {'; '.join(entry.get('messages', []))}"
            for entry in blocked_bindings
        )
        raise RuntimeError(f"Service binding live-state read failed; refusing to apply runtime '{rt['name']}': {details}")

    blocked_root_parameter_context = mod.get("root_parameter_context_changes", {}).get("blocked", [])
    if blocked_root_parameter_context:
        raise RuntimeError(
            "Root parameter context live-state read failed; refusing to apply runtime "
            f"'{rt['name']}': {_format_blocked_entries(blocked_root_parameter_context)}"
        )

    _setup_flow_registries(rt, runtime_url)
    _reconcile_controller_services(rt, runtime_url)
    pp_context_names = _reconcile_parameter_providers(rt, runtime_url)
    _reconcile_root_parameter_context(rt, runtime_url, pp_context_names)
    _reconcile_root_pg_controller_services(rt, runtime_url)
    _reconcile_flows(rt, runtime_url, provider_context_names=pp_context_names)

    flow_changes = mod.get("flow_changes", {})
    if flow_changes.get("deleted"):
        _delete_flows(flow_changes["deleted"], rt, runtime_url)

    pp_changes = mod.get("parameter_provider_changes", {})
    if pp_changes.get("deleted"):
        _delete_parameter_providers(pp_changes["deleted"], runtime_url, nifi_auth=_get_nifi_auth(rt))

    cs_changes = mod.get("controller_service_changes", {})
    if cs_changes.get("deleted"):
        _delete_controller_services(cs_changes["deleted"], runtime_url, nifi_auth=_get_nifi_auth(rt))

    root_pg_cs_changes = mod.get("root_pg_controller_service_changes", {})
    if root_pg_cs_changes.get("deleted"):
        _delete_root_pg_controller_services(root_pg_cs_changes["deleted"], runtime_url, nifi_auth=_get_nifi_auth(rt))

    reg_changes = mod.get("flow_registry_changes", {})
    if reg_changes.get("deleted"):
        _delete_flow_registries(reg_changes["deleted"], runtime_url, rt)



def orchestrate(changes_path, config_path):
    load_config(config_path)
    with open(changes_path) as handle:
        changes = json.load(handle)
    errors = []
    for runtime in changes.get("runtimes", {}).get("modified", []):
        try:
            apply_runtime_modification(runtime)
        except Exception as exc:
            errors.append(f"Runtime {runtime['name']} reconciliation failed: {format_safe_exception(exc)}")
    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        sys.exit(1)
    print("[orchestrate] All NiFi resources reconciled successfully")


def main():
    parser = argparse.ArgumentParser(description="Reconcile resources on existing NiFi instances")
    parser.add_argument("changes", help="Path to translated live diff JSON")
    parser.add_argument("config", help="Path to environment YAML")
    args = parser.parse_args()
    orchestrate(args.changes, args.config)


if __name__ == "__main__":
    main()
