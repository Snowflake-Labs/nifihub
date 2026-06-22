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
import argparse
import json
import os
import sys
import traceback
from collections import defaultdict

from manage_deployment import (
    create_deployment, alter_deployment, delete_deployment, describe_deployment, snow_sql
)
from manage_eai import create_runtime_eai, delete_runtime_eai, eai_name_for_runtime, namespaced_nr_name
from manage_eai import create_network_rule, alter_network_rule, drop_network_rule, drop_eai
from manage_runtime import (
    create_runtime, alter_runtime, delete_runtime, describe_runtime,
    suspend_runtime, resume_runtime
)
from manage_parameters import resolve_value
from setup_registry_client import setup as setup_registry, find_registry_client, delete_registry_client
from manage_flows import reconcile_flows, delete_flows, find_flow_pg_by_name, configure_nifi, start_flow, stop_flow
from manage_parameters import reconcile_flow_parameters, add_inherited_parameter_contexts, apply_parameter_overrides
from manage_assets import reconcile_flow_assets
from manage_controller_services import reconcile_controller_services, delete_controller_services
from manage_parameter_providers import reconcile_parameter_providers, delete_parameter_providers, fetch_auto_provisioned_provider
from manage_connectors import (
    create_connector, connector_exists, describe_connector,
    apply_connector_config, get_connector_config, put_connector_config,
    upload_connector_asset, download_asset, start_connector, stop_connector,
    delete_connector, get_connector_config_uri, add_live_version,
    commit_connector, wait_for_connector
)


def get_conn():
    return {
        "account_url": os.environ.get("SNOWFLAKE_ACCOUNT_URL", ""),
        "pat": os.environ.get("SNOWFLAKE_PAT", ""),
        "user": os.environ.get("SNOWFLAKE_USER", ""),
        "role": os.environ.get("SNOWFLAKE_ROLE", "OPENFLOW_ADMIN"),
    }


def get_nifi_conn():
    return {
        "runtime_url": os.environ.get("NIFI_RUNTIME_URL", ""),
        "nifi_pat": os.environ.get("NIFI_RUNTIME_PAT", ""),
    }


def _get_nifi_pat():
    """Return NIFI_RUNTIME_PAT env var, or empty string if not set.
    When nifi_auth is configured on a runtime (username/password mode), this
    value is unused and it is safe for it to be absent.
    """
    return os.environ.get("NIFI_RUNTIME_PAT", "")


def _get_nifi_auth(rt):
    """Resolve and return the nifi_auth dict for a runtime, or None.

    When nifi_auth is present, credentials are resolved from GH_SECRETS_JSON /
    GH_VARS_JSON so that ${{ secrets.X }} / ${{ vars.X }} syntax works.
    """
    nifi_auth = rt.get("nifi_auth")
    if not nifi_auth:
        return None
    return {
        "type": nifi_auth["type"],
        "username": resolve_value(nifi_auth["username"]),
        "password": resolve_value(nifi_auth["password"]),
        "verify_ssl": nifi_auth.get("verify_ssl", True),
    }


def deployment_exists(name, conn):
    try:
        desc = describe_deployment(name, **conn)
        return desc is not None
    except Exception:
        return False


def runtime_exists(name, database, schema, conn):
    try:
        desc = describe_runtime(name, database, schema, **conn)
        return desc is not None
    except Exception:
        return False


def get_runtime_url(database, schema, runtime_name, account_url="", **conn):
    rows = snow_sql(
        f"DESCRIBE OPENFLOW RUNTIME {database}.{schema}.{runtime_name}",
        account_url=account_url, **conn
    )
    if not rows:
        return ""
    row = rows[0] if isinstance(rows, list) else rows
    server_url = row.get("server_url") or row.get("SERVER_URL")
    if server_url:
        url = server_url.rstrip("/")
        if url.endswith("/nifi"):
            url = url[:-5] + "/nifi-api"
        else:
            url = url + "/nifi-api"
        return url
    key = row.get("key") or row.get("KEY")
    if not key:
        return ""
    host = account_url.replace("https://", "").replace("http://", "").split(".snowflakecomputing.com")[0]
    account_id = host.lower().replace("_", "-")
    return f"https://of--{account_id}.snowflakecomputing.app/{key}/nifi-api"


def _has_som_api(rt):
    """Return True if this runtime is managed via SOM SQL API (no explicit url)."""
    return not rt.get("url")



def _runtime_url(rt, conn):
    """Return the NiFi runtime URL from config if present, otherwise query via SQL."""
    url = rt.get("url", "")
    if url:
        if not url.rstrip("/").endswith("/nifi-api"):
            url = url.rstrip("/") + "/nifi-api"
        return url
    return get_runtime_url(rt["database"], rt["schema"], rt["name"], **conn)


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


def _reconcile_parameter_providers(rt, runtime_url):
    """Reconcile all desired parameter providers on the runtime, plus the auto-provisioned one."""
    providers = rt.get("parameter_providers", [])
    nifi_pat = _get_nifi_pat()
    nifi_auth = _get_nifi_auth(rt)
    from manage_flows import configure_nifi
    configure_nifi(runtime_url, pat=nifi_pat, nifi_auth=nifi_auth)
    all_context_names = []
    if providers:
        names = reconcile_parameter_providers(providers, runtime_url, nifi_pat, nifi_auth=nifi_auth)
        if names:
            all_context_names.extend(names)
    sensitive_pattern = rt.get("sensitive_param_pattern", ".*")
    auto_names = fetch_auto_provisioned_provider(sensitive_pattern=sensitive_pattern)
    if auto_names:
        all_context_names.extend(auto_names)
    return all_context_names


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


def _reconcile_connectors(rt, conn):
    connectors = rt.get("connectors", [])
    if not connectors:
        return
    database = rt["database"]
    schema = rt["schema"]
    runtime_name = rt["name"]

    for c in connectors:
        name = c["name"]
        definition = c["definition"]
        is_new = not connector_exists(name, database, schema, **conn)

        if is_new:
            create_connector(
                name, runtime_name, database, schema, definition,
                display_name=c.get("display_name"),
                comment=c.get("comment"),
                **conn
            )
            wait_for_connector(name, database, schema, "STOPPED", **conn)
        else:
            desc = describe_connector(name, database, schema, **conn)
            status = (desc.get("status") or desc.get("STATUS")) if desc else None
            if status in ("STARTING", "CREATING", "STOPPING"):
                print(f"[connector] {name} is in {status} state — skipping reconciliation")
                continue
            if status == "RUNNING":
                stop_connector(name, database, schema, **conn)
            live_uri = get_connector_config_uri(name, database, schema, **conn)
            if not live_uri:
                add_live_version(name, database, schema, **conn)

        config_json = get_connector_config(name, database, schema, **conn)
        if not config_json:
            print(f"[connector] WARNING: Could not retrieve config for {name}")
            continue

        params = c.get("parameters", {})
        resolved_params = {}
        for k, v in params.items():
            resolved_params[k] = resolve_value(v) if v else v

        for asset in c.get("assets", []):
            if asset.get("parameter"):
                resolved_params[asset["parameter"]] = asset["name"]

        config_json = apply_connector_config(config_json, resolved_params)

        for asset in c.get("assets", []):
            import tempfile as _tf
            tmp_dir = _tf.mkdtemp()
            local_path = os.path.join(tmp_dir, asset["name"])
            download_asset(asset["url"], local_path)
            upload_connector_asset(name, database, schema, local_path, asset["name"], **conn)

        put_connector_config(name, database, schema, config_json, **conn)
        commit_connector(name, database, schema, **conn)
        wait_for_connector(name, database, schema, "STOPPED", **conn)

        if c.get("start"):
            start_connector(name, database, schema, **conn)
        elif "start" in c and not c["start"]:
            stop_connector(name, database, schema, **conn)


def _delete_connectors(connectors, database, schema, conn):
    if not connectors:
        return
    for c in connectors:
        if connector_exists(c["name"], database, schema, **conn):
            stop_connector(c["name"], database, schema, **conn)
            delete_connector(c["name"], database, schema, **conn)
        else:
            print(f"[connector] {c['name']} not found — skipping delete")


def apply_deployment_creates(created_deps, conn, errors):
    for dep in created_deps:
        runtimes = dep.get("runtimes_to_create", [])
        som_needed = any(_has_som_api(rt) for rt in runtimes)
        if som_needed:
            create_deployment(
                dep["name"], dep.get("deployment_type", "SNOWFLAKE"),
                display_name=dep.get("display_name"),
                comment=dep.get("comment"),
                **conn
            )
        else:
            print(f"[orchestrate] Deployment {dep['name']} has only URL-managed runtimes — skipping SOM create")
        for rt in runtimes:
            try:
                apply_runtime_create(dep["name"], rt, conn)
            except Exception as e:
                msg = f"Runtime {rt['name']} create failed: {e}"
                print(f"[orchestrate] ERROR: {msg}")
                traceback.print_exc()
                errors.append(msg)


def apply_runtime_create(deployment_name, rt, conn):
    database = rt["database"]
    schema = rt["schema"]

    if _has_som_api(rt):
        eai = create_runtime_eai(
            rt["name"], rt.get("network_rules", []),
            database, schema, execute_as_role=rt.get("execute_as_role"), **conn
        )
        create_runtime(
            rt["name"], deployment_name, database, schema,
            rt["node_type"], rt["min_nodes"], rt["max_nodes"],
            rt["execute_as_role"],
            eai_names=[eai],
            display_name=rt.get("display_name"),
            comment=rt.get("comment"),
            **conn
        )
    else:
        print(f"[orchestrate] Runtime {rt['name']} has explicit URL — skipping SOM API create")

    if rt.get("suspend"):
        if _has_som_api(rt):
            suspend_runtime(rt["name"], database, schema, **conn)
        print(f"[orchestrate] Runtime {rt['name']} has suspend=true — skipping NiFi reconciliation")
        return

    if rt.get("reconcile") is False:
        print(f"[orchestrate] Runtime {rt['name']} has reconcile=false — skipping NiFi reconciliation")
        return

    runtime_url = _runtime_url(rt, conn)
    if not runtime_url:
        print(f"[orchestrate] No runtime URL for {rt['name']} — skipping registry and flow setup")
        return

    _setup_flow_registries(rt, runtime_url)
    _reconcile_controller_services(rt, runtime_url)
    pp_context_names = _reconcile_parameter_providers(rt, runtime_url)
    _reconcile_flows(rt, runtime_url, provider_context_names=pp_context_names)
    _reconcile_connectors(rt, conn)


def apply_deployment_modifications(modified_deps, conn, errors):
    for dep in modified_deps:
        rtc = dep.get("runtime_changes", {})
        all_rts = (rtc.get("created", [])
                   + [m["new"] for m in rtc.get("modified", [])]
                   + rtc.get("deleted", []))
        som_needed = any(_has_som_api(rt) for rt in all_rts)

        if som_needed and not deployment_exists(dep["name"], conn):
            print(f"[orchestrate] Deployment {dep['name']} not found — falling back to CREATE")
            all_create = rtc.get("created", []) + [m["new"] for m in rtc.get("modified", [])]
            fallback = {**dep, "runtimes_to_create": all_create}
            apply_deployment_creates([fallback], conn, errors)
            continue

        if som_needed and dep.get("changed_fields"):
            alter_deployment(dep["name"], dep["changed_fields"], **conn)

        for rt in rtc.get("created", []):
            try:
                apply_runtime_create(dep["name"], rt, conn)
            except Exception as e:
                msg = f"Runtime {rt['name']} create failed: {e}"
                print(f"[orchestrate] ERROR: {msg}")
                traceback.print_exc()
                errors.append(msg)

        for mod in rtc.get("modified", []):
            rt_new = mod["new"]
            rt_old = mod.get("old", rt_new)
            try:
                if _has_som_api(rt_new):
                    exists = (
                        runtime_exists(rt_old["name"], rt_old["database"], rt_old["schema"], conn)
                        or runtime_exists(rt_new["name"], rt_new["database"], rt_new["schema"], conn)
                    )
                    if not exists:
                        print(f"[orchestrate] Runtime {rt_new['name']} not found — falling back to CREATE")
                        apply_runtime_create(dep["name"], rt_new, conn)
                        continue
                apply_runtime_modification(mod, conn)
            except Exception as e:
                msg = f"Runtime {rt_new['name']} modify failed: {e}"
                print(f"[orchestrate] ERROR: {msg}")
                traceback.print_exc()
                errors.append(msg)

        for rt in rtc.get("deleted", []):
            try:
                if _has_som_api(rt):
                    if not runtime_exists(rt["name"], rt["database"], rt["schema"], conn):
                        print(f"[orchestrate] Runtime {rt['name']} not found — skipping delete")
                        continue
                else:
                    print(f"[orchestrate] Runtime {rt['name']} has explicit URL — skipping SOM API delete (NiFi cleanup only)")
                _delete_connectors(rt.get("connectors", []), rt["database"], rt["schema"], conn)
                runtime_url = _runtime_url(rt, conn)
                if runtime_url:
                    _delete_flows(rt.get("flows", []), rt, runtime_url)
                    _delete_parameter_providers(rt.get("parameter_providers", []), runtime_url, nifi_auth=_get_nifi_auth(rt))
                    _delete_controller_services(rt.get("controller_services", []), runtime_url, nifi_auth=_get_nifi_auth(rt))
                if _has_som_api(rt):
                    delete_runtime(rt["name"], rt["database"], rt["schema"], **conn)
                    delete_runtime_eai(
                        rt["name"], rt.get("network_rules", []),
                        database=rt["database"], schema=rt["schema"], **conn
                    )
            except Exception as e:
                msg = f"Runtime {rt['name']} delete failed: {e}"
                print(f"[orchestrate] ERROR: {msg}")
                traceback.print_exc()
                errors.append(msg)


def apply_runtime_modification(mod, conn):
    rt = mod["new"]
    database = rt["database"]
    schema = rt["schema"]
    som = _has_som_api(rt)
    suspend_change = None

    if som:
        nr_changes = mod.get("network_rule_changes", {})
        nr_created_or_deleted = any(nr_changes.get(k) for k in ("created", "deleted"))
        for nr in nr_changes.get("created", []):
            create_network_rule(namespaced_nr_name(rt["name"], nr["name"]), nr["type"], nr["mode"], nr["values"],
                               database, schema, **conn)
        for nr in nr_changes.get("deleted", []):
            drop_network_rule(namespaced_nr_name(rt["name"], nr["name"]), database, schema, **conn)
        for nr_mod in nr_changes.get("modified", []):
            nr_new = nr_mod["new"]
            alter_network_rule(namespaced_nr_name(rt["name"], nr_new["name"]), nr_new["values"],
                               database, schema, **conn)
        if nr_created_or_deleted:
            create_runtime_eai(
                rt["name"], rt.get("network_rules", []),
                database, schema, execute_as_role=rt.get("execute_as_role"), **conn
            )
            eai = eai_name_for_runtime(rt["name"])
            alter_runtime(rt["name"], database, schema, {},
                          eai_names=[eai], **conn)

        changed_fields = mod.get("changed_fields", {})
        suspend_change = changed_fields.pop("suspend", None)

        if suspend_change and not suspend_change["new"]:
            resume_runtime(rt["name"], database, schema, **conn)

        if changed_fields:
            if rt.get("network_rules") or nr_created_or_deleted:
                create_runtime_eai(
                    rt["name"], rt.get("network_rules", []),
                    database, schema, execute_as_role=rt.get("execute_as_role"), **conn
                )
            eai = eai_name_for_runtime(rt["name"])
            alter_runtime(rt["name"], database, schema, changed_fields,
                          eai_names=[eai], **conn)

    runtime_url = _runtime_url(rt, conn)
    if not runtime_url:
        print(f"[orchestrate] No runtime URL for {rt['name']} — skipping registry and flow updates")
        return

    if rt.get("suspend"):
        print(f"[orchestrate] Runtime {rt['name']} has suspend=true — skipping NiFi reconciliation")
        return

    _setup_flow_registries(rt, runtime_url)
    _reconcile_controller_services(rt, runtime_url)
    pp_context_names = _reconcile_parameter_providers(rt, runtime_url)
    _reconcile_flows(rt, runtime_url, provider_context_names=pp_context_names)
    _reconcile_connectors(rt, conn)

    flow_changes = mod.get("flow_changes", {})
    if flow_changes.get("deleted"):
        _delete_flows(flow_changes["deleted"], rt, runtime_url)

    pp_changes = mod.get("parameter_provider_changes", {})
    if pp_changes.get("deleted"):
        _delete_parameter_providers(pp_changes["deleted"], runtime_url, nifi_auth=_get_nifi_auth(rt))

    cs_changes = mod.get("controller_service_changes", {})
    if cs_changes.get("deleted"):
        _delete_controller_services(cs_changes["deleted"], runtime_url, nifi_auth=_get_nifi_auth(rt))

    reg_changes = mod.get("flow_registry_changes", {})
    if reg_changes.get("deleted"):
        _delete_flow_registries(reg_changes["deleted"], runtime_url, rt)

    connector_changes = mod.get("connector_changes", {})
    if connector_changes.get("deleted"):
        _delete_connectors(connector_changes["deleted"], database, schema, conn)

    if suspend_change and suspend_change["new"]:
        suspend_runtime(rt["name"], database, schema, **conn)


def apply_deployment_deletes(deleted_deps, conn, errors):
    for dep in deleted_deps:
        for rt in dep.get("runtimes_to_delete", []):
            try:
                runtime_url = _runtime_url(rt, conn)
                if runtime_url:
                    _delete_flows(rt.get("flows", []), rt, runtime_url)
                    _delete_parameter_providers(rt.get("parameter_providers", []), runtime_url, nifi_auth=_get_nifi_auth(rt))
                    _delete_controller_services(rt.get("controller_services", []), runtime_url, nifi_auth=_get_nifi_auth(rt))
                _delete_connectors(rt.get("connectors", []), rt["database"], rt["schema"], conn)

                if _has_som_api(rt):
                    delete_runtime(rt["name"], rt["database"], rt["schema"], **conn)
                    delete_runtime_eai(
                        rt["name"], rt.get("network_rules", []),
                        database=rt["database"], schema=rt["schema"], **conn
                    )
            except Exception as e:
                msg = f"Runtime {rt['name']} delete failed: {e}"
                print(f"[orchestrate] ERROR: {msg}")
                traceback.print_exc()
                errors.append(msg)

        som_rts = [rt for rt in dep.get("runtimes_to_delete", []) if _has_som_api(rt)]
        if som_rts:
            delete_deployment(dep["name"], **conn)
        else:
            print(f"[orchestrate] Deployment {dep['name']} has only URL-managed runtimes — skipping deployment delete")


def orchestrate(changes_path, config_path):
    with open(changes_path) as f:
        changes = json.load(f)

    conn = get_conn()
    deployments = changes.get("deployments", {})
    errors = []

    apply_deployment_creates(deployments.get("created", []), conn, errors)
    apply_deployment_modifications(deployments.get("modified", []), conn, errors)
    apply_deployment_deletes(deployments.get("deleted", []), conn, errors)

    if errors:
        print(f"\n[orchestrate] Completed with {len(errors)} error(s):")
        for i, err in enumerate(errors, 1):
            print(f"  {i}. {err}")
        sys.exit(1)
    else:
        print("[orchestrate] All changes applied successfully")


def main():
    parser = argparse.ArgumentParser(description="Orchestrate environment changes")
    parser.add_argument("changes", help="Path to changes JSON from diff_environments.py")
    parser.add_argument("config", help="Path to current config.yaml")
    args = parser.parse_args()

    orchestrate(args.changes, args.config)


if __name__ == "__main__":
    main()