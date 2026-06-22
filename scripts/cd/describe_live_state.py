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
import os
import sys

import yaml

from validate_pr import snow_sql
from describe_nifi_state import describe_nifi_state
from manage_connectors import get_connector_config
from manage_flows import configure_nifi


def _resolve_nifi_auth(rt_cfg):
    """Build a resolved nifi_auth dict from a runtime config entry, or None."""
    nifi_auth_cfg = rt_cfg.get("nifi_auth")
    if not nifi_auth_cfg or nifi_auth_cfg.get("type") != "username_password":
        return None
    try:
        from manage_parameters import resolve_value
        return {
            "type": "username_password",
            "username": resolve_value(nifi_auth_cfg["username"]),
            "password": resolve_value(nifi_auth_cfg["password"]),
            "verify_ssl": nifi_auth_cfg.get("verify_ssl", True),
        }
    except Exception:
        return None


def _extract_connector_params(config_json):
    """Extract parameter values from a connector config.json into a flat dict.

    Returns {param_name: value} where value is:
    - The raw string for STRING_LITERAL parameters
    - The fully qualified secret name for SECRET_REFERENCE parameters
    ASSET_REFERENCE parameters are skipped (managed via assets, not parameters).
    """
    if not config_json:
        return {}
    params = {}
    for section in config_json.get("configuration", []):
        for prop_name, prop_obj in section.get("properties", {}).items():
            vtype = prop_obj.get("valueType", "STRING_LITERAL")
            if vtype == "STRING_LITERAL":
                params[prop_name] = prop_obj.get("value", "") or ""
            elif vtype == "SECRET_REFERENCE":
                params[prop_name] = prop_obj.get("fullyQualifiedSecretName", "") or ""
            # ASSET_REFERENCE: skip — assets are compared separately
    return params


def _conn():
    return {
        "account_url": os.environ["SNOWFLAKE_ACCOUNT_URL"],
        "pat": os.environ["SNOWFLAKE_PAT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "role": os.environ.get("SNOWFLAKE_ROLE", "OPENFLOW_ADMIN"),
    }


def _get(row, key):
    return row.get(key) or row.get(key.upper()) or row.get(key.lower())


def _eai_name_for_runtime(runtime_name):
    return f"OPENFLOW_{runtime_name}_EAI"


def list_deployments(conn):
    rows, err = snow_sql("SHOW OPENFLOW DEPLOYMENTS IN ACCOUNT", **conn)
    if err:
        print(f"[live] Error listing deployments: {err}", file=sys.stderr)
        return []
    return rows or []


def describe_deployment(name, conn):
    rows, err = snow_sql(f"DESCRIBE OPENFLOW DEPLOYMENT {name}", **conn)
    if err:
        return None
    if rows and isinstance(rows, list):
        return rows[0]
    return rows


def list_runtimes(conn):
    rows, err = snow_sql("SHOW OPENFLOW RUNTIMES IN ACCOUNT", **conn)
    if err:
        print(f"[live] Error listing runtimes: {err}", file=sys.stderr)
        return []
    return rows or []


def describe_runtime(name, database, schema, conn):
    fqn = f"{database}.{schema}.{name}"
    rows, err = snow_sql(f"DESCRIBE OPENFLOW RUNTIME {fqn}", **conn)
    if err:
        return None
    if rows and isinstance(rows, list):
        return rows[0]
    return rows


def list_connectors(conn):
    rows, err = snow_sql("SHOW OPENFLOW CONNECTORS IN ACCOUNT", **conn)
    if err:
        print(f"[live] Error listing connectors: {err}", file=sys.stderr)
        return []
    return rows or []


def describe_eai(eai_name, conn):
    rows, err = snow_sql(f"DESCRIBE EXTERNAL ACCESS INTEGRATION {eai_name}", **conn)
    if err:
        return None
    return rows or []


def describe_network_rule(fqn, conn):
    rows, err = snow_sql(f"DESCRIBE NETWORK RULE {fqn}", **conn)
    if err:
        return None
    if rows and isinstance(rows, list):
        return rows[0]
    return rows


def get_network_rules_for_runtime(runtime_name, database, schema, conn):
    eai_name = _eai_name_for_runtime(runtime_name)
    eai_rows = describe_eai(eai_name, conn)
    if not eai_rows:
        return []

    allowed_nr_str = ""
    for row in eai_rows:
        prop = _get(row, "property") or ""
        if prop == "ALLOWED_NETWORK_RULES":
            allowed_nr_str = _get(row, "property_value") or ""
            break

    if not allowed_nr_str:
        return []

    nr_fqns = [nr.strip() for nr in allowed_nr_str.split(",") if nr.strip()]

    prefix = f"{runtime_name.upper()}_"
    internal_nr_name = f"OPENFLOW_{runtime_name.upper()}_NR"
    # Registry NR is now namespaced per runtime; also skip legacy un-namespaced form
    namespaced_registry_nr = f"{database}.{schema}.{prefix}OPENFLOW_NIFIHUB_REGISTRY_NR"
    legacy_registry_nr = f"{database}.{schema}.OPENFLOW_NIFIHUB_REGISTRY_NR"

    rules = []
    for nr_fqn in nr_fqns:
        parts = nr_fqn.split(".")
        nr_name = parts[-1] if parts else nr_fqn
        if nr_name.upper() == internal_nr_name.upper():
            continue
        if nr_fqn.upper() in (namespaced_registry_nr.upper(), legacy_registry_nr.upper()):
            continue
        if not nr_fqn.upper().startswith(f"{database}.{schema}.".upper()):
            continue

        desc = describe_network_rule(nr_fqn, conn)
        if not desc:
            continue

        value_list_str = _get(desc, "value_list") or ""
        values = [v.strip() for v in value_list_str.split(",") if v.strip()]

        # Strip the runtime name prefix to recover the logical name used in the YAML config.
        # Namespaced rules are named {RUNTIME_NAME}_{LOGICAL_NAME}; un-namespaced rules
        # (created before this change) are returned as-is for backward compatibility.
        logical_name = nr_name[len(prefix):] if nr_name.upper().startswith(prefix) else nr_name

        rules.append({
            "name": logical_name,
            "type": _get(desc, "type") or "HOST_PORT",
            "mode": _get(desc, "mode") or "EGRESS",
            "values": values,
        })

    return rules


def build_live_state(config_path, conn):
    with open(config_path) as f:
        config = yaml.safe_load(f) or {}

    all_live_deployments = list_deployments(conn)
    all_live_runtimes = list_runtimes(conn)
    all_live_connectors = list_connectors(conn)

    live_state = {"deployments": []}

    for dep_cfg in config.get("deployments", []):
        dep_name = dep_cfg["name"]

        live_dep = None
        for d in all_live_deployments:
            if (_get(d, "name") or "").upper() == dep_name.upper():
                live_dep = d
                break

        if not live_dep:
            # If all runtimes in this deployment have a 'url' field, the deployment
            # is URL-managed (non-SOM) and won't appear in Snowflake. Synthesize a
            # live entry so the diff can query NiFi directly instead of treating
            # everything as "to_create".
            dep_runtimes_cfg = dep_cfg.get("runtimes", [])
            all_url_managed = dep_runtimes_cfg and all(r.get("url") for r in dep_runtimes_cfg)
            if not all_url_managed:
                continue
            # Build a synthetic live deployment entry representing the current NiFi state
            dep_entry = {
                "name": dep_name,
                "deployment_type": dep_cfg.get("deployment_type", "SNOWFLAKE"),
                "display_name": dep_cfg.get("display_name", ""),
                "comment": dep_cfg.get("comment", ""),
                "status": "ACTIVE",
                "runtimes": [],
            }
            for rt_cfg in dep_runtimes_cfg:
                rt_name = rt_cfg["name"]
                rt_url = rt_cfg["url"]
                runtime_api_url = rt_url.rstrip("/")
                if not runtime_api_url.endswith("/nifi-api"):
                    runtime_api_url += "/nifi-api"
                nifi_auth = _resolve_nifi_auth(rt_cfg)
                nifi_pat = os.environ.get("NIFI_RUNTIME_PAT", "")
                rt_entry = {
                    "name": rt_name,
                    "database": rt_cfg.get("database", ""),
                    "schema": rt_cfg.get("schema", ""),
                    "status": "ACTIVE",
                    "node_type": "",
                    "min_nodes": 0,
                    "max_nodes": 0,
                    "execute_as_role": rt_cfg.get("execute_as_role", ""),
                    "display_name": rt_cfg.get("display_name", ""),
                    "comment": rt_cfg.get("comment", ""),
                    "network_rules": [],
                    "connectors": [],
                }
                if rt_cfg.get("reconcile") is False:
                    rt_entry["nifi"] = None
                else:
                    try:
                        nifi_state = describe_nifi_state(runtime_api_url, pat=nifi_pat, nifi_auth=nifi_auth)
                        rt_entry["nifi"] = nifi_state
                    except Exception as e:
                        print(f"[live] NiFi API error for {rt_name}: {e}", file=sys.stderr)
                        rt_entry["nifi"] = {"error": str(e)}
                dep_entry["runtimes"].append(rt_entry)
            live_state["deployments"].append(dep_entry)
            continue

        dep_entry = {
            "name": _get(live_dep, "name"),
            "deployment_type": _get(live_dep, "type") or "SNOWFLAKE",
            "display_name": _get(live_dep, "display_name") or "",
            "comment": _get(live_dep, "comment") or "",
            "status": _get(live_dep, "status") or "UNKNOWN",
            "runtimes": [],
        }

        dep_runtimes = [
            r for r in all_live_runtimes
            if (_get(r, "deployment") or "").upper() == dep_name.upper()
            and not (_get(r, "name") or "").upper().startswith("CI_")
        ]

        for rt in dep_runtimes:
            rt_name = _get(rt, "name")
            rt_db = _get(rt, "database_name") or _get(rt, "database")
            rt_schema = _get(rt, "schema_name") or _get(rt, "schema")
            rt_status = _get(rt, "status") or "UNKNOWN"

            rt_desc = describe_runtime(rt_name, rt_db, rt_schema, conn)

            rt_entry = {
                "name": rt_name,
                "database": rt_db,
                "schema": rt_schema,
                "status": rt_status,
                "node_type": _get(rt, "node_type") or "",
                "min_nodes": int(_get(rt, "min_nodes") or 0),
                "max_nodes": int(_get(rt, "max_nodes") or 0),
                "execute_as_role": (_get(rt_desc, "execute_as_role") if rt_desc else _get(rt, "execute_as_role")) or "",
                "display_name": _get(rt, "display_name") or "",
                "comment": _get(rt, "comment") or "",
                "network_rules": get_network_rules_for_runtime(rt_name, rt_db, rt_schema, conn),
                "connectors": [],
            }

            rt_connectors = [
                c for c in all_live_connectors
                if (_get(c, "runtime") or "").upper() == rt_name.upper()
                and (_get(c, "database_name") or "").upper() == rt_db.upper()
                and (_get(c, "schema_name") or "").upper() == rt_schema.upper()
            ]

            for c in rt_connectors:
                c_name = _get(c, "name")
                c_database = _get(c, "database_name") or rt_db
                c_schema = _get(c, "schema_name") or rt_schema
                live_params = {}
                try:
                    config_json = get_connector_config(c_name, c_database, c_schema, **conn)
                    live_params = _extract_connector_params(config_json)
                except Exception as e:
                    print(f"[live] Could not fetch config for connector {c_name}: {e}", file=sys.stderr)
                rt_entry["connectors"].append({
                    "name": c_name,
                    "definition": _get(c, "connector_definition") or "",
                    "status": _get(c, "status") or "UNKNOWN",
                    "display_name": _get(c, "display_name") or "",
                    "comment": _get(c, "comment") or "",
                    "parameters": live_params,
                })

            rt_cfg_match = next(
                (r for r in dep_cfg.get("runtimes", []) if r["name"].upper() == rt_name.upper()),
                None
            )
            skip_nifi = rt_cfg_match and rt_cfg_match.get("reconcile") is False

            nifi_pat = os.environ.get("NIFI_RUNTIME_PAT", "")
            nifi_auth = _resolve_nifi_auth(rt_cfg_match) if rt_cfg_match else None
            if skip_nifi:
                rt_entry["nifi"] = None
            elif rt_status.upper() == "ACTIVE" and (nifi_pat or nifi_auth):
                server_url = (_get(rt_desc, "server_url") if rt_desc else None) or ""
                if not server_url and rt_cfg_match:
                    server_url = rt_cfg_match.get("url", "")
                if server_url:
                    runtime_api_url = server_url.rstrip("/")
                    if runtime_api_url.endswith("/nifi"):
                        runtime_api_url = runtime_api_url[:-5] + "/nifi-api"
                    elif not runtime_api_url.endswith("/nifi-api"):
                        runtime_api_url += "/nifi-api"
                    try:
                        nifi_state = describe_nifi_state(runtime_api_url, pat=nifi_pat, nifi_auth=nifi_auth)
                        rt_entry["nifi"] = nifi_state
                    except Exception as e:
                        print(f"[live] NiFi API error for {rt_name}: {e}", file=sys.stderr)
                        rt_entry["nifi"] = {"error": str(e)}
                else:
                    rt_entry["nifi"] = {"error": "No server URL available"}
            else:
                reason = "suspended" if rt_status.upper() == "SUSPENDED" else "no PAT"
                rt_entry["nifi"] = None

            dep_entry["runtimes"].append(rt_entry)

        live_state["deployments"].append(dep_entry)

    return live_state


def main():
    if len(sys.argv) < 2:
        print("Usage: describe_live_state.py <config.yaml>", file=sys.stderr)
        sys.exit(1)

    config_path = sys.argv[1]
    conn = _conn()
    state = build_live_state(config_path, conn)
    json.dump(state, sys.stdout, indent=2)
    print()


if __name__ == "__main__":
    main()