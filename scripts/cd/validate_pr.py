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
import subprocess
import sys
import tempfile

import yaml
from jsonschema import validate, ValidationError

from config_runtime_mode import all_configured_runtimes_are_url_managed


def _parse_account(account_url):
    host = account_url.replace("https://", "").replace("http://", "").rstrip("/")
    account = host.split(".snowflakecomputing.com")[0] if ".snowflakecomputing.com" in host else host
    return account, host


def snow_sql(sql, account_url, pat, user, role, timeout=120):
    account, host = _parse_account(account_url)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".token", delete=False) as f:
        f.write(pat.strip())
        token_file = f.name
    try:
        cmd = [
            "snow", "sql", "-q", sql,
            "-x",
            "--account", account,
            "--host", host,
            "--user", user,
            "--authenticator", "PROGRAMMATIC_ACCESS_TOKEN",
            "--token-file-path", token_file,
            "--role", role,
            "--format", "json",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    finally:
        os.unlink(token_file)
    if result.returncode != 0:
        return None, result.stderr
    if result.stdout.strip():
        return json.loads(result.stdout), None
    return [], None


def validate_schema(config_path, schema_path):
    with open(schema_path) as f:
        schema = json.load(f)
    with open(config_path) as f:
        data = yaml.safe_load(f)
    errors = []
    try:
        validate(instance=data, schema=schema)
    except ValidationError as e:
        errors.append(e.message)
    return data, errors


def check_connectivity(conn):
    rows, err = snow_sql("SELECT 1 AS connected", **conn)
    if err:
        return False, err
    return True, None


def describe_deployment(name, conn):
    rows, err = snow_sql(f"DESCRIBE OPENFLOW DEPLOYMENT {name}", **conn)
    if err:
        return None, err
    if rows and isinstance(rows, list):
        return rows[0], None
    return rows, None


def list_runtimes(conn):
    rows, err = snow_sql("SHOW OPENFLOW RUNTIMES IN ACCOUNT", **conn)
    if err:
        return [], err
    return rows or [], None


def check_duplicate_runtimes(config_data, existing_runtimes, old_config_data=None):
    old_rt_fqns = set()
    if old_config_data:
        for dep in old_config_data.get("deployments", []):
            for rt in dep.get("runtimes", []):
                old_rt_fqns.add(f"{rt['database']}.{rt['schema']}.{rt['name']}".upper())

    issues = []
    for dep in config_data.get("deployments", []):
        for rt in dep.get("runtimes", []):
            rt_fqn = f"{rt['database']}.{rt['schema']}.{rt['name']}"
            if rt_fqn.upper() in old_rt_fqns:
                continue
            for existing in existing_runtimes:
                existing_fqn = f"{existing.get('database_name', '')}.{existing.get('schema_name', '')}.{existing.get('name', '')}"
                if rt_fqn.upper() == existing_fqn.upper():
                    status = existing.get("status", "UNKNOWN")
                    if status not in ("DELETED",):
                        issues.append(
                            f"Runtime `{rt_fqn}` already exists on the account (status: {status}). "
                            f"It should not be added in the YAML if it already exists."
                        )
    return issues


def format_deployment_table(desc):
    if not desc:
        return "No deployment found."
    field_map = [
        ("Name", "name"),
        ("Type", "type"),
        ("Status", "status"),
        ("Display Name", "display_name"),
        ("Owner", "owner"),
        ("Comment", "comment"),
        ("Private Link", "use_private_link"),
    ]
    rows = []
    for label, key in field_map:
        val = desc.get(key, "—")
        if val is None:
            val = "—"
        rows.append(f"| {label} | {val} |")
    return "| Property | Value |\n|----------|-------|\n" + "\n".join(rows)


def format_runtimes_table(runtimes):
    if not runtimes:
        return "No runtimes found on this account."
    header = "| Name | Database | Schema | Status | Deployment | Node Type | Nodes (min/max) |\n"
    header += "|------|----------|--------|--------|------------|-----------|------------------|\n"
    rows = []
    for rt in runtimes:
        name = rt.get("name", rt.get("NAME", "—"))
        db = rt.get("database_name", rt.get("DATABASE_NAME", "—"))
        schema = rt.get("schema_name", rt.get("SCHEMA_NAME", "—"))
        status = rt.get("status", rt.get("STATUS", "—"))
        dep = rt.get("deployment", rt.get("DEPLOYMENT", "—"))
        node_type = rt.get("node_type", rt.get("NODE_TYPE", "—"))
        min_n = rt.get("min_nodes", rt.get("MIN_NODES", "—"))
        max_n = rt.get("max_nodes", rt.get("MAX_NODES", "—"))
        rows.append(f"| {name} | {db} | {schema} | {status} | {dep} | {node_type} | {min_n}/{max_n} |")
    return header + "\n".join(rows)


def main():
    if len(sys.argv) < 3:
        print("Usage: validate_pr.py <config_path> <schema_path> [old_config_path]", file=sys.stderr)
        sys.exit(1)

    config_path = sys.argv[1]
    schema_path = sys.argv[2]
    old_config_path = sys.argv[3] if len(sys.argv) > 3 else None

    old_config_data = None
    if old_config_path:
        try:
            with open(old_config_path) as f:
                old_config_data = yaml.safe_load(f) or {}
        except Exception:
            old_config_data = {}

    sections = []
    has_errors = False

    config_data, schema_errors = validate_schema(config_path, schema_path)
    if schema_errors:
        has_errors = True
        sections.append("### :x: Schema Validation\n\n" + "\n".join(f"- {e}" for e in schema_errors))
    else:
        sections.append("### :white_check_mark: Schema Validation\n\nConfiguration is valid.")

    url_only_config = all_configured_runtimes_are_url_managed(config_data or {})
    conn = None
    connected = False
    if url_only_config:
        sections.append(
            "### :information_source: Snowflake Connectivity\n\n"
            "Skipped Snowflake validation because every configured runtime is URL-managed (`url:` set on each runtime)."
        )
    else:
        conn = {
            "account_url": os.environ["SNOWFLAKE_ACCOUNT_URL"],
            "pat": os.environ["SNOWFLAKE_PAT"],
            "user": os.environ["SNOWFLAKE_USER"],
            "role": os.environ.get("SNOWFLAKE_ROLE", "OPENFLOW_ADMIN"),
        }
        connected, conn_err = check_connectivity(conn)
        if not connected:
            has_errors = True
            err_msg = conn_err.strip().replace("\n", " ") if conn_err else "Unknown error"
            sections.append(f"### :x: Snowflake Connectivity\n\nFailed to connect:\n```\n{err_msg}\n```")
        else:
            sections.append("### :white_check_mark: Snowflake Connectivity\n\nSuccessfully connected to Snowflake.")

    if connected and config_data:
        for dep in config_data.get("deployments", []):
            dep_name = dep.get("name")
            if dep_name:
                desc, dep_err = describe_deployment(dep_name, conn)
                if desc:
                    sections.append(f"### :information_source: Deployment: `{dep_name}`\n\n{format_deployment_table(desc)}")
                elif dep_err and "does not exist" in dep_err:
                    sections.append(f"### :new: Deployment: `{dep_name}`\n\nThis deployment does not exist yet and will be created.")
                else:
                    sections.append(f"### :warning: Deployment: `{dep_name}`\n\nCould not describe deployment.")

        runtimes, rt_err = list_runtimes(conn)
        if rt_err:
            sections.append(f"### :warning: Existing Runtimes\n\nCould not list runtimes:\n```\n{rt_err}\n```")
        else:
            sections.append(f"### :information_source: Existing Runtimes\n\n{format_runtimes_table(runtimes)}")
    elif url_only_config:
        sections.append(
            "### :information_source: Snowflake Inventory\n\n"
            "Skipped deployment and runtime inventory because URL-managed runtimes are reconciled directly through the NiFi API."
        )

    comment_body = "## Environment CD — PR Validation\n\n" + "\n\n".join(sections)
    comment_body += "\n\n---\n> *Auto-generated by Environment CD validation workflow.*"

    with open("/tmp/pr-comment.md", "w") as f:
        f.write(comment_body)

    if has_errors:
        print("Validation failed — see PR comment for details.", file=sys.stderr)
        sys.exit(1)
    else:
        print("Validation passed.")


if __name__ == "__main__":
    main()