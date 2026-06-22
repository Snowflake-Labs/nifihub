#!/usr/bin/env python3
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

"""Local CD runner — executes the full describe→diff→translate→orchestrate pipeline.

Runs the same Python scripts used by the GitHub Actions Environment CD workflow,
allowing you to apply config changes locally without pushing to main or waiting
for a workflow trigger.

Typical usage:

    # Apply changes (Snowflake-managed runtime)
    export SNOWFLAKE_ACCOUNT_URL=https://myorg.snowflakecomputing.com
    export SNOWFLAKE_USER=myuser
    export SNOWFLAKE_PAT=pat-token
    export SNOWFLAKE_ROLE=OPENFLOW_ADMIN
    export NIFI_RUNTIME_PAT=nifi-pat
    export NIFIHUB_REGISTRY_PAT=ghp_...
    python scripts/run-cd.py environments/demo/config.yaml

    # Preview changes without applying (dry run)
    python scripts/run-cd.py environments/demo/config.yaml --dry-run

    # Apply against a local Apache NiFi using username/password auth
    # (SNOWFLAKE_* vars still needed for describe_live_state even if no SQL runs)
    export GH_SECRETS_JSON='{"NIFI_PASSWORD":"admin","NIFIHUB_REGISTRY_PAT":"ghp_..."}'
    export GH_VARS_JSON='{"NIFI_USERNAME":"admin"}'
    python scripts/run-cd.py environments/local/config.yaml

Required environment variables:
    SNOWFLAKE_ACCOUNT_URL   Snowflake account URL (required even for non-SOM runtimes,
                            used by describe_live_state.py to list connectors)
    SNOWFLAKE_USER          Snowflake username
    SNOWFLAKE_PAT           Snowflake Programmatic Access Token
    SNOWFLAKE_ROLE          Snowflake role (e.g. OPENFLOW_ADMIN)
    NIFI_RUNTIME_PAT        NiFi Bearer token — not required if nifi_auth is set
                            in config.yaml with type: username_password
    NIFIHUB_REGISTRY_PAT    GitHub PAT for Flow Registry Clients

Optional environment variables:
    GH_SECRETS_JSON         JSON object of secrets (for ${{ secrets.NAME }} resolution)
    GH_VARS_JSON            JSON object of variables (for ${{ vars.NAME }} resolution)
"""
import argparse
import json
import os
import subprocess
import sys
import tempfile


def _run(args, stdout=None):
    result = subprocess.run(args, stdout=stdout, stderr=sys.stderr)
    if result.returncode != 0:
        sys.exit(result.returncode)
    return result


def main():
    parser = argparse.ArgumentParser(
        description="Run the NiFi Hub CD pipeline locally against an environments config.yaml.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("config", help="Path to environments/<name>/config.yaml")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute and print the change plan without applying it.",
    )
    args = parser.parse_args()

    config_path = os.path.abspath(args.config)
    if not os.path.exists(config_path):
        print(f"Error: config file not found: {config_path}", file=sys.stderr)
        sys.exit(1)

    cd_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "cd"))

    # Provide defaults for optional env vars so scripts don't crash on KeyError
    os.environ.setdefault("GH_SECRETS_JSON", "{}")
    os.environ.setdefault("GH_VARS_JSON", "{}")

    with tempfile.TemporaryDirectory() as tmp:
        live_state = os.path.join(tmp, "live-state.json")
        live_diff = os.path.join(tmp, "live-diff.json")
        changes = os.path.join(tmp, "changes.json")

        # Step 1: Describe live state (queries Snowflake + NiFi REST API)
        print("[run-cd] Step 1/4: Describing live state...", file=sys.stderr)
        with open(live_state, "w") as out:
            _run(
                [sys.executable, os.path.join(cd_dir, "describe_live_state.py"), config_path],
                stdout=out,
            )

        # Step 2: Diff live state against desired config
        print("[run-cd] Step 2/4: Computing diff...", file=sys.stderr)
        with open(live_diff, "w") as out:
            _run(
                [sys.executable, os.path.join(cd_dir, "diff_live.py"), live_state, config_path],
                stdout=out,
            )

        # Step 3: Translate diff to orchestrate.py changes format
        print("[run-cd] Step 3/4: Translating diff...", file=sys.stderr)
        with open(changes, "w") as out:
            _run(
                [sys.executable, os.path.join(cd_dir, "translate_live_diff.py"), live_diff],
                stdout=out,
            )

        if args.dry_run:
            print("[run-cd] Dry run — change plan (no changes applied):", file=sys.stderr)
            with open(changes) as f:
                print(json.dumps(json.load(f), indent=2))
            print("[run-cd] Done (dry run).", file=sys.stderr)
            return

        # Step 4: Apply changes
        print("[run-cd] Step 4/4: Applying changes...", file=sys.stderr)
        _run(
            [sys.executable, os.path.join(cd_dir, "orchestrate.py"), changes, config_path],
        )
        print("[run-cd] Done.", file=sys.stderr)


if __name__ == "__main__":
    main()
