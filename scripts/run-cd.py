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

"""Run describe → diff → translate → reconcile against existing Apache NiFi instances.

Set NIFI_USERNAME and NIFI_PASSWORD for NiFi login, or NIFI_TOKEN for an existing
bearer token. GH_SECRETS_JSON and GH_VARS_JSON resolve configuration references.
Use --dry-run to inspect the plan without modifying NiFi.
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

        # Step 1: Describe live NiFi resources
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
