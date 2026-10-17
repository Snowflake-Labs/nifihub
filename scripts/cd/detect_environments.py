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
"""Detect changed config environments and output GitHub Actions matrix.

Emits one item per changed config environment, keyed by the configuration's
github_environment field.
"""
import json
import glob
import re
import os
import subprocess
import sys

import yaml


def _last_success_sha():
    """Return the HEAD SHA of the last fully-applied CD run.

    After a failure, the next success may have an empty diff (HEAD~1
    already contained the failed changes). To find a reliable base,
    walk runs newest-first and find a success whose immediate
    predecessor was also a success (meaning its diff was computed
    against a clean state).
    """
    token = os.environ.get("GITHUB_TOKEN", "")
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    if not token or not repo:
        return None
    try:
        result = subprocess.run(
            [
                "gh", "api",
                f"repos/{repo}/actions/workflows/environment-cd.yml/runs",
                "--method", "GET",
                "-f", "branch=main",
                "-f", "per_page=30",
                "--jq", "[.workflow_runs[] | {sha: .head_sha, conclusion: .conclusion}]",
            ],
            capture_output=True,
            text=True,
            timeout=15,
        )
        runs = json.loads(result.stdout.strip())
        if not runs:
            return None

        terminal = [r for r in runs if r.get("conclusion") in ("success", "failure")]
        if not terminal:
            return None

        for i in range(len(terminal)):
            if terminal[i]["conclusion"] != "success":
                continue
            if i + 1 >= len(terminal):
                print(f"[detect] First-ever success: {terminal[i]['sha'][:12]}")
                return terminal[i]["sha"]
            if terminal[i + 1]["conclusion"] == "success":
                print(f"[detect] Clean success (preceded by success): {terminal[i]['sha'][:12]}")
                return terminal[i]["sha"]

        print("[detect] No clean success found — full reconciliation needed")
        return None
    except Exception as e:
        print(f"[detect] Could not query last success: {e}")
    return None


def _diff_base():
    sha = _last_success_sha()
    if sha:
        check = subprocess.run(
            ["git", "cat-file", "-t", sha],
            capture_output=True, text=True,
        )
        if check.returncode == 0:
            return sha
        print(f"[detect] SHA {sha[:12]} not in local history, falling back to HEAD~1")
    return "HEAD~1"


def main(output_path=None):
    requested = os.environ.get("INPUT_ENVIRONMENT", "")
    if requested:
        if not re.fullmatch(r"[A-Za-z0-9_-]+", requested):
            raise ValueError("Invalid environment directory name")
        filepath = f"environments/{requested}/config.yaml"
        if not os.path.isfile(filepath):
            raise ValueError("Requested environment configuration does not exist")
        changed = [filepath]
    elif os.environ.get("MANUAL_RUN") == "true":
        changed = sorted(glob.glob("environments/*/config.yaml"))
    else:
        base = os.environ.get("DIFF_BASE") or _diff_base()
        result = subprocess.run(["git", "diff", "--name-only", base, "HEAD"], capture_output=True, text=True, check=True)
        paths = result.stdout.splitlines()
        shared_changed = any(p == "environments/schema.json" or p.startswith("scripts/cd/") or p == "scripts/run-cd.py" for p in paths)
        changed = sorted(glob.glob("environments/*/config.yaml")) if shared_changed else [p for p in paths if re.fullmatch(r"environments/[^/]+/config\.yaml", p)]

    items = []
    seen = set()

    for filepath in changed:
        parts = filepath.split("/")
        if len(parts) < 3:
            continue
        config_env = parts[1]

        try:
            with open(filepath) as f:
                cfg = yaml.safe_load(f) or {}
        except FileNotFoundError:
            if requested or os.environ.get("MANUAL_RUN") == "true":
                raise
            continue

        github_env = cfg.get("github_environment", config_env)

        if config_env not in seen:
            seen.add(config_env)
            items.append({"config_env": config_env, "github_env": github_env})

    has_changes = len(items) > 0

    lines = [
        f"has_changes={'true' if has_changes else 'false'}",
        f"items={json.dumps(items)}",
    ]
    output = "\n".join(lines) + "\n"

    if output_path:
        with open(output_path, "a") as f:
            f.write(output)
    else:
        print(output, end="")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--base-sha-only":
        base = _diff_base()
        print(base, end="")
    else:
        out = sys.argv[1] if len(sys.argv) > 1 else None
        main(out)
