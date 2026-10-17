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
"""Read resource state from the NiFi instances explicitly listed in an environment."""
from contextlib import redirect_stdout
import json
import os
import sys

from describe_nifi_state import describe_nifi_state
from nifi_config import get_nifi_auth, load_config
from safe_exceptions import format_safe_exception


def build_live_state(config_path):
    config = load_config(config_path)
    state = {"runtimes": []}
    for runtime in config["runtimes"]:
        entry = {"name": runtime["name"], "url": runtime["url"]}
        if runtime.get("reconcile") is False:
            entry["nifi"] = {"skipped": True}
        else:
            try:
                entry["nifi"] = describe_nifi_state(
                    runtime["url"],
                    pat=os.environ.get("NIFI_TOKEN", ""),
                    nifi_auth=get_nifi_auth(runtime),
                    desired_flows=runtime.get("flows", []),
                    desired_root_parameter_context=runtime.get("root_parameter_context"),
                )
            except Exception as exc:
                entry["nifi"] = {"error": format_safe_exception(exc)}
        state["runtimes"].append(entry)
    return state


def main():
    if len(sys.argv) != 2:
        sys.exit("Usage: describe_live_state.py <config.yaml>")
    with redirect_stdout(sys.stderr):
        state = build_live_state(sys.argv[1])
    json.dump(state, sys.stdout, indent=2)
    print()
    if any(rt["nifi"].get("error") for rt in state["runtimes"]):
        sys.exit(1)


if __name__ == "__main__":
    main()
