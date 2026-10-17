#!/usr/bin/env python3
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
"""
Clean up a deployed NiFi process group from an Apache NiFi instance.

Sequence: stop PG -> disable controller services -> purge queues -> delete PG.

Usage:
    python scripts/cleanup-flow.py --pg-id <uuid> \
        --runtime-url https://localhost:8443 --token <token>
"""

import argparse
from graphlib import TopologicalSorter
import os
import sys
from pathlib import Path

import nipyapi

sys.path.insert(0, str(Path(__file__).resolve().parent / "cd"))
from manage_flows import configure_nifi
from nifi_config import get_nifi_auth


def cleanup(pg_id, runtime_url, pat=None):
    """Remove a deployed process group from the runtime."""
    configure_nifi(runtime_url, pat=pat, nifi_auth=get_nifi_auth())
    contexts = _test_parameter_contexts(pg_id)

    print("Stopping process group %s ..." % pg_id, file=sys.stderr)
    nipyapi.canvas.schedule_process_group(pg_id, False)

    print("Disabling controller services ...", file=sys.stderr)
    nipyapi.canvas.schedule_all_controllers(pg_id, False)

    print("Purging queues ...", file=sys.stderr)
    nipyapi.canvas.purge_process_group(pg_id)

    print("Deleting process group ...", file=sys.stderr)
    nipyapi.canvas.delete_process_group(pg_id, force=True)

    api = nipyapi.nifi.ParameterContextsApi()
    for context_id in contexts:
        context = api.get_parameter_context(id=context_id)
        api.delete_parameter_context(id=context_id, version=context.revision.version)

    print("Cleanup complete for %s" % pg_id, file=sys.stderr)


def _test_parameter_contexts(pg_id):
    """Collect only contexts owned by this uniquely named CI process group."""
    group = nipyapi.nifi.ProcessGroupsApi().get_process_group(id=pg_id)
    if not group.component.name.startswith("CI_"):
        return []
    prefix = group.component.name + " - "
    api = nipyapi.nifi.ParameterContextsApi()
    contexts = {
        context.id: api.get_parameter_context(id=context.id)
        for context in nipyapi.nifi.FlowApi().get_parameter_contexts().parameter_contexts or []
        if context.component.name.startswith(prefix)
    }
    delete_after = {context_id: set() for context_id in contexts}
    for context_id, context in contexts.items():
        for inherited in context.component.inherited_parameter_contexts or []:
            if inherited.id in delete_after:
                delete_after[inherited.id].add(context_id)
    return list(TopologicalSorter(delete_after).static_order())


def main():
    parser = argparse.ArgumentParser(
        description="Clean up a deployed NiFi process group from an Apache NiFi instance"
    )
    parser.add_argument(
        "--pg-id", required=True, help="Process group UUID to remove"
    )
    parser.add_argument(
        "--runtime-url", default=os.environ.get("NIFI_URL"), required=not os.environ.get("NIFI_URL"), help="Existing Apache NiFi URL"
    )
    parser.add_argument(
        "--token", default=os.environ.get("NIFI_TOKEN", ""), help="NiFi bearer token (or use NIFI_USERNAME/NIFI_PASSWORD)"
    )
    args = parser.parse_args()

    try:
        cleanup(args.pg_id, args.runtime_url, args.token)
    except Exception as exc:
        print("Cleanup failed for %s: %s" % (args.pg_id, exc), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
