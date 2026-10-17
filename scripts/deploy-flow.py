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
Deploy a NiFi flow definition to an Apache NiFi instance.

Uploads the flow JSON as a new process group under root using the
NiFi REST API.

Usage:
    python scripts/deploy-flow.py --flow-path flows/bucket/flow.json \
        --runtime-url https://localhost:8443 --token <token>

The runtime URL should be the instance base path (without /nifi/ or /nifi-api).
"""

import argparse
import json
import os
import sys
from pathlib import Path
import uuid

import nipyapi

sys.path.insert(0, str(Path(__file__).resolve().parent / "cd"))
from manage_flows import configure_nifi
from nifi_config import get_nifi_auth


def _collect_components(pg_id):
    """Fetch processors and controller services from a deployed process group via nipyapi."""
    processors = []
    controller_services = []

    flow_dto = nipyapi.nifi.FlowApi().get_flow(pg_id)
    pg_flow = flow_dto.process_group_flow.flow

    for p in (pg_flow.processors or []):
        entry = {
            "name": p.component.name,
            "type": p.component.type.rsplit(".", 1)[-1],
            "state": p.component.state,
            "validation_status": p.component.validation_status,
        }
        if p.component.validation_errors:
            entry["validation_errors"] = p.component.validation_errors
        processors.append(entry)

    for cs_entity in (nipyapi.nifi.FlowApi().get_controller_services_from_group(pg_id).controller_services or []):
        entry = {
            "name": cs_entity.component.name,
            "type": cs_entity.component.type.rsplit(".", 1)[-1],
            "state": cs_entity.component.state,
            "validation_status": cs_entity.component.validation_status,
        }
        if cs_entity.component.validation_errors:
            entry["validation_errors"] = cs_entity.component.validation_errors
        controller_services.append(entry)

    for child_pg in (pg_flow.process_groups or []):
        child_p, child_cs = _collect_components(child_pg.id)
        processors.extend(child_p)
        controller_services.extend(child_cs)

    return processors, controller_services


def deploy(flow_path, runtime_url, pat=None):
    """Upload a flow definition to the Apache NiFi instance and return deployment summary."""
    configure_nifi(runtime_url, pat=pat, nifi_auth=get_nifi_auth())

    root_pg_id = nipyapi.canvas.get_root_pg_id()
    print("Connected to runtime. Root process group: %s" % root_pg_id, file=sys.stderr)

    with open(flow_path, "r") as f:
        flow_def = json.load(f)
    group_name = flow_def.get("flowContents", {}).get("name", flow_path)
    print("Flow name: %s" % group_name, file=sys.stderr)

    filename = os.path.basename(flow_path)
    with open(flow_path, "rb") as fh:
        file_tuple = (filename, fh.read(), "application/json")
    result = nipyapi.nifi.ProcessGroupsApi().upload_process_group(
        id=root_pg_id,
        file=file_tuple,
        group_name=group_name,
        position_x="0.0",
        position_y="0.0",
        client_id=str(uuid.uuid4()),
    )

    pg_id = result.id
    print("Deployed process group: %s" % pg_id, file=sys.stderr)

    processors, controller_services = _collect_components(pg_id)

    summary = {
        "pg_id": pg_id,
        "name": group_name,
        "processors": processors,
        "controller_services": controller_services,
    }
    print(json.dumps(summary))
    return summary


def main():
    parser = argparse.ArgumentParser(
        description="Deploy a NiFi flow definition to an Apache NiFi instance"
    )
    parser.add_argument(
        "--flow-path", required=True, help="Path to the flow definition JSON file"
    )
    parser.add_argument(
        "--runtime-url", default=os.environ.get("NIFI_URL"), required=not os.environ.get("NIFI_URL"), help="Existing Apache NiFi URL"
    )
    parser.add_argument(
        "--token", default=os.environ.get("NIFI_TOKEN", ""), help="NiFi bearer token (or use NIFI_USERNAME/NIFI_PASSWORD)"
    )
    args = parser.parse_args()

    try:
        deploy(args.flow_path, args.runtime_url, args.token)
    except Exception as exc:
        print("Deployment failed: %s" % exc, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
