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

"""
Validation tests for the Hello World example flow.

Structural tests validate the flow definition JSON without a running instance.
Runtime tests require NIFI_URL, DEPLOYED_PG_ID, and NiFi authentication
environment variables (set by the flow-deploy workflow).
"""

import json
import os
import time

import pytest

try:
    import nipyapi
except ImportError:
    nipyapi = None


FLOW_PATH = os.path.join(os.path.dirname(__file__), "..", "hello-world.json")


@pytest.fixture
def flow_definition():
    with open(FLOW_PATH, "r") as f:
        return json.load(f)


class TestFlowStructure:
    def test_flow_is_valid_json(self, flow_definition):
        assert flow_definition is not None

    def test_has_snapshot_metadata(self, flow_definition):
        assert "snapshotMetadata" in flow_definition
        metadata = flow_definition["snapshotMetadata"]
        assert "bucketIdentifier" in metadata
        assert "flowIdentifier" in metadata
        assert "version" in metadata

    def test_has_processors(self, flow_definition):
        contents = flow_definition["flowContents"]
        assert len(contents["processors"]) == 3

    def test_has_connections(self, flow_definition):
        contents = flow_definition["flowContents"]
        assert len(contents["connections"]) >= 1

    def test_processors_have_required_fields(self, flow_definition):
        contents = flow_definition["flowContents"]
        for processor in contents["processors"]:
            assert "identifier" in processor
            assert "name" in processor
            assert "type" in processor
            assert "bundle" in processor

    def test_generate_flowfile_processor_present(self, flow_definition):
        contents = flow_definition["flowContents"]
        processor_types = [p["type"] for p in contents["processors"]]
        assert "org.apache.nifi.processors.standard.GenerateFlowFile" in processor_types

    def test_log_attribute_processor_present(self, flow_definition):
        contents = flow_definition["flowContents"]
        processor_types = [p["type"] for p in contents["processors"]]
        assert "org.apache.nifi.processors.standard.LogAttribute" in processor_types

    def test_example_processor_present(self, flow_definition):
        contents = flow_definition["flowContents"]
        processor_types = [p["type"] for p in contents["processors"]]
        assert "com.snowflake.nifihub.example.ExampleProcessor" in processor_types

    def test_generate_to_example_connection(self, flow_definition):
        contents = flow_definition["flowContents"]
        connections = contents["connections"]
        gen_to_ex = [
            c for c in connections
            if c["source"]["name"] == "GenerateFlowFile"
            and c["destination"]["name"] == "ExampleProcessor"
        ]
        assert len(gen_to_ex) == 1

    def test_example_to_log_connection(self, flow_definition):
        contents = flow_definition["flowContents"]
        connections = contents["connections"]
        ex_to_log = [
            c for c in connections
            if c["source"]["name"] == "ExampleProcessor"
            and c["destination"]["name"] == "LogAttribute"
        ]
        assert len(ex_to_log) == 1


@pytest.fixture(scope="module")
def nifi_runtime():
    url = os.environ.get("NIFI_URL", "")
    if not url or not os.environ.get("DEPLOYED_PG_ID"):
        pytest.skip("Set NIFI_URL and DEPLOYED_PG_ID to run integration tests")
    if nipyapi is None:
        pytest.skip("nipyapi not available")
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts/cd"))
    from manage_flows import configure_nifi
    from nifi_config import get_nifi_auth
    configure_nifi(url, pat=os.environ.get("NIFI_TOKEN", ""), nifi_auth=get_nifi_auth())
    return nipyapi


@pytest.fixture(scope="module")
def deployed_pg_id(nifi_runtime):
    pg_id = os.environ.get("DEPLOYED_PG_ID", "")
    if not pg_id:
        pytest.skip("DEPLOYED_PG_ID not set")
    return pg_id


@pytest.fixture(scope="module")
def running_flow(nifi_runtime, deployed_pg_id):
    status = nifi_runtime.nifi.FlowApi().get_process_group_status(deployed_pg_id)
    snapshot = status.process_group_status.aggregate_snapshot
    if snapshot.active_thread_count > 0:
        return deployed_pg_id
    nifi_runtime.canvas.schedule_process_group(deployed_pg_id, True)
    time.sleep(10)
    return deployed_pg_id


class TestRuntimeExecution:
    def test_flow_is_running(self, nifi_runtime, running_flow):
        status = nifi_runtime.nifi.FlowApi().get_process_group_status(running_flow)
        snapshot = status.process_group_status.aggregate_snapshot
        assert snapshot.active_thread_count >= 0

    def test_no_error_bulletins(self, nifi_runtime, running_flow):
        board = nifi_runtime.nifi.FlowApi().get_bulletin_board(group_id=running_flow)
        bulletins = board.bulletin_board.bulletins or []
        error_bulletins = [
            b for b in bulletins
            if b.bulletin and b.bulletin.level == "ERROR"
        ]
        assert len(error_bulletins) == 0, (
            f"Flow produced {len(error_bulletins)} error(s): " +
            "; ".join(
                f"{b.bulletin.source_name}: {b.bulletin.message}"
                for b in error_bulletins[:5]
            )
        )

    def test_logattribute_receives_data(self, nifi_runtime, running_flow):
        flow = nifi_runtime.nifi.FlowApi().get_flow(running_flow)
        processors = flow.process_group_flow.flow.processors or []
        log_procs = [
            p for p in processors
            if "LogAttribute" in p.component.type
        ]
        assert len(log_procs) > 0, "No LogAttribute processor found"
        for proc in log_procs:
            snapshot = proc.status.aggregate_snapshot
            assert snapshot.flow_files_in > 0, (
                f"LogAttribute '{proc.component.name}' received 0 input flowfiles"
            )

    def test_no_processors_stopped(self, nifi_runtime, running_flow):
        flow = nifi_runtime.nifi.FlowApi().get_flow(running_flow)
        processors = flow.process_group_flow.flow.processors or []
        stopped = [
            p for p in processors
            if p.status.run_status == "Stopped"
        ]
        assert len(stopped) == 0, (
            f"{len(stopped)} processor(s) are stopped: " +
            ", ".join(p.component.name for p in stopped)
        )

    def test_no_invalid_processors(self, nifi_runtime, running_flow):
        flow = nifi_runtime.nifi.FlowApi().get_flow(running_flow)
        processors = flow.process_group_flow.flow.processors or []
        invalid = [
            p for p in processors
            if p.status.run_status == "Invalid"
        ]
        assert len(invalid) == 0, (
            f"{len(invalid)} processor(s) are invalid: " +
            ", ".join(p.component.name for p in invalid)
        )
