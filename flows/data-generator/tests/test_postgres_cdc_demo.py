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
Validation tests for the CDC Postgres Demo - Data Generator flow.

All tests are structural — they validate the flow definition JSON
without requiring a running NiFi instance or Postgres database.
"""

import json
import os
import re

import pytest


FLOW_PATH = os.path.join(os.path.dirname(__file__), "..", "postgres-cdc-demo.json")


@pytest.fixture
def flow_data():
    with open(FLOW_PATH, "r") as f:
        return json.load(f)


@pytest.fixture
def flow_contents(flow_data):
    return flow_data["flowContents"]


@pytest.fixture
def processors(flow_contents):
    return flow_contents.get("processors", [])


@pytest.fixture
def connections(flow_contents):
    return flow_contents.get("connections", [])


@pytest.fixture
def controller_services(flow_contents):
    return flow_contents.get("controllerServices", [])


@pytest.fixture
def parameter_contexts(flow_data):
    return flow_data.get("parameterContexts", {})


class TestFlowStructure:
    def test_flow_is_valid_json(self, flow_data):
        assert flow_data is not None

    def test_has_flow_contents(self, flow_data):
        assert "flowContents" in flow_data

    def test_has_parameter_contexts(self, flow_data):
        assert "parameterContexts" in flow_data

    def test_flow_name(self, flow_contents):
        assert flow_contents["name"] == "CDC Postgres Demo - Data Generator"

    def test_parameter_context_name_matches(self, flow_contents):
        assert flow_contents.get("parameterContextName") == "CDC Postgres Demo - Data Generator"


class TestProcessors:
    EXPECTED_PROCESSORS = [
        ("GenerateFlowFile", "org.apache.nifi.processors.standard.GenerateFlowFile"),
        ("ExecuteScript", "org.apache.nifi.processors.script.ExecuteScript"),
        ("RouteOnAttribute", "org.apache.nifi.processors.standard.RouteOnAttribute"),
        ("PutDatabaseRecord", "org.apache.nifi.processors.standard.PutDatabaseRecord"),
    ]

    def test_minimum_processor_count(self, processors):
        assert len(processors) >= 5

    @pytest.mark.parametrize("name,proc_type", EXPECTED_PROCESSORS)
    def test_expected_processor_exists(self, processors, name, proc_type):
        matches = [p for p in processors if p["name"] == name and p["type"] == proc_type]
        assert len(matches) >= 1, f"Processor '{name}' ({proc_type}) not found"

    def test_all_processors_have_required_fields(self, processors):
        for proc in processors:
            assert "identifier" in proc, f"Processor missing 'identifier': {proc.get('name')}"
            assert "name" in proc, f"Processor missing 'name'"
            assert "type" in proc, f"Processor missing 'type': {proc.get('name')}"
            assert "bundle" in proc, f"Processor missing 'bundle': {proc.get('name')}"

    def test_execute_script_uses_groovy(self, processors):
        scripts = [p for p in processors if p["type"] == "org.apache.nifi.processors.script.ExecuteScript"]
        for proc in scripts:
            assert proc["properties"].get("Script Engine") == "Groovy"

    def test_execute_script_has_body(self, processors):
        scripts = [p for p in processors if p["type"] == "org.apache.nifi.processors.script.ExecuteScript"]
        for proc in scripts:
            body = proc["properties"].get("Script Body", "")
            assert body and len(body) > 100, f"ExecuteScript '{proc['name']}' has no script body"

    def test_generate_flowfile_exists(self, processors):
        gff_procs = [p for p in processors if p["type"] == "org.apache.nifi.processors.standard.GenerateFlowFile"]
        assert len(gff_procs) >= 1, "Expected at least one GenerateFlowFile processor"

    def test_route_on_attribute_has_delete_route(self, processors):
        routers = [p for p in processors if p["type"] == "org.apache.nifi.processors.standard.RouteOnAttribute"]
        assert len(routers) >= 1
        router = routers[0]
        assert "delete" in router["properties"], "RouteOnAttribute missing 'delete' route"
        assert "delete" in router["properties"]["delete"], "delete route should check operation attribute"


class TestConnections:
    def test_minimum_connection_count(self, connections):
        assert len(connections) >= 4

    def test_all_connections_have_source_and_destination(self, connections):
        for conn in connections:
            assert "source" in conn, f"Connection {conn.get('identifier')} missing source"
            assert "destination" in conn, f"Connection {conn.get('identifier')} missing destination"
            assert conn["source"].get("id"), f"Connection source has no id"
            assert conn["destination"].get("id"), f"Connection destination has no id"

    def test_no_orphaned_processors(self, processors, connections):
        connected_ids = set()
        for conn in connections:
            connected_ids.add(conn["source"]["id"])
            connected_ids.add(conn["destination"]["id"])
        funnel_type = "FUNNEL"
        for proc in processors:
            assert proc["identifier"] in connected_ids, (
                f"Processor '{proc['name']}' ({proc['identifier']}) is not connected to anything"
            )

    def test_priority_on_route_connections(self, connections, processors):
        router_ids = {p["identifier"] for p in processors
                      if p["type"] == "org.apache.nifi.processors.standard.RouteOnAttribute"}
        for conn in connections:
            if conn["source"]["id"] in router_ids:
                prioritizers = conn.get("prioritizers", [])
                assert "org.apache.nifi.prioritizer.PriorityAttributePrioritizer" in prioritizers, (
                    f"Connection from RouteOnAttribute ({conn['selectedRelationships']}) "
                    f"should use PriorityAttributePrioritizer"
                )


class TestControllerServices:
    def test_dbcp_connection_pool_exists(self, controller_services):
        dbcp = [cs for cs in controller_services if "DBCPConnectionPool" in cs["type"]]
        assert len(dbcp) >= 1, "DBCP Connection Pool not found"

    def test_json_reader_exists(self, controller_services):
        readers = [cs for cs in controller_services if "JsonTreeReader" in cs["type"]]
        assert len(readers) >= 1, "JSON Reader not found"

    def test_dbcp_uses_parameter_references(self, controller_services):
        dbcp = [cs for cs in controller_services if "DBCPConnectionPool" in cs["type"]][0]
        props = dbcp["properties"]
        assert "#{Database Connection URL}" in props.get("Database Connection URL", ""), \
            "DBCP should reference #{Database Connection URL} parameter"
        assert "#{Database User}" in props.get("Database User", ""), \
            "DBCP should reference #{Database User} parameter"
        assert "#{Database Password}" in props.get("Password", ""), \
            "DBCP should reference #{Database Password} parameter"
        assert "#{Database Driver}" in props.get("Database Driver Locations", ""), \
            "DBCP should reference #{Database Driver} parameter"

    def test_no_hardcoded_credentials(self, controller_services):
        sensitive_keys = {"Password", "Secret", "Private Key"}
        for cs in controller_services:
            for key, value in cs.get("properties", {}).items():
                if value and isinstance(value, str) and key in sensitive_keys:
                    assert value.startswith("#{"), (
                        f"Controller service '{cs['name']}' property '{key}' "
                        f"appears to have a hardcoded value instead of a parameter reference"
                    )


class TestParameterContexts:
    EXPECTED_PARAMS = [
        "Database Connection URL",
        "Database User",
        "Database Name",
        "Schema Name",
        "Database Driver",
    ]

    def test_main_context_exists(self, parameter_contexts):
        assert "CDC Postgres Demo - Data Generator" in parameter_contexts

    @pytest.mark.parametrize("param_name", EXPECTED_PARAMS)
    def test_expected_parameter_exists(self, parameter_contexts, param_name):
        ctx = parameter_contexts["CDC Postgres Demo - Data Generator"]
        param_names = [p["name"] for p in ctx.get("parameters", [])]
        assert param_name in param_names, f"Parameter '{param_name}' not found in context"

    def test_no_hardcoded_connection_url(self, parameter_contexts):
        ctx = parameter_contexts["CDC Postgres Demo - Data Generator"]
        for param in ctx.get("parameters", []):
            if param["name"] == "Database Connection URL" and param.get("value"):
                assert "jdbc:postgresql://" in param["value"], \
                    "Database Connection URL should be a JDBC PostgreSQL URL"


class TestGroovyScript:
    @pytest.fixture
    def groovy_script(self, processors):
        scripts = [p for p in processors
                   if p["type"] == "org.apache.nifi.processors.script.ExecuteScript"
                   and p["name"] == "ExecuteScript"]
        assert len(scripts) == 1, "Expected exactly one ExecuteScript processor named 'ExecuteScript'"
        return scripts[0]["properties"]["Script Body"]

    def test_script_generates_customers(self, groovy_script):
        assert "customer_id" in groovy_script
        assert "target_table: 'customers'" in groovy_script or "target_table: \"customers\"" in groovy_script

    def test_script_generates_orders(self, groovy_script):
        assert "order_id" in groovy_script
        assert "target_table: 'orders'" in groovy_script or "target_table: \"orders\"" in groovy_script

    def test_script_generates_order_items(self, groovy_script):
        assert "target_table: 'order_items'" in groovy_script or "target_table: \"order_items\"" in groovy_script

    def test_script_sets_priority_attributes(self, groovy_script):
        assert "priority:" in groovy_script

    def test_script_sets_statement_type(self, groovy_script):
        assert "'statement.type'" in groovy_script or "\"statement.type\"" in groovy_script

    def test_delete_uses_same_order_id(self, groovy_script):
        assert "deleteOrderId" in groovy_script, (
            "Delete operations should use a shared 'deleteOrderId' variable "
            "to ensure order_items are deleted before orders (FK safety)"
        )

    def test_script_has_session_remove(self, groovy_script):
        assert "session.remove(flowFile)" in groovy_script, \
            "Script should remove the original trigger flowFile"

    def test_script_has_upsert_operations(self, groovy_script):
        assert "'UPSERT'" in groovy_script or "\"UPSERT\"" in groovy_script

    def test_script_has_delete_operations(self, groovy_script):
        assert "'DELETE'" in groovy_script or "\"DELETE\"" in groovy_script


class TestInitSQL:
    @pytest.fixture
    def init_sql(self, processors):
        sql_procs = [p for p in processors
                     if p["type"] == "org.apache.nifi.processors.standard.PutSQL"]
        if not sql_procs:
            pytest.fail("Apache NiFi PutSQL initialization processor is required")
        return sql_procs[0]["properties"]["SQL Statement"]

    def test_creates_schema(self, init_sql):
        assert "CREATE SCHEMA IF NOT EXISTS" in init_sql

    def test_creates_customers_table(self, init_sql):
        assert "customers" in init_sql.lower()
        assert "customer_id" in init_sql.lower()
        assert "PRIMARY KEY" in init_sql

    def test_creates_orders_table(self, init_sql):
        assert "orders" in init_sql.lower()
        assert "order_id" in init_sql.lower()

    def test_creates_order_items_table(self, init_sql):
        assert "order_items" in init_sql.lower()
        assert "item_id" in init_sql.lower()

    def test_creates_publication(self, init_sql):
        assert "CREATE PUBLICATION" in init_sql

    def test_publication_includes_all_tables(self, init_sql):
        pub_section = init_sql[init_sql.index("CREATE PUBLICATION"):]
        assert "customers" in pub_section.lower()
        assert "orders" in pub_section.lower()
        assert "order_items" in pub_section.lower()

    def test_no_replica_identity_full(self, init_sql):
        assert "REPLICA IDENTITY FULL" not in init_sql, (
            "Tables should NOT use REPLICA IDENTITY FULL — "
            "it causes the CDC connector to report MISSING_PRIMARY_KEYS. "
            "Use DEFAULT (the default) when tables have explicit PKs."
        )


import time

try:
    import nipyapi
except ImportError:
    nipyapi = None


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
def deployed_pg_id():
    pg_id = os.environ.get("DEPLOYED_PG_ID", "")
    if not pg_id:
        pytest.skip("No DEPLOYED_PG_ID — skipping runtime tests")
    return pg_id


@pytest.fixture(scope="module")
def running_flow(nifi_runtime, deployed_pg_id):
    status = nifi_runtime.nifi.FlowApi().get_process_group_status(deployed_pg_id)
    snapshot = status.process_group_status.aggregate_snapshot
    if snapshot.active_thread_count > 0:
        return deployed_pg_id
    api = nifi_runtime.nifi.FlowApi()
    api.activate_controller_services(
        id=deployed_pg_id,
        body=nifi_runtime.nifi.ActivateControllerServicesEntity(
            id=deployed_pg_id, state="ENABLED"
        )
    )
    time.sleep(5)
    nifi_runtime.canvas.schedule_process_group(deployed_pg_id, True)
    time.sleep(60)
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
            and "violates foreign key constraint" not in (b.bulletin.message or "")
        ]
        assert len(error_bulletins) == 0, (
            f"Flow produced {len(error_bulletins)} error(s): " +
            "; ".join(
                f"{b.bulletin.source_name}: {b.bulletin.message}"
                for b in error_bulletins[:5]
            )
        )

    def test_no_warning_bulletins(self, nifi_runtime, running_flow):
        board = nifi_runtime.nifi.FlowApi().get_bulletin_board(group_id=running_flow)
        bulletins = board.bulletin_board.bulletins or []
        warnings = [
            b for b in bulletins
            if b.bulletin and b.bulletin.level == "WARNING"
        ]
        assert len(warnings) == 0, (
            f"Flow produced {len(warnings)} warning(s): " +
            "; ".join(
                f"{b.bulletin.source_name}: {b.bulletin.message}"
                for b in warnings[:5]
            )
        )

    def test_putdatabaserecord_has_output(self, nifi_runtime, running_flow):
        flow = nifi_runtime.nifi.FlowApi().get_flow(running_flow)
        processors = flow.process_group_flow.flow.processors or []
        put_procs = [
            p for p in processors
            if "PutDatabaseRecord" in p.component.type
        ]
        assert len(put_procs) > 0, "No PutDatabaseRecord processor found"
        for proc in put_procs:
            snapshot = proc.status.aggregate_snapshot
            assert snapshot.flow_files_in > 0, (
                f"PutDatabaseRecord '{proc.component.name}' received 0 input flowfiles — "
                f"upstream processors may not be generating data"
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
