# Postgres CDC Demo — Data Generator

**Bucket:** `data-generator`
**File:** [`flows/data-generator/postgres-cdc-demo.json`](../flows/data-generator/postgres-cdc-demo.json)

Simulates random data generation with INSERTs, UPDATEs, and DELETEs across three tables (`customers`, `orders`, `order_items`). Use it with a PostgreSQL CDC consumer to demonstrate change data capture pipelines.

---

## Purpose

This flow provides a continuous stream of realistic database mutations against a PostgreSQL instance. It automatically creates the required schema, tables, and publication on first run — no manual SQL setup is needed.

## Components

| Component | Type | Description |
|-----------|------|-------------|
| PutSQL (Initialize Database) | Processor | Creates schema, tables, and publication on first run |
| GenerateFlowFile | Processor | Triggers periodic data generation |
| ExecuteScript | Processor | Generates random records and operation attributes |
| PutDatabaseRecord | Processor | Applies inserts, upserts, and deletes |

## Required NARs

- `org.apache.nifi:nifi-standard-nar:2.12.0` — included with standard NiFi installations
- `org.apache.nifi:nifi-scripting-nar:2.12.0` — Groovy data generation
- `org.apache.nifi:nifi-dbcp-service-nar:2.12.0` — JDBC connection pool
- `org.apache.nifi:nifi-record-serialization-services-nar:2.12.0` — JSON record reader
- PostgreSQL JDBC driver (`postgresql-42.7.10.jar`) — uploaded as a Parameter Context Asset

## Parameters

| Parameter | Description |
|-----------|-------------|
| `Database Connection URL` | JDBC URL to the Postgres instance |
| `Database Name` | Postgres database name |
| `Database User` | Postgres username |
| `Database Password` | Postgres password (sensitive — use parameter provider) |
| `Schema Name` | Schema for the generated tables |
| `Database Driver` | JDBC driver asset (bound to the uploaded JAR) |

## Configuration

Deploy via the Environment CD pipeline using an `environments/<env>/config.yaml` entry, or test in CI using the test YAML at `flows/data-generator/tests/test_postgres_cdc_demo.yaml`.

For secrets, use explicitly configured NiFi parameter providers or `${{ secrets.NAME }}` references in flow parameters. The companion test YAML uses `POSTGRES_URL` and `POSTGRES_PASSWORD` from the selected GitHub Environment.

## Expected Behaviour

Once started, the flow continuously generates random INSERT, UPDATE, and DELETE operations against the three tables. The PostgreSQL publication enables downstream CDC connectors to capture these changes in real time.

## Validation Tests

The test file at [`flows/data-generator/tests/test_postgres_cdc_demo.py`](../flows/data-generator/tests/test_postgres_cdc_demo.py) validates structure offline and can check execution on an existing NiFi test instance connected to PostgreSQL. Configure `NIFI_URL`, NiFi credentials, `POSTGRES_URL`, and `POSTGRES_PASSWORD` in the test GitHub Environment.
