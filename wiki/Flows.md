# Flows

This section documents the NiFi flow definitions available in this repository. Each flow can be imported into NiFi or deployed to a Apache NiFi instance.

See [How to Use This Repo](How-to-Use-This-Repo) for instructions on importing or deploying a flow.

---

## Available Flows

### Examples

| Flow | Description |
|------|-------------|
| [Hello World](Flows--Hello-World) | Minimal example demonstrating the NiFi Hub flow structure. Generates a FlowFile and logs its attributes. |

### Data Generator

| Flow | Description |
|------|-------------|
| [Postgres CDC Demo](Flows--Postgres-CDC-Demo) | Simulates random data generation (INSERTs, UPDATEs, DELETEs) across multiple tables for use with the CDC PostgreSQL Connector. |

---

## Flow Structure

Each flow in this repository follows a standard structure:

```
flows/<bucket>/
├── <flow-name>.json    # NiFi flow definition (exported JSON)
├── <flow-name>.md      # Documentation
└── tests/
    └── test_<flow-name>.py  # Structural validation tests (nipyapi)
```

- **Bucket** — a grouping category (e.g., `examples`, `salesforce`, `data`)
- **JSON** — the flow definition, directly importable into NiFi
- **Documentation** — purpose, components, required NARs, parameters, and expected behaviour
- **Tests** — pytest tests using `nipyapi` that validate the flow structure without running it

---

## CI for Flows

When a PR modifies a flow, the [CI pipeline](Introduction-and-Concepts--CI) runs:

- **Snowflake Flow Diff** — posts a human-readable diff of flow changes
- **Flow checkstyle** — checks for best-practice violations (concurrent tasks, backpressure, self-loops, etc.)
- **Validation tests** — runs the `tests/` suite for changed flows

Maintainers can also trigger a live deploy test against a Apache NiFi instance by commenting `deploy this flow` on the PR. See [CD Pipeline](Introduction-and-Concepts--CD).
