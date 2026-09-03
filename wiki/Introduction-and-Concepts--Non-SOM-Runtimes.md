# Non-SOM Runtimes (URL-Managed)

By default, NiFi Hub manages the full lifecycle of Openflow runtimes via Snowflake SQL — creating deployments, runtimes, network rules, and External Access Integrations through the Snowflake Object Model (SOM). This is the standard mode for Snowflake Openflow customers.

However, not every NiFi instance is managed by SOM. The CD pipeline supports a second mode — **URL-managed runtimes** — where the runtime already exists (or runs outside Snowflake entirely) and only the NiFi content (flows, registries, parameters) is managed declaratively.

---

## When to Use URL-Managed Runtimes

| Scenario | Use URL-managed? |
|---|---|
| Standard Snowflake Openflow (SOM enabled) | No — omit `url`, use SOM mode |
| Snowflake Openflow without SOM | Yes — set `url` to skip SQL lifecycle |
| Local Apache NiFi for development/testing | Yes — set `url` to NiFi endpoint |
| Pre-provisioned NiFi managed by another tool (Terraform, etc.) | Yes — set `url`, let NiFi Hub manage content only |
| Multi-cloud or on-premises NiFi | Yes |

---

## How It Works

Setting the `url` field on a runtime switches the pipeline into URL-managed mode:

```yaml
runtimes:
  - name: MY_RUNTIME
    database: OPENFLOW
    schema: OPENFLOW
    url: "https://of--my-account.snowflakecomputing.app/my-runtime"
    flow_registries: [...]
    flows: [...]
```

### What the Pipeline Skips (SQL lifecycle)

- `CREATE OPENFLOW DEPLOYMENT`
- `CREATE / ALTER / SUSPEND / RESUME / DROP OPENFLOW RUNTIME`
- `CREATE / ALTER / DROP OPENFLOW CONNECTOR`
- Network rules and External Access Integrations
- All `DESCRIBE OPENFLOW *` SQL for this runtime

### What the Pipeline Still Manages (NiFi API)

- Flow Registry Clients (GitHub registry client setup)
- Flow checkout and versioning
- Parameter contexts and parameter values
- Controller-level controller services (`controller_services` — used by parameter providers)
- Root process group controller services (`root_pg_controller_services` — shared across all flows)
- Root parameter context inheritance, shared parameters, and assets (`root_parameter_context`)
- Service bindings from root-PG controller services to processors or flow-local controller services (`flows[].service_bindings`)
- Parameter providers (including the auto-provisioned Snowflake Parameter Provider, if available)
- Flow start/stop state

---

## Configuration Reference

```yaml
runtimes:
  - name: MY_RUNTIME
    database: ""          # ignored in URL-managed mode — can be set to any value
    schema: ""            # ignored in URL-managed mode — can be set to any value
    url: "https://..."    # NiFi base URL (without /nifi-api suffix)

    # Optional: NiFi authentication (for OSS NiFi with username/password)
    # If omitted, NIFI_RUNTIME_PAT env var is used (Bearer token for Snowflake Openflow)
    nifi_auth:
      type: username_password
      username: ${{ vars.NIFI_USERNAME }}
      password: ${{ secrets.NIFI_PASSWORD }}
      verify_ssl: false   # set false for local self-signed certificates

    flow_registries: [...]
    root_parameter_context:
      provided_parameter_contexts: ".*SECRETS"
      parameters:
        Shared Query Timeout: "30 secs"
      assets:
        - name: "driver.jar"
          url: "https://example.invalid/driver.jar"
          parameter: "Database Driver"
    root_pg_controller_services: [...]
    flows:
      - name: MY_FLOW
        bucket: examples
        flow: my-flow
        version: latest
        start: true
        service_bindings: [...]
```

Bound flows use the same `service_bindings` contract as SOM-managed runtimes. Declare referenced services under `root_pg_controller_services`, set `start` explicitly, and see the [complete environment schema reference](../environments/README.md#service-bindings) for validation and lifecycle behavior.

The `root_parameter_context` contract is also identical for SOM and URL-managed runtimes. NiFi Hub can inherit matching parameter-provider contexts, upsert shared non-sensitive parameters, and upload assets before it enables root-PG controller services. See [Root Parameter Context](../environments/README.md#root-parameter-context).

### nifi_auth Fields

| Field | Required | Description |
|---|---|---|
| `type` | Yes | Authentication type. Currently only `username_password` is supported. |
| `username` | Yes | NiFi username. Supports `${{ vars.NAME }}` syntax. |
| `password` | Yes | NiFi password. Supports `${{ secrets.NAME }}` syntax. |
| `verify_ssl` | No | SSL verification mode. `true` (default) — verify against system CA store. `false` — disable verification (local dev / self-signed certs only). String path — path to a custom CA bundle (e.g. `/path/to/ca-bundle.crt`). Never set `false` in production. |

---

## Authentication: PAT vs Username/Password

**Snowflake Openflow (default):** The runtime API uses Bearer token authentication. The token is read from `NIFI_RUNTIME_PAT` env var. No `nifi_auth` config needed.

**OSS Apache NiFi:** Uses username/password login. NiFi provides a JWT token via `POST /nifi-api/access/token`. Set `nifi_auth` in the config and provide credentials via `GH_SECRETS_JSON` / `GH_VARS_JSON` (local) or GitHub Environment secrets/variables (Actions).

---

## Reconciliation Behaviour

URL-managed runtimes are **always reconciled** on every CD run. Since there is no Snowflake SQL state to diff against, the pipeline treats them as always needing NiFi content alignment. If the NiFi state already matches the desired config, no changes are applied.

This differs from SOM-managed runtimes, which go through a full live diff and are only acted on when changes are detected.

---

## Connectors Are Not Supported

Connectors (`CREATE OPENFLOW CONNECTOR`) are SOM objects and require Snowflake SQL to manage. They are not supported for URL-managed runtimes. Remove any `connectors:` section from a runtime config that has `url:` set.

---

## See Also

- [Running CD Locally](How-to-Run-CD-Locally) — step-by-step guide for running the pipeline from your machine and testing against local Apache NiFi
- [CD Pipeline](Introduction-and-Concepts--CD) — overview of the full CD pipeline
- [`environments/README.md`](../environments/README.md) — complete YAML schema reference
