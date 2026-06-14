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
- Controller services
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
    flows: [...]
```

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
