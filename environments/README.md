# Environments — Openflow as Code

> **Disclaimer:** The contents of this repository are community-driven and provided on an "AS IS" basis, without warranties of any kind, express or implied. They are NOT supported by Snowflake and do not constitute a Snowflake product or service. Snowflake makes no guarantees regarding functionality, compatibility, availability, or fitness for any particular purpose, and assumes no liability arising from use of this repository. Community support is available through GitHub Issues and Pull Requests.

Declarative management of Openflow deployments, runtimes, and flows via YAML configuration.

## Directory Structure

```
environments/
  schema.json                    # JSON Schema for validation
  <env-name>/
    config.yaml                  # Environment configuration
```

Each `config.yaml` defines one Snowflake account's Openflow resources. Changes merged to `main` are automatically applied by the **Environment CD** workflow.

## YAML Schema Reference

### Account

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Human-readable account name (used in logs and comments) |
| `github_environment` | Yes | GitHub Environment name that holds secrets for this account |

### Deployment

Only one deployment per account is allowed.

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Snowflake identifier (uppercase, pattern: `^[A-Z][A-Z0-9_-]{0,254}$`) |
| `deployment_type` | Yes | Must be `SNOWFLAKE` (SPCS) |
| `display_name` | No | Free-text alias shown in the Openflow UI (max 256 chars) |
| `comment` | No | Description (max 1024 chars) |
| `runtimes` | No | Array of runtime definitions |

### Runtime

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Snowflake identifier (uppercase, pattern: `^[A-Z][A-Z0-9_-]{0,254}$`) |
| `database` | Yes | Database where the runtime is scoped |
| `schema` | Yes | Schema where the runtime is scoped |
| `url` | No | NiFi runtime URL (without `/nifi-api` suffix). When set, SOM SQL-based lifecycle management is skipped entirely — only NiFi API operations are performed. Use this for pre-provisioned or non-SOM runtimes. |
| `node_type` | No | `SMALL`, `MEDIUM`, or `LARGE` |
| `min_nodes` | No | Minimum node count (1-50) |
| `max_nodes` | No | Maximum node count (1-50) |
| `suspend` | No | When `true`, runtime is suspended after creation. Defaults to `false`. |
| `reconcile` | No | When `false`, CD skips NiFi-level reconciliation (flows, parameters, registries, controller services). Infrastructure (SQL-level) is still managed. Defaults to `true`. |
| `sensitive_param_pattern` | No | Regex pattern to classify parameters from the auto-provisioned Snowflake Parameter Provider as sensitive. Parameters matching this pattern are marked `SENSITIVE`; others are `NON_SENSITIVE`. Defaults to `.*` (all sensitive). |
| `execute_as_role` | No | Role that connectors and runtime operations use for Snowflake data access |
| `display_name` | No | Free-text alias (max 256 chars) |
| `comment` | No | Description (max 1024 chars) |
| `network_rules` | No | Array of network rule definitions |
| `flow_registries` | No | Array of Flow Registry Client definitions |
| `controller_services` | No | Array of controller-level services — visible to parameter providers and reporting tasks; shared across the entire NiFi instance. See [Controller Services](#controller-services). |
| `root_pg_controller_services` | No | Array of root process group-scoped services — accessible to all flow processors deployed to this runtime. See [Controller Services](#controller-services). |
| `root_parameter_context` | No | Parameter context managed on the root process group: inherited provider contexts, shared non-sensitive parameters, and assets for root-PG controller services. See [Root Parameter Context](#root-parameter-context). |
| `parameter_providers` | No | Array of parameter provider definitions |
| `flows` | No | Array of flow definitions |
| `connectors` | No | Array of Openflow connector definitions |

### Network Rule

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Snowflake identifier (uppercase, pattern: `^[A-Z][A-Z0-9_-]{0,254}$`) |
| `type` | Yes | Must be `HOST_PORT` |
| `mode` | Yes | Must be `EGRESS` |
| `values` | Yes | Array of host:port strings (at least one required) |

### Flow Registry Client

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Name of the registry client in NiFi (used as unique identity for reconciliation) |
| `type` | No | Fully-qualified Java type. If omitted, the first available Git-based type is used. |
| `properties` | Yes | Key-value properties passed to the registry client API. Sensitive values (e.g. Personal Access Token) are injected from GitHub Environment secrets using `${{ secrets.NAME }}` syntax. |

### Controller Services

NiFi has two scopes for controller services. Use the right key depending on what needs to reference the service:

```yaml
# These fields belong inside a runtime definition.
controller_services:
  - name: SnowflakeConnection
    type: com.snowflake.openflow.runtime.services.snowflake.SnowflakeConnectionService
    properties:
      Authentication Strategy: SNOWFLAKE_MANAGED

root_pg_controller_services:
  - name: SharedSSLContext
    type: org.apache.nifi.ssl.StandardRestrictedSSLContextService
    properties:
      TrustStore Filename: /etc/ssl/certs/ca-certificates.crt
      TrustStore Type: JKS
      Keystore Password: ${{ secrets.KEYSTORE_PASSWORD }}
```

> **`controller_services`** — Created at the NiFi controller level (`POST /controller/controller-services`). Visible to parameter providers and reporting tasks across the entire instance. Use this when a service must be referenced by a **parameter provider** (e.g. `SnowflakeConnectionService` consumed by `SnowflakeParameterProvider`). Controller service names in parameter provider `properties` are automatically resolved to their NiFi UUIDs at apply time.
>
> **`root_pg_controller_services`** — Created inside the root process group (`POST /process-groups/root/controller-services`). Accessible to processors in all flows deployed to this runtime, but not visible outside the process group hierarchy. Use this for services shared across multiple flows that do not need to be at the NiFi controller level (e.g. `StandardRestrictedSSLContextService`, `StandardHttpContextMap`).

Both keys accept the same object structure:

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Controller service name in NiFi (unique identity for reconciliation) |
| `type` | Yes | Fully-qualified Java type |
| `bundle.group` | No | Exact NiFi bundle group coordinate to use when type lookup is ambiguous |
| `bundle.artifact` | No | Exact NiFi bundle artifact coordinate |
| `bundle.version` | No | Exact NiFi bundle version coordinate |
| `properties` | No | Key-value configuration properties |

When `bundle` is omitted, NiFi infers the implementation from `type`. When `bundle` is provided, all three fields are required and are passed through unchanged during creation. For an existing controller service, `bundle` is creation-time selection only; NiFi Hub does not attempt to switch an existing service to a different bundle.

### Root Parameter Context

Use `root_parameter_context` when root process group controller services need parameters: secrets coming from a parameter provider, shared values reused across services, or files such as a JDBC driver.

```yaml
parameter_providers:
  - name: SnowflakeSecrets
    type: com.snowflake.openflow.runtime.parameter.snowflake.SnowflakeParameterProvider
    properties:
      Snowflake Connection Service: SnowflakeConnection

root_parameter_context:
  provided_parameter_contexts: ".*POSTGRES"
  parameters:
    Shared Query Timeout: "30 secs"
  assets:
    - name: "postgresql-42.7.10.jar"
      url: "https://jdbc.postgresql.org/download/postgresql-42.7.10.jar"
      parameter: "Database Driver"

root_pg_controller_services:
  - name: SharedDBCPConnectionPool
    type: org.apache.nifi.dbcp.DBCPConnectionPool
    properties:
      Database Driver Class Name: org.postgresql.Driver
      Database Driver Locations: "#{Database Driver}"
      Password: "#{POSTGRES_PWD}"
      Validation Query Timeout: "#{Shared Query Timeout}"
```

| Field | Required | Description |
|-------|----------|-------------|
| `provided_parameter_contexts` | No | Regex matched (full match) against parameter provider context names, including the auto-provisioned Snowflake provider. Matching contexts are added as inherited contexts of the root context. Inheritance is additive: existing inherited contexts are never removed. |
| `parameters` | No | Non-sensitive parameters created or updated directly in the root context. Values support `${{ vars.NAME }}`; `${{ secrets.NAME }}` is rejected because these parameters are stored in NiFi as non-sensitive values. Use provided parameter contexts for secrets. `null` clears the value; omitted parameters are left untouched. Existing sensitive or asset-bound parameters with the same name are rejected. |
| `assets` | No | Files downloaded and uploaded as assets, each bound to a non-sensitive parameter. Same shape as flow `assets`. A parameter may not be declared in both `parameters` and `assets`. |

If the root process group already has a parameter context, NiFi Hub uses it. Otherwise, it reuses an exact-name `Root Parameter Context` or creates and attaches one. Reconciliation order on every apply is: attach/reuse context, add inherited provider contexts, upsert direct parameters, then reconcile assets; root-PG controller services are reconciled afterwards so `#{...}` references resolve when they are enabled. Direct root parameters are always non-sensitive by design: `${{ secrets.* }}` is rejected here, secrets should come from provided contexts (`#{POSTGRES_PWD}`), and live-state output shows direct root-parameter values as stored.

Assets: sensitive parameters, non-empty literal parameters, duplicate asset names, and multiple assets targeting one parameter are rejected. Asset filenames are treated as immutable content identities because NiFi does not retain the source URL; to deploy changed bytes, use a new filename.

Removing `root_parameter_context` entries is intentionally non-destructive: NiFi Hub leaves inherited contexts, parameters, references, assets, and the context intact. This differs from `root_pg_controller_services`, which are deleted when removed from YAML.

If NiFi Hub cannot safely inspect a declared root parameter context, its inherited contexts, parameters, or assets, the live diff reports a blocker and refuses to apply that runtime. Asset downloads permit only HTTP/HTTPS (including redirects), verify TLS with the bundled CA roots, use a 60-second timeout, and reject files larger than 256 MiB.

### Parameter Provider

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Parameter provider name in NiFi (unique identity for reconciliation) |
| `type` | Yes | Fully-qualified Java type |
| `sensitive_param_pattern` | No | Regex applied to parameter names to determine sensitivity. Defaults to `.*` (all sensitive). |
| `properties` | No | Key-value configuration properties. Controller service names are automatically resolved to their NiFi UUIDs. |

### Flow

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Process group name in NiFi. Uniquely identifies this checkout — the same versioned flow may be checked out multiple times under different names. |
| `registry` | No | Name of the Flow Registry Client to use. Defaults to the first entry in `flow_registries`. |
| `bucket` | Yes | Flow bucket name in the registry |
| `flow` | Yes | Flow name in the registry bucket |
| `version` | Yes | Version to check out. Use `latest` for the newest version, or a specific version string (commit SHA for GitHub-backed registries, semver for connector registries). |
| `start` | No | If `true`, enable controller services and start processors after parameters are applied. Defaults to `false`. Required when `service_bindings` is declared. |
| `provided_parameter_contexts` | No | Regex pattern to filter which parameter contexts from providers are added as inherited. Only contexts with names matching this pattern are added. If not specified, no provider contexts are inherited by this flow. |
| `dedicated_parameter_context` | No | If `true`, create a dedicated parameter context for this flow instance instead of reusing existing ones. Required when deploying multiple instances of the same flow with different parameters. Defaults to `false`. |
| `parameters` | No | Key-value parameter values. Keys are parameter names (context membership is resolved automatically). Use `null` to clear a parameter. |
| `parameter_overrides` | No | Parameter values set as overrides in the flow's direct parameter context, shadowing inherited values. Use with `dedicated_parameter_context: true` for multi-instance deployments. |
| `assets` | No | Array of files to download and upload as NiFi Parameter Context Assets. |
| `service_bindings` | No | Array of bindings that map root-PG controller services onto processor or flow-local controller service properties inside the imported flow. |

#### Service Bindings

Use `service_bindings` when a registry-sourced flow must reference controller services that are declared at the runtime root process group level.

```yaml
root_pg_controller_services:
  - name: SharedJsonReader
    type: org.apache.nifi.json.JsonTreeReader
    bundle:
      group: org.apache.nifi
      artifact: nifi-record-serialization-services-nar
      version: 2.9.0

flows:
  - name: "My Versioned Flow"
    bucket: examples
    flow: my-flow
    version: latest
    start: true
    service_bindings:
      - target: Parse Records
        properties:
          Record Reader: SharedJsonReader
      - target: Reader Lookup
        properties:
          json: SharedJsonReader
          obsolete: null
```

Rules and behavior:

- `target` must match exactly one processor or flow-local controller service in the imported flow. Duplicate names across kinds or nested process groups fail.
- Binding values must match exactly one declared `root_pg_controller_services` entry. Empty strings and unresolved names are rejected before any write.
- The referenced root-PG service must implement the controller service API required by the target property. NiFi Hub validates compatibility after applying the binding and rolls back an incompatible binding.
- Only declared binding properties are managed. Omitted properties are left untouched. Explicit `null` removes an optional or dynamic property.
- Required controller service properties cannot be removed with `null`; NiFi Hub rejects the removal rather than reporting success.
- Before changing any bound target, NiFi Hub quiesces the whole imported flow. The final `start` value then re-establishes the declared running state.
- Existing descriptors must identify a controller service and must not be sensitive. Sensitive service-binding properties are not supported in the first version.
- When the property does not exist yet, NiFi Hub probes it as a dynamic property, re-fetches the descriptor, and rolls back if the property did not materialize as a non-sensitive controller-service reference.
- Every apply re-checks bindings after flow import/version update, parameter inheritance, parameter updates, overrides, and asset reconciliation. Matching binding values are a no-op; validation health is reported separately.
- Live diff and change plans render service names, never raw controller service UUIDs. Unknown live UUIDs are reported as unmanaged binding warnings.
- Because the flow is locally mutated after registry import, the process group is expected to show `LOCALLY_MODIFIED` inside NiFi.

#### Flow Asset

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Filename for the asset in the NiFi Parameter Context |
| `url` | Yes | URL to download the asset file from |
| `parameter` | Yes | Parameter name to bind this asset to |

### Connector

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Snowflake identifier (uppercase, pattern: `^[A-Z][A-Z0-9_-]{0,254}$`) |
| `definition` | Yes | Connector definition name (e.g. `OPENFLOW_POSTGRES_CDC`) |
| `display_name` | No | Free-text alias (max 256 chars) |
| `comment` | No | Description (max 1024 chars) |
| `start` | No | If `true`, start the connector after configuration. Defaults to `false`. |
| `parameters` | No | Parameter values for the connector config. Keys are property names. For `STRING_LITERAL`: value is set directly. For `SECRET_REFERENCE`: value is the fully qualified secret name (e.g. `MY_DATABASE.MY_SCHEMA.SECRET_NAME`). For `ASSET_REFERENCE`: handled via the `assets` array. |
| `assets` | No | Array of files to upload to the connector stage. |

#### Connector Asset

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Filename for the asset in the connector stage |
| `url` | Yes | URL to download the asset file from |
| `parameter` | Yes | `ASSET_REFERENCE` parameter name to bind this asset to |

## Full Example

```yaml
account:
  name: example
  github_environment: example

deployments:
  - name: MY_DEPLOYMENT
    deployment_type: SNOWFLAKE
    display_name: "My Deployment"
    runtimes:
      - name: MY_RUNTIME
        database: OPENFLOW
        schema: OPENFLOW
        node_type: SMALL
        min_nodes: 1
        max_nodes: 1
        execute_as_role: OPENFLOW_RUNTIME_ROLE
        sensitive_param_pattern: ".*PASSWORD.*|.*SECRET.*|.*_KEY"
        network_rules:
          - name: GITHUB_API
            type: HOST_PORT
            mode: EGRESS
            values:
              - "api.github.com:443"
        flow_registries:
          - name: nifihub
            type: org.apache.nifi.github.GitHubFlowRegistryClient
            properties:
              Repository Owner: Snowflake-Labs
              Repository Name: nifihub
              Authentication Type: PERSONAL_ACCESS_TOKEN
              Personal Access Token: ${{ secrets.NIFIHUB_REGISTRY_PAT }}
              Default Branch: main
              Repository Path: flows
        root_parameter_context:
          provided_parameter_contexts: ".*POSTGRES"
          assets:
            - name: "postgresql-42.7.10.jar"
              url: "https://jdbc.postgresql.org/download/postgresql-42.7.10.jar"
              parameter: "Database Driver"
        root_pg_controller_services:
          - name: SharedDBCPConnectionPool
            type: org.apache.nifi.dbcp.DBCPConnectionPool
            bundle:
              group: org.apache.nifi
              artifact: nifi-dbcp-service-nar
              version: 2.9.0
            properties:
              Database Connection URL: "jdbc:postgresql://postgres.example.com:5432/mydb"
              Database Driver Class Name: org.postgresql.Driver
              Database Driver Locations: "#{Database Driver}"
              Database User: my_user
              Password: "#{POSTGRES_PWD}"
        flows:
          - name: "CDC Postgres Demo - Data Generator"
            bucket: data-generator
            flow: postgres-cdc-demo
            version: latest
            start: true
            service_bindings:
              - target: ExecuteSQLStatement
                properties:
                  Connection Pooling Service: SharedDBCPConnectionPool
            provided_parameter_contexts: ".*"
            parameters:
              My Parameter: "some value"
```

## SOM vs Non-SOM

**SOM-enabled accounts** (default): The pipeline manages the full lifecycle via Snowflake SQL (`CREATE/ALTER/DROP OPENFLOW DEPLOYMENT/RUNTIME/CONNECTOR`, network rules, EAIs). Declare runtimes with `node_type`, `min_nodes`, `max_nodes`.

**Non-SOM accounts** (URL-managed): Set `url` on a runtime to point at an existing NiFi endpoint. All SOM SQL operations are skipped. Only NiFi API operations (registries, flows, parameters, controller services) are performed.

```yaml
runtimes:
  - name: MY_RUNTIME
    database: OPENFLOW
    schema: OPENFLOW
    url: "https://of--my-account.snowflakecomputing.app/my-runtime"
    flows:
      - name: "My Flow"
        bucket: examples
        flow: hello-world
        version: latest
```

## GitHub Environment Secrets

Each environment requires a GitHub Environment with the following secrets and variables:

| Name | Type | Description |
|------|------|-------------|
| `SNOWFLAKE_ACCOUNT_URL` | Variable | Snowflake account URL (e.g. `https://myorg-myaccount.snowflakecomputing.com`) |
| `SNOWFLAKE_USER` | Variable | Snowflake user for PAT-based authentication |
| `SNOWFLAKE_ROLE` | Variable | Role for SOM operations (e.g. `OPENFLOW_ADMIN`) |
| `SNOWFLAKE_PAT` | Secret | Programmatic Access Token for Snowflake SQL operations |
| `NIFI_RUNTIME_PAT` | Secret | Programmatic Access Token for NiFi REST API calls |
| `NIFIHUB_REGISTRY_PAT` | Secret | GitHub PAT for Flow Registry Client (needs repo read access) |

Additional secrets/variables can be referenced in `config.yaml` using `${{ secrets.NAME }}` or `${{ vars.NAME }}` syntax — they are resolved from the same GitHub Environment at deploy time.

## Prerequisites

Before adding an environment, ensure the Snowflake account has the required grants:

```sql
USE ROLE ACCOUNTADMIN;

GRANT CREATE OPENFLOW DEPLOYMENT ON ACCOUNT TO ROLE OPENFLOW_ADMIN;
GRANT CREATE OPENFLOW RUNTIME ON SCHEMA <database>.<schema> TO ROLE OPENFLOW_ADMIN;
GRANT CREATE COMPUTE POOL ON ACCOUNT TO ROLE OPENFLOW_ADMIN;
GRANT CREATE NETWORK RULE ON SCHEMA <database>.<schema> TO ROLE OPENFLOW_ADMIN;
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE OPENFLOW_ADMIN;
```

## Adding a New Environment

1. Create `environments/<name>/config.yaml` using `schema.json` for validation
2. Create a GitHub Environment named `<name>` with the required secrets
3. Open a PR — the **Environment CD Validate** check will show a change plan
4. Merge — the **Environment CD** workflow creates all resources

## Modifying Resources

Edit the YAML and merge. The workflow detects changes via live state diffing and runs the appropriate `ALTER` commands.

## Removing Resources

Remove entries from the YAML and merge. The workflow runs the full delete lifecycle:
- **Flows**: stop process group, delete
- **Connectors**: STOP -> TERMINATE -> DROP
- **Runtimes**: SUSPEND -> TERMINATE -> DROP
- **EAIs / Network Rules**: DROP
- **Deployments**: TERMINATE -> DROP
