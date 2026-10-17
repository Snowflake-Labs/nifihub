# Environments — Apache NiFi resource configuration

Each `environments/<name>/config.yaml` describes resources on existing NiFi
instances. Servers, clusters, storage, networking, and operating-system Python
installations are managed outside NiFi Hub.

## Environment and runtime

```yaml
name: development
github_environment: development
runtimes:
  - name: primary
    url: https://nifi.example.com:8443
    reconcile: true
```

`runtimes` is required and must contain at least one entry. Each entry requires a
unique `name` and HTTP(S) `url`. The URL may end in `/nifi` or `/nifi-api`; it must
not contain credentials, query parameters, or a fragment. `name` and
`github_environment` at the top level are optional; GitHub Environment selection
defaults to the config directory name. `reconcile: false` skips reads and writes
for that instance. A removed runtime is simply no longer managed.

Runtime resource keys are `flow_registries`, `controller_services`,
`root_pg_controller_services`, `parameter_providers`, `root_parameter_context`,
and `flows`. An omitted list leaves that scope untouched. A declared list is
authoritative: entries absent from it are removed, and an explicit empty list
removes all resources in that scope. The root process group is the managed flow
scope. Use a dedicated instance or declare all resources in a managed scope.

## Authentication and TLS

By default, set `NIFI_USERNAME` and `NIFI_PASSWORD`, or supply `NIFI_TOKEN` for an
existing bearer token. For different credentials per instance:

```yaml
nifi_auth:
  type: username_password
  username: ${{ vars.NIFI_USERNAME }}
  password: ${{ secrets.NIFI_PASSWORD }}
  verify_ssl: true
```

The explicit block overrides environment authentication. `verify_ssl` accepts
`true`, `false` for local development, or a CA bundle path. Without an explicit
setting, `NIFI_VERIFY_SSL` is used and defaults to `true`. Authentication requires
HTTPS. Local unsecured HTTP works without credentials. NiFi API permissions must
allow reading and modifying the resources declared by the configuration.

`${{ vars.NAME }}` and `${{ secrets.NAME }}` resolve from the selected GitHub
Environment. Locally, set `GH_VARS_JSON` and `GH_SECRETS_JSON` instead. Secrets are
not compared in the change plan; declared resources are reconciled on every run
so secret changes and `version: latest` can be applied.

## Resource reference

### Flow Registry Client

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Name of the registry client in NiFi (used as unique identity for reconciliation) |
| `type` | No | Fully-qualified Java type. If omitted, the first available Git-based type is used. |
| `properties` | Yes | Key-value properties passed to the registry client API. Sensitive values (e.g. Personal Access Token) are injected from GitHub Environment secrets using `${{ secrets.NAME }}` syntax. |

### Controller Services

`controller_services` creates controller-scoped services for parameter providers
and reporting tasks. `root_pg_controller_services` creates shared services in the
root process group for use by processors in child flows. Both accept `name`,
fully qualified `type`, optional `bundle` (`group`, `artifact`, `version`), and
`properties`. Secret and variable references are resolved before writing.
Controller service names in provider properties resolve to their current UUIDs.

```yaml
root_pg_controller_services:
  - name: SharedSSLContext
    type: org.apache.nifi.ssl.StandardRestrictedSSLContextService
    properties:
      TrustStore Filename: /etc/nifi/truststore.jks
      TrustStore Type: JKS
      TrustStore Password: ${{ secrets.TRUSTSTORE_PASSWORD }}
```

### Root Parameter Context

Use `root_parameter_context` when root process group controller services need parameters: secrets coming from a parameter provider, shared values reused across services, or files such as a JDBC driver.

```yaml
# Assumes an explicitly configured NiFi parameter provider exposes a context named Postgres.

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
| `provided_parameter_contexts` | No | Regex matched (full match) against parameter provider context names. Matching contexts are added as inherited contexts of the root context. Inheritance is additive: existing inherited contexts are never removed. |
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

## Running the pipeline

```bash
python scripts/cd/validate_pr.py environments/local/config.yaml environments/schema.json
python scripts/run-cd.py environments/local/config.yaml --dry-run
python scripts/run-cd.py environments/local/config.yaml
```

The pipeline validates configuration, reads live NiFi state, computes a resource
diff, translates it, and applies it. Registry clients, controller-scoped services,
parameter providers, the root parameter context, root process-group services, and
flows are reconciled in that order. Flow assets, parameters, and service bindings
are configured before the requested start/stop state. Required NARs must already
be installed. Unreadable live state blocks reconciliation; one instance's apply
failure does not prevent other instances from being attempted.

## Migrating older configurations

Move each entry from `deployments[].runtimes` to the top-level `runtimes` list and
give it an explicit NiFi `url`. Move `account.github_environment` to top-level
`github_environment`, and optionally move `account.name` to `name`.

Remove deployment, database/schema, node sizing, suspension, network-rule,
connector, and infrastructure role fields. These fields are rejected by the new
schema. Rename authentication variables to `NIFI_USERNAME` / `NIFI_PASSWORD` or
`NIFI_TOKEN`; flow integration tests use `NIFI_URL`. Declare parameter providers
explicitly; there is no automatic platform-specific provider lookup.

Java package names and Maven coordinates retain their original values, preserving
compatibility with existing released NARs and flow snapshots. Changing the
configuration format does not migrate or delete any running infrastructure.
