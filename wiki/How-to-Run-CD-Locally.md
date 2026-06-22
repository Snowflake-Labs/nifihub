# Running CD Locally and Against OSS Apache NiFi

The NiFi Hub CD pipeline is implemented as a set of standalone Python scripts. You can run them directly from your machine — no GitHub Actions required. This is useful for:

- Testing config changes before committing
- Previewing what would change with `--dry-run`
- Deploying against a local Apache NiFi instance for development
- Force-reconciling a runtime after a manual change
- Demonstrating that the same GitOps tooling works against any NiFi endpoint

---

## Prerequisites

- Python 3.12+
- Dependencies installed: `pip install -r scripts/cd/requirements-cd.txt`
- Snowflake CLI installed (`snow`) — required by `describe_live_state.py` even for non-SOM runtimes
- A running NiFi instance (Snowflake Openflow or local Apache NiFi)

---

## Quick Start — Snowflake Openflow

Set environment variables and run:

```bash
export SNOWFLAKE_ACCOUNT_URL="https://myorg-myaccount.snowflakecomputing.com"
export SNOWFLAKE_USER="myuser"
export SNOWFLAKE_PAT="pat-token"
export SNOWFLAKE_ROLE="OPENFLOW_ADMIN"
export NIFI_RUNTIME_PAT="nifi-pat-token"
export NIFIHUB_REGISTRY_PAT="ghp_..."

# Preview what would change (no apply)
python scripts/run-cd.py environments/demo/config.yaml --dry-run

# Apply changes
python scripts/run-cd.py environments/demo/config.yaml
```

The script runs the same 4-step pipeline used by GitHub Actions:
1. **Describe** — queries Snowflake SQL and NiFi REST API to build the live state
2. **Diff** — compares live state against the desired YAML config
3. **Translate** — converts diff output to the orchestrate.py change format
4. **Apply** — executes creates, modifications, and deletes

With `--dry-run`, the pipeline stops after step 3 and prints the change plan JSON without applying anything.

### Environment Variable Reference

| Variable | Required | Description |
|---|---|---|
| `SNOWFLAKE_ACCOUNT_URL` | Yes | Snowflake account URL (e.g. `https://myorg.snowflakecomputing.com`) |
| `SNOWFLAKE_USER` | Yes | Snowflake username |
| `SNOWFLAKE_PAT` | Yes | Snowflake Programmatic Access Token |
| `SNOWFLAKE_ROLE` | Yes | Role for SQL operations (e.g. `OPENFLOW_ADMIN`) |
| `NIFI_RUNTIME_PAT` | Yes* | NiFi Bearer token (*not required when `nifi_auth` is set in config.yaml) |
| `NIFIHUB_REGISTRY_PAT` | Yes | GitHub PAT for Flow Registry Clients (repo read access) |
| `GH_SECRETS_JSON` | No | JSON object of secrets for `${{ secrets.NAME }}` resolution (default `{}`) |
| `GH_VARS_JSON` | No | JSON object of variables for `${{ vars.NAME }}` resolution (default `{}`) |

> **Note:** `SNOWFLAKE_*` variables are read by `describe_live_state.py` even for non-SOM runtimes (to list connectors). If your runtime uses `url:` and has no Snowflake connectors, you can set them to placeholder values.

---

## Manual Trigger via GitHub Actions

If you want to run the CD pipeline on GitHub's infrastructure without pushing a config change, use the `workflow_dispatch` trigger:

### From the GitHub UI

1. Go to your repository → **Actions** → **Environment CD**
2. Click **Run workflow**
3. Enter the environment name (e.g. `demo`) and click **Run workflow**

### From the GitHub CLI

```bash
# Deploy a specific environment
gh workflow run environment-cd.yml --field environment_name=demo

# List recent CD runs to check status
gh run list --workflow=environment-cd.yml
```

This is equivalent to merging a change to `environments/demo/config.yaml` — the pipeline runs describe → diff → apply. Useful for:
- Force-reconciling after a manual change on the Snowflake side
- Triggering after a script fix (that doesn't affect any config YAML)
- Testing the pipeline without a dummy commit

---

## Running Against a Local Apache NiFi Instance

This demonstrates that NiFi Hub's GitOps tooling is portable — the same `config.yaml` format and Python scripts work against any NiFi endpoint, not just Snowflake Openflow.

### What Changes for Local NiFi

When a runtime has a `url:` field, the CD pipeline:
- **Skips** all Snowflake SQL operations (no `CREATE/ALTER OPENFLOW RUNTIME`, EAI, network rules)
- **Still runs** NiFi API reconciliation (flow registries, flows, parameters, controller services)

Authentication uses `nifi_auth` instead of `NIFI_RUNTIME_PAT`. See [Non-SOM Runtimes](Introduction-and-Concepts--Non-SOM-Runtimes) for the full model.

### Step 1 — Start a Local NiFi Instance

The simplest way is Docker. Apache NiFi 2.x includes a built-in single-user login:

```bash
docker run --name local-nifi \
  -p 8443:8443 \
  -e SINGLE_USER_CREDENTIALS_USERNAME=admin \
  -e SINGLE_USER_CREDENTIALS_PASSWORD=adminpassword \
  apache/nifi:latest
```

Wait for NiFi to start (check `docker logs -f local-nifi` until you see `Started Application`). The UI is at `https://localhost:8443/nifi`.

> **Note on SSL:** The Docker image uses a self-signed certificate. Set `verify_ssl: false` in the `nifi_auth` config to skip SSL verification for local development.

### Step 2 — Create a Local Environment Config

Create `environments/local/config.yaml`:

```yaml
# yaml-language-server: $schema=../schema.json
account:
  name: local
  github_environment: local

deployments:
  - name: LOCAL_DEPLOYMENT
    deployment_type: SNOWFLAKE
    runtimes:
      - name: LOCAL_NIFI
        database: ""
        schema: ""
        url: "https://localhost:8443"
        nifi_auth:
          type: username_password
          username: ${{ vars.NIFI_USERNAME }}
          password: ${{ secrets.NIFI_PASSWORD }}
          verify_ssl: false   # local self-signed cert — remove in production
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
        flows:
          - name: "Hello World"
            bucket: examples
            flow: hello-world
            version: latest
            start: true
```

### Step 3 — Set Environment Variables and Run

```bash
# Credentials for the local NiFi (resolved via GH_SECRETS_JSON / GH_VARS_JSON)
export GH_SECRETS_JSON='{"NIFI_PASSWORD":"adminpassword","NIFIHUB_REGISTRY_PAT":"ghp_..."}'
export GH_VARS_JSON='{"NIFI_USERNAME":"admin"}'

# Snowflake vars (required by describe_live_state.py even though no SQL runs)
# Set to placeholder values if you have no Snowflake account
export SNOWFLAKE_ACCOUNT_URL="https://placeholder.snowflakecomputing.com"
export SNOWFLAKE_USER="placeholder"
export SNOWFLAKE_PAT="placeholder"
export SNOWFLAKE_ROLE="SYSADMIN"

# Run (dry run first to preview)
python scripts/run-cd.py environments/local/config.yaml --dry-run

# Apply
python scripts/run-cd.py environments/local/config.yaml
```

### What Happens

1. **Describe** — no Snowflake SQL for the URL-managed runtime; NiFi state is read via REST API
2. **Diff** — compares live NiFi state (flow registries, flows) against desired config
3. **Translate** — URL-managed runtimes are always included in the apply list
4. **Apply** — creates the `nifihub` registry client in NiFi and deploys the Hello World flow

After the run, open `https://localhost:8443/nifi` and you should see the Hello World flow running on the canvas.

### Full End-to-End Example: Postgres CDC Demo

For a more realistic demonstration including a data generator flow:

```yaml
runtimes:
  - name: LOCAL_NIFI
    database: ""
    schema: ""
    url: "https://localhost:8443"
    nifi_auth:
      type: username_password
      username: ${{ vars.NIFI_USERNAME }}
      password: ${{ secrets.NIFI_PASSWORD }}
      verify_ssl: false
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
    flows:
      - name: "CDC Postgres Demo - Data Generator"
        bucket: data-generator
        flow: postgres-cdc-demo
        version: latest
        start: false
        assets:
          - name: "postgresql-42.7.10.jar"
            url: "https://jdbc.postgresql.org/download/postgresql-42.7.10.jar"
            parameter: "Database Driver"
        parameters:
          Database Connection URL: "jdbc:postgresql://your-postgres-host:5432/mydb?sslmode=require"
          Database Name: "mydb"
          Database User: "myuser"
          Database Password: "mypassword"
          Schema Name: "demo"
          Publication Name: "demo_publication"
```

---

## How nifi_auth Works

When `nifi_auth` with `type: username_password` is configured, the pipeline calls `nipyapi.security.service_login()` instead of setting a Bearer token. This:

1. POSTs to `/nifi-api/access/token` with the username and password (form-encoded)
2. Receives a JWT token from NiFi
3. Caches the JWT in nipyapi's config for all subsequent API calls

This is the standard authentication flow for OSS Apache NiFi instances that use username/password login (the `SingleUserLoginIdentityProvider` in NiFi 2.x).

The credentials are **never hardcoded** in `config.yaml`. They are referenced via `${{ secrets.NAME }}` and `${{ vars.NAME }}` syntax, which is resolved from `GH_SECRETS_JSON` / `GH_VARS_JSON` environment variables at runtime.

---

## Troubleshooting

**SSL verification (`verify_ssl`):**

| Value | Behavior |
|---|---|
| `true` (default) | Verify certificate against the system CA store |
| `false` | Disable SSL verification — use only for local dev with self-signed certs |
| `"/path/to/ca-bundle.crt"` | Verify against a custom CA certificate bundle (e.g. internal PKI) |

Example with a custom CA bundle:
```yaml
nifi_auth:
  type: username_password
  username: ${{ vars.NIFI_USERNAME }}
  password: ${{ secrets.NIFI_PASSWORD }}
  verify_ssl: "/etc/ssl/certs/my-company-ca.crt"
```

**SSL certificate error (`CERTIFICATE_VERIFY_FAILED`)**
Add `verify_ssl: false` to the `nifi_auth` block. Only use this for local development with self-signed certificates. For production NiFi instances with a custom CA, use `verify_ssl: "/path/to/ca-bundle.crt"` instead.

**Connection refused**
Verify NiFi is running: `docker logs local-nifi | tail -20`. Check that the port matches the `url` field.

**Authentication failed (401)**
Check that `NIFI_USERNAME`/`NIFI_PASSWORD` match the credentials in `SINGLE_USER_CREDENTIALS_USERNAME`/`SINGLE_USER_CREDENTIALS_PASSWORD` used when starting the container.

**`SNOWFLAKE_ACCOUNT_URL not set` or Snowflake connection errors**
Even for non-SOM runtimes, `describe_live_state.py` tries to connect to Snowflake to list connectors and runtimes. Set the `SNOWFLAKE_*` variables to real or placeholder values. If you have no Snowflake account, set them to placeholders — the script will log errors but continue.

**`NIFI_RUNTIME_PAT` not set warning**
This is expected when using `nifi_auth` in `config.yaml`. The `NIFI_RUNTIME_PAT` env var is not required when username/password auth is configured.

**Flow registry client type not found**
Ensure NiFi has the GitHub Flow Registry Client extension installed. This is included in standard NiFi distributions as of NiFi 2.x. Check NiFi's extension bundles if the registry type is unavailable.
