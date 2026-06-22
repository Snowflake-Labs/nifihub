# CD Pipeline

NiFi Hub has two CD mechanisms: **Flow Deploy** for testing flows against ephemeral Snowflake runtimes during PR review, and **Environment CD** for managing Openflow infrastructure declaratively as code.

---

## Flow Deploy

**Workflow:** `flow-deploy.yml`
**Trigger:** A maintainer with admin or maintain permission comments `deploy this flow` on a PR

This workflow lets maintainers test a flow against a real Snowflake Openflow runtime before merging. It provisions an **ephemeral runtime**, deploys the flow, runs tests, and tears everything down automatically.

### What Happens

1. The workflow identifies flow JSON files changed in the PR
2. For each changed flow that has a test YAML (`flows/<bucket>/tests/test_<flow-name>.yaml`):
   - **Provisions an ephemeral runtime** named `CI_<FLOW>_<PR>_<RUN_ID>` with the configuration from the test YAML (node type, network rules, registries, etc.)
   - **Uploads custom NARs** from GitHub Releases if specified in the test YAML's `nars` field
   - **Deploys the flow** from the PR branch as a process group on the runtime
   - **Applies parameters and assets** from the test YAML's `flow` section
   - **Fetches the auto-provisioned Snowflake Parameter Provider** to inject secrets from Snowflake
   - **Waits** for the flow to process data (60 seconds)
   - **Runs validation tests** (`flows/<bucket>/tests/test_<flow-name>.py`) against the deployed process group
   - **Tears down** the ephemeral runtime (unless the comment includes "do not clean")
3. **Posts a comment** on the PR with per-test results and failure details
4. **Creates a Check Run** that blocks the PR merge if tests fail

### Test YAML Configuration

Each flow that needs CI testing has a `flows/<bucket>/tests/test_<flow-name>.yaml` file following the [CI runtime schema](../scripts/ci/ci-runtime-schema.json). Key fields:

| Field | Description |
|-------|-------------|
| `github_environment` | GitHub Environment holding Snowflake secrets |
| `deployment` | Existing Openflow deployment to create the ephemeral runtime in |
| `database` / `schema` | Where the runtime is scoped |
| `node_type` | Runtime size (`SMALL`, `MEDIUM`, `LARGE`) |
| `execute_as_role` | Snowflake role for runtime data access |
| `network_rules` | Egress rules needed by the flow under test |
| `flow_registries` | Git registry clients to configure |
| `nars` | Custom NARs to upload (URLs or `${GITHUB_RELEASES}/` references) |
| `sensitive_param_pattern` | Regex for classifying auto-provisioned provider parameters as sensitive |
| `flow.parameters` | Parameter values to apply after deployment |
| `flow.assets` | Files to download and bind as parameter context assets |

### GitHub Environment Secrets

The GitHub Environment referenced by `github_environment` must provide:
- `SNOWFLAKE_PAT` — Snowflake PAT for provisioning the ephemeral runtime
- `NIFI_RUNTIME_PAT` — PAT for NiFi REST API operations
- `NIFIHUB_REGISTRY_PAT` — GitHub PAT for the Flow Registry Client

---

## Environment CD

**Workflows:** `environment-cd.yml` (apply) and `environment-cd-validate.yml` (PR validation)
**Triggers:** Changes to `environments/**/config.yaml`

NiFi Hub uses a **GitOps** model for managing Openflow infrastructure. The `environments/` directory contains one subdirectory per environment, each with a `config.yaml` that declaratively describes the desired state of that environment's Openflow resources.

### What Gets Managed

| Resource | Operation |
|---|---|
| Openflow Deployments | Created, altered, or terminated via Snowflake SQL |
| Network Rules & EAIs | Created automatically from YAML entries to allow runtime egress access |
| Openflow Runtimes | Created with EAI bindings; polled until ACTIVE |
| Flow Registry Clients | Git-based clients configured via NiFi REST API |
| Imported Flows | Pulled from the Git registry at a specified version |

### PR Validation

When a PR modifies an environment config, the **Environment CD Validate** workflow:

1. Computes the diff between the current and proposed config
2. Validates the proposed config against the [JSON schema](../environments/schema.json)
3. Runs connectivity checks against the target Snowflake account
4. Posts a **change plan** comment on the PR showing exactly what resources will be created, modified, or deleted

This gives reviewers a clear picture of the infrastructure impact before merging.

### Applying Changes

When the PR merges to `main`, the **Environment CD** workflow applies the change plan to the target Snowflake account. Each environment runs in a separate GitHub Environment with its own credentials (`SNOWFLAKE_ACCOUNT_URL`, `SNOWFLAKE_USER`, `SNOWFLAKE_PAT`, `SNOWFLAKE_ROLE`).

### Triggering CD Manually

The Environment CD workflow can also be triggered manually without pushing a config change:

**From the GitHub UI:** Go to Actions → Environment CD → Run workflow → enter the environment name.

**From the GitHub CLI:**
```bash
gh workflow run environment-cd.yml --field environment_name=demo
```

This is useful for force-reconciling after a manual change on the runtime, or for re-running the pipeline after a script fix.

### Running Locally

The same pipeline can be run directly from your machine using `scripts/run-cd.py`:

```bash
python scripts/run-cd.py environments/demo/config.yaml --dry-run   # preview only
python scripts/run-cd.py environments/demo/config.yaml              # apply
```

See [Running CD Locally](How-to-Run-CD-Locally) for full instructions including how to run against a local Apache NiFi instance.

### Forking for Your Own Environment

1. Fork this repository
2. Create `environments/<your-env>/config.yaml` following the [schema](../environments/README.md)
3. Set up a GitHub Environment named `<your-env>` with your Snowflake credentials
4. Open a PR — the validate workflow will show you the change plan
5. Merge to `main` — your infrastructure is provisioned automatically
