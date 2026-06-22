# NiFi Hub

A community-driven repository of independently-versioned [Apache NiFi](https://nifi.apache.org/) extension bundles and versioned flow definitions.

> **Disclaimer:** The contents of this repository are community-driven and provided on an "AS IS" basis, without warranties of any kind, express or implied. They are NOT supported by Snowflake and do not constitute a Snowflake product or service. Snowflake makes no guarantees regarding functionality, compatibility, availability, or fitness for any particular purpose, and assumes no liability arising from use of this repository. Community support is available through GitHub Issues and Pull Requests.

## Table of Contents

- [Overview](#overview)
- [Repository Structure](#repository-structure)
- [Getting Started](#getting-started)
- [Creating a New Bundle](#creating-a-new-bundle)
- [Creating a New Flow](#creating-a-new-flow)
- [Quality Gates](#quality-gates)
- [Releases](#releases)
- [Environments (CD)](#environments-cd)
- [CI/CD Workflows](#cicd-workflows)
- [Technology Stack](#technology-stack)
- [License](#license)

## Overview

NiFi Hub provides:

- **Extension Bundles**: Reusable NiFi processors, controller services, and reporting tasks packaged as NARs
- **Flow Definitions**: Versioned NiFi flow definitions with documentation and automated validation
- **Strict Quality Gates**: Checkstyle, PMD, license header checks, and 80% code coverage enforced on every PR
- **Independent Versioning**: Each bundle has its own version and release lifecycle
- **Agent-Friendly**: Every bundle and flow includes machine-readable documentation for AI coding assistants

## Repository Structure

```
nifihub/
├── extensions/                    # NiFi extension bundles
│   └── <category>/
│       └── <sub-category>/
│           └── nifi-<name>-bundle/
│               ├── pom.xml        # Bundle aggregator POM
│               ├── SKILL.md       # Agent-readable documentation
│               ├── nifi-<name>-processors/
│               └── nifi-<name>-nar/
├── environments/                  # Openflow-as-Code environment configs
│   ├── schema.json                # JSON Schema for validation
│   └── <env-name>/
│       └── config.yaml            # Deployments, runtimes, and flows
├── flows/                         # NiFi flow definitions
│   └── <bucket>/
│       ├── <flow>.json            # Flow definition
│       ├── <flow>.md              # Flow documentation
│       └── tests/
│           ├── test_<flow>.py     # Validation + runtime tests
│           └── test_<flow>.yaml   # CI runtime config
├── scripts/cd/                    # CD orchestration scripts
├── scripts/ci/                    # CI ephemeral runtime scripts
├── pom.xml                        # Root parent POM
├── checkstyle.xml                 # Checkstyle rules (NiFi conventions)
├── pmd-ruleset.xml                # PMD rules (NiFi conventions)
└── codecov.yml                    # Coverage configuration
```

## Getting Started

### Recommended Approach

The recommended way to use NiFi Hub is to **fork this repository** into your organization (public or private). Your fork is where you make environment-specific changes (account configs, custom flows, proprietary extensions), while contributions to improve the framework itself, submit reusable flows, or add extensions are welcome back to this upstream repository via Pull Requests.

### GitHub Environment Setup

The CD pipeline requires a [GitHub Environment](https://docs.github.com/en/actions/deployment/targeting-different-environments/using-environments-for-deployment) with the following secrets and variables:

| Name | Type | Purpose |
|------|------|---------|
| `SNOWFLAKE_PAT` | Secret | Programmatic Access Token for Snowflake SQL operations (CREATE/ALTER/DROP deployment, runtime, connectors, etc.) |
| `NIFI_RUNTIME_PAT` | Secret | Programmatic Access Token for the NiFi runtime REST API (flow checkout, parameters, controller services) |
| `NIFIHUB_REGISTRY_PAT` | Secret | GitHub Personal Access Token with repo read access, injected into Flow Registry Clients so NiFi can pull flow definitions |
| `SNOWFLAKE_ACCOUNT_URL` | Variable | Snowflake account URL (e.g. `https://myorg-myaccount.snowflakecomputing.com`) |
| `SNOWFLAKE_USER` | Variable | Snowflake user for PAT-based authentication |
| `SNOWFLAKE_ROLE` | Variable | Role used for SQL operations (e.g. `OPENFLOW_ADMIN`) |

Additional secrets/variables referenced via `${{ secrets.* }}` or `${{ vars.* }}` in `config.yaml` are resolved from the same GitHub Environment at deploy time.

### Prerequisites

- Java 21+
- Python 3.12+ (for flow tests)
- Git

Maven is provided via the included wrapper (`./mvnw`).

### Building an Extension Bundle

Each bundle builds independently from its own directory:

```bash
# Build and test
./mvnw clean verify -f extensions/examples/getting-started/nifi-example-bundle/pom.xml

# Build with all quality checks (checkstyle + PMD + license headers)
./mvnw clean verify -Pcontrib-check -f extensions/examples/getting-started/nifi-example-bundle/pom.xml

# Build with code coverage report
./mvnw clean verify -Preport-code-coverage -f extensions/examples/getting-started/nifi-example-bundle/pom.xml
```

### Running Flow Tests

```bash
pip install -r flows/requirements.txt
pytest flows/examples/tests/ -v
```

## Creating a New Bundle

Use the provided scaffold script:

```bash
./create-bundle.sh <category> <sub-category> <name>
# Example: ./create-bundle.sh data snowflake nifi-snowflake
```

Or manually create the directory structure following the [example bundle](extensions/examples/getting-started/nifi-example-bundle/) as a template.

See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed instructions.

## Creating a New Flow

1. Export your flow definition as JSON from NiFi
2. Place it at `flows/<bucket>/<flow-name>.json`
3. Create a companion `flows/<bucket>/<flow-name>.md` with documentation
4. Add validation tests at `flows/<bucket>/tests/test_<flow-name>.py`

See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed instructions.

## Quality Gates

All pull requests must pass these checks before merging:

### Extension Bundles
- **Checkstyle**: NiFi coding conventions (200 char lines, no star imports, etc.)
- **PMD**: Static analysis rules aligned with Apache NiFi
- **Apache RAT**: License header verification on all source files
- **Unit Tests**: All tests pass via Maven Surefire
- **Code Coverage**: Minimum 80% line coverage (enforced via Codecov)

### Flow Definitions
- **Flow Diff**: Human-readable diff of flow changes (via [Snowflake Flow Diff](https://github.com/Snowflake-Labs/snowflake-flow-diff))
- **Flow Checkstyle**: Best practice violations (concurrent tasks, self-loops, backpressure)
- **Validation Tests**: Structural validation via pytest + nipyapi

### Maintainer Actions
- Members of the `nifihub-maintainers` team can comment `deploy this flow` on a PR to deploy changed flows to an ephemeral Snowflake runtime for live testing (see [Flow Deploy CI](#flow-deploy-ci))

## Releases

Each bundle is versioned independently using Maven CI-friendly versions. The version is defined once in the bundle's aggregator `pom.xml` via the `<revision>` property:

```xml
<properties>
    <revision>0.2.0-SNAPSHOT</revision>
</properties>
```

### Automatic Release (recommended)

1. In a PR, change the bundle's `<revision>` from `X.Y.Z-SNAPSHOT` to `X.Y.Z`
2. Merge the PR into `main`
3. The **Auto Release** workflow detects the non-SNAPSHOT version and automatically:
   - Builds and verifies the bundle with all quality checks
   - Creates a Git tag and GitHub Release with the NAR attached
   - Bumps the version to the next patch SNAPSHOT (e.g., `0.2.0` -> `0.2.1-SNAPSHOT`) and pushes it back to `main`

### Manual Release (fallback)

The **Release Bundle** workflow can be triggered manually from the Actions tab for cases where you need full control:

1. Go to **Actions > Release Bundle > Run workflow**
2. Specify the bundle path, release version, and next development version
3. The workflow handles the rest (build, tag, release, version bump)

This is useful for version jumps (e.g., `0.2.0` -> `1.0.0`) or retrying a failed auto-release.

## Environments (CD)

NiFi Hub includes a **GitOps CD pipeline** for declaratively managing Openflow deployments, runtimes, and flow assignments. Infrastructure is defined as YAML in the `environments/` directory — when changes are merged to `main`, GitHub Actions automatically apply them to Snowflake.

### How It Works

1. Define your Openflow infrastructure in `environments/<env>/config.yaml`
2. Open a PR — the **Environment CD Validate** workflow queries the **live Snowflake environment** (via `SHOW`/`DESCRIBE OPENFLOW` and NiFi REST API), diffs it against the desired YAML, and posts a change plan as a PR comment
3. Merge to `main` — the **Environment CD** workflow performs the same live diff and applies changes

The pipeline compares the **actual deployed state** against the desired configuration, not successive git commits. This means drift is always detected and corrected.

### What Gets Managed

- **Openflow Deployments** — created/altered/terminated via SOM SQL
- **Network Rules & EAIs** — auto-created from YAML for runtime egress access
- **Openflow Runtimes** — created with EAI bindings, polled until ACTIVE
- **Flow Registry Clients** — Git-based, configured via NiFi REST API
- **Imported Flows** — pulled from the Git registry at a specific version
- **Openflow Connectors** — created, configured, and started via the SOM SQL API (lifecycle: STOP → TERMINATE → DROP)
- **NiFi Resources** — controller services, parameter providers, parameters (compared via NiFi REST API)
- **Auto-Provisioned Snowflake Parameter Provider** — every runtime has a built-in "Openflow - Snowflake Parameter Provider" that exposes Snowflake secrets as parameters. The CD pipeline automatically fetches this provider and adds its parameter contexts as inherited to all flows (no YAML declaration needed). By default all parameters are marked sensitive; to customize, declare the provider explicitly in the YAML with a `sensitive_param_pattern`.

### Runtime Options

- **`suspend: true`** — creates the runtime then immediately suspends it (useful for cost savings)
- **`reconcile: false`** — skips all NiFi-level reconciliation (flow registries, flows, parameters, controller services) for a runtime; useful for runtimes managed externally or by CI

Runtimes prefixed with `CI_` are automatically ignored by the live diff (they are ephemeral, managed by CI workflows).

### Forking for Your Own Environments

1. Fork this repository
2. Create `environments/<your-env>/config.yaml` following the [schema](environments/README.md)
3. Set up a GitHub Environment with your Snowflake account secrets
4. Merge to `main` — your infrastructure is provisioned automatically

See [`environments/README.md`](environments/README.md) for the full YAML schema reference, prerequisites, and setup guide.

### SOM-Enabled vs Non-SOM Environments

The CD pipeline supports two modes depending on whether your Snowflake account has SOM (Service Object Model) enabled for Openflow:

**SOM-enabled accounts** (default): The pipeline manages the full lifecycle of deployments and runtimes via Snowflake SQL (`CREATE/ALTER/DROP OPENFLOW DEPLOYMENT`, `CREATE/ALTER/SUSPEND/RESUME OPENFLOW RUNTIME`). It also manages network rules, EAIs, and connectors via SQL. This is the standard mode — you declare runtimes with `node_type`, `min_nodes`, `max_nodes`, etc. and the pipeline provisions them.

**Non-SOM accounts** (URL-managed): If your account does not support SOM, runtimes must be pre-provisioned outside of this pipeline. To use the CD pipeline for NiFi-level resource management only (flow registries, flows, parameters, controller services), add a `url` field to the runtime configuration pointing to the existing NiFi API endpoint:

```yaml
runtimes:
  - name: MY_RUNTIME
    database: OPENFLOW
    schema: OPENFLOW
    url: "https://of--my-account.snowflakecomputing.app/my-runtime"
    flow_registries:
      - name: nifihub
        # ...
    flows:
      - name: "My Flow"
        # ...
```

When `url` is present, the pipeline skips all SOM SQL operations (no deployment creation, no runtime create/alter/suspend/resume, no EAI or network rule management). It connects directly to the NiFi REST API at the given URL to manage flow registries, flows, parameters, controller services, and parameter providers.

For open-source Apache NiFi instances that use username/password login (rather than Snowflake Bearer tokens), add a `nifi_auth` block:

```yaml
nifi_auth:
  type: username_password
  username: ${{ vars.NIFI_USERNAME }}
  password: ${{ secrets.NIFI_PASSWORD }}
  verify_ssl: false   # for self-signed certificates
```

See the [Non-SOM Runtimes wiki page](../../wiki/Introduction-and-Concepts--Non-SOM-Runtimes) for full details.

### Running the CD Pipeline Locally

The CD pipeline scripts can be run directly from your machine without pushing to `main`:

```bash
# Preview changes (no apply)
python scripts/run-cd.py environments/demo/config.yaml --dry-run

# Apply changes
python scripts/run-cd.py environments/demo/config.yaml
```

This runs the same describe → diff → translate → orchestrate pipeline used by GitHub Actions, and works against any NiFi endpoint including local Apache NiFi instances.

See the [Running CD Locally wiki page](../../wiki/How-to-Run-CD-Locally) for the full guide, environment variable reference, and a step-by-step walkthrough using Docker NiFi.

## CI/CD Workflows

| Workflow | Trigger | Purpose |
|----------|---------|---------|
| Bundle CI | PR modifying `extensions/` | Build, test, coverage for changed bundles |
| Flow CI | PR modifying `flows/` | Flow diff, validation tests, flow checkstyle, deploy hint |
| Flow Deploy | Maintainer comment `deploy this flow` | Deploy flows to ephemeral runtime, run tests, report via check run |
| Code Compliance | PR + push | Checkstyle, PMD, RAT, CodeQL analysis |
| Dependency Check | PR modifying `extensions/` | Fail if dependencies are not on latest versions |
| Auto Release | Push to `main` changing `extensions/**/pom.xml` | Automatic release when SNAPSHOT suffix is removed |
| Release Bundle | Manual dispatch | Release a bundle to GitHub Releases (fallback) |
| Wiki Docs | Push to `main` changing `extensions/` | Generate and publish extension documentation to the wiki |
| Environment CD | Push to `main` changing `environments/` or manual trigger | Apply Openflow changes via live state diff |
| Environment CD Validate | PR modifying `environments/` | Live state diff + change plan posted as PR comment |

### Flow Deploy CI

When a maintainer comments `deploy this flow` on a PR that modifies flow JSON files, the **Flow Deploy** workflow:

1. **Detects** changed flows and their test YAML configs
2. **Provisions** an ephemeral runtime (named `CI_<FLOW>_<PR>_<RUN_ID>`) with the configuration defined in the test YAML
3. **Deploys** the flow from the PR branch using CD helpers (uploads flow JSON, adds inherited parameter contexts, applies parameters, uploads assets, starts the flow)
4. **Waits** 60 seconds for the flow to process data
5. **Runs** pytest tests (structural + runtime execution tests via nipyapi)
6. **Reports** results as a PR comment (with per-test pass/fail table and failure details) and a GitHub Check Run (blocks merge on failure)
7. **Tears down** the ephemeral runtime (unless `do not clean` is included in the comment)

#### Test YAML Configuration

Each flow can define a test configuration at `flows/<bucket>/tests/test_<flow_name>.yaml`:

```yaml
# yaml-language-server: $schema=../../../scripts/ci/ci-runtime-schema.json
github_environment: example          # GitHub Environment for secrets
deployment: MY_DEPLOYMENT
database: OPENFLOW
schema: OPENFLOW
node_type: MEDIUM
min_nodes: 1
max_nodes: 1
network_rules:
  - name: POSTGRES
    type: HOST_PORT
    mode: EGRESS
    values:
      - "my-host.snowflake.app:5432"
flow:
  assets:
    - name: "postgresql-42.7.10.jar"
      url: "https://jdbc.postgresql.org/download/postgresql-42.7.10.jar"
      parameter: "Database Driver"
  parameters:
    Database Connection URL: "jdbc:postgresql://..."
    Database Name: "mydb"
```

The `flow` section defines parameters and assets to apply after deployment. The `github_environment` field determines which GitHub Environment secrets are used (for Snowflake credentials, PATs, etc.).

#### Runtime Execution Tests

Beyond structural validation, test files can include a `TestRuntimeExecution` class that runs against the live deployed flow:

```python
class TestRuntimeExecution:
    def test_flow_is_running(self, nifi_runtime, running_flow):
        # Verify flow has active threads

    def test_no_error_bulletins(self, nifi_runtime, running_flow):
        # Check NiFi bulletin board for errors

    def test_putdatabaserecord_has_output(self, nifi_runtime, running_flow):
        # Verify processor received data
```

These tests are automatically skipped when run locally without the runtime environment variables (`SNOWFLAKE_RUNTIME_URL`, `SNOWFLAKE_RUNTIME_PAT`, `DEPLOYED_PG_ID`).

### Environment CD Pipeline

The CD pipeline uses **live state diffing** rather than comparing git commits:

1. Queries the Snowflake account (`SHOW`/`DESCRIBE OPENFLOW DEPLOYMENT/RUNTIME/CONNECTOR`, network rules, EAIs)
2. Queries each active runtime's NiFi REST API (controller services, parameter providers, flow registries, flows, parameters)
3. Compares the live state against the desired YAML configuration
4. Generates a change plan (create/modify/delete) and either posts it as a PR comment (validate) or executes it (CD)

The pipeline uses a **continue-on-error** pattern: if one runtime fails to reconcile, it collects the error and continues with other runtimes, reporting all failures at the end.

## Technology Stack

- **Java 21** with Maven for extension bundles
- **Apache NiFi 2.9.x** (nifi-api 2.8.0)
- **Python 3.12** with [nipyapi](https://github.com/Chaffelson/nipyapi) for flow testing
- **GitHub Actions** for CI/CD
- **Codecov** for coverage tracking
- **Snowflake Flow Diff** for flow change visualization

## License

Copyright (c) Snowflake Inc. All rights reserved.

This project is licensed under the [Apache License 2.0](LICENSE).
