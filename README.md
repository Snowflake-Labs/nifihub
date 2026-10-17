# NiFi Hub

A community repository of independently versioned Apache NiFi extension bundles,
versioned flow definitions, and automation for managing resources on existing
Apache NiFi instances.

NiFi Hub connects directly to the NiFi REST API. You provide and operate the NiFi
servers; this repository manages flow registries, flows, controller services,
parameter providers, parameter contexts, parameters, assets, and service bindings.

## Repository structure

```text
extensions/<category>/<sub-category>/nifi-<name>-bundle/
  pom.xml                         Independent bundle version and modules
  nifi-<name>-processors/          Java processor code and tests
  nifi-<name>-nar/                 NiFi Archive packaging
  SKILL.md                        Agent-readable bundle documentation
flows/<bucket>/
  <flow>.json                     Versioned NiFi flow
  <flow>.md                       Flow documentation
  tests/test_<flow>.py            Structural and integration tests
  tests/test_<flow>.yaml          Integration test parameters and NARs
environments/<name>/config.yaml   Desired resources on existing NiFi URLs
environments/schema.json         Configuration schema
scripts/cd/                      Discover, diff, render, and reconcile resources
scripts/ci/                      Deploy isolated test process groups
scripts/run-cd.py                 Local deployment entry point
wiki/                            Usage and architecture guides
```

## Build an extension

Use Java 21+ and the included Maven wrapper. Each bundle builds independently;
the root POM supplies shared dependencies and quality rules.

```bash
./mvnw clean verify -Pcontrib-check,report-code-coverage \
  -f extensions/examples/getting-started/nifi-example-bundle/pom.xml
```

Bundle versions are defined once through `<revision>` in the bundle POM. Removing
`-SNAPSHOT` and merging the version change triggers a GitHub release with the NAR
attached. The release workflow opens a PR for the next development version. A
manual Release Bundle workflow is also available.

Install the required NARs on each target NiFi instance before deploying a flow.
Use the NiFi extension installation mechanism or `scripts/upload-nars.py` for
instances that allow NAR uploads. Match the extension API and referenced processor
bundle versions to the target NiFi version. The Hello World flow requires the
example NAR as well as standard NiFi processors.

## Manage an existing NiFi instance

Install Python 3.12+ dependencies:

```bash
python -m pip install -r scripts/cd/requirements-cd.txt -r flows/requirements.txt
```

Create `environments/<name>/config.yaml` using the [example](environments/example/config.yaml):

```yaml
name: development
github_environment: development
runtimes:
  - name: primary
    url: https://nifi.example.com:8443
    root_parameter_context:
      parameters:
        Environment: development
```

Authenticate using a NiFi username/password or an existing NiFi bearer token:

```bash
export NIFI_USERNAME=admin
export NIFI_PASSWORD='your-nifi-password'
python scripts/run-cd.py environments/local/config.yaml --dry-run
python scripts/run-cd.py environments/local/config.yaml
```

Use `NIFI_TOKEN` instead of username/password when a token is supplied by your
authentication system. `NIFI_VERIFY_SSL` defaults to `true`; set it to a CA bundle
path for private certificates, or `false` only for local development. Credentials
require HTTPS. An unsecured local HTTP instance can be used without credentials.
Per-instance `nifi_auth` blocks are also supported.

The pipeline reads live NiFi state, compares it with YAML, renders or translates
the plan, then reconciles resources in dependency order. `--dry-run` performs reads
and produces a plan without applying resource changes. Discovery failures block
application. Failures during reconciliation are reported while other configured
instances continue.

An omitted resource section leaves those resources untouched. An explicitly
declared `flows`, `flow_registries`, `controller_services`,
`root_pg_controller_services`, or `parameter_providers` list is authoritative for
that scope: resources missing from the list are removed, including when the list
is empty. Review the plan before using an existing instance with unrelated flows.
Removing an entire runtime entry stops managing it; it never removes the server.
Root parameter context inheritance, parameters, and assets are additive and are
preserved when their declarations are removed.

See [the configuration reference](environments/README.md) for shared services,
service bindings, provider inheritance, secrets, and migration instructions.

## GitHub Actions

Create a GitHub Environment for each deployment configuration. Set:

| Name | Location | Purpose |
|---|---|---|
| `NIFI_USERNAME` | Variable | NiFi login username |
| `NIFI_PASSWORD` | Secret | NiFi login password |
| `NIFI_TOKEN` | Secret | Alternative to username/password |
| `NIFI_VERIFY_SSL` | Variable | Optional TLS verification setting |
| `NIFI_URL` | Variable | Existing dedicated NiFi instance for flow integration tests |

Environment CD uses the URLs declared in YAML. Integration tests use `NIFI_URL`
and environment credentials consistently for deployment, assertions, and cleanup.
The runner must be able to reach the NiFi API. Configure a reachable endpoint or
adapt `runs-on` to a runner on your network. Protect credential-bearing GitHub
Environments with the appropriate reviewers and access rules.

`${{ secrets.NAME }}` and `${{ vars.NAME }}` references resolve from the selected
GitHub Environment. For local runs, supply equivalent `GH_SECRETS_JSON` and
`GH_VARS_JSON` objects when your config uses these references.

| Workflow | Purpose |
|---|---|
| Bundle CI / Code Compliance | Maven builds, tests, Checkstyle, PMD, RAT, and coverage |
| Flow CI | Flow diffs, best-practice checks, and structural tests |
| Python CI | CD regression tests, flow structural tests, and example validation |
| Flow Deploy | Maintainer-triggered live tests on an existing NiFi test instance |
| Environment CD Validate | Validate configuration; preview live changes for same-repository PRs |
| Environment CD | Apply configuration on main or manual dispatch |
| Auto Release / Release Bundle | Publish independently versioned NAR artifacts |
| Wiki Docs | Generate extension documentation |

Maintainers with admin/maintain repository permission can comment `deploy this
flow` on a PR. The workflow installs required NARs, imports a uniquely named test
process group, configures it, runs tests, and deletes that process group and its isolated parameter contexts. It never
provisions or destroys a NiFi server. Parameter context names are isolated per test run. Cleanup also runs if configuration fails
after import. Add `do not clean` to retain the test process group for debugging.
NARs and explicitly configured shared services/providers remain installed, so use
a dedicated test instance. Flow test settings live in the companion test YAML.

## Tests and contributions

```bash
pytest scripts/cd/tests flows/ --import-mode=importlib -q
```

Flow structural tests run offline. Live tests require `NIFI_URL`, `DEPLOYED_PG_ID`,
and the same NiFi credentials used by the deployment scripts. Without a target
and process group ID they are skipped. Bundle unit tests use NiFi's `TestRunner`;
coverage targets are 80%.

See [CONTRIBUTING.md](CONTRIBUTING.md) for bundle and flow authoring conventions.

## License

Licensed under [Apache License 2.0](LICENSE). Original copyright and artifact
coordinates are retained for attribution and compatibility.
