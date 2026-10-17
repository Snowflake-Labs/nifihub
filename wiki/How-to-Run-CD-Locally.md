# Running CD Locally

Point a configuration at an existing NiFi instance you can reach. Install Python
dependencies and provide NiFi credentials:

```bash
python -m pip install -r scripts/cd/requirements-cd.txt
export NIFI_USERNAME=admin
export NIFI_PASSWORD='your-nifi-password'
python scripts/run-cd.py environments/local/config.yaml --dry-run
python scripts/run-cd.py environments/local/config.yaml
```

Edit the URL in `environments/local/config.yaml` to match your instance. The
example flow requires the example NAR to be installed first. For a quick check
without installing a custom NAR, use a configuration containing only a
`root_parameter_context` with a non-sensitive test parameter.

`NIFI_TOKEN` can supply an existing bearer token instead of username/password.
`NIFI_VERIFY_SSL` defaults to `true`, accepts a private CA bundle path, or can be
`false` for local self-signed development certificates. Authentication requires
HTTPS. With unsecured local HTTP, leave credentials unset.

Per-instance `nifi_auth` blocks support username/password with `${{ vars.NAME }}`
and `${{ secrets.NAME }}` references. Locally those references resolve from
`GH_VARS_JSON` and `GH_SECRETS_JSON`. Missing references fail before writes.

The dry run reads NiFi resources and prints the translated plan. The apply run
repeats discovery and reconciliation; it never provisions or destroys servers.
Review deletions: explicitly declared resource lists manage their whole scope,
while omitted sections leave it untouched.

For flow integration tests, set `NIFI_URL` and credentials, run
`scripts/ci/deploy_ci_flow.py` with the flow and its test YAML, and capture its
`--output-file` process group ID. Set `DEPLOYED_PG_ID` to that ID when running
pytest. Clean up that group with `scripts/cleanup-flow.py --pg-id <id>`; the URL
and authentication are read from the same environment variables.

See [the full configuration reference](../environments/README.md).
