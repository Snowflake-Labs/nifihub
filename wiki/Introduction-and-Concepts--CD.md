# CD Pipeline

NiFi Hub manages resources on existing Apache NiFi instances using their REST API.
Each environment declares named URLs and the resources to reconcile.

The pipeline runs `describe_live_state.py` → `diff_live.py` →
`translate_live_diff.py` → `orchestrate.py`. `format_change_plan.py` renders a
reviewable Markdown plan. The local runner and GitHub Actions share these scripts.

On PRs, configurations are validated without credentials. Same-repository PRs
can additionally read the live instances using a protected GitHub Environment.
Merging configuration changes to main applies them. Changes to shared CD scripts
or the schema select all configured environments; manual dispatch can select one
environment or all of them.

Reconciliation configures registries, shared services, explicit parameter
providers, the root parameter context, root process-group services, then flows.
Assets, parameters, and service bindings are applied before flows start.
Omitted resource sections leave existing resources alone; explicitly declared
lists are authoritative. Removing a runtime stops managing its resources and
never deletes the server. Unreadable live state blocks application.

For integration testing, maintainers can comment `deploy this flow` on a PR.
The Flow Deploy workflow uses an existing dedicated NiFi test instance from
`NIFI_URL`. It uploads required NARs, imports a uniquely named process group,
configures and starts it, runs tests, and removes that process group and its isolated parameter contexts.
Partial deployments are also cleaned up. NARs and explicitly configured shared
resources remain installed. Add `do not clean` to retain the group for debugging.

See [the environment reference](../environments/README.md),
[running locally](How-to-Run-CD-Locally), and [NiFi runtimes](Introduction-and-Concepts--NiFi-Runtimes).
