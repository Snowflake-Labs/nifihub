# Existing Apache NiFi Instances

Every managed runtime is an existing Apache NiFi instance addressed by a URL.
NiFi Hub configures resources through the REST API; server installation, scaling,
upgrades, networking, and storage are operated separately.

```yaml
name: development
runtimes:
  - name: primary
    url: https://nifi.example.com:8443
    root_parameter_context:
      parameters:
        Environment: development
```

Supply `NIFI_USERNAME` / `NIFI_PASSWORD`, a `NIFI_TOKEN`, or a per-instance
`nifi_auth` block. TLS verification defaults to enabled. The same resource
configuration works for standalone and clustered NiFi when the runner can reach
the API and has the required permissions.

See [the configuration reference](../environments/README.md) and
[running CD locally](How-to-Run-CD-Locally).
