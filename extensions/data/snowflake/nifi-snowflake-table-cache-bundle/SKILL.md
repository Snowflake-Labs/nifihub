# Snowflake Table Cache Bundle

## Purpose

Materialize a Snowflake reference table per node and enrich records using local
lookups. This is a community controller service, not a supported Snowflake product.

## Controller Services

`SnowflakeTableCacheLookupService` implements `RecordLookupService`. Select it in
`LookupRecord` and map each configured Key Column to a RecordPath coordinate.
It requires an enabled DBCP service and SELECT access to the reference table.
Heap is the default; Embedded requires its own writable persistent directory.
Incremental Refresh uses Snowflake CHANGES and requires source change tracking.

Read [README.md](README.md) before configuration. Values are strings. Initial
load failure is distinct from an unmatched key. Failed refreshes after hydration
serve stale data with no age limit; never claim fail-closed revocation. Never
reuse a storage directory for a different source or another service.

## Building

```bash
./mvnw clean verify -Pcontrib-check -f extensions/data/snowflake/nifi-snowflake-table-cache-bundle/pom.xml
```

## Testing

```bash
./mvnw test -f extensions/data/snowflake/nifi-snowflake-table-cache-bundle/pom.xml
```

Add `-Preport-code-coverage` to `verify` to generate JaCoCo reports. Ordinary builds
must remain account-free; live Snowflake tests require explicit opt-in and create
fixture tables. Do not invoke them as a side effect of building this bundle.
