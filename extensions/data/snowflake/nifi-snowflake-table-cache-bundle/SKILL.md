# Snowflake Table Cache Bundle

## Purpose

Materialize a Snowflake reference table per node and enrich records using local
lookups. This is a community controller service, not a supported Snowflake product.

## Controller Services

`SnowflakeTableCacheLookupService` implements `RecordLookupService`. Select it in
`LookupRecord` and map each configured Key Column to a RecordPath coordinate.
It requires an enabled DBCP service and SELECT/metadata visibility for a fully
qualified reference object. Standard tables, native FULL/INCREMENTAL dynamic
tables, Snowflake-managed Iceberg v2/v3, views and hybrid tables are supported.
Heap is the default; Embedded requires its own writable persistent directory.
Incremental Refresh allows CHANGES on qualified tables and requires change
tracking and at least one day of retention. Views, hybrid tables and FULL-mode DTs
always reload fully. False forces full loads for all sources. Capabilities are
checked on every refresh. Full-only results are returned directly without
RESULT_SCAN, and their diagnostic marker must never resume a delta. Incremental
sources use exact child query IDs and bounded changes. JDBC remains supplied by
DBCP at runtime. The README owns the source/variant qualification matrix.

Borrowed connections require auto-commit, no active transaction, and
`QUOTED_IDENTIFIERS_IGNORE_CASE=FALSE`; reject rather than modify incompatible
session settings. Lookup keys never enter SQL. Preserve identifier parsing,
quoting and binds; do not add punctuation-stripping sanitizers. Return detached
string records/schemas and reject malformed persisted rows before allocation or
publication. Storage directories and their parents must be trusted and access-controlled.

Read [README.md](README.md) before configuration. Values are strings. Initial
load failure is distinct from an unmatched key. Persistent starts wait for source
validation. Transient refresh failures after validation serve stale data with no
age limit; detected source changes block lookups until a full reload succeeds.
Never claim immediate fail-closed revocation or comprehensive policy detection.
Never reuse a storage directory for a different source or another service; old
timestamp-format caches require a fresh directory. Nodes remain independent.

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
Source-type tests also require SNOWFLAKE_TYPES_IT=true and a permanent scratch
database for hybrid fixtures. They exercise controlled DT refreshes and managed
Iceberg v2/v3 without creating cloud integrations. Treat failures as evidence,
not a reason to silently skip the selected capability.
