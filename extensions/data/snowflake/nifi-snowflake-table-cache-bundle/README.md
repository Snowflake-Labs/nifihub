# Snowflake Table Cache

A self-refreshing `RecordLookupService` for `LookupRecord`. It materializes a
Snowflake reference table locally, avoiding one network query for every lookup.
It is a controller service, not a processor. Each node maintains its own cache.

Community contribution provided AS IS, not a Snowflake-supported product or
service. See the repository's support disclaimer and Apache 2.0 license.

## Configure

| Property | Meaning |
|---|---|
| Snowflake Connection Service | Existing `DBCPService` with access to the reference table |
| Source Table | Reference table or view; use a fully qualified identifier |
| Key Columns | Comma-separated lookup columns; supply each as a LookupRecord coordinate |
| Value Columns | Columns to return, or `*`; all returned values are strings |
| Storage Mode | Heap (default) or Embedded (RocksDB) |
| Storage Directory | Dedicated writable directory per Embedded service and node |
| Refresh Interval | Delay after each refresh completes; default one hour; zero disables scheduled refresh |
| Incremental Refresh | Opt-in Snowflake `CHANGES` reads; requires change tracking and suitable retention |

Use an existing connection service whose runtime identity has only the required
database/schema usage, reference-table SELECT, and warehouse access. Change
tracking must be configured by an authorized table owner. The cache does not
create streams, tasks, source tables or grants.

Enable the connection service before the cache. In `LookupRecord`, configure
record reader/writer services, choose this lookup service, map a key column such
as `ITEM_ID` to `/item_id`, and write the returned record at `/reference`.
For example, a reference row `ITEM_ID=example-1, LABEL=blue` enriches
`{"item_id":"example-1"}` with a reference record containing string values.
An unknown key is unmatched, not a lookup failure.

Keys must be unique and non-null in the source, including every composite-key
part. Full loads reject duplicates or nulls without publishing a partial cache.
Keep uniqueness enforced upstream: an incremental delta cannot detect a duplicate
against an unchanged source row. Composite keys use length-prefixed encoding.

## Refresh and failure semantics

- Lookups fail before the first successful load, rather than reporting a miss.
- After hydration, refresh failures retain the previous cache **without an age
  limit**. Warm starts can serve old data before their first scheduled refresh.
  This is not fail-closed authorization or guaranteed real-time revocation.
- Full reloads replace the cache. Incremental reads advance a node-local watermark
  alongside its data. SQL failures on the incremental path attempt a full reload.
- Incremental reads retain both update halves, remove the old key when a key
  changes, and let inserts win over deletes for the same key regardless of row
  order. Reads have an explicit end watermark. Local tests cover row application;
  the changed Snowflake query still requires live verification.
- Initial-load failures are retried only when the refresh interval is positive.
- Every value is a string, including numeric and semi-structured values. Convert
  downstream when native types are required.

## Storage and operating limits

Heap needs room for both the old and new maps during refresh. RocksDB uses disk,
native memory and the operating-system page cache. Incremental changes are first
collected in JVM heap; Embedded is not a bounded-memory guarantee for large deltas.
Full reloads temporarily keep old and new on-disk generations.

Use a dedicated persistent directory, never a directory belonging to NiFi state,
another service, or a different reference source. Do not assume a writable path
is persistent. Backup/recovery, permissions and capacity belong to the runtime
operator. Keep source credentials out of paths and logs.

Embedded mode locks its directory and checks a versioned identity containing the
connection-service ID, source expression, key/value columns and incremental mode.
Mismatches fail enablement; existing generations are not deleted on open errors.
This does not detect a changed account, role, database, or session configuration
inside the same DBCP service, nor source replacement under the same table name.
Use a fresh directory for those changes. Fully qualify source names and keep the
connection context stable. This is a configuration guard, not a source identity
or authorization guarantee.

Disable waits for an in-flight refresh before closing native storage. Configure
connection/query timeouts in the supplied DBCP service: a driver that ignores
interrupts can delay disablement. Incremental changes are not synchronously fsynced
on every refresh; abrupt crash/power-loss durability is not claimed.

A predecessor build was exercised on one managed Medium Openflow node using
`/nifi/configuration_resources`, including incremental refresh and adoption by a
new JVM after graceful suspend/resume on the same host. That observation is not
a platform storage guarantee or qualification of this newly packaged artifact.
This bundle still requires testing on the exact intended runtime. Crash recovery,
host replacement, multi-node behavior, Small runtimes and performance at scale are
not established by that smoke test.

## Build and test

From the repository root, with Java 21:

```bash
./mvnw clean verify -Pcontrib-check,report-code-coverage \
  -f extensions/data/snowflake/nifi-snowflake-table-cache-bundle/pom.xml
```

Normal builds use H2 and temporary RocksDB directories and do not need an account.
Live tests are skipped unless `SNOWFLAKE_IT_CONNECTION` explicitly selects a test
connection. They create and drop unique fixture tables, so use a dedicated test
database/schema and explicitly scoped warehouse/role, not production.
The test helper reads the named `~/.snowflake/config.toml` connection with a TOML
parser; `SNOWFLAKE_IT_*` settings can override it using an approved secret provider.
It never needs credentials committed in this repository. JDBC is test-only;
the deployed service obtains connections from the supplied DBCP service.

## Install and release boundary

Install the generated `nifi-snowflake-table-cache-nar` on a runtime with the
matching `nifi-standard-services-api-nar`. Check controller-service discovery,
JNI load, cold and warm lookups, and incremental refresh on the installed artifact.
Record its checksum and runtime version when qualifying it.

The class and bundle coordinates differ from predecessor private builds. Use a
new service and a fresh dedicated directory; old flow exports do not automatically
resolve to this bundle. No on-disk migration or compatibility alias is provided.

Before redistribution, complete source-rights and native-dependency license review.
The RocksDB JNI artifact includes multiple native libraries but no embedded license
files; a passing RAT check of this bundle is not third-party redistribution clearance.

Keep the version at `0.1.0-SNAPSHOT` during contribution. A public PR is source
disclosure; a release version merged to main can trigger automatic NAR publication.
