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
| Source Table | Fully qualified table or view; see the source modes below |
| Key Columns | Comma-separated lookup columns; supply each as a LookupRecord coordinate |
| Value Columns | Columns to return, or `*`; all returned values are strings |
| Storage Mode | Heap (default) or Embedded (RocksDB) |
| Storage Directory | Dedicated writable directory per Embedded service and node |
| Refresh Interval | Delay after each refresh completes; default one hour; zero disables scheduled refresh |
| Incremental Refresh | Allow `CHANGES` on eligible tables; false forces full reloads; full-only sources always reload |

### Source modes

| Source | When Incremental Refresh is true |
|---|---|
| Standard permanent or transient table | Initial full load, then bounded changes |
| Native dynamic table with resolved INCREMENTAL mode | Initial full load, then bounded changes to its materialized output |
| Snowflake-managed Iceberg v2/v3 table | Initial full load, then bounded changes |
| View | Full reload every time; no change tracking required |
| Hybrid table | Full reload every time |
| Native dynamic table with resolved FULL mode | Full reload every time |

Incremental Refresh=false forces full reloads for all supported sources. Each
refresh checks source metadata and reports the actual FULL/INCREMENTAL mode in
the refresh log. Missing change tracking, privileges or history are not evidence
that an otherwise incremental table is a full-only source.

Temporary tables, external tables, materialized views, dynamic Iceberg tables,
externally managed Iceberg tables and other dynamic-table refresh modes are not
qualified by this bundle. Do not infer support solely from a change-tracking flag.
Managed Iceberg qualification covers Snowflake writes, not external writers.

View queries may include joins and aggregates, provided their result has unique,
non-null lookup keys. The service re-evaluates the entire view on every refresh;
unchanged DDL never skips a query. View dependency changes are reflected in the
next successful full result, without reconstructing a delta. Views over external
systems remain subject to their source's consistency and availability semantics.
For dynamic tables the cache reads the latest published output; it neither
triggers a DT refresh nor removes the DT's own freshness lag.

Use an existing connection service whose runtime identity has only the required
database/schema usage, source SELECT, warehouse access and source metadata visibility.
DT capability inspection requires a visible `refresh_mode` in SHOW DYNAMIC TABLES;
additional MONITOR privileges may be needed for that metadata. Least-privilege
runtime qualification is still required; the live source tests use a lab role. Change
tracking must be configured by an authorized table owner. The cache does not
create streams, tasks, source tables, stored procedures or grants. Each refresh
reads source metadata (including SHOW for DT/Iceberg capabilities) and submits
an anonymous SQL procedure through the same JDBC connection. That
call performs multiple child queries; it is not one transaction or one snapshot.
Borrowed connections must use auto-commit and have no caller-owned transaction;
the service rejects auto-commit-disabled connections rather than committing or
rolling back another component's work. Use a pool that resets session state.

Use Snowflake identifier syntax in table and column properties: unquoted names
are uppercase, while double-quoted names preserve case and punctuation. For
example, `MY_DB.PUBLIC.REFERENCE_DATA` and key columns `ITEM_ID,"Region Code"`.
Lookup coordinates use the resolved names (`ITEM_ID` and `Region Code`), without
the surrounding quotes. SQL expressions and arbitrary source queries are not
accepted. Value Columns `*` includes all source columns; explicit projections
also include the configured key columns.

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
- Persistent starts validate and refresh the existing cache before serving it;
  they can resume incrementally when the source and checkpoint remain usable.
- After validation, transient refresh failures retain the previous cache
  **without an age limit**. Detected source discontinuity blocks lookups until a
  full reload succeeds. This is not guaranteed real-time revocation: source checks
  are optimistic and do not lock DDL or detect every policy/session change.
- Full reloads replace the cache. For incremental-capable sources with incremental
  mode enabled, each node stores the full SELECT's exact child query ID, or the
  explicit endpoint of an incremental read, with its rows, schema and source
  signature. The outer anonymous CALL ID is not the checkpoint.
  Full-only reads return one query directly without RESULT_SCAN; their stored
  marker ID is diagnostic only, not a resumable snapshot boundary. This avoids
  dependence on persisted hybrid-table query results. The complete response still
  includes a control row for an empty source and a checked data-row count.
  Missing historical statements/time travel and source-signature changes attempt
  an aligned full reload. Other SQL/protocol failures surface without silently
  treating a failed incremental refresh as a successful full load.
- Incremental reads retain both update halves, remove the old key when a key
  changes, and let inserts win over deletes for the same key regardless of row
  order. Reads have an explicit end checkpoint. A complete control row and the
  expected number of data rows must arrive before any cache change is published;
  an empty table or delta still carries a checkpoint.
- Initial-load failures are retried only when the refresh interval is positive.
- Every value is a string, including numeric and semi-structured values. Convert
  downstream when native types are required.
- Nodes refresh independently and can serve different valid point-in-time
  snapshots. There is no primary-only reader, shared cursor or cache replication.

## Storage and operating limits

Heap needs room for both the old and new maps during refresh. RocksDB uses disk,
native memory and the operating-system page cache. Incremental changes are first
collected in JVM heap; Embedded is not a bounded-memory guarantee for large deltas.
Full reloads temporarily keep old and new on-disk generations.
Full-only responses use a window count, which can require warehouse buffering;
full-scan cost grows with source size. This path has correctness tests, not a
large-table performance qualification.

Use a dedicated persistent directory, never a directory belonging to NiFi state,
another service, or a different reference source. Do not assume a writable path
is persistent. Backup/recovery, permissions and capacity belong to the runtime
operator. Keep source credentials out of paths and logs.

Embedded mode locks its directory and checks a versioned identity containing the
connection-service ID, source expression, key/value columns and incremental mode.
Mismatches fail enablement; existing generations are not deleted on open errors.
The query-ID checkpoint format is `table-cache-v2`; use a fresh directory for
older caches. No migration or timestamp-checkpoint compatibility is provided.

The source signature checks object type, creation/DDL metadata, retention, account,
user and primary role before and after the data query. The anonymous procedure
also verifies the signature observed during capability selection. Only effective
incremental mode requires at least one day of retention. Full-only sources need
no Time Travel or change tracking. The guard does not cover every masking/row-access
policy, secondary-role or session-setting change. Keep the connection evaluation
context stable. A signature is not an authorization guarantee, and historical
query IDs remain subject to Snowflake's statement lookup and data-retention limits.

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

Normal builds use deterministic JDBC response fixtures and temporary RocksDB
directories and do not need an account. Fixtures test response handling, not
Snowflake syntax or change-tracking semantics.
Live tests are skipped unless `SNOWFLAKE_IT_CONNECTION` explicitly selects a test
connection. They create and drop unique fixture tables, so use a dedicated test
database/schema and explicitly scoped warehouse/role, not production.
The test helper reads the named `~/.snowflake/config.toml` connection with a TOML
parser; `SNOWFLAKE_IT_*` settings can override it using an approved secret provider.
It never needs credentials committed in this repository. JDBC is test-only;
the deployed service obtains connections from the supplied DBCP service.

`SourceTypesProtocolIT` additionally requires `SNOWFLAKE_TYPES_IT=true` and a
dedicated permanent database/schema (hybrid tables cannot live in a transient
database). It creates unique view, hybrid, DT and managed Iceberg fixtures, uses
the configured warehouse for controlled DT refreshes, and drops its fixtures.
It does not create external volumes or catalog integrations. An explicitly
selected capability that is unavailable fails the test rather than being skipped.

## Install and release boundary

Install the generated `nifi-snowflake-table-cache-nar` on a runtime with the
matching `nifi-standard-services-api-nar`. Check controller-service discovery,
JNI load, cold and warm lookups, and incremental refresh on the installed artifact.
Record its checksum and runtime version when qualifying it.

The class and bundle coordinates differ from predecessor private builds. Use a
new service and a fresh dedicated directory; old flow exports do not automatically
resolve to this bundle. No on-disk migration or compatibility alias is provided.

The NAR includes reviewed RocksDB/native dependency notices and full license texts.
See [THIRD_PARTY.md](THIRD_PARTY.md) for the inventory, pinned sources and remaining
source-rights/build-provenance approvals. Account-free `verify` checks the actual
packaged texts and JNI checksum. Passing RAT and packaging tests is not legal or
organizational redistribution approval.

Keep the version at `0.1.0-SNAPSHOT` during contribution. A public PR is source
disclosure; a release version merged to main can trigger automatic NAR publication.
