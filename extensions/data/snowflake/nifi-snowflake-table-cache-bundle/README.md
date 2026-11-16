# Snowflake Table Cache

A self-refreshing controller service for `LookupRecord`. It keeps a Snowflake
reference table or view in a local cache instead of querying the database for each
lookup. `SnowflakeTableCacheLookupService` implements `RecordLookupService`; it is
not a processor. Each NiFi node maintains its own cache and refresh schedule.

**Incremental refresh is off by default.** Successful refreshes publish complete
data with its checkpoint. Nodes may serve different snapshots, and transient
refresh failures can leave previously validated data available **without an age limit**.

Community contribution provided AS IS, not a Snowflake-supported product or
service. See the repository's support disclaimer and Apache 2.0 license.

## Supported sources

| Source | With Incremental Refresh=true |
|---|---|
| Standard permanent or transient table | Initial full load, then bounded changes |
| Native dynamic table with resolved INCREMENTAL mode | Initial full load, then bounded changes to materialized output |
| Snowflake-managed Iceberg v2/v3 table | Initial full load, then bounded changes |
| View | Full reload every time |
| Hybrid table | Full reload every time |
| Native dynamic table with resolved FULL mode | Full reload every time |

`Incremental Refresh=false` forces full reloads for every supported source.
Capabilities are checked on every refresh; logs report the actual FULL/INCREMENTAL
mode. Missing change tracking or privileges does not silently select full-only mode.

Temporary tables, external tables, materialized views, dynamic Iceberg tables,
externally managed Iceberg tables and other dynamic-table refresh modes are not
qualified. Managed Iceberg qualification covers Snowflake writes, not external writers.

Views can contain joins, aggregates and nested views if their results satisfy the
key requirements below. The entire view is re-evaluated on each refresh; unchanged
DDL never skips a read. Views over external systems retain those systems' consistency
and availability limits. Dynamic tables expose their latest published output; this
service does not trigger their refreshes or remove their upstream freshness lag.

## Prerequisites

- An enabled database connection pool (`DBCPService`) with database/schema usage,
  source SELECT, warehouse access and source metadata visibility. Dynamic-table
  inspection needs a visible `refresh_mode` in SHOW DYNAMIC TABLES; additional
  MONITOR privileges may be needed. Qualify the least-privilege role on your runtime.
- Borrowed connections with auto-commit, no active caller-owned transaction, and
  `QUOTED_IDENTIFIERS_IGNORE_CASE=FALSE`. Use a pool that resets session state and
  keep the connection's evaluation context stable. Incompatible settings are rejected,
  not changed; the service does not commit or roll back another component's work.
- For incremental reads: owner-configured change tracking and at least one day of
  retained history. Full-only reads need neither change tracking nor Time Travel.
- Unique, non-null source keys, including every composite-key part. Enforce uniqueness
  upstream: a delta cannot detect a duplicate against an unchanged cached source row.
- Capacity for the source and refresh staging; Embedded additionally needs a dedicated
  writable directory per service and node. See [operating limits](#operating-limits).

The service creates no streams, tasks, source tables, permanent procedures or grants.

## Configure and use

Install the generated `nifi-snowflake-table-cache-nar` on a runtime with the matching
`nifi-standard-services-api-nar`. See [build and qualification](CONTRIBUTING.md).

| Property | Default | Configuration |
|---|---|---|
| Snowflake Connection Service | None | Existing `DBCPService` |
| Source Table | None | Fully qualified table or view, e.g. `MY_DB.PUBLIC.REFERENCE_DATA` |
| Key Columns | None | Comma-separated lookup columns |
| Value Columns | `*` | Columns to return; explicit projections also include key columns |
| Storage Mode | Heap | Heap or Embedded (RocksDB) |
| Storage Directory | None | Required for Embedded; dedicated writable path |
| Refresh Interval | `1 hour` | Delay after refresh completion; `0` disables scheduled refresh and retries |
| Incremental Refresh | `false` | Permit changes on eligible sources; full-only sources still reload |

Table and column properties use Snowflake identifier syntax, not SQL expressions
or arbitrary queries. Unquoted names resolve uppercase; double quotes preserve case
and punctuation. For key columns `ITEM_ID,"Region Code"`, supply coordinates named
`ITEM_ID` and `Region Code`, without surrounding quotes. All returned values are strings
or null; convert numeric and semi-structured strings downstream when native types are needed.

1. Enable the connection service, configure the cache properties, then enable the cache.
2. In `LookupRecord`, configure record reader/writer services and select this lookup service.
3. Map each key column to a RecordPath, e.g. `ITEM_ID` to `/item_id`, and write the returned record at `/reference`.

A source row `ITEM_ID=example-1, LABEL=blue` enriches `{"item_id":"example-1"}`
with a reference record containing string values. An unknown key value is unmatched.
A missing/null required coordinate or an unavailable cache is a lookup failure, not a miss.

## Refresh and failure behavior

| Condition | Behavior |
|---|---|
| Before the first successful load | Lookups fail; a positive refresh interval enables retries |
| Persistent restart | Validate and refresh before serving; eligible sources can resume incrementally when the source/checkpoint remain usable |
| Successful refresh | Full loads replace contents; deltas apply inserts, updates and deletes; data and checkpoint publish together |
| Null or duplicate keys in a full result; incomplete or invalid response | Reject the refresh without publishing partial contents |
| Transient refresh failure after validation | Keep serving the previous cache, with no age limit |
| Detected source discontinuity | Block lookups and attempt a full reload; resume serving only after success |
| Unusable incremental history or statement checkpoint | Attempt a full reload; failure preserves prior validated data unless discontinuity is detected |
| Other SQL/protocol failure | Surface the failure; do not reinterpret it as full-only capability |

Source checks run at refresh time. They are optimistic checks, not DDL locks,
comprehensive policy-change detection or real-time authorization revocation.
See [refresh alignment and source signatures](DESIGN.md) for the exact guarantees.

## Security

Restrict controller-service configuration to trusted NiFi administrators. Identifiers
are parsed/quoted and query/checkpoint values are bound or escaped; lookup coordinates
stay local and never enter SQL. Choose explicit Value Columns to limit cached data.

The cache uses the connection service's identity, not each lookup caller's identity.
Restrict access to consuming flows accordingly; this is not per-caller authorization.
Returned records and schemas are detached, so consumer mutation cannot alter the cache.

Restrict the storage directory and its parents to trusted operating-system users.
Cache files contain source values and are not encrypted by this service. Hostile local
filesystem modification is outside its trust boundary. Keep credentials out of paths
and logs. Component errors omit lookup keys, but JDBC/native exception causes may
contain database diagnostics; protect logs as well.

## Operating limits

- Nodes refresh independently: no primary-only reader, shared cursor or replication.
  Source reads multiply with node count; there is no bound on cross-node staleness.
- Heap needs room for old and new maps. RocksDB uses disk, native memory and the
  operating-system page cache; full reloads temporarily retain two generations.
  Deltas are staged in JVM heap even in Embedded mode. Full scans and the full-only
  window count can require substantial warehouse work/buffering. Size against your workload.
- Use dedicated persistent storage, never NiFi state or another service/source's
  directory. A writable path is not necessarily persistent. Operators own capacity,
  permissions and backup/recovery. Identity mismatches fail enablement without deleting
  existing generations. Use a fresh directory for older caches; no migration is provided.
- Disable waits for an in-flight refresh. Set connection/query timeouts in DBCP;
  drivers that ignore interrupts can delay shutdown. Incremental writes are not
  synchronously fsynced on every refresh; hard-crash/power-loss durability is not claimed.
- This implementation has correctness tests, not qualification on your exact runtime
  or at representative scale. See [qualification and release](CONTRIBUTING.md#qualify-the-artifact-and-release)
  and [third-party provenance](THIRD_PARTY.md) before deployment or publication.
