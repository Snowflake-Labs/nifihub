# Refresh and consistency design

This reference describes the implementation behind the [operator contract](README.md).
The README owns the supported-source matrix and configuration requirements.

## One cache per node

Each controller-service instance has a local store, checkpoint and serialized
refresher. Concurrent lookups read locally. Nodes do not share a cursor, RocksDB
directory, primary reader or replication protocol. Each successful refresh yields
a locally consistent cache; it does not synchronize the cluster's snapshots.

Single-reader distribution is not implemented. A primary-only refresh process
without a distribution protocol would leave other nodes unrefreshed.

## Refresh sequence

1. Borrow a connection and validate its session settings, source identity and source capabilities.
2. Submit an anonymous SQL procedure on that same connection. No permanent procedure is created.
3. Check the source signature before and after reading, returning data and control metadata together.
4. Validate and stage the entire JDBC response; close the result, statement and borrowed connection.
5. Publish rows, schema and checkpoint atomically to the local store.

Preflight and the anonymous call are separate statements. The call itself executes
multiple child queries: it is neither one transaction nor one snapshot. Snapshot
alignment comes from the exact statement boundaries below, not from grouping SQL
inside a procedure. A failed read, validation or JDBC close cannot publish staged data.

## Checkpoints by refresh path

| Path | Read | Stored query ID |
|---|---|---|
| Initial/full reload with incremental enabled on an eligible source | Full source SELECT | That SELECT's exact child query ID, captured with `SQLID` |
| Subsequent incremental refresh | `CHANGES(INFORMATION => DEFAULT)` between explicit statement boundaries | A marker captured before the changes query, used as its explicit END boundary |
| Full-only source or Incremental Refresh=false | One direct response query containing the full source result | A preceding diagnostic marker; never used to resume changes |

The outer CALL ID is not a resumable checkpoint. For incremental reads, both
AT and END statement IDs are bound explicitly. The next refresh starts from the
previous successful endpoint. An empty delta still advances that endpoint.

The full SELECT's exact ID matters because net changes can cancel an insert and
delete. An independently timed checkpoint can then leave a row in the cache that
no longer exists upstream. The initial result and its boundary must describe the
same read; idempotent delta application cannot repair a mismatched starting point.

Incremental-capable reads assemble their response using `RESULT_SCAN` of the data
query. Full-only reads return a result directly, avoiding reliance on persisted
hybrid-table query results. They use a singleton seed and window count so an empty
source still returns control metadata. Their diagnostic marker does not identify
the full data SELECT. The generated procedure contains only the selected read path.

## Source signature and limits

The signature hashes account, user, primary role, table type, Iceberg/dynamic/hybrid
flags, creation and last-DDL timestamps formatted in UTC, and retention. It is
**metadata-derived**, not a `GET_DDL` hash or recursive dependency fingerprint.

Quoted-identifier case folding is rejected before source inspection: it can make
case-sensitive metadata equality and a quoted SELECT resolve to different objects.
Identifiers are quoted, not sanitized by removing punctuation; session validation
preserves that resolution contract without altering the borrowed connection.

Preflight rejects missing, ambiguous, temporary or incomplete source metadata and
qualifies source variants. Dynamic-table mode and managed Iceberg catalog/version
come from exact-identity SHOW results. A saved-signature mismatch is detected before
capability inspection can fail and hide that discontinuity. The preflight signature
is bound into the anonymous procedure and compared with its before/after checks.

Detected discontinuity gates lookups and requires a successful full reload. These
checks are optimistic, not locks: they do not establish uninterrupted identity
under every concurrent DDL sequence, masking/row-access policy, secondary-role or
session-context change. A signature is not an authorization guarantee.

Views always reload fully. Nested view/dependency changes are reflected by the next
successful full evaluation without reconstructing changes through those definitions;
an unchanged signature never skips the read. This is a service policy, not a claim
that Snowflake views universally lack change-tracking support.

Historical query IDs remain subject to Snowflake's statement-lookup and data-retention
limits. An unusable boundary attempts full reload rather than inventing an offset.
The [failure table](README.md#refresh-and-failure-behavior) defines when prior data
remains available versus when serving is blocked.

## Response validation and local publication

The response uses ordinal columns: kind, query ID, signature, mode, expected data-row
count, action, then source fields. Exactly one CONTROL row is required, even for
empty data. Random per-call aliases are checked against source columns. Native JDBC
metadata is preserved; cached field values are read using `getString`.

The reader validates mode, schema, count, actions and separation of control/payload
fields before publication. Full loads reject duplicate/null keys. Deltas retain both
update halves, delete old keys after key changes, and make inserts win over deletes
for the same key regardless of response order. Composite keys use length-prefixed
encoding. Upstream uniqueness remains a requirement: unchanged rows are not part of
a delta, so duplicate-key detection is not a global uniqueness check.

Heap publication swaps a single volatile snapshot. RocksDB full loads write a new
generation and swap the active handle under a lock; deltas use an atomic WriteBatch.
Both publish rows/schema/checkpoint together. Inputs and returned records/schemas are
detached flat string/null values. Persistence decoding rejects malformed lengths,
counts, UTF-8, schemas and row widths; this is not protection from a hostile filesystem owner.

Embedded mode locks its directory against another service owner. Startup validates
and refreshes before serving persisted data; eligible sources can resume incrementally
when their source signature and checkpoint remain usable. The directory identity includes
connection-service ID, source expression, encoded key/value columns
and incremental mode. The persisted format is `table-cache-v2`; older caches require a
fresh directory, without migration or timestamp-checkpoint compatibility. Existing
generations are preserved on open errors. Atomic publication is not a guarantee of
hard-crash or power-loss durability; see [operating limits](README.md#operating-limits).
