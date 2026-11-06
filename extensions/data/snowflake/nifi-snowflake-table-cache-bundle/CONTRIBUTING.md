# Build, test and qualify

Read the [operator requirements](README.md) and [refresh design](DESIGN.md) before
changing behavior. Keep source-policy, configuration and failure semantics consistent
with controller-service property descriptions and tests.

## Build without an account

From the repository root, with Java 21:

```bash
./mvnw clean verify -Pcontrib-check,report-code-coverage \
  -f extensions/data/snowflake/nifi-snowflake-table-cache-bundle/pom.xml
```

Ordinary builds use deterministic JDBC response fixtures and temporary RocksDB
directories. They require no Snowflake account. Checkstyle, PMD, RAT, JaCoCo and the
NAR packaging test run here; the repository requires at least 80% coverage.
Fixtures validate response handling, not Snowflake syntax or change-tracking semantics.

For unit tests alone, use `./mvnw test -f extensions/data/snowflake/nifi-snowflake-table-cache-bundle/pom.xml`.
Run the full `verify` command before committing behavior changes.

| Layer | Contract |
|---|---|
| Identifier and codec tests | Identifier grammar/escaping; bounded persistence decoding |
| Heap/RocksDB store tests | Atomic publication, mutation isolation, corruption and restart behavior |
| Reader fixtures | Session/source validation, complete responses and JDBC closure before publication |
| Controller-service tests | LookupRecord integration, serving/recovery gates and refresh concurrency |
| Live JDBC tests | Production-generated SQL, native values, exact boundaries and supported-source behavior |

Preserve independent result oracles when changing production SQL. Required regression
contracts include the initial insert/delete boundary, explicit delta endpoint replay,
empty results, source replacement, native/quoted columns, injection-shaped names and
values, caller transactions, source-mode transitions, persistent validation and concurrent
publication. Use deterministic synchronization rather than assuming thread overlap.

## Run live JDBC tests

Live tests are skipped unless `SNOWFLAKE_IT_CONNECTION` explicitly selects a test
connection. They create and drop unique fixtures. Use a dedicated test database/schema
and explicitly scoped warehouse/role, never production. These tests do not qualify
an actual NiFi connection pool or prove the minimum runtime grants.

The test helper parses the named `~/.snowflake/config.toml` connection with TOML.
`SNOWFLAKE_IT_*` overrides may be supplied using an approved secret provider; never
commit credentials. Snowflake JDBC and TOML are test-only dependencies. Runtime
connections come from the supplied DBCP service.

For an existing authenticated test connection, replace these non-secret scope names:

```bash
SNOWFLAKE_IT_CONNECTION='test-connection' \
SNOWFLAKE_IT_DATABASE='DEDICATED_PERMANENT_TEST_DATABASE' \
SNOWFLAKE_IT_SCHEMA=PUBLIC SNOWFLAKE_IT_WAREHOUSE='TEST_WAREHOUSE' \
SNOWFLAKE_TYPES_IT=true \
./mvnw verify -f extensions/data/snowflake/nifi-snowflake-table-cache-bundle/pom.xml
```

`SourceTypesProtocolIT` also requires `SNOWFLAKE_TYPES_IT=true`. Its database must
be permanent because hybrid tables cannot live in a transient database. It creates
unique view, hybrid, dynamic-table and managed Iceberg fixtures, refreshes dynamic
tables using the configured warehouse, and drops its fixtures. It creates no external
volumes or catalog integrations. An explicitly selected capability that is unavailable
fails rather than being silently skipped. Verify cleanup after an interrupted test run.

## Qualify the artifact and release

1. Build the NAR and record its checksum and the target runtime version.
2. Install it with the matching `nifi-standard-services-api-nar`. Check controller-service
   discovery, Java Native Interface (JNI) loading, cold/warm lookups, refresh modes and
   restart on the actual runtime and DBCP pool using the intended least-privilege role.
3. Qualify resource usage, throughput, failure handling and multi-node behavior against
   the representative workload. Do not infer large-table performance from correctness tests.
4. Complete the [third-party release checklist](THIRD_PARTY.md#release-checklist), including
   source-release approval and publisher build-provenance/GCC-exception eligibility.
5. Obtain explicit publication approval. Keep `0.1.0-SNAPSHOT` during contribution;
   a public PR discloses source, and a release version merged to main can trigger NAR publication.

The class and bundle coordinates differ from predecessor private builds. Use a new
service and fresh dedicated directory; old flow exports do not automatically resolve
to this bundle. No compatibility alias or on-disk migration is provided.

The NAR contains reviewed RocksDB/native dependency notices and full license texts.
The packaging test verifies those texts and the JNI checksum. Passing RAT or the
packaging test is not legal or organizational redistribution approval. Keep license
inventory, pinned sources and unresolved provenance in THIRD_PARTY.md, not duplicate lists.

### Historical evidence, not current qualification

A predecessor build was exercised on one managed Medium Openflow node with
`/nifi/configuration_resources`, including incremental refresh and adoption by a new
JVM after graceful suspend/resume on the same host. This is not a platform storage
guarantee or qualification of the newly packaged artifact. Hard crashes, host replacement,
multi-node operation, Small runtimes and representative scale were not established by
that smoke test. Qualify the exact artifact rather than carrying that result forward.
