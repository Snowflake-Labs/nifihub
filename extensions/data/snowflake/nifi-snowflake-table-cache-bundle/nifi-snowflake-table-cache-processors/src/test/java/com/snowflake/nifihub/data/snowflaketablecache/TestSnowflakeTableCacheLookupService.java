/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.snowflake.nifihub.data.snowflaketablecache;

import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the one component this bundle ships.
 *
 * <p>Core behaviours run against <b>both</b> storage modes, because a backend that is correct on
 * heap and subtly wrong on disk is exactly the failure this component cannot afford. Everything
 * uses in-memory H2, so no Snowflake connection is required.
 */
class TestSnowflakeTableCacheLookupService {

    private static final AtomicInteger DB_COUNTER = new AtomicInteger();

    @TempDir
    Path storageDirectory;

    private TestRunner runner;
    private SnowflakeTableCacheLookupService service;
    private H2ConnectionService connectionService;

    @BeforeEach
    void setUp() throws Exception {
        connectionService = new H2ConnectionService("tablecache" + DB_COUNTER.incrementAndGet());
        connectionService.execute(
                "CREATE TABLE REFERENCE_DATA (ITEM_ID VARCHAR(32) PRIMARY KEY, LABEL VARCHAR(256), REGION VARCHAR(16))",
                "INSERT INTO REFERENCE_DATA VALUES ('ITEM_ID001', 'b2b', 'NA')",
                "INSERT INTO REFERENCE_DATA VALUES ('ITEM_ID002', 'b2b,advertiser', 'NA')",
                "INSERT INTO REFERENCE_DATA VALUES ('ITEM_ID003', 'advertiser', 'EU')");

        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        service = new SnowflakeTableCacheLookupService();
    }

    @AfterEach
    void stopServices() {
        service.onDisabled();
    }

    @Test
    void embeddedRejectsChangedSourceInsteadOfServingPreviousData() throws Exception {
        enableService("EMBEDDED_KV", "ITEM_ID", "*");
        runner.disableControllerService(service);
        connectionService.execute("CREATE TABLE OTHER_DATA AS SELECT * FROM REFERENCE_DATA");
        runner.setProperty(service, SnowflakeTableCacheLookupService.SOURCE_TABLE, "OTHER_DATA");
        assertThrows(AssertionError.class, () -> runner.enableControllerService(service));
        assertThrows(LookupFailureException.class, () -> service.lookup(Map.of("ITEM_ID", "ITEM_ID001")));
    }

    @ParameterizedTest
    @ValueSource(strings = {"HEAP", "EMBEDDED_KV"})
    void compositeKeysWithSeparatorBytesRemainDistinct(final String mode) throws Exception {
        connectionService.execute("CREATE TABLE KEYS_TEST (K1 VARCHAR, K2 VARCHAR, LABEL VARCHAR)");
        try (final Connection connection = connectionService.getConnection();
                final PreparedStatement statement = connection.prepareStatement("INSERT INTO KEYS_TEST VALUES (?, ?, ?)")) {
            statement.setString(1, "a\u0000b");
            statement.setString(2, "c");
            statement.setString(3, "first");
            statement.executeUpdate();
            statement.setString(1, "a");
            statement.setString(2, "b\u0000c");
            statement.setString(3, "second");
            statement.executeUpdate();
        }
        enableService(mode, "K1,K2", "LABEL", "0 sec", "KEYS_TEST");
        assertEquals("first", service.lookup(Map.of("K1", "a\u0000b", "K2", "c")).orElseThrow().getAsString("LABEL"));
        assertEquals("second", service.lookup(Map.of("K1", "a", "K2", "b\u0000c")).orElseThrow().getAsString("LABEL"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"HEAP", "EMBEDDED_KV"})
    void duplicateKeysFailLoadRatherThanChooseAnArbitraryRow(final String mode) throws Exception {
        connectionService.execute("CREATE TABLE DUPLICATE_KEYS (K VARCHAR, V VARCHAR)",
                "INSERT INTO DUPLICATE_KEYS VALUES ('a','first'),('a','second')");
        enableService(mode, "K", "V", "0 sec", "DUPLICATE_KEYS");
        assertFalse(service.isHydrated());
        assertThrows(LookupFailureException.class, () -> service.lookup(Map.of("K", "a")));
    }

    @ParameterizedTest
    @ValueSource(strings = {"HEAP", "EMBEDDED_KV"})
    void nullCompositeKeyFailsLoad(final String mode) throws Exception {
        connectionService.execute("CREATE TABLE NULL_KEYS (K1 VARCHAR, K2 VARCHAR, V VARCHAR)",
                "INSERT INTO NULL_KEYS VALUES ('a',NULL,'not-addressable')");
        enableService(mode, "K1,K2", "V", "0 sec", "NULL_KEYS");
        assertFalse(service.isHydrated());
    }

    @Test
    void secondStoreCannotDeleteAnOpenStoresData() throws Exception {
        final RecordSchema recordSchema = schema("K", "V");
        try (RocksDbTableCacheStore first = new RocksDbTableCacheStore(storageDirectory)) {
            try (TableCacheStore.BulkLoad load = first.beginBulkLoad(recordSchema)) {
                load.put("first", record(recordSchema, "first", "value"));
                load.commit("watermark");
            }
            assertThrows(java.io.IOException.class, () -> {
                try (RocksDbTableCacheStore ignored = new RocksDbTableCacheStore(storageDirectory)) {
                    assertFalse(ignored.isHydrated());
                }
            });
        }
        try (RocksDbTableCacheStore reopened = new RocksDbTableCacheStore(storageDirectory)) {
            assertEquals("value", reopened.get("first").orElseThrow().getAsString("V"));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"HEAP", "EMBEDDED_KV"})
    void changeRowsHandleKeyChangesAndEitherUpdateOrder(final String mode) throws Exception {
        enableService(mode, "ITEM_ID", "*");
        connectionService.execute("CREATE TABLE DELTA (ITEM_ID VARCHAR, LABEL VARCHAR, REGION VARCHAR, __TABLE_CACHE_IS_DELETE BOOLEAN)",
                "INSERT INTO DELTA VALUES ('ITEM_ID900','new','EU',FALSE),('ITEM_ID001','old','NA',TRUE),"
                        + "('ITEM_ID002','updated','EU',FALSE),('ITEM_ID002','old','NA',TRUE),"
                        + "('ITEM_ID003','old','NA',TRUE),('ITEM_ID003','updated','EU',FALSE)");
        try (final Connection connection = connectionService.getConnection();
             final Statement statement = connection.createStatement();
             final ResultSet rows = statement.executeQuery("SELECT * FROM DELTA")) {
            service.applyChangeRows(service.storeForTest(), rows, "new-watermark", System.currentTimeMillis());
        }
        assertEquals(3L, service.size());
        assertTrue(service.lookup(Map.of("ITEM_ID", "ITEM_ID001")).isEmpty());
        assertEquals("new", service.lookup(Map.of("ITEM_ID", "ITEM_ID900")).orElseThrow().getAsString("LABEL"));
        assertEquals("updated", service.lookup(Map.of("ITEM_ID", "ITEM_ID002")).orElseThrow().getAsString("LABEL"));
        assertEquals("updated", service.lookup(Map.of("ITEM_ID", "ITEM_ID003")).orElseThrow().getAsString("LABEL"));
    }

    @Test
    void metadataLikeUserKeysDoNotOverwriteStoreMetadata() throws Exception {
        final RecordSchema recordSchema = schema("K", "V");
        try (RocksDbTableCacheStore cache = new RocksDbTableCacheStore(storageDirectory)) {
            try (TableCacheStore.BulkLoad load = cache.beginBulkLoad(recordSchema)) {
                load.put("\u0000__schema__", record(recordSchema, "\u0000__schema__", "value"));
                load.commit("watermark");
            }
        }
        try (RocksDbTableCacheStore cache = new RocksDbTableCacheStore(storageDirectory)) {
            assertEquals(1L, cache.size());
            assertEquals("value", cache.get("\u0000__schema__").orElseThrow().getAsString("V"));
        }
    }

    @Test
    void mismatchedIdentityDoesNotLeakDirectoryLock() throws Exception {
        try (RocksDbTableCacheStore cache = new RocksDbTableCacheStore(storageDirectory, new byte[]{1})) {
            assertFalse(cache.isHydrated());
        }
        assertThrows(java.io.IOException.class, () -> new RocksDbTableCacheStore(storageDirectory, new byte[]{2}));
        try (RocksDbTableCacheStore cache = new RocksDbTableCacheStore(storageDirectory, new byte[]{1})) {
            assertFalse(cache.isHydrated());
        }
    }

    @Test
    void failedGenerationOpenPreservesFilesAndReleasesLock() throws Exception {
        try (RocksDbTableCacheStore cache = new RocksDbTableCacheStore(storageDirectory)) {
            assertFalse(cache.isHydrated());
        }
        final Path incomplete = Files.createDirectory(storageDirectory.resolve("gen-1"));
        final Path evidence = Files.writeString(incomplete.resolve("evidence"), "preserve me");
        assertThrows(java.io.IOException.class, () -> new RocksDbTableCacheStore(storageDirectory));
        assertEquals("preserve me", Files.readString(evidence));
        Files.move(incomplete, storageDirectory.resolve("preserved"));
        try (RocksDbTableCacheStore cache = new RocksDbTableCacheStore(storageDirectory)) {
            assertFalse(cache.isHydrated());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"HEAP", "EMBEDDED_KV"})
    void invalidRefreshRetainsPreviousDataAndWatermark(final String mode) throws Exception {
        enableService(mode, "ITEM_ID", "*");
        final String watermark = service.watermarkForTest();
        connectionService.execute("ALTER TABLE REFERENCE_DATA DROP PRIMARY KEY",
                "INSERT INTO REFERENCE_DATA VALUES ('ITEM_ID001','duplicate','NA')");
        assertThrows(TableCacheException.class, () -> service.refresh());
        assertEquals(3L, service.size());
        assertEquals(watermark, service.watermarkForTest());
        assertEquals("b2b", service.lookup(Map.of("ITEM_ID", "ITEM_ID001")).orElseThrow().getAsString("LABEL"));
    }

    @Test
    void disableWaitsForInFlightRefreshBeforeClosingStorage() throws Exception {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        connectionService = new H2ConnectionService("tablecache" + DB_COUNTER.get()) {
            private int connections;

            @Override
            public Connection getConnection() {
                if (++connections > 1) {
                    entered.countDown();
                    try {
                        if (!release.await(10, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("Test did not release JDBC connection");
                        }
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(e);
                    }
                }
                return super.getConnection();
            }
        };
        enableService("EMBEDDED_KV", "ITEM_ID", "*");
        try (final ExecutorService executor = Executors.newFixedThreadPool(2)) {
            final Future<RefreshResult> refresh = executor.submit(() -> service.refresh());
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            final Future<?> disable = executor.submit(() -> service.onDisabled());
            try {
                assertThrows(TimeoutException.class, () -> disable.get(100, TimeUnit.MILLISECONDS));
            } finally {
                release.countDown();
            }
            refresh.get(5, TimeUnit.SECONDS);
            disable.get(5, TimeUnit.SECONDS);
            assertFalse(service.isHydrated());
        } finally {
            release.countDown();
        }
    }

    @Test
    void namedTomlConnectionSupportsQuotedNamesWithoutLeakingInvalidInput() throws Exception {
        final Path config = storageDirectory.resolve("connections.toml");
        Files.writeString(config, "[connections.\"test.name\"]\nuser = 'test-user'\nport = 123\n");
        assertEquals(Map.of("user", "test-user"), SnowflakeConfigConnectionService.readConnection(config, "test.name"));
        assertTrue(SnowflakeConfigConnectionService.readConnection(config, "absent").isEmpty());
        Files.writeString(config, "[connections.test]\npassword = 'synthetic-invalid-input");
        final IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> SnowflakeConfigConnectionService.readConnection(config, "test"));
        assertFalse(error.getMessage().contains("synthetic-invalid-input"));
    }

    private void enableService(final String mode, final String keyColumns,
                               final String valueColumns, final String refreshInterval,
                               final String sourceTable) throws InitializationException {
        enableService(mode, keyColumns, valueColumns, refreshInterval, sourceTable, false);
    }

    private void enableService(final String mode, final String keyColumns,
                               final String valueColumns, final String refreshInterval,
                               final String sourceTable, final boolean incrementalRefresh)
            throws InitializationException {
        runner.addControllerService("h2", connectionService);
        runner.enableControllerService(connectionService);

        runner.addControllerService("cache", service);
        runner.setProperty(service, SnowflakeTableCacheLookupService.SNOWFLAKE_CONNECTION_SERVICE, "h2");
        runner.setProperty(service, SnowflakeTableCacheLookupService.SOURCE_TABLE, sourceTable);
        runner.setProperty(service, SnowflakeTableCacheLookupService.KEY_COLUMNS, keyColumns);
        runner.setProperty(service, SnowflakeTableCacheLookupService.VALUE_COLUMNS, valueColumns);
        runner.setProperty(service, SnowflakeTableCacheLookupService.REFRESH_INTERVAL, refreshInterval);
        runner.setProperty(service, SnowflakeTableCacheLookupService.INCREMENTAL_REFRESH,
                Boolean.toString(incrementalRefresh));
        runner.setProperty(service, SnowflakeTableCacheLookupService.STORAGE_MODE, mode);
        if (!"HEAP".equals(mode)) {
            runner.setProperty(service, SnowflakeTableCacheLookupService.STORAGE_DIRECTORY,
                    storageDirectory.toString());
        }
        runner.enableControllerService(service);
    }

    private void enableService(final String mode, final String keyColumns, final String valueColumns)
            throws InitializationException {
        enableService(mode, keyColumns, valueColumns, "0 sec", "REFERENCE_DATA");
    }

    // ---------- behaviours that must hold for every backend ----------

    @ParameterizedTest
    @ValueSource(strings = {"HEAP", "EMBEDDED_KV"})
    void loadsOnEnableAndServesLookups(final String mode) throws Exception {
        enableService(mode, "ITEM_ID", "*");

        assertTrue(service.isHydrated());
        assertEquals(3L, service.size());

        final Optional<Record> hit = service.lookup(Map.of("ITEM_ID", "ITEM_ID002"));
        assertTrue(hit.isPresent());
        assertEquals("b2b,advertiser", hit.get().getAsString("LABEL"));
        assertEquals("NA", hit.get().getAsString("REGION"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"HEAP", "EMBEDDED_KV"})
    void missReturnsEmptyRatherThanFailing(final String mode) throws Exception {
        enableService(mode, "ITEM_ID", "*");
        assertTrue(service.lookup(Map.of("ITEM_ID", "NOT_A_ITEM_ID")).isEmpty());
    }

    @ParameterizedTest
    @ValueSource(strings = {"HEAP", "EMBEDDED_KV"})
    void refreshReplacesContentsIncludingDeletes(final String mode) throws Exception {
        enableService(mode, "ITEM_ID", "*");
        assertEquals(3L, service.size());

        connectionService.execute(
                "UPDATE REFERENCE_DATA SET LABEL = 'b2b,advertiser,fleet' WHERE ITEM_ID = 'ITEM_ID001'",
                "DELETE FROM REFERENCE_DATA WHERE ITEM_ID = 'ITEM_ID003'",
                "INSERT INTO REFERENCE_DATA VALUES ('ITEM_ID004', 'fleet', 'JP')");

        service.refresh();

        assertEquals(3L, service.size());
        assertEquals("b2b,advertiser,fleet",
                service.lookup(Map.of("ITEM_ID", "ITEM_ID001")).orElseThrow().getAsString("LABEL"));
        // A refresh must REPLACE, not merge. If a revoked key lingered here, an entitlement
        // revocation would silently never take effect.
        assertTrue(service.lookup(Map.of("ITEM_ID", "ITEM_ID003")).isEmpty());
        assertTrue(service.lookup(Map.of("ITEM_ID", "ITEM_ID004")).isPresent());
    }

    @ParameterizedTest
    @ValueSource(strings = {"HEAP", "EMBEDDED_KV"})
    void explicitValueColumnsProjectOnlyWhatIsAsked(final String mode) throws Exception {
        enableService(mode, "ITEM_ID", "LABEL");

        final Record record = service.lookup(Map.of("ITEM_ID", "ITEM_ID001")).orElseThrow();
        assertEquals("b2b", record.getAsString("LABEL"));
        assertFalse(record.getSchema().getFieldNames().contains("REGION"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"HEAP", "EMBEDDED_KV"})
    void compositeKeyRequiresEveryCoordinate(final String mode) throws Exception {
        enableService(mode, "ITEM_ID,REGION", "*");

        assertEquals(List.of("ITEM_ID", "REGION"), List.copyOf(service.getRequiredKeys()));
        assertTrue(service.lookup(Map.of("ITEM_ID", "ITEM_ID003", "REGION", "EU")).isPresent());
        assertTrue(service.lookup(Map.of("ITEM_ID", "ITEM_ID003", "REGION", "NA")).isEmpty());
        assertThrows(LookupFailureException.class, () -> service.lookup(Map.of("ITEM_ID", "ITEM_ID003")));
    }

    @ParameterizedTest
    @ValueSource(strings = {"HEAP", "EMBEDDED_KV"})
    void refreshFailureRetainsPreviousContents(final String mode) throws Exception {
        enableService(mode, "ITEM_ID", "*");
        assertEquals(3L, service.size());

        connectionService.execute("DROP TABLE REFERENCE_DATA");

        assertThrows(TableCacheException.class, () -> service.refresh());
        assertTrue(service.isHydrated());
        assertEquals(3L, service.size());
        assertTrue(service.lookup(Map.of("ITEM_ID", "ITEM_ID001")).isPresent());
    }

    // ---------- enable-time behaviour ----------

    @Test
    void enablesEvenWhenTheInitialLoadFailsAndFailsLookupsClosed() throws Exception {
        // A controller service that refuses to enable stops the whole flow and needs manual
        // intervention, so a bad source must not be fatal at enable time.
        enableService("HEAP", "ITEM_ID", "*", "0 sec", "NO_SUCH_TABLE");

        assertFalse(service.isHydrated());
        // Critical: an unloaded cache must NOT answer "no such key" — for an entitlement dataset
        // that is a wrong answer, not a missing one.
        assertThrows(LookupFailureException.class, () -> service.lookup(Map.of("ITEM_ID", "ITEM_ID001")));
    }

    @Test
    void requiredKeysDrivesLookupRecordContract() throws Exception {
        enableService("HEAP", "ITEM_ID", "*");
        assertEquals(Set.of("ITEM_ID"), service.getRequiredKeys());
        assertEquals(Record.class, service.getValueType());
    }

    // ---------- the scheduled refresh, which replaces the companion processor ----------

    @Test
    void scheduledRefreshPicksUpChangesWithoutAnyProcessor() throws Exception {
        enableService("HEAP", "ITEM_ID", "*", "100 millis", "REFERENCE_DATA");
        assertEquals(3L, service.size());

        connectionService.execute("INSERT INTO REFERENCE_DATA VALUES ('ITEM_ID004', 'fleet', 'JP')");

        // The service polls itself; nothing else drives it.
        final long deadline = System.currentTimeMillis() + 10_000;
        while (service.size() != 4L && System.currentTimeMillis() < deadline) {
            Thread.sleep(25);
        }

        assertEquals(4L, service.size());
        assertTrue(service.lookup(Map.of("ITEM_ID", "ITEM_ID004")).isPresent());
    }

    @Test
    void scheduledRefreshSurvivesAFailedAttempt() throws Exception {
        enableService("HEAP", "ITEM_ID", "*", "100 millis", "REFERENCE_DATA");
        assertEquals(3L, service.size());

        // Break the source, let at least one scheduled attempt fail, then repair it. The schedule
        // must still be alive: an exception escaping the task would cancel it permanently.
        connectionService.execute("DROP TABLE REFERENCE_DATA");
        Thread.sleep(400);
        assertTrue(service.isHydrated());
        assertEquals(3L, service.size());

        connectionService.execute(
                "CREATE TABLE REFERENCE_DATA (ITEM_ID VARCHAR(32) PRIMARY KEY, LABEL VARCHAR(256), REGION VARCHAR(16))",
                "INSERT INTO REFERENCE_DATA VALUES ('ITEM_ID009', 'b2b', 'NA')");

        final long deadline = System.currentTimeMillis() + 10_000;
        while (service.size() != 1L && System.currentTimeMillis() < deadline) {
            Thread.sleep(25);
        }

        assertEquals(1L, service.size());
        assertTrue(service.lookup(Map.of("ITEM_ID", "ITEM_ID009")).isPresent());
    }

    @Test
    void zeroIntervalLoadsOnceAndNeverRefreshes() throws Exception {
        enableService("HEAP", "ITEM_ID", "*", "0 sec", "REFERENCE_DATA");
        assertEquals(3L, service.size());

        connectionService.execute("INSERT INTO REFERENCE_DATA VALUES ('ITEM_ID004', 'fleet', 'JP')");
        Thread.sleep(300);

        assertEquals(3L, service.size(), "Refresh Interval of 0 must not schedule a reload");
    }

    // ---------- durability, the reason the embedded backend exists ----------

    @Test
    void embeddedStoreSurvivesRestartWithoutReadingTheSource() throws Exception {
        enableService("EMBEDDED_KV", "ITEM_ID", "*");
        assertEquals(3L, service.size());

        runner.disableControllerService(service);

        // Remove the source entirely. If re-enabling still serves data it can only have come from
        // disk, which is what makes a restarted node warm instead of re-reading the table.
        connectionService.execute("DROP TABLE REFERENCE_DATA");

        runner.enableControllerService(service);

        assertTrue(service.isHydrated());
        assertEquals(3L, service.size());
        assertEquals("b2b,advertiser",
                service.lookup(Map.of("ITEM_ID", "ITEM_ID002")).orElseThrow().getAsString("LABEL"));
    }

    // ---------- incremental refresh ----------
    //
    // The CHANGES clause is Snowflake-only, so H2 cannot exercise the incremental read itself; that
    // path needs a live integration test for this artifact. Local coverage includes row application,
    // store-level apply and the fallback: an
    // incremental attempt that fails must degrade to a correct full reload rather than to a wrong
    // cache.

    @ParameterizedTest
    @ValueSource(strings = {"HEAP", "EMBEDDED_KV"})
    void incrementalAttemptFallsBackToFullReloadWhenChangesIsUnsupported(final String mode)
            throws Exception {
        enableService(mode, "ITEM_ID", "*", "0 sec", "REFERENCE_DATA", true);
        assertEquals(3L, service.size());
        assertNotNull(service.watermarkForTest(), "a full load must record a watermark");

        connectionService.execute(
                "INSERT INTO REFERENCE_DATA VALUES ('ITEM_ID004','fleet','JP')",
                "DELETE FROM REFERENCE_DATA WHERE ITEM_ID = 'ITEM_ID003'");

        // H2 rejects the CHANGES clause, so this exercises the fallback for real.
        final RefreshResult result = service.refresh();

        assertFalse(result.incremental(), "should have fallen back to a full reload");
        assertEquals(3L, service.size());
        assertTrue(service.lookup(Map.of("ITEM_ID", "ITEM_ID004")).isPresent());
        assertTrue(service.lookup(Map.of("ITEM_ID", "ITEM_ID003")).isEmpty());
    }

    @ParameterizedTest
    @ValueSource(strings = {"HEAP", "EMBEDDED_KV"})
    void storeAppliesADeltaAtomicallyAndAdvancesTheWatermark(final String mode) throws Exception {
        enableService(mode, "ITEM_ID", "*", "0 sec", "REFERENCE_DATA", false);
        final TableCacheStore store = service.storeForTest();
        final RecordSchema recordSchema = store.schema();

        // Exactly the shape refreshIncremental() produces: a new ITEM_ID, plus a revocation.
        final Map<String, Record> upserts = new HashMap<>();
        final Map<String, Object> newRow = new HashMap<>();
        newRow.put("ITEM_ID", "ITEM_ID900");
        newRow.put("LABEL", "b2b");
        newRow.put("REGION", "NA");
        upserts.put("ITEM_ID900", new MapRecord(recordSchema, newRow));

        store.applyChanges(recordSchema, upserts, Set.of("ITEM_ID003"), "2026-02-02 02:02:02.000 -0700");

        assertEquals(3L, store.size());
        assertEquals("b2b", store.get("ITEM_ID900").orElseThrow().getAsString("LABEL"));
        assertTrue(store.get("ITEM_ID003").isEmpty(), "a delete in the delta must remove the key");
        assertEquals("2026-02-02 02:02:02.000 -0700", store.watermark());
    }

    @Test
    void embeddedStoreKeepsWatermarkAcrossRestart() throws Exception {
        enableService("EMBEDDED_KV", "ITEM_ID", "*", "0 sec", "REFERENCE_DATA", true);
        final String before = service.watermarkForTest();
        assertNotNull(before);

        runner.disableControllerService(service);
        runner.enableControllerService(service);

        // Watermark and contents are written together, so a restart can never resume from a point
        // that does not match the data it holds.
        assertEquals(before, service.watermarkForTest());
        assertEquals(3L, service.size());
    }

    // ---------- store-level invariants ----------

    private static RecordSchema schema(final String... fieldNames) {
        return new SimpleRecordSchema(Arrays.stream(fieldNames)
                .map(n -> new RecordField(n, RecordFieldType.STRING.getDataType()))
                .toList());
    }

    private static Record record(final RecordSchema recordSchema, final String... values) {
        final Map<String, Object> map = new HashMap<>();
        final List<String> names = recordSchema.getFieldNames();
        for (int i = 0; i < names.size() && i < values.length; i++) {
            map.put(names.get(i), values[i]);
        }
        return new MapRecord(recordSchema, map);
    }

    @Test
    void heapStoreBulkLoadIsInvisibleUntilCommitted() {
        final RecordSchema recordSchema = schema("K", "V");
        final HeapTableCacheStore store = new HeapTableCacheStore();
        assertFalse(store.isHydrated());

        try (TableCacheStore.BulkLoad load = store.beginBulkLoad(recordSchema)) {
            load.put("k1", record(recordSchema, "k1", "v1"));
            assertFalse(store.isHydrated());
            assertEquals(0L, store.size());
            load.commit("2026-01-01 00:00:00.000 -0700");
        }

        assertTrue(store.isHydrated());
        assertEquals(1L, store.size());
    }

    @Test
    void abandonedBulkLoadDiscardsStagedEntries() {
        final RecordSchema recordSchema = schema("K", "V");
        final HeapTableCacheStore store = new HeapTableCacheStore();
        try (TableCacheStore.BulkLoad load = store.beginBulkLoad(recordSchema)) {
            load.put("k1", record(recordSchema, "k1", "v1"));
            // fall out of scope without commit
        }
        assertFalse(store.isHydrated());
        assertEquals(0L, store.size());
    }

    @Test
    void rocksStoreBulkLoadIsInvisibleUntilCommitted() throws Exception {
        final RecordSchema recordSchema = schema("K", "V");
        try (RocksDbTableCacheStore store = new RocksDbTableCacheStore(storageDirectory)) {
            assertFalse(store.isHydrated());

            try (TableCacheStore.BulkLoad load = store.beginBulkLoad(recordSchema)) {
                load.put("k1", record(recordSchema, "k1", "v1"));
                // The generation is written but not published, so readers still see nothing.
                assertFalse(store.isHydrated());
                assertEquals(0L, store.size());
                load.commit("2026-01-01 00:00:00.000 -0700");
            }

            assertTrue(store.isHydrated());
            assertEquals(1L, store.size());
            assertEquals("v1", store.get("k1").orElseThrow().getAsString("V"));
            assertTrue(store.get("nope").isEmpty());
            assertTrue(store.diskBytes() > 0);
        }
    }

    @Test
    void rocksStoreAbandonedLoadLeavesPreviousGenerationLive() throws Exception {
        final RecordSchema recordSchema = schema("K", "V");
        try (RocksDbTableCacheStore store = new RocksDbTableCacheStore(storageDirectory)) {
            try (TableCacheStore.BulkLoad first = store.beginBulkLoad(recordSchema)) {
                first.put("k1", record(recordSchema, "k1", "v1"));
                first.commit("2026-01-01 00:00:00.000 -0700");
            }

            try (TableCacheStore.BulkLoad abandoned = store.beginBulkLoad(recordSchema)) {
                abandoned.put("k2", record(recordSchema, "k2", "v2"));
                // no commit
            }

            assertEquals(1L, store.size());
            assertTrue(store.get("k1").isPresent());
            assertTrue(store.get("k2").isEmpty());
        }
    }

    @Test
    void rocksStoreAdoptsExistingGenerationOnReopen() throws Exception {
        final RecordSchema recordSchema = schema("K", "V");
        try (RocksDbTableCacheStore store = new RocksDbTableCacheStore(storageDirectory)) {
            try (TableCacheStore.BulkLoad load = store.beginBulkLoad(recordSchema)) {
                load.put("k1", record(recordSchema, "k1", "v1"));
                load.commit("2026-01-01 00:00:00.000 -0700");
            }
        }

        try (RocksDbTableCacheStore reopened = new RocksDbTableCacheStore(storageDirectory)) {
            assertTrue(reopened.isHydrated());
            assertEquals(1L, reopened.size());
            assertEquals("v1", reopened.get("k1").orElseThrow().getAsString("V"));
            reopened.applyChanges(recordSchema, Map.of(), Set.of(), null);
        }
        try (RocksDbTableCacheStore reopened = new RocksDbTableCacheStore(storageDirectory)) {
            assertEquals(null, reopened.watermark());
        }
    }

    @Test
    void rowCodecRoundTripsNullsAndSeparatorBytes() {
        // A delimiter-based encoding would corrupt the third value here.
        final List<String> values = Arrays.asList("plain", null, "has\u0000separator", "");
        assertEquals(values, RowCodec.decode(RowCodec.encode(values)));
    }
}
