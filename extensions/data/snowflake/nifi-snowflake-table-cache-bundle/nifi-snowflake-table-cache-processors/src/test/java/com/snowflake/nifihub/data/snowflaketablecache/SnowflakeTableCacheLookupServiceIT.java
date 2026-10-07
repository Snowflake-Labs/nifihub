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

import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Integration test for the one behaviour unit tests cannot reach: incremental refresh through
 * Snowflake's {@code CHANGES} clause, which H2 does not implement.
 *
 * <p><b>Skipped unless {@code SNOWFLAKE_IT_CONNECTION} names a connection</b> in
 * {@code ~/.snowflake/config.toml}, so a normal build never requires an account:
 *
 * <pre>
 *   SNOWFLAKE_IT_CONNECTION=test_connection mvn verify
 * </pre>
 *
 * <p>Creates and drops its own uniquely-named table, so it can run against a shared account without
 * colliding with anything.
 */
@EnabledIfEnvironmentVariable(named = SnowflakeConfigConnectionService.CONNECTION_ENV, matches = ".+")
class SnowflakeTableCacheLookupServiceIT {

    /** Long enough to cover CHANGES visibility after a commit without making a failure slow. */
    private static final long SETTLE_TIMEOUT_MILLIS = 30_000;

    @TempDir
    Path storageDirectory;

    private TestRunner runner;
    private SnowflakeTableCacheLookupService service;
    private SnowflakeConfigConnectionService connectionService;
    private String table;

    @BeforeEach
    void setUp() throws Exception {
        table = "TABLE_CACHE_IT_" + System.currentTimeMillis();
        connectionService = new SnowflakeConfigConnectionService();

        execute("CREATE OR REPLACE TABLE " + table
                        + " (ITEM_ID STRING, LABEL STRING, REGION STRING) CHANGE_TRACKING = TRUE",
                "INSERT INTO " + table + " VALUES "
                        + "('ITEM_ID001','b2b','NA'),('ITEM_ID002','b2b,advertiser','NA'),('ITEM_ID003','advertiser','EU')");

        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        service = new SnowflakeTableCacheLookupService();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (table != null) {
            execute("DROP TABLE IF EXISTS " + table);
        }
    }

    private void enableService(final String mode) throws Exception {
        runner.addControllerService("snowflake", connectionService);
        runner.enableControllerService(connectionService);

        runner.addControllerService("cache", service);
        runner.setProperty(service, SnowflakeTableCacheLookupService.SNOWFLAKE_CONNECTION_SERVICE, "snowflake");
        runner.setProperty(service, SnowflakeTableCacheLookupService.SOURCE_TABLE, table);
        runner.setProperty(service, SnowflakeTableCacheLookupService.KEY_COLUMNS, "ITEM_ID");
        runner.setProperty(service, SnowflakeTableCacheLookupService.VALUE_COLUMNS, "*");
        runner.setProperty(service, SnowflakeTableCacheLookupService.REFRESH_INTERVAL, "0 sec");
        runner.setProperty(service, SnowflakeTableCacheLookupService.INCREMENTAL_REFRESH, "true");
        runner.setProperty(service, SnowflakeTableCacheLookupService.STORAGE_MODE, mode);
        if (!"HEAP".equals(mode)) {
            runner.setProperty(service, SnowflakeTableCacheLookupService.STORAGE_DIRECTORY,
                    storageDirectory.toString());
        }
        runner.enableControllerService(service);

        // onEnabled deliberately swallows a failed initial load so a transient outage cannot stop a
        // flow. That is right in production and useless in a test, so surface the cause here.
        if (!service.isHydrated()) {
            service.refresh();
            fail("Service enabled but the initial load produced nothing");
        }
    }

    /**
     * The behaviour the whole feature exists for: an inserted row, an updated row and a deleted row
     * each reach the cache through an incremental refresh, with no full reload.
     */
    @ParameterizedTest
    @ValueSource(strings = {"HEAP", "EMBEDDED_KV"})
    void incrementalRefreshAppliesInsertsUpdatesAndDeletes(final String mode) throws Exception {
        enableService(mode);
        assertEquals(3L, service.size());
        final String initialWatermark = service.watermarkForTest();
        assertNotNull(initialWatermark, "the initial full load must record a watermark");

        // ---- a brand new ITEM_ID, which is the demo case ----
        execute("INSERT INTO " + table + " VALUES ('ITEM_ID004','fleet','JP')");
        RefreshResult result = refreshUntil(() -> service.size() == 4L, "ITEM_ID004 to appear");
        assertTrue(result.incremental(), "must have refreshed incrementally, not fully reloaded");
        assertEquals("fleet", lookup("ITEM_ID004").orElseThrow().getAsString("LABEL"));
        assertNotEquals(initialWatermark, service.watermarkForTest(), "watermark must advance");

        // ---- an update: the DELETE half of the pair must not resurrect the old value ----
        execute("UPDATE " + table + " SET LABEL = 'b2b,advertiser,fleet' WHERE ITEM_ID = 'ITEM_ID001'");
        refreshUntil(() -> lookup("ITEM_ID001").map(r -> r.getAsString("LABEL"))
                .filter("b2b,advertiser,fleet"::equals).isPresent(), "ITEM_ID001 to update");
        assertEquals(4L, service.size(), "an update must not change the row count");

        // ---- a revocation ----
        execute("DELETE FROM " + table + " WHERE ITEM_ID = 'ITEM_ID003'");
        result = refreshUntil(() -> lookup("ITEM_ID003").isEmpty(), "ITEM_ID003 to be revoked");
        assertTrue(result.incremental());
        assertEquals(3L, service.size());
        assertTrue(lookup("ITEM_ID001").isPresent(), "unrelated keys must survive a delta");
    }

    /** A refresh with nothing to do must be a no-op, not a silent full reload. */
    @ParameterizedTest
    @ValueSource(strings = {"HEAP", "EMBEDDED_KV"})
    void incrementalRefreshWithNoChangesAppliesNothing(final String mode) throws Exception {
        enableService(mode);
        assertEquals(3L, service.size());

        final RefreshResult result = service.refresh();

        assertTrue(result.incremental());
        assertEquals(0L, result.rowsUpserted());
        assertEquals(0L, result.rowsDeleted());
        assertEquals(3L, service.size());
    }

    /**
     * An unusable watermark must degrade to a correct full reload rather than to a wrong cache. This
     * is what protects a node that has been down longer than the table's time travel retention.
     */
    @ParameterizedTest
    @ValueSource(strings = {"HEAP", "EMBEDDED_KV"})
    void watermarkOutsideRetentionFallsBackToFullReload(final String mode) throws Exception {
        enableService(mode);

        // Well before the table existed, which Snowflake rejects with "Time travel data is not
        // available" — the same error a long-absent node would hit.
        service.storeForTest().applyChanges(service.storeForTest().schema(), Map.of(), java.util.Set.of(),
                "2020-01-01 00:00:00.000 -0700");
        execute("INSERT INTO " + table + " VALUES ('ITEM_ID005','fleet','JP')");

        final RefreshResult result = service.refresh();

        assertTrue(!result.incremental(), "should have fallen back to a full reload");
        assertEquals(4L, service.size());
        assertTrue(lookup("ITEM_ID005").isPresent());
        assertNotNull(service.watermarkForTest(), "the fallback reload must reset the watermark");
    }

    // ---------- helpers ----------

    private Optional<Record> lookup(final String itemId) {
        try {
            return service.lookup(Map.of("ITEM_ID", itemId));
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Refresh until a condition holds. A commit is not necessarily visible to CHANGES the instant it
     * returns, so a single refresh would make this test flaky for reasons that have nothing to do
     * with the code under test.
     */
    private RefreshResult refreshUntil(final BooleanSupplierWithException condition,
                                       final String description) throws Exception {
        final long deadline = System.currentTimeMillis() + SETTLE_TIMEOUT_MILLIS;
        RefreshResult last = null;
        while (System.currentTimeMillis() < deadline) {
            last = service.refresh();
            if (condition.getAsBoolean()) {
                return last;
            }
            Thread.sleep(500);
        }
        fail("Timed out after " + SETTLE_TIMEOUT_MILLIS + " ms waiting for " + description
                + " (last refresh: " + last + ")");
        return last;
    }

    private void execute(final String... statements) throws Exception {
        try (Connection connection = connectionService.getConnection();
             Statement statement = connection.createStatement()) {
            for (final String sql : statements) {
                statement.execute(sql);
            }
        }
    }

    @FunctionalInterface
    private interface BooleanSupplierWithException {
        boolean getAsBoolean() throws Exception;
    }
}
