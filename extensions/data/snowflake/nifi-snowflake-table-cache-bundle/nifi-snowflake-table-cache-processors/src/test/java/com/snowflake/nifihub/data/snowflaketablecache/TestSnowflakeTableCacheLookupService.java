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
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

import static com.snowflake.nifihub.data.snowflaketablecache.RefreshResponseConnectionService.control;
import static com.snowflake.nifihub.data.snowflaketablecache.RefreshResponseConnectionService.data;
import static com.snowflake.nifihub.data.snowflaketablecache.RefreshResponseConnectionService.response;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises service lifecycle and cache semantics against both real storage backends. The JDBC
 * fixture supplies deterministic CONTROL/DATA responses; it does not execute Snowflake SQL.
 */
class TestSnowflakeTableCacheLookupService {
    private static final String FIRST_ID = "00000000-0000-0000-0000-000000000001";
    private static final String SECOND_ID = "00000000-0000-0000-0000-000000000002";
    private static final String THIRD_ID = "00000000-0000-0000-0000-000000000003";
    private static final String SIGNATURE = "validated-source";
    private static final List<String> FIELDS = List.of("ITEM_ID", "LABEL", "REGION");

    @TempDir
    Path storageDirectory;

    private TestRunner runner;
    private SnowflakeTableCacheLookupService service;
    private RefreshResponseConnectionService connectionService;

    @BeforeEach
    void setUp() {
        connectionService = new RefreshResponseConnectionService();
        connectionService.sourceSignature(SIGNATURE);
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        service = new SnowflakeTableCacheLookupService();
    }

    @AfterEach
    void stopServices() {
        service.onDisabled();
        assertTrue(connectionService.resourcesClosed(), "all refresh JDBC resources must be closed");
    }

    static Stream<Arguments> backendsAndRefreshModes() {
        return Stream.of("HEAP", "EMBEDDED_KV").flatMap(mode -> Stream.of(false, true).map(delta -> Arguments.of(mode, delta)));
    }

    @ParameterizedTest
    @MethodSource("backendsAndRefreshModes")
    void loadsOnEnableAndImplementsLookupRecordContract(final String mode, final boolean incremental) throws Exception {
        initialLoad(mode, incremental);
        assertTrue(service.isHydrated());
        assertEquals(3L, service.size());
        assertEquals("b2b,advertiser", lookup("ITEM_ID002").getAsString("LABEL"));
        assertEquals("NA", lookup("ITEM_ID002").getAsString("REGION"));
        assertTrue(service.lookup(Map.of("ITEM_ID", "absent")).isEmpty());
        assertThrows(LookupFailureException.class, () -> service.lookup(Map.of()));
        assertEquals(Set.of("ITEM_ID"), service.getRequiredKeys());
        assertEquals(Record.class, service.getValueType());
        assertEquals(new CacheCheckpoint(FIRST_ID, SIGNATURE), service.checkpointForTest());
        final RefreshResponseConnectionService.Call call = connectionService.calls().getFirst();
        assertFalse(call.incremental(), "first load has no checkpoint to resume");
        assertNull(call.parameters().get(1));
        assertNull(call.parameters().get(2));
        assertEquals("PUBLIC", call.parameters().get(3));
        assertEquals("REF", call.parameters().get(4));
        assertEquals(SIGNATURE, call.parameters().get(5));
        assertEquals(Map.of(1, "PUBLIC", 2, "REF"), connectionService.metadataCalls().getFirst().parameters());
        assertEquals(1, connectionService.connectionCount());
        assertEquals(1, connectionService.sessionCalls().size());
    }

    @ParameterizedTest
    @ValueSource(strings = {"HEAP", "EMBEDDED_KV"})
    void fullRefreshReplacesContentsIncludingRevokedKeys(final String mode) throws Exception {
        initialLoad(mode, false);
        connectionService.enqueue(full(SECOND_ID, SIGNATURE, data("INSERT", "ITEM_ID001", "changed", "NA"), data("INSERT", "ITEM_ID004", "new", "JP")));
        final RefreshResult result = service.refresh();
        assertFalse(result.incremental());
        assertEquals(2L, service.size());
        assertEquals("changed", lookup("ITEM_ID001").getAsString("LABEL"));
        assertEquals("new", lookup("ITEM_ID004").getAsString("LABEL"));
        assertTrue(service.lookup(Map.of("ITEM_ID", "ITEM_ID002")).isEmpty());
        assertTrue(service.lookup(Map.of("ITEM_ID", "ITEM_ID003")).isEmpty());
        assertEquals(new CacheCheckpoint(SECOND_ID, SIGNATURE), service.checkpointForTest());
        assertNull(connectionService.calls().getLast().parameters().get(1));
        assertEquals(SIGNATURE, connectionService.calls().getLast().parameters().get(2));
    }

    @ParameterizedTest
    @ValueSource(strings = {"HEAP", "EMBEDDED_KV"})
    void changeRowsHandleKeyChangesAndEitherUpdateOrder(final String mode) throws Exception {
        initialLoad(mode, true);
        connectionService.enqueue(response(FIELDS,
                data("INSERT", "ITEM_ID900", "new", "EU"),
                data("DELETE", "ITEM_ID001", "old", "NA"),
                data("INSERT", "ITEM_ID002", "updated", "EU"),
                control(3, SECOND_ID, SIGNATURE, "INCREMENTAL", 6L),
                data("DELETE", "ITEM_ID002", "old", "NA"),
                data("DELETE", "ITEM_ID003", "old", "NA"),
                data("INSERT", "ITEM_ID003", "updated", "EU")));
        final RefreshResult result = service.refresh();
        assertTrue(result.incremental());
        assertEquals(3L, result.rowsUpserted());
        assertEquals(1L, result.rowsDeleted());
        assertEquals(3L, service.size());
        assertTrue(service.lookup(Map.of("ITEM_ID", "ITEM_ID001")).isEmpty());
        assertEquals("new", lookup("ITEM_ID900").getAsString("LABEL"));
        assertEquals("updated", lookup("ITEM_ID002").getAsString("LABEL"));
        assertEquals("updated", lookup("ITEM_ID003").getAsString("LABEL"));
        assertEquals(new CacheCheckpoint(SECOND_ID, SIGNATURE), service.checkpointForTest());
        assertEquals(FIRST_ID, connectionService.calls().getLast().parameters().get(1));
        assertEquals(SIGNATURE, connectionService.calls().getLast().parameters().get(2));
    }

    @ParameterizedTest
    @ValueSource(strings = {"HEAP", "EMBEDDED_KV"})
    void quotedSourceKeysAndValuesPreserveExactNames(final String mode) throws Exception {
        final List<String> fields = List.of("key,One", "k\"Two", "Value.With.Dot");
        connectionService.enqueue(response(fields, control(3, FIRST_ID, SIGNATURE, "FULL", 1L), data("INSERT", "a", "b", "value")));
        enableService(mode, "\"key,One\",\"k\"\"Two\"", "\"Value.With.Dot\"", "0 sec", "\"Db.Name\".\"s,chema\".\"ta\"\"ble\"", true);
        assertEquals(List.of("key,One", "k\"Two"), List.copyOf(service.getRequiredKeys()));
        final Record record = service.lookup(Map.of("key,One", "a", "k\"Two", "b")).orElseThrow();
        assertEquals("value", record.getAsString("Value.With.Dot"));
        assertEquals(fields, record.getSchema().getFieldNames());
        assertThrows(LookupFailureException.class, () -> service.lookup(Map.of("KEY,ONE", "a", "k\"Two", "b")));
        final RefreshResponseConnectionService.Call call = connectionService.calls().getFirst();
        assertEquals("s,chema", call.parameters().get(3));
        assertEquals("ta\"ble", call.parameters().get(4));
        assertTrue(call.sql().contains("\"key,One\", \"k\"\"Two\", \"Value.With.Dot\""));
        assertTrue(call.sql().contains("FROM \"Db.Name\".\"s,chema\".\"ta\"\"ble\""));
    }

    @ParameterizedTest
    @ValueSource(strings = {"HEAP", "EMBEDDED_KV"})
    void compositeKeysWithSeparatorBytesRemainDistinct(final String mode) throws Exception {
        final List<String> fields = List.of("K1", "K2", "LABEL");
        connectionService.enqueue(response(fields, control(3, FIRST_ID, SIGNATURE, "FULL", 2L),
                data("INSERT", "a\u0000b", "c", "first"), data("INSERT", "a", "b\u0000c", "second")));
        enableService(mode, "k1,k2", "label", "0 sec", "db.public.ref", false);
        assertEquals("first", service.lookup(Map.of("K1", "a\u0000b", "K2", "c")).orElseThrow().getAsString("LABEL"));
        assertEquals("second", service.lookup(Map.of("K1", "a", "K2", "b\u0000c")).orElseThrow().getAsString("LABEL"));
        assertThrows(LookupFailureException.class, () -> service.lookup(Map.of("K1", "a")));
    }

    @ParameterizedTest
    @ValueSource(strings = {"HEAP", "EMBEDDED_KV"})
    void explicitProjectionIncludesKeysAndOmitsUnrequestedColumns(final String mode) throws Exception {
        connectionService.enqueue(response(List.of("ITEM_ID", "LABEL"), control(2, FIRST_ID, SIGNATURE, "FULL", 1L), data("INSERT", "1", "value")));
        enableService(mode, "item_id", "label,item_id", "0 sec", "DB.PUBLIC.REF", false);
        assertEquals(List.of("ITEM_ID", "LABEL"), lookup("1").getSchema().getFieldNames());
        assertTrue(connectionService.calls().getFirst().sql().contains("\"ITEM_ID\", \"LABEL\" FROM"));
        assertFalse(connectionService.calls().getFirst().sql().contains("\"REGION\""));
    }

    @ParameterizedTest
    @ValueSource(strings = {"HEAP", "EMBEDDED_KV"})
    void duplicateKeysFailInitialLoadRatherThanChooseAnArbitraryRow(final String mode) throws Exception {
        connectionService.enqueue(full(FIRST_ID, SIGNATURE, data("INSERT", "duplicate", "one", "NA"), data("INSERT", "duplicate", "two", "EU")));
        enableService(mode, "ITEM_ID", "*", "0 sec", "DB.PUBLIC.REF", true);
        assertFalse(service.isHydrated());
        assertEquals(0L, service.size());
        assertNull(service.checkpointForTest());
        assertThrows(LookupFailureException.class, () -> lookup("duplicate"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"HEAP", "EMBEDDED_KV"})
    void nullCompositeKeyFailsLoadButNullValuesAreAllowed(final String mode) throws Exception {
        connectionService.enqueue(full(FIRST_ID, SIGNATURE, data("INSERT", "a", "not-addressable", null)));
        enableService(mode, "ITEM_ID,REGION", "*", "0 sec", "DB.PUBLIC.REF", true);
        assertFalse(service.isHydrated());
        assertNull(service.checkpointForTest());
        connectionService.enqueue(full(SECOND_ID, SIGNATURE, data("INSERT", "a", null, "EU")));
        service.refresh();
        assertNull(service.lookup(Map.of("ITEM_ID", "a", "REGION", "EU")).orElseThrow().getAsString("LABEL"));
    }

    @ParameterizedTest
    @MethodSource("backendsAndRefreshModes")
    void transientSqlFailureRetainsValidatedContentsAndDoesNotReload(final String mode, final boolean incremental) throws Exception {
        initialLoad(mode, incremental);
        connectionService.enqueueFailure(new SQLException("Temporary connection failure", "08006"));
        assertThrows(TableCacheException.class, () -> service.refresh());
        assertEquals(2, connectionService.calls().size(), "unrecognized errors must not silently fall back to a full load");
        assertTrue(service.isHydrated());
        assertEquals(3L, service.size());
        assertEquals(new CacheCheckpoint(FIRST_ID, SIGNATURE), service.checkpointForTest());
        assertEquals("b2b", lookup("ITEM_ID001").getAsString("LABEL"));
    }

    @ParameterizedTest
    @MethodSource("backendsAndRefreshModes")
    void invalidResponseRetainsValidatedContentsAndCheckpoint(final String mode, final boolean incremental) throws Exception {
        initialLoad(mode, incremental);
        connectionService.enqueue(response(FIELDS,
                control(3, SECOND_ID, SIGNATURE, incremental ? "INCREMENTAL" : "FULL", 2L), data("INSERT", "ITEM_ID900", "not-committed", "NA")));
        assertThrows(TableCacheException.class, () -> service.refresh());
        assertEquals(2, connectionService.calls().size());
        assertTrue(service.isHydrated());
        assertEquals(3L, service.size());
        assertEquals(new CacheCheckpoint(FIRST_ID, SIGNATURE), service.checkpointForTest());
        assertTrue(service.lookup(Map.of("ITEM_ID", "ITEM_ID900")).isEmpty());
    }

    static Stream<Arguments> sourceErrors() {
        return Stream.of("HEAP", "EMBEDDED_KV").flatMap(mode -> Stream.of(false, true).flatMap(incremental ->
                Stream.of("positiveChanged", "negativeChanged", "positiveInvalid", "negativeInvalid", "changedMarker", "invalidMarker", "chained")
                        .map(error -> Arguments.of(mode, incremental, error))));
    }

    @ParameterizedTest
    @ValueSource(strings = {"HEAP", "EMBEDDED_KV"})
    void sourceReplacementCanRecoverInTheSameRefresh(final String mode) throws Exception {
        initialLoad(mode, true);
        connectionService.enqueueFailure(sourceFailure("changedMarker"), () -> connectionService.sourceSignature("replacement"));
        connectionService.enqueue(full(SECOND_ID, "replacement", data("INSERT", "new", "replacement-value", "EU")));
        assertFalse(service.refresh().incremental());
        assertEquals(3, connectionService.calls().size(), "queued SQL failure must be consumed before the replacement reload");
        assertEquals(SIGNATURE, connectionService.calls().get(1).parameters().get(5));
        assertEquals("replacement", connectionService.calls().getLast().parameters().get(5));
        assertEquals(0, connectionService.remainingSteps());
        assertTrue(service.isHydrated());
        assertEquals(1L, service.size());
        assertEquals("replacement-value", lookup("new").getAsString("LABEL"));
        assertEquals(new CacheCheckpoint(SECOND_ID, "replacement"), service.checkpointForTest());
    }

    @ParameterizedTest
    @MethodSource("sourceErrors")
    void sourceFailureGatesLookupsThroughOneImmediateReloadAndLaterRetries(final String mode, final boolean incremental, final String error) throws Exception {
        initialLoad(mode, incremental);
        connectionService.enqueueFailure(sourceFailure(error));
        connectionService.enqueueFailure(new SQLException("Deferred reload temporarily unavailable", "08006"));
        assertThrows(TableCacheException.class, () -> service.refresh());
        assertEquals(3, connectionService.calls().size(), "source change attempts one immediate full reload");
        assertEquals(incremental, connectionService.calls().get(1).incremental());
        assertFalse(connectionService.calls().get(2).incremental());
        assertEquals(0, connectionService.remainingSteps());
        assertNull(connectionService.calls().getLast().parameters().get(1));
        assertNull(connectionService.calls().getLast().parameters().get(2));
        assertFalse(service.isHydrated());
        assertEquals(new CacheCheckpoint(FIRST_ID, SIGNATURE), service.checkpointForTest());
        assertEquals(3L, service.size());
        assertEquals("b2b", service.storeForTest().get("ITEM_ID001").orElseThrow().getAsString("LABEL"));
        assertThrows(LookupFailureException.class, () -> lookup("ITEM_ID001"));

        connectionService.enqueueFailure(new SQLException("Reload still unavailable", "08006"));
        assertThrows(TableCacheException.class, () -> service.refresh());
        assertEquals(4, connectionService.calls().size());
        assertFalse(connectionService.calls().getLast().incremental());
        assertNull(connectionService.calls().getLast().parameters().get(1));
        assertNull(connectionService.calls().getLast().parameters().get(2));
        assertFalse(service.isHydrated());
        assertTrue(service.storeForTest().isHydrated());
        assertEquals(new CacheCheckpoint(FIRST_ID, SIGNATURE), service.checkpointForTest());
        assertEquals(3L, service.size());
        assertEquals("b2b", service.storeForTest().get("ITEM_ID001").orElseThrow().getAsString("LABEL"));
        assertThrows(LookupFailureException.class, () -> lookup("ITEM_ID001"));

        connectionService.sourceSignature("replacement");
        connectionService.enqueue(full(SECOND_ID, "replacement", data("INSERT", "new", "validated", "EU")));
        assertFalse(service.refresh().incremental());
        assertNull(connectionService.calls().getLast().parameters().get(1));
        assertNull(connectionService.calls().getLast().parameters().get(2));
        assertTrue(service.isHydrated());
        assertEquals(new CacheCheckpoint(SECOND_ID, "replacement"), service.checkpointForTest());
        assertEquals(1L, service.size());
        assertTrue(service.lookup(Map.of("ITEM_ID", "ITEM_ID001")).isEmpty());
        assertEquals("validated", lookup("new").getAsString("LABEL"));
        connectionService.enqueue(response(FIELDS, control(3, THIRD_ID, "replacement", incremental ? "INCREMENTAL" : "FULL", 1L),
                data("INSERT", "new", "revalidated", "EU")));
        assertEquals(incremental, service.refresh().incremental());
        assertEquals(incremental ? SECOND_ID : null, connectionService.calls().getLast().parameters().get(1));
        assertEquals("replacement", connectionService.calls().getLast().parameters().get(2));
        assertEquals(new CacheCheckpoint(THIRD_ID, "replacement"), service.checkpointForTest());
    }

    static Stream<Arguments> staleHistoryErrors() {
        return Stream.of("HEAP", "EMBEDDED_KV").flatMap(mode -> Stream.of("Time travel data is not available", "Statement not found", "Statement does not exist")
                .map(message -> Arguments.of(mode, message)));
    }

    @ParameterizedTest
    @MethodSource("staleHistoryErrors")
    void unusableHistoryFallsBackToFullAndReplacesCheckpoint(final String mode, final String message) throws Exception {
        initialLoad(mode, true);
        connectionService.enqueueFailure(new SQLException(message));
        connectionService.enqueue(full(SECOND_ID, SIGNATURE, data("INSERT", "new", "reloaded", "NA")));
        assertFalse(service.refresh().incremental());
        assertEquals(3, connectionService.calls().size());
        assertFalse(connectionService.calls().getLast().incremental());
        assertNull(connectionService.calls().getLast().parameters().get(1));
        assertNull(connectionService.calls().getLast().parameters().get(2));
        assertEquals(1L, service.size());
        assertEquals("reloaded", lookup("new").getAsString("LABEL"));
        assertEquals(new CacheCheckpoint(SECOND_ID, SIGNATURE), service.checkpointForTest());
    }

    @Test
    void failedHistoryFallbackRetainsDataUnlessFallbackDetectsChangedSource() throws Exception {
        initialLoad("HEAP", true);
        connectionService.enqueueFailure(new SQLException("Statement not found"));
        connectionService.enqueueFailure(new SQLException("Network unavailable", "08006"));
        assertThrows(TableCacheException.class, () -> service.refresh());
        assertTrue(service.isHydrated());
        assertEquals(new CacheCheckpoint(FIRST_ID, SIGNATURE), service.checkpointForTest());
        connectionService.enqueueFailure(new SQLException("Time travel data is not available"));
        connectionService.enqueueFailure(sourceFailure("invalidMarker"));
        assertThrows(TableCacheException.class, () -> service.refresh());
        assertFalse(service.isHydrated());
        assertEquals(5, connectionService.calls().size());
        assertThrows(LookupFailureException.class, () -> lookup("ITEM_ID001"));
    }

    @ParameterizedTest
    @MethodSource("backendsAndRefreshModes")
    void initialFailureEnablesButLookupsFailClosedUntilSuccessfulRetry(final String mode, final boolean incremental) throws Exception {
        connectionService.enqueueFailure(new SQLException("Unavailable", "08006"));
        enableService(mode, "ITEM_ID", "*", "0 sec", "DB.PUBLIC.REF", incremental);
        assertFalse(service.isHydrated());
        assertEquals(0L, service.size());
        assertThrows(LookupFailureException.class, () -> lookup("absent"));
        connectionService.enqueue(full(SECOND_ID, SIGNATURE, data("INSERT", "new", "ready", "NA")));
        assertFalse(service.refresh().incremental());
        assertEquals("ready", lookup("new").getAsString("LABEL"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void persistentRestartMustValidateBeforeServingEvenWhenRowsExist(final boolean incremental) throws Exception {
        initialLoad("EMBEDDED_KV", incremental);
        runner.disableControllerService(service);
        connectionService.enqueueFailure(new SQLException("Restart validation unavailable", "08006"));
        runner.enableControllerService(service);
        assertEquals(2, connectionService.calls().size(), "restart must read the source even with persisted rows");
        assertEquals(incremental, connectionService.calls().getLast().incremental());
        assertEquals(incremental ? FIRST_ID : null, connectionService.calls().getLast().parameters().get(1));
        assertEquals(SIGNATURE, connectionService.calls().getLast().parameters().get(2));
        assertFalse(service.isHydrated());
        assertTrue(service.storeForTest().isHydrated());
        assertEquals(3L, service.size());
        assertEquals(new CacheCheckpoint(FIRST_ID, SIGNATURE), service.checkpointForTest());
        assertThrows(LookupFailureException.class, () -> lookup("ITEM_ID001"));
        if (incremental) {
            assertEquals(FIRST_ID, connectionService.calls().getLast().parameters().get(1));
            connectionService.enqueue(response(FIELDS, control(3, SECOND_ID, SIGNATURE, "INCREMENTAL", 0L)));
        } else {
            connectionService.enqueue(full(SECOND_ID, SIGNATURE, data("INSERT", "ITEM_ID001", "revalidated", "NA")));
        }
        assertEquals(incremental, service.refresh().incremental());
        assertTrue(service.isHydrated());
        assertNotNull(lookup("ITEM_ID001"));
        assertEquals(new CacheCheckpoint(SECOND_ID, SIGNATURE), service.checkpointForTest());
    }

    static Stream<Arguments> fullOnlySourcesAndBackends() {
        return Stream.of("HEAP", "EMBEDDED_KV").flatMap(mode -> Stream.of("view", "hybrid", "dynamicFull").map(type -> Arguments.of(mode, type)));
    }

    @ParameterizedTest
    @MethodSource("fullOnlySourcesAndBackends")
    void fullOnlySourcesReplaceRowsDespiteIncrementalConfiguration(final String mode, final String type) throws Exception {
        connectionService.sourceType(type);
        initialLoad(mode, true);
        connectionService.enqueue(full(SECOND_ID, SIGNATURE, data("INSERT", "new", "replacement", "EU")));
        assertFalse(service.refresh().incremental());
        assertTrue(service.isHydrated());
        assertEquals(1L, service.size());
        assertEquals("replacement", lookup("new").getAsString("LABEL"));
        assertTrue(service.lookup(Map.of("ITEM_ID", "ITEM_ID001")).isEmpty());
        assertEquals(new CacheCheckpoint(SECOND_ID, SIGNATURE), service.checkpointForTest());
        assertEquals(2, connectionService.calls().size());
        assertFalse(connectionService.calls().getLast().incremental());
        assertNull(connectionService.calls().getLast().parameters().get(1));
        assertEquals(SIGNATURE, connectionService.calls().getLast().parameters().get(2));
        assertEquals(SIGNATURE, connectionService.calls().getLast().parameters().get(5));
    }

    @ParameterizedTest
    @ValueSource(strings = {"view", "hybrid", "dynamicFull"})
    void persistentFullOnlyRestartRevalidatesWithoutResumingChanges(final String type) throws Exception {
        connectionService.sourceType(type);
        initialLoad("EMBEDDED_KV", true);
        runner.disableControllerService(service);
        connectionService.enqueueFailure(new SQLException("Restart unavailable", "08006"));
        runner.enableControllerService(service);
        assertFalse(service.isHydrated());
        assertTrue(service.storeForTest().isHydrated());
        assertEquals(new CacheCheckpoint(FIRST_ID, SIGNATURE), service.checkpointForTest());
        assertEquals(3L, service.size());
        assertThrows(LookupFailureException.class, () -> lookup("ITEM_ID001"));
        assertFalse(connectionService.calls().getLast().incremental());
        assertNull(connectionService.calls().getLast().parameters().get(1));
        assertEquals(SIGNATURE, connectionService.calls().getLast().parameters().get(2));
        connectionService.enqueue(full(SECOND_ID, SIGNATURE, data("INSERT", "new", "revalidated", "NA")));
        assertFalse(service.refresh().incremental());
        assertTrue(service.isHydrated());
        assertEquals(1L, service.size());
        assertTrue(service.lookup(Map.of("ITEM_ID", "ITEM_ID001")).isEmpty());
        assertEquals("revalidated", lookup("new").getAsString("LABEL"));
        assertEquals(new CacheCheckpoint(SECOND_ID, SIGNATURE), service.checkpointForTest());
        assertEquals(3, connectionService.calls().size());
        assertTrue(connectionService.calls().stream().noneMatch(RefreshResponseConnectionService.Call::incremental));
    }

    @ParameterizedTest
    @MethodSource("fullOnlySourcesAndBackends")
    void incrementalProtocolRowsAreRejectedWhenMetadataForcesFull(final String mode, final String type) throws Exception {
        connectionService.sourceType(type);
        initialLoad(mode, true);
        connectionService.enqueue(response(FIELDS, control(3, SECOND_ID, SIGNATURE, "INCREMENTAL", 1L), data("INSERT", "new", "uncommitted", "NA")));
        assertThrows(TableCacheException.class, () -> service.refresh());
        assertTrue(service.isHydrated());
        assertEquals(3L, service.size());
        assertEquals(new CacheCheckpoint(FIRST_ID, SIGNATURE), service.checkpointForTest());
        assertEquals("b2b", lookup("ITEM_ID001").getAsString("LABEL"));
        assertTrue(service.lookup(Map.of("ITEM_ID", "new")).isEmpty());
        assertEquals(2, connectionService.calls().size(), "protocol errors must not cause a second refresh");
        assertFalse(connectionService.calls().getLast().incremental());
    }

    @ParameterizedTest
    @ValueSource(strings = {"managedIceberg", "dynamicIncremental"})
    void qualifiedIncrementalSourcesResumePersistentCheckpoints(final String type) throws Exception {
        connectionService.sourceType(type);
        initialLoad("EMBEDDED_KV", true);
        runner.disableControllerService(service);
        connectionService.enqueue(response(FIELDS, control(3, SECOND_ID, SIGNATURE, "INCREMENTAL", 1L), data("DELETE", "ITEM_ID001", "b2b", "NA")));
        runner.enableControllerService(service);
        assertTrue(service.isHydrated());
        assertEquals(2L, service.size());
        assertTrue(service.lookup(Map.of("ITEM_ID", "ITEM_ID001")).isEmpty());
        assertEquals(new CacheCheckpoint(SECOND_ID, SIGNATURE), service.checkpointForTest());
        assertTrue(connectionService.calls().getLast().incremental());
        assertEquals(FIRST_ID, connectionService.calls().getLast().parameters().get(1));
        assertEquals(SIGNATURE, connectionService.calls().getLast().parameters().get(5));
        assertEquals(2, connectionService.connectionCount());
        assertEquals(4, connectionService.metadataCalls().size(), "one capability SHOW per refresh");
        assertEquals(2, connectionService.sessionCalls().size());
    }

    @ParameterizedTest
    @ValueSource(strings = {"missing", "ambiguous", "invalid", "temporary", "external", "externalIceberg", "dynamicIceberg", "dynamicAuto", "noRetention"})
    void invalidPreflightGatesLookupsAndLeavesPublishedRowsUntouched(final String type) throws Exception {
        initialLoad("HEAP", true);
        connectionService.sourceType(type);
        assertThrows(TableCacheException.class, () -> service.refresh());
        assertFalse(service.isHydrated());
        assertTrue(service.storeForTest().isHydrated());
        assertEquals(3L, service.size());
        assertEquals(new CacheCheckpoint(FIRST_ID, SIGNATURE), service.checkpointForTest());
        assertEquals("b2b", service.storeForTest().get("ITEM_ID001").orElseThrow().getAsString("LABEL"));
        assertThrows(LookupFailureException.class, () -> lookup("ITEM_ID001"));
        assertEquals(1, connectionService.calls().size(), "invalid metadata prevents anonymous reads, including reload");
        assertEquals(3, connectionService.connectionCount(), "source invalidation attempts exactly one reload");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void unavailablePreflightDoesNotInvalidateOrRetryPublishedCache(final boolean showFailure) throws Exception {
        connectionService.sourceType(showFailure ? "managedIceberg" : "standard");
        initialLoad("HEAP", true);
        final SQLException failure = new SQLException("Preflight unavailable", "08006");
        if (showFailure) {
            connectionService.capabilityFailure(failure);
        } else {
            connectionService.metadataFailure(failure);
        }
        assertThrows(TableCacheException.class, () -> service.refresh());
        assertTrue(service.isHydrated());
        assertEquals(new CacheCheckpoint(FIRST_ID, SIGNATURE), service.checkpointForTest());
        assertEquals("b2b", lookup("ITEM_ID001").getAsString("LABEL"));
        assertEquals(1, connectionService.calls().size());
        assertEquals(2, connectionService.connectionCount());
    }

    static Stream<Arguments> changedSignatureFailures() {
        return Stream.of("HEAP", "EMBEDDED_KV").flatMap(mode -> Stream.of(false, true).flatMap(incremental ->
                Stream.of("dynamicIncremental", "managedIceberg").map(type -> Arguments.of(mode, incremental, type))));
    }

    @ParameterizedTest
    @MethodSource("changedSignatureFailures")
    void changedSignatureClosesServingGateBeforeFailedCapabilityReload(final String mode, final boolean incremental, final String type) throws Exception {
        connectionService.sourceType(type);
        initialLoad(mode, incremental);
        final int metadataBefore = connectionService.metadataCalls().size();
        connectionService.sourceSignature("replacement-source");
        final SQLException unavailable = new SQLException("Capability metadata unavailable", "08006");
        connectionService.capabilityFailure(unavailable);
        connectionService.enqueue(full(SECOND_ID, "replacement-source", data("INSERT", "new", "validated", "EU")));

        final TableCacheException failure = assertThrows(TableCacheException.class, () -> service.refresh());
        assertEquals(unavailable, failure.getCause(), "one full reload must reach and fail capability validation");
        final List<RefreshResponseConnectionService.Call> metadata = connectionService.metadataCalls().subList(metadataBefore, connectionService.metadataCalls().size());
        assertEquals(3, metadata.size());
        assertTrue(metadata.get(0).sql().startsWith("SELECT TABLE_TYPE,"));
        assertTrue(metadata.get(1).sql().startsWith("SELECT TABLE_TYPE,"), "changed signature must skip SHOW before the reload preflight");
        assertTrue(metadata.get(2).sql().startsWith("SHOW "));
        assertEquals(3, connectionService.connectionCount(), "only the original load, rejected preflight and one reload may connect");
        assertEquals(1, connectionService.calls().size(), "neither rejected preflight nor failed SHOW may read rows");
        assertEquals(1, connectionService.remainingSteps());
        assertFalse(service.isHydrated());
        assertTrue(service.storeForTest().isHydrated());
        assertEquals(3L, service.size());
        assertEquals(new CacheCheckpoint(FIRST_ID, SIGNATURE), service.checkpointForTest());
        assertEquals("b2b", service.storeForTest().get("ITEM_ID001").orElseThrow().getAsString("LABEL"));
        assertThrows(LookupFailureException.class, () -> lookup("ITEM_ID001"));

        connectionService.capabilityFailure(null);
        assertFalse(service.refresh().incremental(), "recovery must discard the old checkpoint");
        assertTrue(service.isHydrated());
        assertEquals(0, connectionService.remainingSteps());
        assertEquals(1L, service.size());
        assertTrue(service.lookup(Map.of("ITEM_ID", "ITEM_ID001")).isEmpty());
        assertEquals("validated", lookup("new").getAsString("LABEL"));
        assertEquals(new CacheCheckpoint(SECOND_ID, "replacement-source"), service.checkpointForTest());
        assertNull(connectionService.calls().getLast().parameters().get(1));
        assertNull(connectionService.calls().getLast().parameters().get(2));
        assertEquals("replacement-source", connectionService.calls().getLast().parameters().get(5));
    }

    @Test
    void restartWithChangedSourceReplacesRatherThanServesPersistedRows() throws Exception {
        initialLoad("EMBEDDED_KV", true);
        runner.disableControllerService(service);
        connectionService.enqueueFailure(sourceFailure("changedMarker"), () -> connectionService.sourceSignature("replacement"));
        connectionService.enqueue(full(SECOND_ID, "replacement", data("INSERT", "new", "validated", "NA")));
        runner.enableControllerService(service);
        assertTrue(service.isHydrated());
        assertEquals(1L, service.size());
        assertTrue(service.lookup(Map.of("ITEM_ID", "ITEM_ID001")).isEmpty());
        assertEquals("validated", lookup("new").getAsString("LABEL"));
        assertEquals(new CacheCheckpoint(SECOND_ID, "replacement"), service.checkpointForTest());
    }

    @ParameterizedTest
    @ValueSource(strings = {"source", "keys", "values", "incremental", "connection"})
    void embeddedRejectsConfigurationIdentityMismatchAndCanReopenOriginal(final String changed) throws Exception {
        initialLoad("EMBEDDED_KV", true);
        runner.disableControllerService(service);
        switch (changed) {
            case "source" -> runner.setProperty(service, SnowflakeTableCacheLookupService.SOURCE_TABLE, "DB.PUBLIC.OTHER");
            case "keys" -> runner.setProperty(service, SnowflakeTableCacheLookupService.KEY_COLUMNS, "ITEM_ID,REGION");
            case "values" -> runner.setProperty(service, SnowflakeTableCacheLookupService.VALUE_COLUMNS, "LABEL");
            case "incremental" -> runner.setProperty(service, SnowflakeTableCacheLookupService.INCREMENTAL_REFRESH, "false");
            case "connection" -> {
                final RefreshResponseConnectionService other = new RefreshResponseConnectionService();
                runner.addControllerService("other", other);
                runner.enableControllerService(other);
                runner.setProperty(service, SnowflakeTableCacheLookupService.SNOWFLAKE_CONNECTION_SERVICE, "other");
            }
            default -> throw new IllegalArgumentException(changed);
        }
        assertThrows(AssertionError.class, () -> runner.enableControllerService(service));
        assertFalse(service.isHydrated());
        assertThrows(LookupFailureException.class, () -> lookup("ITEM_ID001"));
        assertEquals(1, connectionService.calls().size(), "identity mismatch must fail before querying the source");
        runner.setProperty(service, SnowflakeTableCacheLookupService.SOURCE_TABLE, "DB.PUBLIC.REF");
        runner.setProperty(service, SnowflakeTableCacheLookupService.KEY_COLUMNS, "ITEM_ID");
        runner.setProperty(service, SnowflakeTableCacheLookupService.VALUE_COLUMNS, "*");
        runner.setProperty(service, SnowflakeTableCacheLookupService.INCREMENTAL_REFRESH, "true");
        runner.setProperty(service, SnowflakeTableCacheLookupService.SNOWFLAKE_CONNECTION_SERVICE, "fixture");
        connectionService.enqueue(response(FIELDS, control(3, SECOND_ID, SIGNATURE, "INCREMENTAL", 0L)));
        runner.enableControllerService(service);
        assertTrue(service.isHydrated());
        assertEquals("b2b", lookup("ITEM_ID001").getAsString("LABEL"));
    }

    @Test
    void scheduledRefreshSurvivesFailureAndRecoversWithoutAProcessor() throws Exception {
        connectionService.enqueueFailure(new SQLException("Initial load unavailable", "08006"));
        connectionService.enqueueFailure(new SQLException("Still unavailable", "08006"));
        connectionService.enqueue(full(SECOND_ID, SIGNATURE, data("INSERT", "new", "scheduled", "NA")));
        enableService("HEAP", "ITEM_ID", "*", "25 millis", "DB.PUBLIC.REF", true);
        await(service::isHydrated);
        assertEquals("scheduled", lookup("new").getAsString("LABEL"));
        assertEquals(new CacheCheckpoint(SECOND_ID, SIGNATURE), service.checkpointForTest());
        runner.disableControllerService(service);
        assertTrue(connectionService.calls().size() >= 3);
        assertFalse(connectionService.calls().get(2).incremental());
        assertEquals(0, connectionService.remainingSteps(), "the scheduled retry must consume the successful response");
        final int callsAtDisable = connectionService.calls().size();
        Thread.sleep(100);
        assertEquals(callsAtDisable, connectionService.calls().size(), "disable must terminate the refresh schedule");
    }

    @Test
    void scheduledRefreshPicksUpAChangeAfterSuccessfulStartup() throws Exception {
        connectionService.enqueue(full(FIRST_ID, SIGNATURE, data("INSERT", "old", "old", "NA")));
        connectionService.enqueue(response(FIELDS, control(3, SECOND_ID, SIGNATURE, "INCREMENTAL", 2L),
                data("DELETE", "old", "old", "NA"), data("INSERT", "new", "scheduled", "EU")));
        enableService("HEAP", "ITEM_ID", "*", "25 millis", "DB.PUBLIC.REF", true);
        await(() -> new CacheCheckpoint(SECOND_ID, SIGNATURE).equals(service.checkpointForTest()));
        assertEquals("scheduled", lookup("new").getAsString("LABEL"));
        assertTrue(service.lookup(Map.of("ITEM_ID", "old")).isEmpty());
        assertTrue(connectionService.calls().get(1).incremental());
    }

    @Test
    void zeroIntervalNeverSchedulesAndDisableStopsFurtherRefreshes() throws Exception {
        initialLoad("HEAP", true);
        connectionService.enqueue(full(SECOND_ID, SIGNATURE, data("INSERT", "new", "not-scheduled", "NA")));
        Thread.sleep(150);
        assertEquals(1, connectionService.calls().size());
        assertEquals(1, connectionService.remainingSteps());
        assertEquals(3L, service.size());
        runner.disableControllerService(service);
        assertFalse(service.isHydrated());
        assertThrows(TableCacheException.class, () -> service.refresh());
        assertThrows(LookupFailureException.class, () -> lookup("ITEM_ID001"));
        assertEquals(1, connectionService.calls().size());
    }

    @ParameterizedTest
    @MethodSource("backendsAndRefreshModes")
    void concurrentReadersSeeOldDataWhileQueryExecutionIsBlocked(final String mode, final boolean incremental) throws Exception {
        initialLoad(mode, incremental);
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        connectionService.enqueueBlocked(response(FIELDS, control(3, SECOND_ID, SIGNATURE, incremental ? "INCREMENTAL" : "FULL", 1L),
                data("INSERT", "ITEM_ID001", "new", "EU")), entered, release);
        try (final ExecutorService executor = Executors.newFixedThreadPool(2)) {
            final Future<RefreshResult> refresh = executor.submit(() -> service.refresh());
            try {
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                final Future<?> reader = executor.submit(() -> {
                    for (int index = 0; index < 100; index++) {
                        assertEquals("b2b", lookup("ITEM_ID001").getAsString("LABEL"));
                        assertEquals(new CacheCheckpoint(FIRST_ID, SIGNATURE), service.checkpointForTest());
                    }
                    return null;
                });
                reader.get(5, TimeUnit.SECONDS);
            } finally {
                release.countDown();
            }
            refresh.get(5, TimeUnit.SECONDS);
            assertEquals("new", lookup("ITEM_ID001").getAsString("LABEL"));
            assertEquals(new CacheCheckpoint(SECOND_ID, SIGNATURE), service.checkpointForTest());
        } finally {
            release.countDown();
        }
    }

    static Stream<Arguments> publicationCloseBoundaries() {
        return backendsAndRefreshModes().flatMap(arguments -> Stream.of("resultSet", "statement", "connection").flatMap(resource ->
                Stream.of(false, true).map(failClose -> Arguments.of(arguments.get()[0], arguments.get()[1], resource, failClose))));
    }

    @ParameterizedTest
    @MethodSource("publicationCloseBoundaries")
    void consumedRefreshRemainsUnpublishedUntilJdbcClosureSucceeds(final String mode, final boolean incremental,
                                                                  final String resource, final boolean failClose) throws Exception {
        initialLoad(mode, incremental);
        final CacheCheckpoint before = service.checkpointForTest();
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        connectionService.enqueue(response(FIELDS, control(3, SECOND_ID, SIGNATURE, incremental ? "INCREMENTAL" : "FULL", 2L),
                data("INSERT", "ITEM_ID001", "replacement", "EU"), data("INSERT", "new", "added", "EU")));
        connectionService.blockOnClose(resource, entered, release);
        if (failClose) {
            connectionService.failOnClose(resource);
        }
        try (final ExecutorService executor = Executors.newFixedThreadPool(2)) {
            final Future<RefreshResult> refresh = executor.submit(() -> service.refresh());
            try {
                assertTrue(entered.await(5, TimeUnit.SECONDS), "refresh must consume the response and reach JDBC closure");
                assertFalse(refresh.isDone());
                assertEquals(0, connectionService.remainingSteps());
                final Future<?> reader = executor.submit(() -> {
                    assertTrue(service.isHydrated());
                    assertEquals(3L, service.size());
                    assertEquals("b2b", lookup("ITEM_ID001").getAsString("LABEL"));
                    assertTrue(service.lookup(Map.of("ITEM_ID", "new")).isEmpty());
                    assertEquals(before, service.checkpointForTest());
                    return null;
                });
                reader.get(5, TimeUnit.SECONDS);
            } finally {
                release.countDown();
            }
            if (failClose) {
                final ExecutionException failure = assertThrows(ExecutionException.class, () -> refresh.get(5, TimeUnit.SECONDS));
                assertTrue(failure.getCause() instanceof TableCacheException);
                assertTrue(service.isHydrated());
                assertEquals(3L, service.size());
                assertEquals("b2b", lookup("ITEM_ID001").getAsString("LABEL"));
                assertTrue(service.lookup(Map.of("ITEM_ID", "new")).isEmpty());
                assertEquals(before, service.checkpointForTest());
            } else {
                assertEquals(incremental, refresh.get(5, TimeUnit.SECONDS).incremental());
                assertEquals(incremental ? 4L : 2L, service.size());
                assertEquals("replacement", lookup("ITEM_ID001").getAsString("LABEL"));
                assertEquals("added", lookup("new").getAsString("LABEL"));
                assertEquals(new CacheCheckpoint(SECOND_ID, SIGNATURE), service.checkpointForTest());
            }
        } finally {
            release.countDown();
        }
        assertTrue(connectionService.resourcesClosed());
    }

    @Test
    void concurrentRefreshesSerializeAndSecondUsesCommittedCheckpoint() throws Exception {
        initialLoad("EMBEDDED_KV", true);
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch secondStarted = new CountDownLatch(1);
        connectionService.enqueueBlocked(response(FIELDS, control(3, SECOND_ID, SIGNATURE, "INCREMENTAL", 0L)), entered, release);
        connectionService.enqueue(response(FIELDS, control(3, THIRD_ID, SIGNATURE, "INCREMENTAL", 0L)));
        try (final ExecutorService executor = Executors.newFixedThreadPool(2)) {
            final Future<RefreshResult> first = executor.submit(() -> service.refresh());
            try {
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                final Future<RefreshResult> second = executor.submit(() -> {
                    secondStarted.countDown();
                    return service.refresh();
                });
                assertTrue(secondStarted.await(5, TimeUnit.SECONDS));
                assertThrows(TimeoutException.class, () -> second.get(100, TimeUnit.MILLISECONDS));
                assertEquals(2, connectionService.calls().size());
                release.countDown();
                first.get(5, TimeUnit.SECONDS);
                second.get(5, TimeUnit.SECONDS);
            } finally {
                release.countDown();
            }
        }
        assertEquals(1, connectionService.maximumExecutions());
        assertEquals(SECOND_ID, connectionService.calls().getLast().parameters().get(1));
        assertEquals(new CacheCheckpoint(THIRD_ID, SIGNATURE), service.checkpointForTest());
    }

    @ParameterizedTest
    @ValueSource(strings = {"HEAP", "EMBEDDED_KV"})
    void disableWaitsForRefreshAndNeverReopensServingGate(final String mode) throws Exception {
        initialLoad(mode, true);
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch disableStarted = new CountDownLatch(1);
        connectionService.enqueueBlocked(response(FIELDS, control(3, SECOND_ID, SIGNATURE, "INCREMENTAL", 0L)), entered, release);
        try (final ExecutorService executor = Executors.newFixedThreadPool(2)) {
            final Future<RefreshResult> refresh = executor.submit(() -> service.refresh());
            try {
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                final Future<?> disable = executor.submit(() -> {
                    disableStarted.countDown();
                    service.onDisabled();
                });
                assertTrue(disableStarted.await(5, TimeUnit.SECONDS));
                await(() -> !service.isHydrated());
                assertThrows(TimeoutException.class, () -> disable.get(100, TimeUnit.MILLISECONDS));
                assertNotNull(service.storeForTest(), "disable must not close storage while refresh still owns it");
                assertThrows(LookupFailureException.class, () -> lookup("ITEM_ID001"));
                release.countDown();
                refresh.get(5, TimeUnit.SECONDS);
                disable.get(5, TimeUnit.SECONDS);
                assertNull(service.storeForTest());
                assertFalse(service.isHydrated());
                assertThrows(TableCacheException.class, () -> service.refresh());
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    void sourceInvalidationClosesGateWhileReplacementLoadIsInFlight() throws Exception {
        initialLoad("HEAP", true);
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        connectionService.enqueueFailure(sourceFailure("changedMarker"), () -> connectionService.sourceSignature("replacement"));
        connectionService.enqueueBlocked(full(SECOND_ID, "replacement", data("INSERT", "new", "validated", "EU")), entered, release);
        try (final ExecutorService executor = Executors.newSingleThreadExecutor()) {
            final Future<RefreshResult> refresh = executor.submit(() -> service.refresh());
            try {
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                assertFalse(service.isHydrated());
                assertEquals(3L, service.size());
                assertThrows(LookupFailureException.class, () -> lookup("ITEM_ID001"));
            } finally {
                release.countDown();
            }
            refresh.get(5, TimeUnit.SECONDS);
            assertTrue(service.isHydrated());
            assertEquals("validated", lookup("new").getAsString("LABEL"));
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

    private void initialLoad(final String mode, final boolean incremental) throws InitializationException {
        connectionService.enqueue(full(FIRST_ID, SIGNATURE,
                data("INSERT", "ITEM_ID001", "b2b", "NA"), data("INSERT", "ITEM_ID002", "b2b,advertiser", "NA"), data("INSERT", "ITEM_ID003", "advertiser", "EU")));
        enableService(mode, "ITEM_ID", "*", "0 sec", "DB.PUBLIC.REF", incremental);
    }

    private void enableService(final String mode, final String keys, final String values, final String interval,
                               final String source, final boolean incremental) throws InitializationException {
        runner.addControllerService("fixture", connectionService);
        runner.enableControllerService(connectionService);
        runner.addControllerService("cache", service);
        runner.setProperty(service, SnowflakeTableCacheLookupService.SNOWFLAKE_CONNECTION_SERVICE, "fixture");
        runner.setProperty(service, SnowflakeTableCacheLookupService.SOURCE_TABLE, source);
        runner.setProperty(service, SnowflakeTableCacheLookupService.KEY_COLUMNS, keys);
        runner.setProperty(service, SnowflakeTableCacheLookupService.VALUE_COLUMNS, values);
        runner.setProperty(service, SnowflakeTableCacheLookupService.REFRESH_INTERVAL, interval);
        runner.setProperty(service, SnowflakeTableCacheLookupService.INCREMENTAL_REFRESH, Boolean.toString(incremental));
        runner.setProperty(service, SnowflakeTableCacheLookupService.STORAGE_MODE, mode);
        if (!"HEAP".equals(mode)) {
            runner.setProperty(service, SnowflakeTableCacheLookupService.STORAGE_DIRECTORY, storageDirectory.toString());
        }
        runner.enableControllerService(service);
    }

    private Record lookup(final String key) throws LookupFailureException {
        return service.lookup(Map.of("ITEM_ID", key)).orElseThrow();
    }

    private static RefreshResponseConnectionService.Response full(final String queryId, final String signature, final Object[]... dataRows) {
        final Object[][] rows = new Object[dataRows.length + 1][];
        rows[0] = control(3, queryId, signature, "FULL", (long) dataRows.length);
        System.arraycopy(dataRows, 0, rows, 1, dataRows.length);
        return response(FIELDS, rows);
    }

    private static SQLException sourceFailure(final String kind) {
        return switch (kind) {
            case "positiveChanged" -> new SQLException("Source continuity failure", "P0001", 20001);
            case "negativeChanged" -> new SQLException("Source continuity failure", "P0001", -20001);
            case "positiveInvalid" -> new SQLException("Invalid source", "P0001", 20002);
            case "negativeInvalid" -> new SQLException("Invalid source", "P0001", -20002);
            case "changedMarker" -> new SQLException("TABLE_CACHE_SOURCE_CHANGED");
            case "invalidMarker" -> new SQLException("TABLE_CACHE_INVALID_SOURCE");
            case "chained" -> {
                final SQLException outer = new SQLException("Driver wrapper");
                outer.setNextException(new SQLException("TABLE_CACHE_SOURCE_CHANGED"));
                yield outer;
            }
            default -> throw new IllegalArgumentException(kind);
        };
    }

    private static void await(final BooleanSupplier condition) throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertTrue(condition.getAsBoolean(), "condition did not become true before the deadline");
    }
}
