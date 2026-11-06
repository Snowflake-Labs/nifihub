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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.snowflake.nifihub.data.snowflaketablecache.RefreshResponseConnectionService.control;
import static com.snowflake.nifihub.data.snowflaketablecache.RefreshResponseConnectionService.data;
import static com.snowflake.nifihub.data.snowflaketablecache.RefreshResponseConnectionService.response;
import static com.snowflake.nifihub.data.snowflaketablecache.RefreshResponseConnectionService.truncated;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** SQL assertions verify generated text only; scripted JDBC responses do not validate live Snowflake SQL. */
class TestAnonymousTableRefresh {
    private static final String FIRST_ID = "00000000-0000-0000-0000-000000000001";
    private static final String SECOND_ID = "00000000-0000-0000-0000-000000000002";
    private static final String SIGNATURE = "source-signature";
    private static final List<String> FIELDS = List.of("K", "V");

    @TempDir
    Path storageDirectory;

    @Test
    void fullLoadUsesChildQueryIdAndReturnsTypedControlAndDataRows() {
        final String call = reader().sql(FIELDS, false, "TCFIXTURE");
        assertTrue(call.contains("WITH refresh_cache AS PROCEDURE"));
        assertTrue(call.contains("RETURNS TABLE() LANGUAGE SQL"));
        final String sql = procedureBody(call);
        assertTrue(sql.contains("SELECT ''INSERT''::VARCHAR AS \"TCFIXTURE\", \"K\", \"V\" FROM \"DB\".\"PUBLIC\".\"REF\""));
        final Matcher childBoundary = Pattern.compile("(\\w+) := \\(EXECUTE IMMEDIATE :(\\w+)\\);\\s*(\\w+) := SQLID;\\s*(\\w+) := \\3;").matcher(sql);
        assertTrue(childBoundary.find(), "the full snapshot checkpoint must be its child query ID");
        assertTrue(sql.matches("(?s).*RETURN TABLE\\(\\w+\\);.*"));
        assertTrue(sql.matches("(?s).*USING \\(%s, \\w+, \\w+, %s, %s, %s\\).*"
                .formatted(childBoundary.group(4), childBoundary.group(3), childBoundary.group(3), childBoundary.group(3))));
        assertTrue(sql.contains("''CONTROL'' AS TCFIXTUREK"));
        assertTrue(sql.contains("''DATA'', NULL::VARCHAR"));
        assertTrue(sql.contains("LEFT JOIN TABLE(RESULT_SCAN(?)) d ON FALSE"));
        assertTrue(sql.contains("(SELECT COUNT(*) FROM TABLE(RESULT_SCAN(?))) AS TCFIXTUREN"));
        assertFalse(sql.contains("CURRENT_STATEMENT()"));
        assertFalse(sql.contains("LAST_QUERY_ID()"));
        assertFalse(sql.contains("CHANGES(INFORMATION"));
        assertTrue(sql.matches("(?s).*\\w+ VARCHAR DEFAULT 'FULL';.*"));
        assertFalse(sql.contains("FULL OUTER JOIN"));
        assertFalse(sql.contains("CURRENT_TIMESTAMP()"), "resumable full reads use the snapshot child, not a diagnostic marker");
    }

    @Test
    void fullOnlySqlContainsOnlyTheDirectSourceResponse() {
        final String sql = procedureBody(reader().sql(FIELDS, false, "TCFIXTURE", true));
        assertTrue(sql.contains("''FULL''::VARCHAR"));
        assertTrue(sql.contains("FULL OUTER JOIN (SELECT 1 AS marker) seed ON FALSE"));
        assertTrue(sql.contains("COUNT(d.\"TCFIXTURE\") OVER ()"));
        assertEquals(1, sql.split(Pattern.quote("EXECUTE IMMEDIATE"), -1).length - 1, "the full-only source must be read once");
        assertFalse(sql.contains("RESULT_SCAN"));
        assertFalse(sql.contains("CHANGES(INFORMATION"));
        assertFalse(sql.contains("IF (TRUE)"));
        assertFalse(sql.contains("IF (NOT TRUE)"));
    }

    @Test
    void callerOwnedTransactionIsRejectedWithoutRunningAQuery() throws Exception {
        final RefreshResponseConnectionService connection = new RefreshResponseConnectionService();
        connection.autoCommit(false);
        try (TableCacheStore store = new HeapTableCacheStore()) {
            assertThrows(SQLException.class, () -> reader().refresh(connection, store, false));
            assertTrue(connection.calls().isEmpty());
            assertTrue(connection.metadataCalls().isEmpty());
            assertTrue(connection.resourcesClosed());
            assertFalse(store.isHydrated());
        }
    }

    @Test
    void deltaHasExplicitStatementEndAndBindsBothBoundaries() {
        final String sql = procedureBody(reader().sql(FIELDS, true, "TCFIXTURE"));
        assertTrue(sql.contains("CHANGES(INFORMATION => DEFAULT) AT(STATEMENT => ?) END(STATEMENT => ?)"));
        assertTrue(sql.contains("METADATA$ACTION AS \"TCFIXTURE\""));
        final Matcher boundary = Pattern.compile("SELECT CURRENT_TIMESTAMP\\(\\) INTO :(\\w+);\\s*(\\w+) := SQLID;").matcher(sql);
        assertTrue(boundary.find(), "delta must capture an explicit end statement before reading changes");
        assertTrue(sql.matches("(?s).*EXECUTE IMMEDIATE :\\w+ USING \\(previous_id, %s\\).*".formatted(boundary.group(2))));
        assertTrue(sql.matches("(?s).*\\w+ VARCHAR DEFAULT 'INCREMENTAL';.*"));
        assertFalse(sql.contains("FULL OUTER JOIN"));
        assertEquals(1, sql.split(Pattern.quote("CHANGES(INFORMATION"), -1).length - 1, "inactive full-only SQL must not duplicate the delta read");
        assertFalse(sql.contains("AT(TIMESTAMP"));
        assertFalse(sql.contains("END(TIMESTAMP"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void sourceValidationRunsBeforeAndAfterTheReadAndChecksAliasCollisions(final boolean delta) {
        final String sql = procedureBody(reader().sql(FIELDS, delta, "TCFIXTURE"));
        assertEquals(2, sql.split(Pattern.quote("FROM \"DB\".INFORMATION_SCHEMA.TABLES"), -1).length - 1);
        assertTrue(sql.contains("CURRENT_ACCOUNT(), CURRENT_USER(), CURRENT_ROLE()"));
        assertTrue(sql.contains("CREATED IS NOT NULL AND LAST_DDL IS NOT NULL"));
        assertTrue(sql.contains("TABLE_TYPE IN ('BASE TABLE', 'VIEW') AND IS_TEMPORARY = 'NO'"));
        assertTrue(sql.contains(SourceMetadata.SIGNATURE));
        assertTrue(sql.contains("CURRENT_ACCOUNT(), CURRENT_USER(), CURRENT_ROLE(), TABLE_TYPE, IS_ICEBERG, IS_DYNAMIC, IS_HYBRID"));
        assertFalse(sql.contains("RETENTION_TIME >= 1"), "capability and retention qualification belong to preflight");
        final Matcher expectedSignature = Pattern.compile("IF \\(expected_signature <> (\\w+)\\) THEN\\s*RAISE (\\w+);").matcher(sql);
        assertTrue(expectedSignature.find(), "the first source check must reject a preflight signature mismatch");
        assertTrue(sql.matches("(?s).*IF \\(previous_signature IS NOT NULL AND previous_signature <> %s\\) THEN\\s*RAISE %s;.*"
                .formatted(expectedSignature.group(1), expectedSignature.group(2))));
        assertFalse(sql.contains("previous_id IS NOT NULL"), "full loads must check the saved signature without a query boundary");
        assertTrue(Pattern.compile("IF \\(\\w+ <> 1 OR (\\w+) IS NULL OR \\1 <> %s\\) THEN\\s*RAISE %s;"
                .formatted(expectedSignature.group(1), expectedSignature.group(2))).matcher(sql).find(),
                "the post-read signature must still match the first source check");
        final Matcher sourceQueries = Pattern.compile("SELECT COUNT\\(\\*\\), MAX\\(IFF\\(.*?FROM \\\"DB\\\"\\.INFORMATION_SCHEMA\\.TABLES\\s+WHERE ([^;]+);",
                Pattern.DOTALL).matcher(sql);
        int sourceQueryCount = 0;
        while (sourceQueries.find()) {
            assertEquals("TABLE_SCHEMA = :source_schema AND TABLE_NAME = :source_name", sourceQueries.group(1).trim(),
                    "object count must include unsupported same-name objects, including temporary shadows");
            assertTrue(sourceQueries.group().contains("IFF(TABLE_TYPE IN ('BASE TABLE', 'VIEW') AND IS_TEMPORARY = 'NO'"));
            assertTrue(sourceQueries.group().contains("RETENTION_TIME)), 256), NULL))"));
            sourceQueryCount++;
        }
        assertEquals(2, sourceQueryCount, "both source checks must count all names and qualify only the signature");
        assertTrue(sql.contains("TABLE_CACHE_SOURCE_CHANGED"));
        assertTrue(sql.contains("TABLE_CACHE_INVALID_SOURCE"));
        assertTrue(sql.contains("COLUMN_NAME IN ('TCFIXTURE', 'TCFIXTUREK', 'TCFIXTUREQ', 'TCFIXTURES', 'TCFIXTUREM', 'TCFIXTUREN')"));
        assertFalse(new AnonymousTableRefresh("DB.PUBLIC.REF", List.of("K"), List.of("V"), false, false)
                .sql(FIELDS, false, "TCFIXTURE").contains("RETENTION_TIME >= 1"));
    }

    @Test
    void sqlEscapesQuotedIdentifiersAndNestedStringLiterals() {
        final AnonymousTableRefresh refresh = new AnonymousTableRefresh("\"Db.Name\".\"s'chema\".\"ta\"\"ble\"",
                List.of("ke\"y"), List.of("v'alue"), false, true);
        final String sql = procedureBody(refresh.sql(List.of("ke\"y", "v'alue"), false, "TCFIXTURE"));
        assertTrue(sql.contains("\"ke\"\"y\", \"v''alue\" FROM \"Db.Name\".\"s''chema\".\"ta\"\"ble\""));
        assertTrue(sql.contains("FROM \"Db.Name\".INFORMATION_SCHEMA.TABLES"));
        assertTrue(sql.contains("TABLE_SCHEMA = :source_schema AND TABLE_NAME = :source_name"));
    }

    @Test
    void checkpointAndSourceFiltersAreBoundNotInterpolated() throws Exception {
        final String signature = "scope'; DROP TABLE victim; --";
        final String schemaName = "s'); SELECT 'injected";
        final String tableName = "t\"; DROP TABLE victim; --";
        final AnonymousTableRefresh refresh = new AnonymousTableRefresh(
                "DB.%s.%s".formatted(SnowflakeIdentifier.quote(schemaName), SnowflakeIdentifier.quote(tableName)),
                List.of("K"), List.of("V"), false, true);
        final RefreshResponseConnectionService jdbc = new RefreshResponseConnectionService();
        jdbc.sourceSignature(signature);
        jdbc.enqueue(response(FIELDS, control(2, FIRST_ID, signature, "FULL", 1L), data("INSERT", "a", "old")));
        jdbc.enqueue(response(FIELDS, control(2, SECOND_ID, signature, "INCREMENTAL", 0L)));
        try (final TableCacheStore store = new HeapTableCacheStore()) {
            refresh.refresh(jdbc, store, false);
            refresh.refresh(jdbc, store, true);
        }
        final RefreshResponseConnectionService.Call full = jdbc.calls().getFirst();
        final RefreshResponseConnectionService.Call delta = jdbc.calls().getLast();
        assertNull(full.parameters().get(1));
        assertNull(full.parameters().get(2));
        assertEquals(Map.of(1, FIRST_ID, 2, signature, 3, schemaName, 4, tableName, 5, signature), delta.parameters());
        assertFalse(delta.sql().contains(FIRST_ID));
        assertFalse(delta.sql().contains(signature));
        assertEquals(signature, full.parameters().get(5));
        assertEquals(2, jdbc.connectionCount(), "metadata and refresh must reuse one connection per refresh");
        assertEquals(2, jdbc.metadataCalls().size());
        for (final RefreshResponseConnectionService.Call metadata : jdbc.metadataCalls()) {
            assertEquals(Map.of(1, schemaName, 2, tableName), metadata.parameters());
            assertFalse(metadata.sql().contains(schemaName));
            assertFalse(metadata.sql().contains(tableName));
            assertTrue(metadata.sql().contains("WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"));
        }
        assertTrue(delta.sql().contains("CALL refresh_cache(?, ?, ?, ?, ?)"));
        assertTrue(procedureBody(delta.sql()).contains("\"s''); SELECT ''injected\".\"t\"\"; DROP TABLE victim; --\""));
        assertTrue(jdbc.resourcesClosed());
    }

    @ParameterizedTest
    @ValueSource(strings = {"HEAP", "EMBEDDED_KV"})
    void fullRefreshBindsPreviousSignatureButReloadDiscardsTheOldPair(final String mode) throws Exception {
        final String signature = "scope'; SELECT 'not-interpolated";
        final String replacementSignature = "replacement-scope";
        final AnonymousTableRefresh refresh = new AnonymousTableRefresh("DB.PUBLIC.REF", List.of("K"), List.of("V"), false, false);
        final RefreshResponseConnectionService jdbc = new RefreshResponseConnectionService();
        jdbc.sourceSignature(signature);
        jdbc.enqueue(response(FIELDS, control(2, FIRST_ID, signature, "FULL", 1L), data("INSERT", "a", "original")));
        jdbc.enqueueFailure(new SQLException("TABLE_CACHE_SOURCE_CHANGED", "P0001", -20001));
        jdbc.enqueueFailure(new SQLException("Replacement read unavailable", "08006"));
        jdbc.enqueue(response(FIELDS, control(2, SECOND_ID, replacementSignature, "FULL", 1L), data("INSERT", "b", "replacement")));
        jdbc.enqueue(response(FIELDS, control(2, SECOND_ID, replacementSignature, "FULL", 1L), data("INSERT", "b", "revalidated")));
        try (final TableCacheStore store = store(mode)) {
            refresh.refresh(jdbc, store, false);
            assertNull(jdbc.calls().getFirst().parameters().get(1));
            assertNull(jdbc.calls().getFirst().parameters().get(2));
            assertThrows(SQLException.class, () -> refresh.refresh(jdbc, store, false));
            final RefreshResponseConnectionService.Call validation = jdbc.calls().getLast();
            assertFalse(validation.incremental());
            assertNull(validation.parameters().get(1));
            assertEquals(signature, validation.parameters().get(2));
            assertFalse(validation.sql().contains(signature));
            assertEquals(new CacheCheckpoint(FIRST_ID, signature), store.checkpoint());
            assertEquals("original", store.get("a").orElseThrow().getAsString("V"));

            assertThrows(SQLException.class, () -> refresh.reload(jdbc, store));
            assertNull(jdbc.calls().getLast().parameters().get(1));
            assertNull(jdbc.calls().getLast().parameters().get(2));
            assertEquals(new CacheCheckpoint(FIRST_ID, signature), store.checkpoint());
            assertEquals(1L, store.size());
            assertEquals("original", store.get("a").orElseThrow().getAsString("V"));

            jdbc.sourceSignature(replacementSignature);
            assertFalse(refresh.reload(jdbc, store).incremental());
            assertNull(jdbc.calls().getLast().parameters().get(1));
            assertNull(jdbc.calls().getLast().parameters().get(2));
            assertTrue(store.get("a").isEmpty());
            assertEquals(new CacheCheckpoint(SECOND_ID, replacementSignature), store.checkpoint());
            assertEquals("replacement", store.get("b").orElseThrow().getAsString("V"));
            refresh.refresh(jdbc, store, false);
            assertNull(jdbc.calls().getLast().parameters().get(1));
            assertEquals(replacementSignature, jdbc.calls().getLast().parameters().get(2));
            assertEquals("revalidated", store.get("b").orElseThrow().getAsString("V"));
        }
        assertTrue(jdbc.resourcesClosed());
    }

    @Test
    void eachRequestUsesDistinctControlAliasesAndProjectsKeysOnce() throws Exception {
        final AnonymousTableRefresh refresh = new AnonymousTableRefresh("DB.PUBLIC.REF", List.of("K"), List.of("V", "K"), false, true);
        final RefreshResponseConnectionService jdbc = new RefreshResponseConnectionService();
        jdbc.enqueue(response(FIELDS, control(2, FIRST_ID, SIGNATURE, "FULL", 0L)));
        jdbc.enqueue(response(FIELDS, control(2, SECOND_ID, SIGNATURE, "FULL", 0L)));
        try (final TableCacheStore store = new HeapTableCacheStore()) {
            refresh.refresh(jdbc, store, false);
            refresh.refresh(jdbc, store, false);
        }
        final String firstPrefix = controlPrefix(jdbc.calls().getFirst().sql());
        final String secondPrefix = controlPrefix(jdbc.calls().getLast().sql());
        assertNotEquals(firstPrefix, secondPrefix);
        for (final RefreshResponseConnectionService.Call call : jdbc.calls()) {
            final String prefix = controlPrefix(call.sql());
            assertEquals(6, Set.of(prefix, prefix.concat("K"), prefix.concat("Q"), prefix.concat("S"), prefix.concat("M"), prefix.concat("N")).size());
            assertTrue(call.sql().contains("\"K\", \"V\" FROM"));
            assertFalse(call.sql().contains("\"K\", \"V\", \"K\""));
        }
    }

    static Stream<Arguments> modesAndControlPositions() {
        return Stream.of("HEAP", "EMBEDDED_KV").flatMap(mode -> Stream.of(false, true).flatMap(delta ->
                Stream.of(0, 1, 2).map(position -> Arguments.of(mode, delta, position))));
    }

    @ParameterizedTest
    @MethodSource("modesAndControlPositions")
    void acceptsControlBeforeBetweenOrAfterData(final String mode, final boolean delta, final int position) throws Exception {
        final Object[][] rows = {data("INSERT", "a", "first"), data("INSERT", "b", null), null};
        System.arraycopy(rows, position, rows, position + 1, 2 - position);
        rows[position] = control(2, SECOND_ID, SIGNATURE, delta ? "INCREMENTAL" : "FULL", 2L);
        final RefreshResponseConnectionService jdbc = new RefreshResponseConnectionService();
        if (delta) {
            jdbc.enqueue(response(FIELDS, control(2, FIRST_ID, SIGNATURE, "FULL", 0L)));
        }
        jdbc.enqueue(response(FIELDS, rows));
        try (final TableCacheStore store = store(mode)) {
            if (delta) {
                reader().refresh(jdbc, store, false);
            }
            final RefreshResult result = reader().refresh(jdbc, store, delta);
            assertEquals(2L, result.rowsUpserted());
            assertEquals(delta, result.incremental());
            assertEquals(2L, store.size());
            assertEquals("first", store.get("a").orElseThrow().getAsString("V"));
            assertNull(store.get("b").orElseThrow().getAsString("V"));
            assertEquals(new CacheCheckpoint(SECOND_ID, SIGNATURE), store.checkpoint());
            assertEquals(FIELDS, store.schema().getFieldNames());
        }
        assertTrue(jdbc.resourcesClosed());
    }

    @ParameterizedTest
    @ValueSource(strings = {"HEAP", "EMBEDDED_KV"})
    void zeroRowsStillPublishAValidatedCheckpoint(final String mode) throws Exception {
        final RefreshResponseConnectionService jdbc = new RefreshResponseConnectionService();
        jdbc.enqueue(response(FIELDS, control(2, FIRST_ID, SIGNATURE, "FULL", 0L)));
        jdbc.enqueue(response(FIELDS, control(2, SECOND_ID, SIGNATURE, "INCREMENTAL", 0L)));
        try (final TableCacheStore store = store(mode)) {
            reader().refresh(jdbc, store, false);
            assertTrue(store.isHydrated());
            assertEquals(0L, store.size());
            final RefreshResult delta = reader().refresh(jdbc, store, true);
            assertTrue(delta.incremental());
            assertEquals(new CacheCheckpoint(SECOND_ID, SIGNATURE), store.checkpoint());
        }
    }

    @Test
    void payloadNamesThatResembleControlsAreReadByPosition() throws Exception {
        final List<String> fields = List.of("K", "KIND", "QUERY_ID", "ACTION");
        final RefreshResponseConnectionService jdbc = new RefreshResponseConnectionService();
        jdbc.enqueue(response(fields, control(4, FIRST_ID, SIGNATURE, "FULL", 1L), data("INSERT", "a", "value-kind", "value-id", "value-action")));
        try (final TableCacheStore store = new HeapTableCacheStore()) {
            new AnonymousTableRefresh("DB.PUBLIC.REF", List.of("K"), List.of(), true, false).refresh(jdbc, store, false);
            final Record record = store.get("a").orElseThrow();
            assertEquals(fields, record.getSchema().getFieldNames());
            assertEquals("value-kind", record.getAsString("KIND"));
            assertEquals("value-id", record.getAsString("QUERY_ID"));
            assertEquals("value-action", record.getAsString("ACTION"));
        }
    }

    static Stream<Arguments> malformedResponses() {
        return Stream.of("HEAP", "EMBEDDED_KV").flatMap(mode -> Stream.of(false, true).flatMap(delta ->
                Stream.of("missingControl", "duplicateControl", "wrongMode", "badQueryId", "nullQueryId", "blankSignature", "nullSignature",
                        "negativeCount", "nullCount", "tooFewRows", "tooManyRows", "truncated", "unknownKind", "nullKind", "unknownAction",
                        "nullAction", "nullKey", "duplicateKeys", "missingKeyColumn", "duplicateColumnLabels", "noPayloadColumns",
                        "fractionalCount", "overflowCount", "controlHasAction", "controlHasPayload", "dataHasMetadata", "wrongProjection")
                        .map(fault -> Arguments.of(mode, delta, fault))));
    }

    @ParameterizedTest(name = "{0}, delta={1}, {2}")
    @MethodSource("malformedResponses")
    void rejectsMalformedResponseWithoutPublishingAnything(final String mode, final boolean delta, final String fault) throws Exception {
        final RefreshResponseConnectionService jdbc = new RefreshResponseConnectionService();
        jdbc.enqueue(response(FIELDS, control(2, FIRST_ID, SIGNATURE, "FULL", 1L), data("INSERT", "a", "original")));
        jdbc.enqueue(malformed(fault, delta));
        try (final TableCacheStore store = store(mode)) {
            reader().refresh(jdbc, store, false);
            if ("duplicateKeys".equals(fault) && !delta) {
                assertThrows(IllegalArgumentException.class, () -> reader().refresh(jdbc, store, false));
            } else {
                assertThrows(SQLException.class, () -> reader().refresh(jdbc, store, delta));
            }
            assertEquals(new CacheCheckpoint(FIRST_ID, SIGNATURE), store.checkpoint());
            assertEquals(1L, store.size());
            assertEquals("original", store.get("a").orElseThrow().getAsString("V"));
            assertTrue(store.get("b").isEmpty());
            assertEquals(FIELDS, store.schema().getFieldNames());
        }
        assertTrue(jdbc.resourcesClosed(), "failure must close result set, statement and connection");
    }

    @Test
    void deltaRejectsPayloadFieldReorderingAndFullRejectsDeletes() throws Exception {
        final RefreshResponseConnectionService jdbc = new RefreshResponseConnectionService();
        jdbc.enqueue(response(FIELDS, control(2, FIRST_ID, SIGNATURE, "FULL", 1L), data("INSERT", "a", "original")));
        jdbc.enqueue(response(List.of("V", "K"), control(2, SECOND_ID, SIGNATURE, "INCREMENTAL", 1L), data("INSERT", "new", "a")));
        jdbc.enqueue(response(FIELDS, control(2, SECOND_ID, SIGNATURE, "FULL", 1L), data("DELETE", "a", "original")));
        try (final TableCacheStore store = new HeapTableCacheStore()) {
            reader().refresh(jdbc, store, false);
            assertThrows(SQLException.class, () -> reader().refresh(jdbc, store, true));
            assertThrows(SQLException.class, () -> reader().refresh(jdbc, store, false));
            assertEquals(new CacheCheckpoint(FIRST_ID, SIGNATURE), store.checkpoint());
            assertEquals("original", store.get("a").orElseThrow().getAsString("V"));
        }
        assertTrue(jdbc.resourcesClosed());
    }

    static Stream<Arguments> closeFailures() {
        return Stream.of("HEAP", "EMBEDDED_KV").flatMap(mode -> Stream.of(false, true).flatMap(delta ->
                Stream.of("resultSet", "statement", "connection", "metadataResultSet", "metadataStatement", "sessionResultSet", "sessionStatement")
                        .map(resource -> Arguments.of(mode, delta, resource))));
    }

    @ParameterizedTest
    @MethodSource("closeFailures")
    void jdbcCloseFailureMustNotPublishRowsOrCheckpoint(final String mode, final boolean delta, final String resource) throws Exception {
        final RefreshResponseConnectionService jdbc = new RefreshResponseConnectionService();
        jdbc.enqueue(response(FIELDS, control(2, FIRST_ID, SIGNATURE, "FULL", 1L), data("INSERT", "a", "original")));
        jdbc.enqueue(response(FIELDS, control(2, SECOND_ID, SIGNATURE, delta ? "INCREMENTAL" : "FULL", 1L), data("INSERT", "a", "uncommitted")));
        try (final TableCacheStore store = store(mode)) {
            reader().refresh(jdbc, store, false);
            jdbc.failOnClose(resource);
            assertThrows(SQLException.class, () -> reader().refresh(jdbc, store, delta));
            assertEquals("original", store.get("a").orElseThrow().getAsString("V"));
            assertEquals(new CacheCheckpoint(FIRST_ID, SIGNATURE), store.checkpoint());
            assertEquals(1L, store.size());
        }
        assertTrue(jdbc.resourcesClosed());
    }

    @Test
    void metadataSignatureDoesNotDependOnQueuedRefreshResponses() throws Exception {
        final RefreshResponseConnectionService jdbc = new RefreshResponseConnectionService();
        jdbc.enqueue(response(FIELDS, control(2, SECOND_ID, "replacement", "FULL", 0L)));
        try (final Connection connection = jdbc.getConnection()) {
            assertEquals(SIGNATURE, SourceMetadata.read(connection, List.of("DB", "PUBLIC", "REF"), true).signature());
            jdbc.sourceSignature("explicit-source");
            assertEquals("explicit-source", SourceMetadata.read(connection, List.of("DB", "PUBLIC", "REF"), true).signature());
        }
        assertEquals(1, jdbc.remainingSteps());
        assertTrue(jdbc.calls().isEmpty());
        assertTrue(jdbc.resourcesClosed());
    }

    static Stream<Arguments> invalidSessionParameters() {
        return Stream.of(false, true).flatMap(requested -> Stream.of("true", "TRUE", "", "invalid", "0", "null", "missing", "wrongKey", "duplicate")
                .map(fault -> Arguments.of(requested, fault)));
    }

    @ParameterizedTest
    @MethodSource("invalidSessionParameters")
    void rejectsUnsafeOrUnverifiableIdentifierSettingsBeforeReadingSource(final boolean requested, final String fault) throws Exception {
        final RefreshResponseConnectionService jdbc = new RefreshResponseConnectionService();
        jdbc.enqueue(response(FIELDS, control(2, FIRST_ID, SIGNATURE, "FULL", 1L), data("INSERT", "a", "unpublished")));
        switch (fault) {
            case "missing" -> jdbc.sessionParameterRows();
            case "wrongKey" -> jdbc.sessionParameterRows(new Object[]{"UNRELATED_PARAMETER", "false"});
            case "duplicate" -> jdbc.sessionParameterRows(new Object[]{"QUOTED_IDENTIFIERS_IGNORE_CASE", "false"},
                    new Object[]{"QUOTED_IDENTIFIERS_IGNORE_CASE", "false"});
            default -> jdbc.sessionParameterValue("null".equals(fault) ? null : fault);
        }
        final AnonymousTableRefresh refresh = new AnonymousTableRefresh("DB.PUBLIC.REF", List.of("K"), List.of("V"), false, requested);
        try (final TableCacheStore store = new HeapTableCacheStore()) {
            final SQLException error = assertThrows(SQLException.class, () -> refresh.refresh(jdbc, store, false));
            assertTrue(error.getMessage().contains("QUOTED_IDENTIFIERS_IGNORE_CASE"));
            assertFalse(store.isHydrated());
            assertNull(store.checkpoint());
        }
        assertEquals(1, jdbc.sessionCalls().size());
        assertTrue(jdbc.metadataCalls().isEmpty(), "session qualification must precede source and capability queries");
        assertTrue(jdbc.calls().isEmpty());
        assertEquals(1, jdbc.remainingSteps());
        assertTrue(jdbc.resourcesClosed());
    }

    @ParameterizedTest
    @ValueSource(strings = {"false", "FALSE"})
    void sessionGuardMatchesTheParameterKeyNotAnUnrelatedRow(final String value) throws Exception {
        final RefreshResponseConnectionService jdbc = new RefreshResponseConnectionService();
        jdbc.sessionParameterRows(new Object[]{"UNRELATED_PARAMETER", "true"}, new Object[]{"QUOTED_IDENTIFIERS_IGNORE_CASE", value});
        try (final Connection connection = jdbc.getConnection()) {
            assertEquals(new SourceMetadata(SIGNATURE, true), SourceMetadata.read(connection, List.of("DB", "PUBLIC", "REF"), true));
        }
        assertEquals(List.of(new RefreshResponseConnectionService.Call("SHOW PARAMETERS LIKE 'QUOTED_IDENTIFIERS_IGNORE_CASE' IN SESSION", Map.of())),
                jdbc.sessionCalls());
        assertTrue(jdbc.resourcesClosed());
    }

    static Stream<Arguments> sourceCapabilities() {
        return Stream.of(false, true).flatMap(requested -> Stream.of("standard", "transient", "managedIceberg", "dynamicIncremental", "view", "hybrid", "dynamicFull")
                .map(type -> Arguments.of(type, requested)));
    }

    @ParameterizedTest
    @MethodSource("sourceCapabilities")
    void metadataSelectsOnlyQualifiedRequestedIncrementalSources(final String type, final boolean requested) throws Exception {
        final RefreshResponseConnectionService jdbc = new RefreshResponseConnectionService();
        jdbc.sourceType(type);
        final boolean incremental = requested && Set.of("standard", "transient", "managedIceberg", "dynamicIncremental").contains(type);
        jdbc.enqueue(response(FIELDS, control(2, FIRST_ID, SIGNATURE, "FULL", 1L), data("INSERT", "a", "old")));
        jdbc.enqueue(response(FIELDS, control(2, SECOND_ID, SIGNATURE, incremental ? "INCREMENTAL" : "FULL", 1L), data("INSERT", "b", "new")));
        final AnonymousTableRefresh refresh = new AnonymousTableRefresh("DB.PUBLIC.REF", List.of("K"), List.of("V"), false, requested);
        try (final TableCacheStore store = new HeapTableCacheStore()) {
            refresh.refresh(jdbc, store, false);
            assertEquals(incremental, refresh.refresh(jdbc, store, true).incremental());
            assertEquals(incremental ? 2L : 1L, store.size());
            assertEquals(new CacheCheckpoint(SECOND_ID, SIGNATURE), store.checkpoint());
        }
        assertEquals(2, jdbc.calls().size(), "metadata calls must not consume anonymous refresh steps");
        assertEquals(0, jdbc.remainingSteps());
        assertEquals(2, jdbc.connectionCount(), "preflight, optional SHOW and refresh share one connection");
        final boolean needsShow = Set.of("managedIceberg", "dynamicIncremental", "dynamicFull").contains(type);
        assertEquals(needsShow ? 4 : 2, jdbc.metadataCalls().size(), "one capability SHOW per refresh when required");
        assertEquals(2, jdbc.sessionCalls().size());
        final RefreshResponseConnectionService.Call call = jdbc.calls().getLast();
        assertEquals(incremental, call.incremental());
        assertEquals(incremental ? FIRST_ID : null, call.parameters().get(1));
        assertEquals(SIGNATURE, call.parameters().get(2));
        assertEquals(SIGNATURE, call.parameters().get(5));
        final String body = procedureBody(call.sql());
        assertEquals(incremental, body.contains("CHANGES(INFORMATION => DEFAULT)"));
        assertEquals(incremental, body.contains("RESULT_SCAN"));
        assertEquals(!incremental, body.contains("FULL OUTER JOIN"));
        assertTrue(jdbc.resourcesClosed());
    }

    static Stream<Arguments> rejectedSources() {
        return Stream.of(false, true).flatMap(requested -> Stream.of("temporary", "invalid", "external", "missing", "ambiguous",
                "dynamicIceberg", "externalIceberg", "dynamicAuto").map(type -> Arguments.of(type, requested)));
    }

    @ParameterizedTest
    @MethodSource("rejectedSources")
    void rejectsUnqualifiedMetadataWithoutReadingOrPublishing(final String type, final boolean requested) throws Exception {
        final RefreshResponseConnectionService jdbc = new RefreshResponseConnectionService();
        jdbc.enqueue(response(FIELDS, control(2, FIRST_ID, SIGNATURE, "FULL", 1L), data("INSERT", "a", "original")));
        jdbc.enqueue(response(FIELDS, control(2, SECOND_ID, SIGNATURE, "FULL", 1L), data("INSERT", "b", "uncommitted")));
        final AnonymousTableRefresh refresh = new AnonymousTableRefresh("DB.PUBLIC.REF", List.of("K"), List.of("V"), false, requested);
        try (final TableCacheStore store = new HeapTableCacheStore()) {
            refresh.refresh(jdbc, store, false);
            jdbc.sourceType(type);
            final SQLException error = assertThrows(SQLException.class, () -> refresh.refresh(jdbc, store, true));
            assertEquals(-20002, error.getErrorCode());
            assertTrue(error.getMessage().contains("TABLE_CACHE_INVALID_SOURCE"));
            assertEquals(new CacheCheckpoint(FIRST_ID, SIGNATURE), store.checkpoint());
            assertEquals(1L, store.size());
            assertEquals("original", store.get("a").orElseThrow().getAsString("V"));
            assertTrue(store.get("b").isEmpty());
            assertEquals(1, jdbc.calls().size());
            assertEquals(1, jdbc.remainingSteps());
        }
        assertTrue(jdbc.resourcesClosed());
    }

    @ParameterizedTest
    @MethodSource("sourceCapabilities")
    void retainedHistoryIsRequiredOnlyForRequestedIncrementalCapability(final String type, final boolean requested) throws Exception {
        final RefreshResponseConnectionService jdbc = new RefreshResponseConnectionService();
        jdbc.sourceType(type);
        jdbc.metadataRows(new Object[]{"view".equals(type) ? "VIEW" : "BASE TABLE", "NO", "managedIceberg".equals(type) ? "YES" : "NO",
                type.startsWith("dynamic") ? "YES" : "NO", "hybrid".equals(type) ? "YES" : "NO", 0L, SIGNATURE});
        try (final Connection connection = jdbc.getConnection()) {
            if (requested && Set.of("standard", "transient", "managedIceberg", "dynamicIncremental").contains(type)) {
                final SQLException error = assertThrows(SQLException.class, () -> SourceMetadata.read(connection, List.of("DB", "PUBLIC", "REF"), true));
                assertEquals(-20002, error.getErrorCode());
                assertTrue(error.getMessage().contains("retained history"));
            } else {
                assertEquals(new SourceMetadata(SIGNATURE, false), SourceMetadata.read(connection, List.of("DB", "PUBLIC", "REF"), requested));
            }
        }
        assertTrue(jdbc.calls().isEmpty());
        assertTrue(jdbc.resourcesClosed());
    }

    static Stream<Arguments> incompleteMetadata() {
        return Stream.concat(Stream.of(0, 1, 2, 3, 4).flatMap(column -> Stream.of(Arguments.of(column, null), Arguments.of(column, "invalid"))),
                Stream.of(Arguments.of(6, null)));
    }

    @ParameterizedTest
    @MethodSource("incompleteMetadata")
    void rejectsMissingOrInvalidSourceTypeFields(final int column, final String value) throws Exception {
        final RefreshResponseConnectionService jdbc = new RefreshResponseConnectionService();
        final Object[] row = {"BASE TABLE", "NO", "NO", "NO", "NO", 1L, SIGNATURE};
        row[column] = value;
        jdbc.metadataRows(row);
        try (final Connection connection = jdbc.getConnection()) {
            final SQLException error = assertThrows(SQLException.class, () -> SourceMetadata.read(connection, List.of("DB", "PUBLIC", "REF"), false));
            assertEquals(-20002, error.getErrorCode());
        }
        assertTrue(jdbc.calls().isEmpty());
        assertTrue(jdbc.resourcesClosed());
    }

    static Stream<Arguments> invalidCapabilityRows() {
        return Stream.of("dynamicIncremental", "managedIceberg").flatMap(type -> Stream.of("missing", "ambiguous", "nullProperty", "wrongDatabase", "wrongSchema", "wrongName")
                .map(fault -> Arguments.of(type, fault)));
    }

    @ParameterizedTest
    @MethodSource("invalidCapabilityRows")
    void rejectsMissingOrAmbiguousShowMetadata(final String type, final String fault) throws Exception {
        final RefreshResponseConnectionService jdbc = new RefreshResponseConnectionService();
        jdbc.sourceType(type);
        final Object[] row = {"DB", "PUBLIC", "REF", "INCREMENTAL", "SNOWFLAKE", "2"};
        switch (fault) {
            case "missing" -> jdbc.capabilityRows();
            case "ambiguous" -> jdbc.capabilityRows(row, row);
            case "nullProperty" -> {
                row["managedIceberg".equals(type) ? 4 : 3] = null;
                jdbc.capabilityRows(row);
            }
            default -> {
                final int column = switch (fault) {
                    case "wrongDatabase" -> 0;
                    case "wrongSchema" -> 1;
                    case "wrongName" -> 2;
                    default -> throw new IllegalArgumentException(fault);
                };
                row[column] = "OTHER";
                jdbc.capabilityRows(row);
            }
        }
        try (final Connection connection = jdbc.getConnection()) {
            final SQLException error = assertThrows(SQLException.class, () -> SourceMetadata.read(connection, List.of("DB", "PUBLIC", "REF"), true));
            assertEquals(-20002, error.getErrorCode());
        }
        assertEquals(2, jdbc.metadataCalls().size());
        assertTrue(jdbc.calls().isEmpty());
        assertTrue(jdbc.resourcesClosed());
    }

    @ParameterizedTest
    @ValueSource(strings = {"dynamicIncremental", "managedIceberg"})
    void showUsesLiteralPrefixAndExactCaseSensitiveObjectMatch(final String type) throws Exception {
        final RefreshResponseConnectionService jdbc = new RefreshResponseConnectionService();
        jdbc.sourceType(type);
        final String name = "t_%'\\suffix";
        jdbc.capabilityRows(new Object[]{"Db", "s\"chema", name, "INCREMENTAL", "SNOWFLAKE", "3"});
        try (final Connection connection = jdbc.getConnection()) {
            assertTrue(SourceMetadata.read(connection, List.of("Db", "s\"chema", name), true).incremental());
        }
        final String show = jdbc.metadataCalls().getLast().sql();
        assertEquals("SHOW %s TABLES IN SCHEMA \"Db\".\"s\"\"chema\" STARTS WITH 't_%%''\\\\suffix' LIMIT 1"
                .formatted("managedIceberg".equals(type) ? "ICEBERG" : "DYNAMIC"), show);
        assertFalse(show.contains(" LIKE "));
        assertEquals(Map.of(1, "s\"chema", 2, name), jdbc.metadataCalls().getFirst().parameters());
        assertTrue(jdbc.resourcesClosed());
    }

    static Stream<Arguments> nonMatchingPrefixes() {
        return Stream.of("dynamicIncremental", "managedIceberg").flatMap(type -> Stream.of("t_%'\\suffix-longer", "T_%'\\suffix", "txx'\\suffix")
                .map(name -> Arguments.of(type, name)));
    }

    @ParameterizedTest
    @MethodSource("nonMatchingPrefixes")
    void showPrefixCandidateMustMatchTheWholeLiteralName(final String type, final String candidate) throws Exception {
        final RefreshResponseConnectionService jdbc = new RefreshResponseConnectionService();
        jdbc.sourceType(type);
        jdbc.capabilityRows(new Object[]{"DB", "PUBLIC", candidate, "INCREMENTAL", "SNOWFLAKE", "3"});
        try (final Connection connection = jdbc.getConnection()) {
            final SQLException error = assertThrows(SQLException.class,
                    () -> SourceMetadata.read(connection, List.of("DB", "PUBLIC", "t_%'\\suffix"), true));
            assertEquals(-20002, error.getErrorCode());
        }
        assertEquals(2, jdbc.metadataCalls().size());
        assertTrue(jdbc.calls().isEmpty());
        assertTrue(jdbc.resourcesClosed());
    }

    static Stream<Arguments> icebergVersions() {
        return Stream.of(false, true).flatMap(requested -> Stream.of("1", "2", "3", "4", "", null).map(version -> Arguments.of(version, requested)));
    }

    @ParameterizedTest
    @MethodSource("icebergVersions")
    void onlyQualifiedManagedIcebergVersionsPassPreflight(final String version, final boolean requested) throws Exception {
        final RefreshResponseConnectionService jdbc = new RefreshResponseConnectionService();
        jdbc.sourceType("managedIceberg");
        jdbc.capabilityRows(new Object[]{"DB", "PUBLIC", "REF", null, "SNOWFLAKE", version});
        try (final Connection connection = jdbc.getConnection()) {
            if ("2".equals(version) || "3".equals(version)) {
                assertEquals(requested, SourceMetadata.read(connection, List.of("DB", "PUBLIC", "REF"), requested).incremental());
            } else {
                assertEquals(-20002, assertThrows(SQLException.class,
                        () -> SourceMetadata.read(connection, List.of("DB", "PUBLIC", "REF"), requested)).getErrorCode());
            }
        }
        assertEquals(2, jdbc.metadataCalls().size(), "catalog and version must come from the same SHOW response");
        assertEquals(1, jdbc.sessionCalls().size());
        assertTrue(jdbc.calls().isEmpty());
        assertTrue(jdbc.resourcesClosed());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void metadataSqlFailuresPropagateWithoutCapabilityFallback(final boolean showFailure) throws Exception {
        final RefreshResponseConnectionService jdbc = new RefreshResponseConnectionService();
        final SQLException failure = new SQLException("metadata unavailable", "08006");
        if (showFailure) {
            jdbc.sourceType("managedIceberg");
            jdbc.capabilityFailure(failure);
        } else {
            jdbc.metadataFailure(failure);
        }
        try (final TableCacheStore store = new HeapTableCacheStore()) {
            assertEquals(failure, assertThrows(SQLException.class, () -> reader().refresh(jdbc, store, false)));
            assertFalse(store.isHydrated());
        }
        assertTrue(jdbc.calls().isEmpty());
        assertTrue(jdbc.resourcesClosed());
    }

    @ParameterizedTest
    @ValueSource(strings = {"HEAP", "EMBEDDED_KV"})
    void preflightSignatureIsBoundAndServerMismatchLeavesPublicationIntact(final String mode) throws Exception {
        final RefreshResponseConnectionService jdbc = new RefreshResponseConnectionService();
        jdbc.enqueue(response(FIELDS, control(2, FIRST_ID, SIGNATURE, "FULL", 1L), data("INSERT", "a", "original")));
        // The fixture scripts the server guard error; generated SQL assertions separately check the guard.
        jdbc.enqueueFailure(new SQLException("TABLE_CACHE_SOURCE_CHANGED", "P0001", -20001));
        try (final TableCacheStore store = store(mode)) {
            reader().refresh(jdbc, store, false);
            jdbc.sourceSignature(SIGNATURE);
            assertEquals(-20001, assertThrows(SQLException.class, () -> reader().refresh(jdbc, store, true)).getErrorCode());
            assertEquals(2, jdbc.calls().size(), "matching preflight must allow the scripted server guard to run");
            assertEquals(0, jdbc.remainingSteps());
            assertEquals(SIGNATURE, jdbc.calls().getLast().parameters().get(2));
            assertEquals(SIGNATURE, jdbc.calls().getLast().parameters().get(5));
            assertEquals(new CacheCheckpoint(FIRST_ID, SIGNATURE), store.checkpoint());
            assertEquals("original", store.get("a").orElseThrow().getAsString("V"));
            assertEquals(1L, store.size());
        }
        assertTrue(jdbc.resourcesClosed());
    }

    static Stream<Arguments> changedSignatureCapabilities() {
        return Stream.of(false, true).flatMap(requested -> Stream.of("dynamicIncremental", "managedIceberg").map(type -> Arguments.of(type, requested)));
    }

    @ParameterizedTest
    @MethodSource("changedSignatureCapabilities")
    void changedPreflightSignatureIsRejectedBeforeAnyCapabilityQuery(final String type, final boolean requested) throws Exception {
        final RefreshResponseConnectionService jdbc = new RefreshResponseConnectionService();
        jdbc.sourceType(type);
        jdbc.sourceSignature("changed-signature");
        jdbc.capabilityFailure(new SQLException("Capability metadata unavailable", "08006"));
        try (final Connection connection = jdbc.getConnection()) {
            final SQLException error = assertThrows(SQLException.class,
                    () -> SourceMetadata.read(connection, List.of("DB", "PUBLIC", "REF"), requested, new CacheCheckpoint(FIRST_ID, SIGNATURE)));
            assertEquals(-20001, error.getErrorCode());
            assertTrue(error.getMessage().contains("TABLE_CACHE_SOURCE_CHANGED"));
        }
        assertEquals(1, jdbc.metadataCalls().size(), "signature change must fail before the unavailable SHOW");
        assertTrue(jdbc.metadataCalls().getFirst().sql().startsWith("SELECT TABLE_TYPE,"));
        assertTrue(jdbc.calls().isEmpty());
        assertTrue(jdbc.resourcesClosed());
    }

    @ParameterizedTest
    @ValueSource(strings = {"HEAP", "EMBEDDED_KV"})
    void changedPreflightPreservesTheQueuedReplacementForExplicitReload(final String mode) throws Exception {
        final RefreshResponseConnectionService jdbc = new RefreshResponseConnectionService();
        jdbc.enqueue(response(FIELDS, control(2, FIRST_ID, SIGNATURE, "FULL", 1L), data("INSERT", "a", "original")));
        jdbc.enqueue(response(FIELDS, control(2, SECOND_ID, "changed-signature", "FULL", 1L), data("INSERT", "b", "replacement")));
        try (final TableCacheStore store = store(mode)) {
            reader().refresh(jdbc, store, false);
            jdbc.sourceSignature("changed-signature");
            assertEquals(-20001, assertThrows(SQLException.class, () -> reader().refresh(jdbc, store, true)).getErrorCode());
            assertEquals(1, jdbc.calls().size());
            assertEquals(1, jdbc.remainingSteps());
            assertEquals(new CacheCheckpoint(FIRST_ID, SIGNATURE), store.checkpoint());
            assertEquals("original", store.get("a").orElseThrow().getAsString("V"));
            assertFalse(reader().reload(jdbc, store).incremental());
            assertEquals(2, jdbc.calls().size());
            assertEquals(0, jdbc.remainingSteps());
            assertNull(jdbc.calls().getLast().parameters().get(2));
            assertEquals("changed-signature", jdbc.calls().getLast().parameters().get(5));
            assertEquals(new CacheCheckpoint(SECOND_ID, "changed-signature"), store.checkpoint());
            assertTrue(store.get("a").isEmpty());
            assertEquals("replacement", store.get("b").orElseThrow().getAsString("V"));
        }
        assertTrue(jdbc.resourcesClosed());
    }

    private static RefreshResponseConnectionService.Response malformed(final String fault, final boolean delta) {
        final String mode = delta ? "INCREMENTAL" : "FULL";
        final Object[] marker = control(2, SECOND_ID, SIGNATURE, mode, 1L);
        final Object[] row = data("INSERT", "b", "uncommitted");
        switch (fault) {
            case "missingControl":
                return response(FIELDS, row);
            case "duplicateControl":
                return response(FIELDS, marker, row, marker);
            case "wrongMode":
                marker[3] = delta ? "FULL" : "INCREMENTAL";
                break;
            case "badQueryId":
                marker[1] = "not-a-query-id";
                break;
            case "nullQueryId":
                marker[1] = null;
                break;
            case "blankSignature":
                marker[2] = " ";
                break;
            case "nullSignature":
                marker[2] = null;
                break;
            case "negativeCount":
                marker[4] = -1L;
                break;
            case "nullCount":
                marker[4] = null;
                break;
            case "tooFewRows":
                marker[4] = 2L;
                break;
            case "tooManyRows":
                marker[4] = 0L;
                break;
            case "truncated":
                return truncated(response(FIELDS, marker, row), 2);
            case "unknownKind":
                row[0] = "OTHER";
                break;
            case "nullKind":
                row[0] = null;
                break;
            case "unknownAction":
                row[5] = "UPDATE";
                break;
            case "nullAction":
                row[5] = null;
                break;
            case "nullKey":
                row[6] = null;
                break;
            case "duplicateKeys":
                marker[4] = 2L;
                return response(FIELDS, marker, row, data("INSERT", "b", "duplicate"));
            case "missingKeyColumn":
                return response(List.of("OTHER", "V"), marker, row);
            case "duplicateColumnLabels":
                return response(List.of("K", "K"), marker, row);
            case "fractionalCount":
                marker[4] = new BigDecimal("1.5");
                break;
            case "overflowCount":
                marker[4] = new BigDecimal("9223372036854775808");
                break;
            case "controlHasAction":
                marker[5] = "INSERT";
                break;
            case "controlHasPayload":
                marker[6] = "payload";
                break;
            case "dataHasMetadata":
                row[1] = SECOND_ID;
                break;
            case "wrongProjection":
                return response(List.of("K", "OTHER"), marker, row);
            case "noPayloadColumns":
                return response(List.of(), control(0, SECOND_ID, SIGNATURE, mode, 0L));
            default:
                throw new IllegalArgumentException("Unknown fault %s".formatted(fault));
        }
        return response(FIELDS, marker, row);
    }

    private TableCacheStore store(final String mode) throws Exception {
        return "HEAP".equals(mode) ? new HeapTableCacheStore() : new RocksDbTableCacheStore(storageDirectory);
    }

    private static AnonymousTableRefresh reader() {
        return new AnonymousTableRefresh("DB.PUBLIC.REF", List.of("K"), List.of("V"), false, true);
    }

    @Test
    void dollarDelimitersAndBackslashesStayInsideQuotedProcedureBody() {
        final AnonymousTableRefresh refresh = new AnonymousTableRefresh("\"db$$x\".PUBLIC.\"ref$$; --\"",
                List.of("K"), List.of("v\\path$$"), false, true);
        final String sql = refresh.sql(List.of("K", "v\\path$$"), false, "TCFIXTURE");
        assertFalse(sql.contains("LANGUAGE SQL AS $$"));
        final String body = procedureBody(sql);
        assertTrue(body.contains("FROM \"db$$x\".INFORMATION_SCHEMA.TABLES"));
        assertTrue(body.contains("\"ref$$; --\""));
        assertTrue(body.contains("\"v\\\\path$$\""));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "prefix'", "prefix$$", "1prefix", "prefix; DROP TABLE ref", "prefix_with_separator"})
    void internalAliasPrefixCannotInjectSql(final String prefix) {
        assertThrows(IllegalArgumentException.class, () -> reader().sql(FIELDS, false, prefix));
    }

    private static String procedureBody(final String sql) {
        final String marker = "LANGUAGE SQL AS ";
        final int start = sql.indexOf(marker) + marker.length();
        assertTrue(start >= marker.length(), "anonymous call must declare a SQL body");
        assertEquals('\'', sql.charAt(start), "body must be single quoted, not dollar delimited");
        final StringBuilder body = new StringBuilder();
        int index = start + 1;
        while (index < sql.length()) {
            final char character = sql.charAt(index++);
            if (character == '\\') {
                assertTrue(index < sql.length(), "truncated escape");
                body.append(sql.charAt(index++));
            } else if (character == '\'') {
                if (index < sql.length() && sql.charAt(index) == '\'') {
                    body.append('\'');
                    index++;
                } else {
                    assertEquals("CALL refresh_cache(?, ?, ?, ?, ?)", sql.substring(index).trim(),
                            "nothing except the bound call may escape the quoted body");
                    return body.toString();
                }
            } else {
                body.append(character);
            }
        }
        throw new AssertionError("Unterminated procedure body");
    }

    private static String controlPrefix(final String sql) {
        final Matcher matcher = Pattern.compile("AS (TC[0-9A-F]{32})K").matcher(sql);
        assertTrue(matcher.find(), "control aliases must have a per-call prefix");
        return matcher.group(1);
    }
}
