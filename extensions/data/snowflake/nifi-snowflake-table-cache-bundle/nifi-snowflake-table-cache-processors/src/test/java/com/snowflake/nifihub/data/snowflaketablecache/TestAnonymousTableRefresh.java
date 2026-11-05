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
        assertTrue(sql.matches("(?s).*data_rows := \\(EXECUTE IMMEDIATE :data_sql\\);\\s*data_id := SQLID;\\s*boundary_id := data_id;.*"));
        assertTrue(sql.contains("RETURN TABLE(response_rows)"));
        assertTrue(sql.contains("USING (boundary_id, signature_value, mode, data_id, data_id, data_id)"));
        assertTrue(sql.contains("''CONTROL'' AS TCFIXTUREK"));
        assertTrue(sql.contains("''DATA'', NULL::VARCHAR"));
        assertTrue(sql.contains("LEFT JOIN TABLE(RESULT_SCAN(?)) d ON FALSE"));
        assertTrue(sql.contains("(SELECT COUNT(*) FROM TABLE(RESULT_SCAN(?))) AS TCFIXTUREN"));
        assertFalse(sql.contains("CURRENT_STATEMENT()"));
        assertFalse(sql.contains("LAST_QUERY_ID()"));
        assertFalse(sql.contains("CHANGES(INFORMATION"));
    }

    @Test
    void callerOwnedTransactionIsRejectedWithoutRunningAQuery() throws Exception {
        final RefreshResponseConnectionService connection = new RefreshResponseConnectionService();
        connection.autoCommit(false);
        try (TableCacheStore store = new HeapTableCacheStore()) {
            assertThrows(SQLException.class, () -> reader().refresh(connection, store, false));
            assertTrue(connection.calls().isEmpty());
            assertTrue(connection.resourcesClosed());
            assertFalse(store.isHydrated());
        }
    }

    @Test
    void deltaHasExplicitStatementEndAndBindsBothBoundaries() {
        final String sql = procedureBody(reader().sql(FIELDS, true, "TCFIXTURE"));
        assertTrue(sql.contains("CHANGES(INFORMATION => DEFAULT) AT(STATEMENT => ?) END(STATEMENT => ?)"));
        assertTrue(sql.contains("METADATA$ACTION AS \"TCFIXTURE\""));
        assertTrue(sql.matches("(?s).*SELECT CURRENT_TIMESTAMP\\(\\) INTO :marker_time;\\s*boundary_id := SQLID;.*"));
        assertTrue(sql.contains("EXECUTE IMMEDIATE :data_sql USING (previous_id, boundary_id)"));
        assertTrue(sql.contains("mode VARCHAR DEFAULT 'INCREMENTAL'"));
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
        assertTrue(sql.contains("TABLE_TYPE = 'BASE TABLE' AND IS_TEMPORARY = 'NO'"));
        assertTrue(sql.contains("IS_ICEBERG = 'NO' AND IS_DYNAMIC = 'NO' AND IS_HYBRID = 'NO'"));
        assertTrue(sql.contains("RETENTION_TIME >= 1"));
        assertTrue(sql.contains("IF (previous_signature IS NOT NULL AND previous_signature <> signature_before) THEN"));
        assertFalse(sql.contains("previous_id IS NOT NULL"), "full loads must check the saved signature without a query boundary");
        assertTrue(sql.contains("signature_value <> signature_before"));
        final Matcher sourceQueries = Pattern.compile("SELECT COUNT\\(\\*\\), MAX\\(IFF\\(.*?FROM \\\"DB\\\"\\.INFORMATION_SCHEMA\\.TABLES\\s+WHERE ([^;]+);",
                Pattern.DOTALL).matcher(sql);
        int sourceQueryCount = 0;
        while (sourceQueries.find()) {
            assertEquals("TABLE_SCHEMA = :source_schema AND TABLE_NAME = :source_name", sourceQueries.group(1).trim(),
                    "object count must include unsupported same-name objects, including temporary shadows");
            assertTrue(sourceQueries.group().contains("IFF(TABLE_TYPE = 'BASE TABLE' AND IS_TEMPORARY = 'NO'"));
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
        assertEquals(Map.of(1, FIRST_ID, 2, signature, 3, schemaName, 4, tableName), delta.parameters());
        assertFalse(delta.sql().contains(FIRST_ID));
        assertFalse(delta.sql().contains(signature));
        assertTrue(delta.sql().contains("CALL refresh_cache(?, ?, ?, ?)"));
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
                Stream.of("resultSet", "statement", "connection").map(resource -> Arguments.of(mode, delta, resource))));
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
                    assertEquals("CALL refresh_cache(?, ?, ?, ?)", sql.substring(index).trim(),
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
