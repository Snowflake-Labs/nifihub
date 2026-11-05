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

import net.snowflake.client.api.statement.SnowflakePreparedStatement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Standalone JDBC protocol qualification; deliberately independent of cache reader and store APIs. */
@EnabledIfEnvironmentVariable(named = SnowflakeConfigConnectionService.CONNECTION_ENV, matches = ".+")
class AnonymousRefreshProtocolIT {
    private static final List<String> SOURCE_LABELS = List.of("Id", "MiXeD", "quote\"name", "NullValue", "Amount",
            "Enabled", "Payload", "KIND", "CHECKPOINT", "__NIFI_CACHE_ACTION");
    private static final String ACTION_BASE = "__NIFI_CACHE_ACTION";
    private static final int SOURCE_OFFSET = 3;

    private Connection connection;
    private String table;
    private boolean tableCreated;
    private List<Column> sourceColumns;
    private String projection;
    private String actionAlias;
    private String deltaSql;
    private String refreshSql;

    @BeforeEach
    void setUp() throws SQLException {
        connection = new SnowflakeConfigConnectionService().getConnection();
        assertTrue(connection.getAutoCommit(), "Fixture writes must commit before the next boundary");
        try (Statement statement = connection.createStatement(); ResultSet context = statement.executeQuery("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")) {
            assertTrue(context.next());
            table = "%s.%s.ANONYMOUS_REFRESH_PROTOCOL_%s".formatted(quoteIdentifier(context.getString(1)),
                    quoteIdentifier(context.getString(2)), UUID.randomUUID().toString().replace("-", ""));
        }
        execute("""
                CREATE TABLE %s (
                    "Id" VARCHAR, "MiXeD" VARCHAR, "quote""name" VARCHAR, "NullValue" VARCHAR,
                    "Amount" NUMBER(12, 2), "Enabled" BOOLEAN, "Payload" VARIANT,
                    "KIND" VARCHAR, "CHECKPOINT" VARCHAR, "__NIFI_CACHE_ACTION" VARCHAR
                ) CHANGE_TRACKING = TRUE
                """.formatted(table));
        tableCreated = true;
        try (final Statement statement = connection.createStatement();
                final ResultSet rows = statement.executeQuery("SELECT * FROM %s WHERE FALSE".formatted(table))) {
            sourceColumns = columns(rows.getMetaData());
        }
        assertEquals(SOURCE_LABELS, sourceColumns.stream().map(Column::label).toList());
        actionAlias = uniqueActionAlias(SOURCE_LABELS);
        assertNotEquals(ACTION_BASE, actionAlias, "The source intentionally occupies the first reserved alias");
        projection = SOURCE_LABELS.stream().map(AnonymousRefreshProtocolIT::quoteIdentifier).collect(Collectors.joining(", "));
        deltaSql = """
                SELECT METADATA$ACTION::VARCHAR AS %s, %s FROM IDENTIFIER(?)
                CHANGES(INFORMATION => DEFAULT) AT(STATEMENT => ?) END(STATEMENT => ?)
                """.formatted(quoteIdentifier(actionAlias), projection);
        refreshSql = anonymousSql();
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) {
            try {
                if (tableCreated) {
                    execute("DROP TABLE %s".formatted(table));
                }
            } finally {
                connection.close();
            }
        }
    }

    @Test
    void fullResponsePreservesEmptySchemaNativeStringsAndDuplicateLabels() throws SQLException {
        final Response empty = refresh(null);
        assertTrue(empty.data().isEmpty());
        assertDataScan(empty.checkpoint(), empty.data());

        insert("full");
        final List<String> expected = sourceRow("full");
        assertEquals("Case-sensitive", expected.get(1));
        assertEquals("O'Reilly \"quoted\" ? $$", expected.get(2));
        assertNull(expected.get(3));
        assertEquals("42.50", expected.get(4));
        assertEquals("true", expected.get(5).toLowerCase(java.util.Locale.ROOT));
        assertNotNull(expected.get(6));
        assertTrue(expected.get(6).contains("O'Reilly"));
        assertEquals("CONTROL", expected.get(7));
        assertEquals("source checkpoint", expected.get(8));
        assertEquals("DELETE", expected.get(9));

        final Response full = refresh(null);
        assertSingleChange(full, "INSERT", expected);
        assertDataScan(full.checkpoint(), full.data());
        assertNotEquals(empty.checkpoint(), full.checkpoint());
    }

    @Test
    void deltaUsesBoundChildEndpointsAndReturnsControlForNoChanges() throws SQLException {
        final Response initial = refresh(null);
        insert("inside");
        final List<String> inside = sourceRow("inside");
        final Response first = refresh(initial.checkpoint());
        assertSingleChange(first, "INSERT", inside);
        assertEndpointMarker(first.checkpoint());

        insert("outside");
        final List<String> outside = sourceRow("outside");
        // Replay after a later commit: an unbounded delta would incorrectly include "outside".
        try (final PreparedStatement statement = connection.prepareStatement(deltaSql)) {
            statement.setString(1, table);
            statement.setString(2, initial.checkpoint());
            statement.setString(3, first.checkpoint());
            try (final ResultSet rows = statement.executeQuery()) {
                assertEquals(first.data(), readData(rows));
            }
        }
        final Response second = refresh(first.checkpoint());
        assertSingleChange(second, "INSERT", outside);
        assertEndpointMarker(second.checkpoint());
        assertNotEquals(first.checkpoint(), second.checkpoint());

        execute("DELETE FROM %s WHERE \"Id\" = 'inside'".formatted(table));
        final Response deleted = refresh(second.checkpoint());
        assertSingleChange(deleted, "DELETE", inside);
        final Response empty = refresh(deleted.checkpoint());
        assertTrue(empty.data().isEmpty());
        assertEndpointMarker(empty.checkpoint());
        assertNotEquals(deleted.checkpoint(), empty.checkpoint(), "An empty delta still advances its endpoint");
    }

    @Test
    void productionReaderPreservesNativeGetStringValues() throws Exception {
        insert("production");
        final List<String> expected = sourceRow("production");
        final AnonymousTableRefresh reader = new AnonymousTableRefresh(table, List.of("Id"), List.of(), true, true);
        try (TableCacheStore store = new HeapTableCacheStore()) {
            reader.refresh(new SnowflakeConfigConnectionService(), store, false);
            final org.apache.nifi.serialization.record.Record row = store.get("production").orElseThrow();
            for (int index = 0; index < SOURCE_LABELS.size(); index++) {
                assertEquals(expected.get(index), row.getAsString(SOURCE_LABELS.get(index)));
            }
            assertTrue(reader.refresh(new SnowflakeConfigConnectionService(), store, true).incremental());
            assertEquals(1L, store.size());
        }
    }

    @Test
    void productionReaderRejectsAutoCommitDisabled() throws Exception {
        final SnowflakeConfigConnectionService transactions = new SnowflakeConfigConnectionService() {
            @Override
            public Connection getConnection() {
                final Connection borrowed = super.getConnection();
                try {
                    borrowed.setAutoCommit(false);
                    return borrowed;
                } catch (final SQLException exception) {
                    try {
                        borrowed.close();
                    } catch (final SQLException closeFailure) {
                        exception.addSuppressed(closeFailure);
                    }
                    throw new IllegalStateException(exception);
                }
            }
        };
        final AnonymousTableRefresh reader = new AnonymousTableRefresh(table, List.of("Id"), List.of(), true, true);
        try (TableCacheStore store = new HeapTableCacheStore()) {
            final SQLException error = org.junit.jupiter.api.Assertions.assertThrows(SQLException.class, () -> reader.refresh(transactions, store, false));
            assertTrue(error.getMessage().contains("auto-commit"));
            assertFalse(store.isHydrated());
        }
    }

    private String anonymousSql() {
        return """
                WITH refresh AS PROCEDURE(source_table VARCHAR, previous_query_id VARCHAR)
                RETURNS TABLE()
                LANGUAGE SQL
                AS $$
                DECLARE
                    endpoint_id VARCHAR;
                    data_id VARCHAR;
                    delta_sql VARCHAR DEFAULT '%s';
                    response_sql VARCHAR;
                    endpoint_rows RESULTSET;
                    data_rows RESULTSET;
                    response_rows RESULTSET;
                BEGIN
                    IF (previous_query_id IS NULL) THEN
                        data_rows := (SELECT 'INSERT'::VARCHAR AS %s, %s FROM IDENTIFIER(:source_table));
                        data_id := SQLID;
                        endpoint_id := data_id;
                    ELSE
                        endpoint_rows := (SELECT CURRENT_TIMESTAMP() AS ENDPOINT_TIME);
                        endpoint_id := SQLID;
                        data_rows := (EXECUTE IMMEDIATE :delta_sql USING(source_table, previous_query_id, endpoint_id));
                        data_id := SQLID;
                    END IF;
                    response_sql := 'SELECT ''CONTROL'' AS KIND, ?::VARCHAR AS CHECKPOINT, d.*
                        FROM (SELECT 1) c LEFT JOIN TABLE(RESULT_SCAN(?)) d ON FALSE
                        UNION ALL SELECT ''DATA'', NULL::VARCHAR, d.* FROM TABLE(RESULT_SCAN(?)) d';
                    response_rows := (EXECUTE IMMEDIATE :response_sql USING(endpoint_id, data_id, data_id));
                    RETURN TABLE(response_rows);
                END;
                $$
                CALL refresh(?,?)
                """.formatted(deltaSql.replace("'", "''"), quoteIdentifier(actionAlias), projection);
    }

    private Response refresh(final String previous) throws SQLException {
        final List<String> checkpoints = new ArrayList<>();
        final List<List<String>> data = new ArrayList<>();
        try (final PreparedStatement statement = connection.prepareStatement(refreshSql)) {
            statement.setString(1, table);
            if (previous == null) {
                statement.setNull(2, Types.VARCHAR);
            } else {
                statement.setString(2, previous);
            }
            try (final ResultSet rows = statement.executeQuery()) {
                // Driver-specific access is only the independent oracle; the protocol reads standard JDBC columns.
                final String outerId = statement.unwrap(SnowflakePreparedStatement.class).getQueryID();
                assertNotNull(outerId);
                final List<Column> metadata = columns(rows.getMetaData());
                assertEquals(SOURCE_OFFSET + sourceColumns.size(), metadata.size());
                assertEquals("KIND", metadata.get(0).label());
                assertEquals("CHECKPOINT", metadata.get(1).label());
                assertEquals(actionAlias, metadata.get(2).label());
                assertEquals(sourceColumns, metadata.subList(SOURCE_OFFSET, metadata.size()),
                        "Source labels and native JDBC types must survive duplicate control names");
                assertEquals(2L, metadata.stream().filter(column -> "KIND".equals(column.label())).count());
                assertEquals(2L, metadata.stream().filter(column -> "CHECKPOINT".equals(column.label())).count());
                while (rows.next()) {
                    final String kind = rows.getString(1);
                    if ("CONTROL".equals(kind)) {
                        final String checkpoint = rows.getString(2);
                        assertNotNull(checkpoint);
                        assertFalse(checkpoint.isBlank());
                        checkpoints.add(checkpoint);
                        for (int ordinal = 3; ordinal <= metadata.size(); ordinal++) {
                            assertNull(rows.getString(ordinal), "CONTROL must not contain action or source values");
                            assertTrue(rows.wasNull());
                        }
                    } else {
                        assertEquals("DATA", kind);
                        assertNull(rows.getString(2));
                        assertTrue(rows.wasNull());
                        data.add(strings(rows, 3));
                    }
                }
                assertEquals(1, checkpoints.size(), "Exactly one control row, including empty results");
                assertNotEquals(outerId, checkpoints.getFirst(), "The checkpoint must be a child SQLID, never the outer CALL");
            }
        }
        return new Response(checkpoints.getFirst(), data);
    }

    private void assertDataScan(final String queryId, final List<List<String>> expected) throws SQLException {
        try (final PreparedStatement statement = connection.prepareStatement("SELECT * FROM TABLE(RESULT_SCAN(?))")) {
            statement.setString(1, queryId);
            try (final ResultSet rows = statement.executeQuery()) {
                assertEquals(expected, readData(rows), "Full checkpoint must identify the child data SELECT");
            }
        }
    }

    private void assertEndpointMarker(final String queryId) throws SQLException {
        try (final PreparedStatement statement = connection.prepareStatement("SELECT * FROM TABLE(RESULT_SCAN(?))")) {
            statement.setString(1, queryId);
            try (final ResultSet rows = statement.executeQuery()) {
                assertEquals(1, rows.getMetaData().getColumnCount());
                assertEquals("ENDPOINT_TIME", rows.getMetaData().getColumnLabel(1));
                assertTrue(rows.next());
                assertNotNull(rows.getString(1));
                assertFalse(rows.next());
            }
        }
    }

    private List<List<String>> readData(final ResultSet rows) throws SQLException {
        final List<Column> metadata = columns(rows.getMetaData());
        assertEquals(1 + sourceColumns.size(), metadata.size());
        assertEquals(actionAlias, metadata.getFirst().label());
        assertEquals(sourceColumns, metadata.subList(1, metadata.size()));
        final List<List<String>> data = new ArrayList<>();
        while (rows.next()) {
            data.add(strings(rows, 1));
        }
        return data;
    }

    private static void assertSingleChange(final Response response, final String action, final List<String> source) {
        assertEquals(1, response.data().size());
        final List<String> row = response.data().getFirst();
        assertEquals(action, row.getFirst());
        assertEquals(source, row.subList(1, row.size()), "Read source values by ordinal, not colliding metadata labels");
    }

    private void insert(final String identifier) throws SQLException {
        try (final PreparedStatement statement = connection.prepareStatement("""
                INSERT INTO %s SELECT ?, ?, ?, NULL, 42.50, TRUE, PARSE_JSON(?), ?, ?, ?
                """.formatted(table))) {
            statement.setString(1, identifier);
            statement.setString(2, "Case-sensitive");
            statement.setString(3, "O'Reilly \"quoted\" ? $$");
            statement.setString(4, "{\"number\":7,\"flag\":true,\"missing\":null,\"text\":\"O'Reilly\"}");
            statement.setString(5, "CONTROL");
            statement.setString(6, "source checkpoint");
            statement.setString(7, "DELETE");
            assertEquals(1, statement.executeUpdate());
        }
    }

    private List<String> sourceRow(final String identifier) throws SQLException {
        try (final PreparedStatement statement = connection.prepareStatement("SELECT %s FROM %s WHERE \"Id\" = ?".formatted(projection, table))) {
            statement.setString(1, identifier);
            try (final ResultSet rows = statement.executeQuery()) {
                assertTrue(rows.next());
                final List<String> values = strings(rows, 1);
                assertFalse(rows.next());
                return values;
            }
        }
    }

    private void execute(final String sql) throws SQLException {
        try (final Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private static List<String> strings(final ResultSet rows, final int firstOrdinal) throws SQLException {
        final List<String> values = new ArrayList<>();
        for (int ordinal = firstOrdinal; ordinal <= rows.getMetaData().getColumnCount(); ordinal++) {
            final String value = rows.getString(ordinal);
            assertEquals(value == null, rows.wasNull());
            values.add(value);
        }
        return values;
    }

    private static List<Column> columns(final ResultSetMetaData metadata) throws SQLException {
        final List<Column> columns = new ArrayList<>();
        for (int ordinal = 1; ordinal <= metadata.getColumnCount(); ordinal++) {
            columns.add(new Column(metadata.getColumnLabel(ordinal), metadata.getColumnType(ordinal), metadata.getColumnTypeName(ordinal)));
        }
        return columns;
    }

    private static String uniqueActionAlias(final List<String> labels) {
        String candidate = ACTION_BASE;
        int suffix = 0;
        while (labels.contains(candidate)) {
            candidate = "%s_%d".formatted(ACTION_BASE, ++suffix);
        }
        return candidate;
    }

    private static String quoteIdentifier(final String identifier) {
        return "\"%s\"".formatted(identifier.replace("\"", "\"\""));
    }

    private record Column(String label, int jdbcType, String typeName) {
    }

    private record Response(String checkpoint, List<List<String>> data) {
    }
}
