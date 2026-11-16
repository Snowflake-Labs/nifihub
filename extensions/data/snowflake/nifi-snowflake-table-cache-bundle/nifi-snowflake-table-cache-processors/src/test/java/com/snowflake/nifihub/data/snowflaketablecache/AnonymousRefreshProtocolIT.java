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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
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

/** Production SQL qualification with JDBC oracles independent of cache reader and store APIs. */
@EnabledIfEnvironmentVariable(named = SnowflakeConfigConnectionService.CONNECTION_ENV, matches = ".+")
class AnonymousRefreshProtocolIT {
    private static final List<String> SOURCE_LABELS = List.of("Id", "MiXeD", "quote\"name", "NullValue", "Amount",
            "Enabled", "Payload", "KIND", "CHECKPOINT", "__NIFI_CACHE_ACTION");
    private static final String PREFIX = "TCPROTOCOL";
    private static final int SOURCE_OFFSET = 6;

    private Connection connection;
    private String table;
    private boolean tableCreated;
    private List<Column> sourceColumns;
    private List<Column> markerColumns;
    private String projection;
    private String deltaSql;

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
        try (final Statement statement = connection.createStatement();
                final ResultSet rows = statement.executeQuery("SELECT CURRENT_TIMESTAMP()")) {
            markerColumns = columns(rows.getMetaData());
        }
        projection = SOURCE_LABELS.stream().map(AnonymousRefreshProtocolIT::quoteIdentifier).collect(Collectors.joining(", "));
        deltaSql = """
                SELECT METADATA$ACTION::VARCHAR AS %s, %s FROM IDENTIFIER(?)
                CHANGES(INFORMATION => DEFAULT) AT(STATEMENT => ?) END(STATEMENT => ?)
                """.formatted(quoteIdentifier(PREFIX), projection);
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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void fullResponsePreservesEmptySchemaNativeStringsAndControlLikeLabels(final boolean fullOnly) throws SQLException {
        final Response empty = refresh(null, fullOnly);
        assertTrue(empty.data().isEmpty());
        if (fullOnly) {
            assertEndpointMarker(empty.checkpoint());
        } else {
            assertDataScan(empty.checkpoint(), empty.data());
        }

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

        final Response full = refresh(fullOnly ? empty : null, fullOnly);
        assertSingleChange(full, "INSERT", expected);
        if (fullOnly) {
            assertEndpointMarker(full.checkpoint());
        } else {
            assertDataScan(full.checkpoint(), full.data());
        }
        assertNotEquals(empty.checkpoint(), full.checkpoint());
    }

    @Test
    void deltaUsesBoundChildEndpointsAndReturnsControlForNoChanges() throws SQLException {
        final Response initial = refresh(null);
        insert("inside");
        final List<String> inside = sourceRow("inside");
        final Response first = refresh(initial);
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
        final Response second = refresh(first);
        assertSingleChange(second, "INSERT", outside);
        assertEndpointMarker(second.checkpoint());
        assertNotEquals(first.checkpoint(), second.checkpoint());

        execute("DELETE FROM %s WHERE \"Id\" = 'inside'".formatted(table));
        final Response deleted = refresh(second);
        assertSingleChange(deleted, "DELETE", inside);
        final Response empty = refresh(deleted);
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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void injectionShapedIdentifiersAndValuesRemainLiteral(final boolean fullOnly) throws Exception {
        final List<String> source = SnowflakeIdentifier.parse(table, '.');
        final String name = "quoted'\\$$\"; SELECT 'injected'; --%s".formatted(UUID.randomUUID().toString().replace("-", ""));
        final String unusualTable = "%s.%s.%s".formatted(quoteIdentifier(source.get(0)), quoteIdentifier(source.get(1)), quoteIdentifier(name));
        final String valueColumn = "value'\\$$\"; --";
        final String key = "'); SELECT 'not SQL'; --";
        execute("CREATE TABLE %s (ITEM_ID VARCHAR, %s VARCHAR) CHANGE_TRACKING=TRUE".formatted(unusualTable, quoteIdentifier(valueColumn)));
        try {
            try (PreparedStatement insert = connection.prepareStatement("INSERT INTO %s VALUES (?, ?)".formatted(unusualTable))) {
                insert.setString(1, key);
                insert.setString(2, "literal");
                assertEquals(1, insert.executeUpdate());
            }
            final AnonymousTableRefresh reader = new AnonymousTableRefresh(unusualTable, List.of("ITEM_ID"), List.of(valueColumn), false, !fullOnly);
            try (TableCacheStore store = new HeapTableCacheStore()) {
                reader.refresh(new SnowflakeConfigConnectionService(), store, false);
                assertEquals("literal", store.get(key).orElseThrow().getAsString(valueColumn));
                assertEquals(!fullOnly, reader.refresh(new SnowflakeConfigConnectionService(), store, true).incremental());
                assertEquals("literal", store.get(key).orElseThrow().getAsString(valueColumn));
                assertEquals(1L, store.size());
            }
        } finally {
            execute("DROP TABLE %s".formatted(unusualTable));
        }
    }

    @Test
    void productionReaderRejectsCaseFoldedIdentifiers() throws Exception {
        final SnowflakeConfigConnectionService caseFolded = new SnowflakeConfigConnectionService() {
            @Override
            public Connection getConnection() {
                final Connection borrowed = super.getConnection();
                try (Statement statement = borrowed.createStatement()) {
                    statement.execute("ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE=TRUE");
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
            final SQLException error = org.junit.jupiter.api.Assertions.assertThrows(SQLException.class, () -> reader.refresh(caseFolded, store, false));
            assertTrue(error.getMessage().contains("QUOTED_IDENTIFIERS_IGNORE_CASE=FALSE"));
            assertFalse(store.isHydrated());
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

    private Response refresh(final Response previous) throws SQLException {
        return refresh(previous, false);
    }

    private Response refresh(final Response previous, final boolean fullOnly) throws SQLException {
        final List<String> source = SnowflakeIdentifier.parse(table, '.');
        final SourceMetadata sourceMetadata = SourceMetadata.read(connection, source, !fullOnly);
        assertEquals(!fullOnly, sourceMetadata.incremental());
        final boolean delta = previous != null && sourceMetadata.incremental();
        final AnonymousTableRefresh reader = new AnonymousTableRefresh(table, List.of("Id"), List.of(), true, !fullOnly);
        final String sql = reader.sql(SOURCE_LABELS, delta, PREFIX, !sourceMetadata.incremental());
        final List<String> checkpoints = new ArrayList<>();
        final List<List<String>> data = new ArrayList<>();
        long expectedCount = -1;
        try (final PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, delta ? previous.checkpoint() : null);
            statement.setString(2, previous == null ? null : previous.sourceSignature());
            statement.setString(3, source.get(1));
            statement.setString(4, source.get(2));
            statement.setString(5, sourceMetadata.signature());
            try (final ResultSet rows = statement.executeQuery()) {
                // Driver-specific access is only the independent oracle; the protocol reads standard JDBC columns.
                final String outerId = statement.unwrap(SnowflakePreparedStatement.class).getQueryID();
                assertNotNull(outerId);
                final List<Column> metadata = columns(rows.getMetaData());
                assertEquals(SOURCE_OFFSET + sourceColumns.size(), metadata.size());
                assertEquals(List.of("TCPROTOCOLK", "TCPROTOCOLQ", "TCPROTOCOLS", "TCPROTOCOLM", "TCPROTOCOLN", PREFIX),
                        metadata.subList(0, SOURCE_OFFSET).stream().map(Column::label).toList());
                assertEquals(sourceColumns, metadata.subList(SOURCE_OFFSET, metadata.size()),
                        "Source labels and native JDBC types must survive control-like source names");
                assertEquals(1L, metadata.stream().filter(column -> "KIND".equals(column.label())).count());
                assertEquals(1L, metadata.stream().filter(column -> "CHECKPOINT".equals(column.label())).count());
                assertEquals(1L, metadata.stream().filter(column -> "__NIFI_CACHE_ACTION".equals(column.label())).count());
                while (rows.next()) {
                    final String kind = rows.getString(1);
                    if ("CONTROL".equals(kind)) {
                        final String checkpoint = rows.getString(2);
                        assertNotNull(checkpoint);
                        assertFalse(checkpoint.isBlank());
                        checkpoints.add(checkpoint);
                        assertEquals(sourceMetadata.signature(), rows.getString(3));
                        assertEquals(delta ? "INCREMENTAL" : "FULL", rows.getString(4));
                        final java.math.BigDecimal count = rows.getBigDecimal(5);
                        assertNotNull(count);
                        expectedCount = count.longValueExact();
                        assertTrue(expectedCount >= 0);
                        for (int ordinal = SOURCE_OFFSET; ordinal <= metadata.size(); ordinal++) {
                            assertNull(rows.getString(ordinal), "CONTROL must not contain action or source values");
                            assertTrue(rows.wasNull());
                        }
                    } else {
                        assertEquals("DATA", kind);
                        for (int ordinal = 2; ordinal < SOURCE_OFFSET; ordinal++) {
                            assertNull(rows.getString(ordinal), "DATA must not contain control metadata");
                            assertTrue(rows.wasNull());
                        }
                        data.add(strings(rows, SOURCE_OFFSET));
                    }
                }
                assertEquals(1, checkpoints.size(), "Exactly one control row, including empty results");
                assertEquals(expectedCount, (long) data.size(), "Control count must match all returned data rows");
                assertNotEquals(outerId, checkpoints.getFirst(), "The checkpoint must be a child SQLID, never the outer CALL");
            }
        }
        return new Response(checkpoints.getFirst(), sourceMetadata.signature(), data);
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
                assertEquals(markerColumns, columns(rows.getMetaData()), "Endpoint must be the CURRENT_TIMESTAMP child SELECT");
                assertTrue(rows.next());
                assertNotNull(rows.getString(1));
                assertNotNull(rows.getTimestamp(1));
                assertFalse(rows.next());
            }
        }
    }

    private List<List<String>> readData(final ResultSet rows) throws SQLException {
        final List<Column> metadata = columns(rows.getMetaData());
        assertEquals(1 + sourceColumns.size(), metadata.size());
        assertEquals(PREFIX, metadata.getFirst().label());
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

    private static String quoteIdentifier(final String identifier) {
        return "\"%s\"".formatted(identifier.replace("\"", "\"\""));
    }

    private record Column(String label, int jdbcType, String typeName) {
    }

    private record Response(String checkpoint, String sourceSignature, List<List<String>> data) {
    }
}
