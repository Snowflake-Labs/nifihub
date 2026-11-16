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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Opt-in capability experiments using owned fixtures in a dedicated permanent database. */
@EnabledIfEnvironmentVariable(named = "SNOWFLAKE_TYPES_IT", matches = "true")
class SourceTypesProtocolIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(SourceTypesProtocolIT.class);
    private Connection connection;
    private String database;
    private String schema;
    private String sourceName;
    private String source;
    private String base;
    private String warehouse;
    private final List<String> cleanup = new ArrayList<>();

    @TempDir
    Path directory;

    @BeforeEach
    void setUp() throws SQLException {
        connection = new SnowflakeConfigConnectionService().getConnection();
        try (Statement statement = connection.createStatement();
             ResultSet context = statement.executeQuery("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE()")) {
            assertTrue(context.next());
            database = context.getString(1);
            schema = context.getString(2);
            warehouse = context.getString(3);
        }
        sourceName = "CACHE_TYPES_%s".formatted(UUID.randomUUID().toString().replace("-", "").toUpperCase(java.util.Locale.ROOT));
        source = "%s.%s.%s".formatted(SnowflakeIdentifier.quote(database), SnowflakeIdentifier.quote(schema), SnowflakeIdentifier.quote(sourceName));
        base = "%s_BASE".formatted(sourceName);
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) {
            SQLException failure = null;
            try {
                for (final String sql : cleanup.reversed()) {
                    try {
                        execute(sql);
                    } catch (final SQLException exception) {
                        if (failure == null) {
                            failure = exception;
                        } else {
                            failure.addSuppressed(exception);
                        }
                    }
                }
            } finally {
                connection.close();
            }
            if (failure != null) {
                throw failure;
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"VIEW", "HYBRID", "DYNAMIC_INCREMENTAL", "DYNAMIC_FULL", "ICEBERG2", "ICEBERG3"})
    void qualifyFullResponseAndStatementBoundedChanges(final String type) throws SQLException {
        createSource(type);
        final Response initial = refresh(false, null);
        assertEquals(Map.of(), initial.rows());
        final boolean incremental = !List.of("VIEW", "HYBRID", "DYNAMIC_FULL").contains(type);
        if (incremental) {
            assertEquals(initial.rows(), scan(initial.checkpoint().queryId()));
        }
        final String writable = type.startsWith("DYNAMIC") || "VIEW".equals(type) ? base : source;
        execute("INSERT INTO %s VALUES ('one', 'first'), ('two', 'second')".formatted(writable));
        refreshDynamic(type);
        final Response full = refresh(false, initial.checkpoint());
        assertEquals(Map.of("one", "first", "two", "second"), full.rows());
        if (incremental) {
            assertEquals(full.rows(), scan(full.checkpoint().queryId()));
        }
        LOGGER.info("{} full response passed; checkpoint {}, signature {}", type, full.checkpoint().queryId(), full.checkpoint().sourceSignature());
        execute("UPDATE %s SET LABEL = 'changed' WHERE ITEM_ID = 'one'".formatted(writable));
        execute("DELETE FROM %s WHERE ITEM_ID = 'two'".formatted(writable));
        execute("INSERT INTO %s VALUES ('three', 'third')".formatted(writable));
        refreshDynamic(type);
        final Map<String, String> expected = Map.of("one", "changed", "three", "third");
        if ("HYBRID".equals(type) || "DYNAMIC_FULL".equals(type) || "VIEW".equals(type)) {
            assertEquals(expected, refresh(false, full.checkpoint()).rows());
            return;
        }
        final Response delta = refresh(true, full.checkpoint());
        assertEquals(full.checkpoint().sourceSignature(), delta.checkpoint().sourceSignature(), "ordinary writes/refresh must preserve source continuity");
        final Map<String, String> reconstructed = new HashMap<>(full.rows());
        delta.deletes().forEach(reconstructed::remove);
        reconstructed.putAll(delta.rows());
        assertEquals(expected, reconstructed);
        assertEquals(expected, endpoint(delta.checkpoint().queryId()));
        execute("INSERT INTO %s VALUES ('later', 'outside')".formatted(writable));
        refreshDynamic(type);
        assertEquals(delta, changes(full.checkpoint(), delta.checkpoint()));
        final Response next = refresh(true, delta.checkpoint());
        assertEquals(Map.of("later", "outside"), next.rows());
        assertTrue(next.deletes().isEmpty());
        final Response empty = refresh(true, next.checkpoint());
        assertTrue(empty.rows().isEmpty());
        assertTrue(empty.deletes().isEmpty());
        LOGGER.info("{} exact incremental boundaries passed; endpoint {}", type, empty.checkpoint().queryId());
    }

    private void createSource(final String type) throws SQLException {
        if ("VIEW".equals(type) || type.startsWith("DYNAMIC")) {
            execute("CREATE TABLE %s (ITEM_ID VARCHAR, LABEL VARCHAR) CHANGE_TRACKING = TRUE".formatted(base));
            cleanup.add("DROP TABLE %s".formatted(base));
            if ("VIEW".equals(type)) {
                execute("CREATE VIEW %s AS SELECT ITEM_ID, LABEL FROM %s".formatted(source, base));
                cleanup.add("DROP VIEW %s".formatted(source));
            } else {
                execute("CREATE DYNAMIC TABLE %s TARGET_LAG = DOWNSTREAM WAREHOUSE = %s REFRESH_MODE = %s AS SELECT ITEM_ID, LABEL FROM %s"
                        .formatted(source, SnowflakeIdentifier.quote(warehouse), type.substring("DYNAMIC_".length()), base));
                cleanup.add("DROP DYNAMIC TABLE %s".formatted(source));
            }
        } else if ("HYBRID".equals(type)) {
            execute("CREATE HYBRID TABLE %s (ITEM_ID VARCHAR PRIMARY KEY, LABEL VARCHAR)".formatted(source));
            cleanup.add("DROP TABLE %s".formatted(source));
        } else {
            execute("CREATE ICEBERG TABLE %s (ITEM_ID VARCHAR, LABEL VARCHAR) CATALOG = 'SNOWFLAKE' EXTERNAL_VOLUME = 'SNOWFLAKE_MANAGED' ICEBERG_VERSION = %s"
                    .formatted(source, type.substring("ICEBERG".length())));
            cleanup.add("DROP ICEBERG TABLE %s".formatted(source));
            execute("ALTER ICEBERG TABLE %s SET CHANGE_TRACKING = TRUE".formatted(source));
        }
    }

    private void refreshDynamic(final String type) throws SQLException {
        if (type.startsWith("DYNAMIC")) {
            execute("ALTER DYNAMIC TABLE %s REFRESH".formatted(source));
        }
    }

    private Response refresh(final boolean delta, final CacheCheckpoint previous) throws SQLException {
        final AnonymousTableRefresh reader = new AnonymousTableRefresh(source, List.of("ITEM_ID"), List.of("LABEL"), false, true);
        final SourceMetadata metadata = SourceMetadata.read(connection, List.of(database, schema, sourceName), true);
        final String sql = reader.sql(List.of("ITEM_ID", "LABEL"), delta, "TCPROBE", !metadata.incremental());
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, previous == null || !delta ? null : previous.queryId());
            statement.setString(2, previous == null ? null : previous.sourceSignature());
            statement.setString(3, schema);
            statement.setString(4, sourceName);
            statement.setString(5, metadata.signature());
            try (ResultSet rows = statement.executeQuery()) {
                final Map<String, String> upserts = new HashMap<>();
                final List<String> deletes = new ArrayList<>();
                CacheCheckpoint checkpoint = null;
                long count = -1;
                long received = 0;
                while (rows.next()) {
                    if ("CONTROL".equals(rows.getString(1))) {
                        checkpoint = new CacheCheckpoint(rows.getString(2), rows.getString(3));
                        assertEquals(delta ? "INCREMENTAL" : "FULL", rows.getString(4));
                        count = rows.getLong(5);
                    } else {
                        assertEquals("DATA", rows.getString(1));
                        if ("DELETE".equals(rows.getString(6))) {
                            deletes.add(rows.getString(7));
                        } else {
                            assertEquals("INSERT", rows.getString(6));
                            upserts.put(rows.getString(7), rows.getString(8));
                        }
                        received++;
                    }
                }
                assertNotNull(checkpoint);
                assertEquals(count, received);
                return new Response(checkpoint, upserts, deletes.stream().sorted().toList());
            }
        }
    }

    private Response changes(final CacheCheckpoint start, final CacheCheckpoint end) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("SELECT ITEM_ID, LABEL, METADATA$ACTION FROM %s CHANGES(INFORMATION => DEFAULT) AT(STATEMENT => ?) END(STATEMENT => ?)"
                .formatted(source))) {
            statement.setString(1, start.queryId());
            statement.setString(2, end.queryId());
            try (ResultSet rows = statement.executeQuery()) {
                final Map<String, String> upserts = new HashMap<>();
                final List<String> deletes = new ArrayList<>();
                while (rows.next()) {
                    if ("DELETE".equals(rows.getString(3))) {
                        deletes.add(rows.getString(1));
                    } else {
                        upserts.put(rows.getString(1), rows.getString(2));
                    }
                }
                return new Response(end, upserts, deletes.stream().sorted().toList());
            }
        }
    }

    private Map<String, String> scan(final String queryId) throws SQLException {
        return query("SELECT ITEM_ID, LABEL FROM TABLE(RESULT_SCAN(?))", queryId);
    }

    private Map<String, String> endpoint(final String queryId) throws SQLException {
        return query("SELECT ITEM_ID, LABEL FROM %s AT(STATEMENT => ?)".formatted(source), queryId);
    }

    private Map<String, String> query(final String sql, final String queryId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, queryId);
            try (ResultSet rows = statement.executeQuery()) {
                final Map<String, String> result = new HashMap<>();
                while (rows.next()) {
                    result.put(rows.getString(1), rows.getString(2));
                }
                return result;
            }
        }
    }

    private void execute(final String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"VIEW", "HYBRID", "DYNAMIC_INCREMENTAL", "DYNAMIC_FULL", "ICEBERG2", "ICEBERG3"})
    void productionStoresUseEffectiveModeAndReopen(final String type) throws Exception {
        createSource(type);
        final boolean incremental = !List.of("VIEW", "HYBRID", "DYNAMIC_FULL").contains(type);
        final String writable = type.startsWith("DYNAMIC") || "VIEW".equals(type) ? base : source;
        final AnonymousTableRefresh reader = new AnonymousTableRefresh(source, List.of("ITEM_ID"), List.of("LABEL"), false, true);
        final SnowflakeConfigConnectionService connections = new SnowflakeConfigConnectionService();
        try (TableCacheStore heap = new HeapTableCacheStore(); TableCacheStore disk = new RocksDbTableCacheStore(directory)) {
            for (final TableCacheStore store : List.of(heap, disk)) {
                assertFalse(reader.refresh(connections, store, false).incremental());
            }
            execute("INSERT INTO %s VALUES ('one', 'first'), ('two', 'second')".formatted(writable));
            refreshDynamic(type);
            for (final TableCacheStore store : List.of(heap, disk)) {
                assertEquals(incremental, reader.refresh(connections, store, true).incremental());
                assertEquals(2, store.size());
                assertEquals("first", store.get("one").orElseThrow().getAsString("LABEL"));
            }
            execute("UPDATE %s SET LABEL = 'changed' WHERE ITEM_ID = 'one'".formatted(writable));
            execute("DELETE FROM %s WHERE ITEM_ID = 'two'".formatted(writable));
            refreshDynamic(type);
            for (final TableCacheStore store : List.of(heap, disk)) {
                assertEquals(incremental, reader.refresh(connections, store, true).incremental());
                assertEquals(1, store.size());
                assertEquals("changed", store.get("one").orElseThrow().getAsString("LABEL"));
                assertTrue(store.get("two").isEmpty());
            }
        }
        try (TableCacheStore reopened = new RocksDbTableCacheStore(directory)) {
            assertEquals(incremental, reader.refresh(connections, reopened, true).incremental());
            assertEquals("changed", reopened.get("one").orElseThrow().getAsString("LABEL"));
            execute("DELETE FROM %s".formatted(writable));
            refreshDynamic(type);
            assertEquals(incremental, reader.refresh(connections, reopened, true).incremental());
            assertEquals(0, reopened.size());
        }
    }

    @Test
    void viewFullRefreshReflectsNestedDefinitionAndAggregateChanges() throws Exception {
        createSource("VIEW");
        final String outer = "%s_OUTER".formatted(sourceName);
        execute("CREATE VIEW %s AS SELECT ref.ITEM_ID, MAX(ref.LABEL) AS LABEL FROM %s ref INNER JOIN %s dim ON ref.ITEM_ID = dim.ITEM_ID GROUP BY ref.ITEM_ID"
                .formatted(outer, source, base));
        cleanup.add("DROP VIEW %s".formatted(outer));
        final String qualified = "%s.%s.%s".formatted(SnowflakeIdentifier.quote(database), SnowflakeIdentifier.quote(schema), outer);
        final AnonymousTableRefresh reader = new AnonymousTableRefresh(qualified, List.of("ITEM_ID"), List.of("LABEL"), false, true);
        final SnowflakeConfigConnectionService connections = new SnowflakeConfigConnectionService();
        execute("INSERT INTO %s VALUES ('one', 'first'), ('two', 'second')".formatted(base));
        try (TableCacheStore store = new HeapTableCacheStore()) {
            reader.refresh(connections, store, false);
            final String originalSignature = store.checkpoint().sourceSignature();
            execute("CREATE OR REPLACE VIEW %s AS SELECT ITEM_ID, LABEL FROM %s WHERE ITEM_ID = 'one'".formatted(source, base));
            assertFalse(reader.refresh(connections, store, true).incremental());
            assertEquals(originalSignature, store.checkpoint().sourceSignature(), "outer definition did not change");
            assertEquals(1, store.size());
            assertTrue(store.get("two").isEmpty());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"DYNAMIC_INCREMENTAL", "ICEBERG2", "ICEBERG3"})
    void sourceDdlRequiresReloadAndDynamicReinitializationReconstructs(final String type) throws Exception {
        createSource(type);
        final String writable = type.startsWith("DYNAMIC") ? base : source;
        final AnonymousTableRefresh reader = new AnonymousTableRefresh(source, List.of("ITEM_ID"), List.of("LABEL"), false, true);
        final SnowflakeConfigConnectionService connections = new SnowflakeConfigConnectionService();
        execute("INSERT INTO %s VALUES ('one', 'old'), ('two', 'remove')".formatted(writable));
        refreshDynamic(type);
        try (TableCacheStore store = new HeapTableCacheStore()) {
            reader.refresh(connections, store, false);
            if (type.startsWith("DYNAMIC")) {
                execute("CREATE OR REPLACE TABLE %s (ITEM_ID VARCHAR, LABEL VARCHAR) CHANGE_TRACKING = TRUE".formatted(base));
                execute("INSERT INTO %s VALUES ('one', 'new'), ('three', 'added')".formatted(base));
                refreshDynamic(type);
                assertTrue(reader.refresh(connections, store, true).incremental());
                assertEquals(2, store.size());
                assertEquals("new", store.get("one").orElseThrow().getAsString("LABEL"));
                assertTrue(store.get("two").isEmpty());
            }
            final CacheCheckpoint beforeDdl = store.checkpoint();
            if (type.startsWith("DYNAMIC")) {
                execute("ALTER DYNAMIC TABLE %s SET COMMENT = 'source metadata changed'".formatted(source));
            } else {
                execute("ALTER ICEBERG TABLE %s ADD COLUMN EXTRA VARCHAR".formatted(source));
            }
            final SQLException changed = assertThrows(SQLException.class, () -> reader.refresh(connections, store, true));
            assertTrue(changed.getMessage().contains("TABLE_CACHE_SOURCE_CHANGED"));
            assertEquals(beforeDdl, store.checkpoint());
            assertFalse(reader.reload(connections, store).incremental());
            assertEquals(2, store.size());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"VIEW", "HYBRID"})
    void fullOnlyNativeColumnsAndRepeatedReadsDoNotDependOnResultScan(final String type) throws Exception {
        createSource(type);
        final String writable = "VIEW".equals(type) ? base : source;
        execute("ALTER TABLE %s ADD COLUMN \"MiXeD\" VARCHAR, \"quote\"\"value\" VARCHAR, AMOUNT NUMBER(12,2), ENABLED BOOLEAN, PAYLOAD VARIANT".formatted(writable));
        if ("VIEW".equals(type)) {
            execute("CREATE OR REPLACE VIEW %s AS SELECT * FROM %s".formatted(source, base));
        }
        execute("INSERT INTO %s SELECT 'one', NULL, 'mixed', 'quoted', 42.50, TRUE, PARSE_JSON('{\"a\":1}')".formatted(writable));
        final Map<String, String> expected = new HashMap<>();
        try (Statement statement = connection.createStatement(); ResultSet rows = statement.executeQuery("SELECT * FROM %s".formatted(source))) {
            assertTrue(rows.next());
            for (int column = 1; column <= rows.getMetaData().getColumnCount(); column++) {
                expected.put(rows.getMetaData().getColumnLabel(column), rows.getString(column));
            }
        }
        final AnonymousTableRefresh reader = new AnonymousTableRefresh(source, List.of("ITEM_ID"), List.of(), true, true);
        final SnowflakeConfigConnectionService connections = new SnowflakeConfigConnectionService();
        try (TableCacheStore store = new RocksDbTableCacheStore(directory)) {
            for (int refresh = 0; refresh < 4; refresh++) {
                assertFalse(reader.refresh(connections, store, store.isHydrated()).incremental());
                final org.apache.nifi.serialization.record.Record row = store.get("one").orElseThrow();
                for (final Map.Entry<String, String> field : expected.entrySet()) {
                    assertEquals(field.getValue(), row.getAsString(field.getKey()));
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"VIEW", "HYBRID"})
    void fullOnlyPublicationUsesReturnedRowsRatherThanLaterSourceState(final String type) throws Exception {
        createSource(type);
        final String writable = "VIEW".equals(type) ? base : source;
        execute("INSERT INTO %s VALUES ('one', 'before')".formatted(writable));
        final AtomicBoolean intercept = new AtomicBoolean(true);
        final SnowflakeConfigConnectionService connections = new SnowflakeConfigConnectionService() {
            @Override
            public Connection getConnection() {
                final Connection borrowed = super.getConnection();
                return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class}, (proxy, method, arguments) -> {
                    try {
                        final Object result = method.invoke(borrowed, arguments);
                        if ("prepareStatement".equals(method.getName()) && ((String) arguments[0]).startsWith("WITH refresh_cache")
                                && intercept.compareAndSet(true, false)) {
                            final PreparedStatement statement = (PreparedStatement) result;
                            return Proxy.newProxyInstance(PreparedStatement.class.getClassLoader(), new Class<?>[]{PreparedStatement.class},
                                    (statementProxy, statementMethod, statementArguments) -> {
                                        try {
                                            final Object response = statementMethod.invoke(statement, statementArguments);
                                            if ("executeQuery".equals(statementMethod.getName())) {
                                                execute("UPDATE %s SET LABEL = 'after'".formatted(writable));
                                            }
                                            return response;
                                        } catch (final InvocationTargetException exception) {
                                            throw exception.getCause();
                                        }
                                    });
                        }
                        return result;
                    } catch (final InvocationTargetException exception) {
                        throw exception.getCause();
                    }
                });
            }
        };
        final AnonymousTableRefresh reader = new AnonymousTableRefresh(source, List.of("ITEM_ID"), List.of("LABEL"), false, true);
        try (TableCacheStore store = new HeapTableCacheStore()) {
            assertFalse(reader.refresh(connections, store, false).incremental());
            assertEquals("before", store.get("one").orElseThrow().getAsString("LABEL"));
            assertFalse(reader.refresh(connections, store, true).incremental());
            assertEquals("after", store.get("one").orElseThrow().getAsString("LABEL"));
        }
    }

    @Test
    void dynamicModeChangeCannotResumeDiagnosticFullMarker() throws Exception {
        createSource("DYNAMIC_FULL");
        execute("INSERT INTO %s VALUES ('one', 'first')".formatted(base));
        refreshDynamic("DYNAMIC_FULL");
        final AnonymousTableRefresh reader = new AnonymousTableRefresh(source, List.of("ITEM_ID"), List.of("LABEL"), false, true);
        final SnowflakeConfigConnectionService connections = new SnowflakeConfigConnectionService();
        try (TableCacheStore store = new HeapTableCacheStore()) {
            reader.refresh(connections, store, false);
            execute("CREATE OR ALTER DYNAMIC TABLE %s TARGET_LAG = DOWNSTREAM WAREHOUSE = %s REFRESH_MODE = INCREMENTAL AS SELECT ITEM_ID, LABEL FROM %s"
                    .formatted(source, SnowflakeIdentifier.quote(warehouse), base));
            refreshDynamic("DYNAMIC_INCREMENTAL");
            final SQLException changed = assertThrows(SQLException.class, () -> reader.refresh(connections, store, true));
            assertTrue(changed.getMessage().contains("TABLE_CACHE_SOURCE_CHANGED"));
            assertFalse(reader.reload(connections, store).incremental());
            execute("UPDATE %s SET LABEL = 'new'".formatted(base));
            refreshDynamic("DYNAMIC_INCREMENTAL");
            assertTrue(reader.refresh(connections, store, true).incremental());
            assertEquals("new", store.get("one").orElseThrow().getAsString("LABEL"));
            execute("CREATE OR ALTER DYNAMIC TABLE %s TARGET_LAG = DOWNSTREAM WAREHOUSE = %s REFRESH_MODE = FULL AS SELECT ITEM_ID, LABEL FROM %s"
                    .formatted(source, SnowflakeIdentifier.quote(warehouse), base));
            refreshDynamic("DYNAMIC_FULL");
            assertThrows(SQLException.class, () -> reader.refresh(connections, store, true));
            assertFalse(reader.reload(connections, store).incremental());
            assertFalse(reader.refresh(connections, store, true).incremental());
        }
    }

    private record Response(CacheCheckpoint checkpoint, Map<String, String> rows, List<String> deletes) {
    }
}
