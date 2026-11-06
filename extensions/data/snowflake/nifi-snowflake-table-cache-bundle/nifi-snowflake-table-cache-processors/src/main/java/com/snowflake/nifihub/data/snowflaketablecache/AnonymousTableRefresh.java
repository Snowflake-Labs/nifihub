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

import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/** Reads complete source results and matched refresh metadata through ordinary JDBC columns. */
final class AnonymousTableRefresh {
    private static final int PREFIX_COLUMNS = 6;
    private final List<String> source;
    private final List<String> keys;
    private final List<String> values;
    private final boolean allColumns;
    private final boolean incrementalEnabled;

    AnonymousTableRefresh(final String sourceTable, final List<String> keys, final List<String> values,
                          final boolean allColumns, final boolean incrementalEnabled) {
        source = SnowflakeIdentifier.parse(sourceTable, '.');
        if (source.size() != 3) {
            throw new IllegalArgumentException("Source Table must be a fully qualified database.schema.table identifier");
        }
        this.keys = List.copyOf(keys);
        this.values = List.copyOf(values);
        this.allColumns = allColumns;
        this.incrementalEnabled = incrementalEnabled;
    }

    RefreshResult refresh(final DBCPService connections, final TableCacheStore store, final boolean delta) throws SQLException {
        return execute(connections, store, delta, store.checkpoint());
    }

    RefreshResult reload(final DBCPService connections, final TableCacheStore store) throws SQLException {
        return execute(connections, store, false, null);
    }

    private RefreshResult execute(final DBCPService connections, final TableCacheStore store,
                                  final boolean delta, final CacheCheckpoint previous) throws SQLException {
        final long started = System.currentTimeMillis();
        final String prefix = "TC%s".formatted(UUID.randomUUID().toString().replace("-", "").toUpperCase(Locale.ROOT));
        try (PendingRefresh pending = new PendingRefresh(store, started)) {
            try (Connection connection = connections.getConnection()) {
                if (!connection.getAutoCommit()) {
                    throw new SQLException("Cache refresh requires an auto-commit connection with no caller-owned transaction");
                }
                final SourceMetadata metadata = SourceMetadata.read(connection, source, incrementalEnabled, previous);
                pending.delta = delta && metadata.incremental();
                final List<String> fields = pending.delta ? store.schema().getFieldNames() : fullProjection();
                try (PreparedStatement statement = connection.prepareStatement(sql(fields, pending.delta, prefix, !metadata.incremental()))) {
                    statement.setString(1, previous == null || !pending.delta ? null : previous.queryId());
                    statement.setString(2, previous == null ? null : previous.sourceSignature());
                    statement.setString(3, source.get(1));
                    statement.setString(4, source.get(2));
                    statement.setString(5, metadata.signature());
                    try (ResultSet rows = statement.executeQuery()) {
                        pending.read(rows);
                    }
                }
            }
            // Even a close/connection-return failure must not advance the cache checkpoint.
            return pending.commit();
        }
    }

    String sql(final List<String> fields, final boolean delta, final String prefix) {
        return sql(fields, delta, prefix, false);
    }

    String sql(final List<String> fields, final boolean delta, final String prefix, final boolean fullOnly) {
        if (!prefix.matches("[A-Za-z][A-Za-z0-9]*")) {
            throw new IllegalArgumentException("Invalid internal response prefix");
        }
        final String qualified = source.stream().map(SnowflakeIdentifier::quote).collect(Collectors.joining("."));
        final String informationSchema = "%s.INFORMATION_SCHEMA".formatted(SnowflakeIdentifier.quote(source.getFirst()));
        final String projection = fields.isEmpty() ? "*" : fields.stream().map(SnowflakeIdentifier::quote).collect(Collectors.joining(", "));
        final String dataSql = "SELECT %s AS %s, %s FROM %s%s".formatted(
                delta ? "METADATA$ACTION" : "'INSERT'::VARCHAR", SnowflakeIdentifier.quote(prefix), projection, qualified,
                delta ? " CHANGES(INFORMATION => DEFAULT) AT(STATEMENT => ?) END(STATEMENT => ?)" : "");
        final String signatureSql = """
                SELECT COUNT(*), MAX(IFF(TABLE_TYPE IN ('BASE TABLE', 'VIEW') AND IS_TEMPORARY = 'NO'
                    AND CREATED IS NOT NULL AND LAST_DDL IS NOT NULL,
                    %s, NULL))
                INTO :source_count, :signature_value
                FROM %s.TABLES
                WHERE TABLE_SCHEMA = :source_schema AND TABLE_NAME = :source_name;
                """.formatted(SourceMetadata.SIGNATURE, informationSchema);
        // The prefix is chosen per request and checked against actual source columns before reading.
        // Payload aliases therefore cannot be confused with control columns after RESULT_SCAN.
        final String responseSql = """
                SELECT 'CONTROL' AS %sK, ?::VARCHAR AS %sQ, ?::VARCHAR AS %sS,
                       ?::VARCHAR AS %sM, (SELECT COUNT(*) FROM TABLE(RESULT_SCAN(?))) AS %sN, d.*
                FROM (SELECT 1) seed LEFT JOIN TABLE(RESULT_SCAN(?)) d ON FALSE
                UNION ALL
                SELECT 'DATA', NULL::VARCHAR, NULL::VARCHAR, NULL::VARCHAR, NULL::NUMBER, d.*
                FROM TABLE(RESULT_SCAN(?)) d
                """.formatted(prefix, prefix, prefix, prefix, prefix);
        // A full-only source needs no resumable offset. Return one query directly: hybrid query
        // results are not always available to RESULT_SCAN. The marker is diagnostic, not a snapshot ID.
        final String fullOnlySql = """
                SELECT IFF(seed.marker IS NULL, 'DATA', 'CONTROL') AS %sK,
                       IFF(seed.marker IS NULL, NULL, ?::VARCHAR) AS %sQ,
                       IFF(seed.marker IS NULL, NULL, ?::VARCHAR) AS %sS,
                       IFF(seed.marker IS NULL, NULL, 'FULL'::VARCHAR) AS %sM,
                       IFF(seed.marker IS NULL, NULL, COUNT(d.%s) OVER ()) AS %sN,
                       d.*
                FROM (%s) d FULL OUTER JOIN (SELECT 1 AS marker) seed ON FALSE
                """.formatted(prefix, prefix, prefix, prefix, SnowflakeIdentifier.quote(prefix), prefix, dataSql);
        final String body = """
                DECLARE
                    signature_value VARCHAR;
                    signature_before VARCHAR;
                    source_count INTEGER;
                    collision_count INTEGER;
                    marker_time TIMESTAMP_LTZ;
                    boundary_id VARCHAR;
                    data_id VARCHAR;
                    mode VARCHAR DEFAULT '%s';
                    data_sql VARCHAR DEFAULT %s;
                    response_sql VARCHAR DEFAULT %s;
                    full_only_sql VARCHAR DEFAULT %s;
                    data_rows RESULTSET;
                    response_rows RESULTSET;
                    source_changed EXCEPTION (-20001, 'TABLE_CACHE_SOURCE_CHANGED');
                    invalid_source EXCEPTION (-20002, 'TABLE_CACHE_INVALID_SOURCE');
                    invalid_session EXCEPTION (-20003, 'TABLE_CACHE_ACTIVE_TRANSACTION');
                BEGIN
                    IF (CURRENT_TRANSACTION() IS NOT NULL) THEN
                        RAISE invalid_session;
                    END IF;
                    %s
                    IF (source_count <> 1 OR signature_value IS NULL) THEN
                        RAISE invalid_source;
                    END IF;
                    signature_before := signature_value;
                    IF (expected_signature <> signature_before) THEN
                        RAISE source_changed;
                    END IF;
                    IF (previous_signature IS NOT NULL AND previous_signature <> signature_before) THEN
                        RAISE source_changed;
                    END IF;
                    SELECT COUNT(*) INTO :collision_count FROM %s.COLUMNS
                    WHERE TABLE_SCHEMA = :source_schema AND TABLE_NAME = :source_name
                      AND COLUMN_NAME IN ('%s', '%sK', '%sQ', '%sS', '%sM', '%sN');
                    IF (collision_count <> 0) THEN
                        RAISE invalid_source;
                    END IF;
                    IF (%s) THEN
                        SELECT CURRENT_TIMESTAMP() INTO :marker_time;
                        boundary_id := SQLID;
                        response_rows := (EXECUTE IMMEDIATE :full_only_sql USING (boundary_id, signature_value));
                    ELSEIF (previous_id IS NULL) THEN
                        data_rows := (EXECUTE IMMEDIATE :data_sql);
                        data_id := SQLID;
                        boundary_id := data_id;
                    ELSE
                        SELECT CURRENT_TIMESTAMP() INTO :marker_time;
                        boundary_id := SQLID;
                        data_rows := (EXECUTE IMMEDIATE :data_sql USING (previous_id, boundary_id));
                        data_id := SQLID;
                    END IF;
                    %s
                    IF (source_count <> 1 OR signature_value IS NULL OR signature_value <> signature_before) THEN
                        RAISE source_changed;
                    END IF;
                    IF (NOT %s) THEN
                        response_rows := (EXECUTE IMMEDIATE :response_sql
                            USING (boundary_id, signature_value, mode, data_id, data_id, data_id));
                    END IF;
                    RETURN TABLE(response_rows);
                END;
                """.formatted(delta ? "INCREMENTAL" : "FULL", literal(dataSql), literal(responseSql), literal(fullOnlySql), signatureSql,
                informationSchema, prefix, prefix, prefix, prefix, prefix, prefix, fullOnly ? "TRUE" : "FALSE", signatureSql, fullOnly ? "TRUE" : "FALSE");
        // Quote the body too: a valid source identifier can itself contain the dollar delimiter.
        return """
                WITH refresh_cache AS PROCEDURE (
                    previous_id VARCHAR, previous_signature VARCHAR, source_schema VARCHAR, source_name VARCHAR, expected_signature VARCHAR)
                RETURNS TABLE() LANGUAGE SQL AS %s
                CALL refresh_cache(?, ?, ?, ?, ?)
                """.formatted(literal(body));
    }

    private final class PendingRefresh implements AutoCloseable {
        private final TableCacheStore store;
        private boolean delta;
        private final long started;
        private final Map<String, Record> upserts = new HashMap<>();
        private final Set<String> deletes = new LinkedHashSet<>();
        private RecordSchema schema;
        private TableCacheStore.BulkLoad load;
        private CacheCheckpoint checkpoint;
        private long received;

        private PendingRefresh(final TableCacheStore store, final long started) {
            this.store = store;
            this.started = started;
        }

        private void read(final ResultSet rows) throws SQLException {
            schema = schema(rows.getMetaData());
            if (delta && !schema.getFieldNames().equals(store.schema().getFieldNames())) {
                throw new SQLException("Refresh response schema differs from the cached schema");
            }
            if (!delta && !allColumns && !schema.getFieldNames().equals(fullProjection())) {
                throw new SQLException("Refresh response differs from the configured projection");
            }
            load = delta ? null : store.beginBulkLoad(schema);
            long expected = -1;
            while (rows.next()) {
                final String kind = rows.getString(1);
                if ("CONTROL".equals(kind)) {
                    if (checkpoint != null || !(delta ? "INCREMENTAL" : "FULL").equals(rows.getString(4))) {
                        throw new SQLException("Invalid or duplicate refresh control row");
                    }
                    try {
                        checkpoint = new CacheCheckpoint(rows.getString(2), rows.getString(3));
                    } catch (final IllegalArgumentException | NullPointerException exception) {
                        throw new SQLException("Invalid refresh checkpoint", exception);
                    }
                    try {
                        final java.math.BigDecimal count = rows.getBigDecimal(5);
                        expected = count == null ? -1 : count.longValueExact();
                    } catch (final ArithmeticException exception) {
                        throw new SQLException("Invalid refresh row count", exception);
                    }
                    if (expected < 0) {
                        throw new SQLException("Invalid refresh row count");
                    }
                    for (int column = PREFIX_COLUMNS; column <= rows.getMetaData().getColumnCount(); column++) {
                        if (rows.getObject(column) != null) {
                            throw new SQLException("Control row contains source data");
                        }
                    }
                } else if ("DATA".equals(kind)) {
                    for (int column = 2; column < PREFIX_COLUMNS; column++) {
                        if (rows.getObject(column) != null) {
                            throw new SQLException("Data row contains control metadata");
                        }
                    }
                    final Record record = record(rows, schema);
                    final String key = key(record);
                    final String action = rows.getString(6);
                    if ("DELETE".equals(action) && delta) {
                        deletes.add(key);
                    } else if ("INSERT".equals(action)) {
                        if (delta) {
                            if (upserts.putIfAbsent(key, record) != null) {
                                throw new SQLException("Change rows contain duplicate lookup keys");
                            }
                        } else {
                            load.put(key, record);
                        }
                    } else {
                        throw new SQLException("Invalid refresh row action");
                    }
                    received++;
                } else {
                    throw new SQLException("Invalid refresh row kind");
                }
            }
            if (checkpoint == null || received != expected) {
                throw new SQLException("Incomplete refresh response; cache checkpoint not advanced");
            }
        }

        private RefreshResult commit() {
            if (delta) {
                deletes.removeAll(upserts.keySet());
                store.applyChanges(schema, upserts, deletes, checkpoint);
            } else {
                load.commit(checkpoint);
            }
            return new RefreshResult(delta ? upserts.size() : received, deletes.size(), System.currentTimeMillis() - started, delta, store.describe());
        }

        @Override
        public void close() {
            if (load != null) {
                load.close();
            }
        }
    }

    private List<String> fullProjection() {
        if (allColumns) {
            return List.of();
        }
        final Set<String> columns = new LinkedHashSet<>(keys);
        columns.addAll(values);
        return List.copyOf(columns);
    }

    private static String literal(final String value) {
        return "'%s'".formatted(value.replace("\\", "\\\\").replace("'", "''"));
    }

    private RecordSchema schema(final ResultSetMetaData metadata) throws SQLException {
        final List<RecordField> fields = new ArrayList<>();
        final Set<String> names = new LinkedHashSet<>();
        for (int column = PREFIX_COLUMNS + 1; column <= metadata.getColumnCount(); column++) {
            final String label = metadata.getColumnLabel(column);
            if (label == null || !names.add(label)) {
                throw new SQLException("Refresh response has missing or duplicate column names");
            }
            fields.add(new RecordField(label, RecordFieldType.STRING.getDataType()));
        }
        if (fields.isEmpty() || !names.containsAll(keys)) {
            throw new SQLException("Refresh response is missing configured key columns");
        }
        return new SimpleRecordSchema(fields);
    }

    private static Record record(final ResultSet rows, final RecordSchema schema) throws SQLException {
        final Map<String, Object> values = new HashMap<>();
        final List<String> names = schema.getFieldNames();
        for (int index = 0; index < names.size(); index++) {
            values.put(names.get(index), rows.getString(PREFIX_COLUMNS + index + 1));
        }
        return new MapRecord(schema, values);
    }

    private String key(final Record record) throws SQLException {
        final List<String> parts = new ArrayList<>();
        for (final String column : keys) {
            final String value = record.getAsString(column);
            if (value == null) {
                throw new SQLException("Source keys must be non-null");
            }
            parts.add(value);
        }
        return parts.size() == 1 ? parts.getFirst() : Base64.getEncoder().encodeToString(RowCodec.encode(parts));
    }
}
