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

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.RecordLookupService;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * A locally-materialized cache of a Snowflake table, exposed as a {@link RecordLookupService}.
 *
 * <p>The table is held locally, avoiding a network query for each lookup.
 *
 * <p>Implements the standard lookup interface consumed by {@code LookupRecord}.
 *
 * <p>The cache refreshes itself on a schedule. Refresh deliberately lives here rather than in a
 * companion processor: a controller service is instantiated <b>once per node</b>, so every node
 * necessarily maintains its own copy. A refresh processor could be set to Primary Node Only, after
 * which the other nodes would serve stale data indefinitely with no error — a silent failure that
 * matters when the table drives an entitlement decision.
 */
@Tags({"snowflake", "lookup", "cache", "enrich", "record", "openflow", "table", "materialized"})
@CapabilityDescription("Materializes a Snowflake table into a local cache and serves it as a record "
        + "lookup service, refreshing on a schedule. Use in place of a per-message database query "
        + "when the lookup rate is high. Lookups fail until the first load completes, so a "
        + "partially-loaded cache is never served.")
public class SnowflakeTableCacheLookupService extends AbstractControllerService
        implements RecordLookupService {

    static final PropertyDescriptor SNOWFLAKE_CONNECTION_SERVICE = new PropertyDescriptor.Builder()
            .name("Snowflake Connection Service")
            .description("Database Connection Service used to read the source table.")
            .identifiesControllerService(DBCPService.class)
            .required(true)
            .build();

    static final PropertyDescriptor SOURCE_TABLE = new PropertyDescriptor.Builder()
            .name("Source Table")
            .description("Table or view to materialize, for example MY_DB.MY_SCHEMA.REFERENCE_DATA. "
                    + "A view is fine and is often the right place to put filtering.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(true)
            .build();

    static final PropertyDescriptor KEY_COLUMNS = new PropertyDescriptor.Builder()
            .name("Key Columns")
            .description("Comma-separated lookup key column(s). These become the required "
                    + "coordinate keys of the lookup service, so a LookupRecord Record Path must "
                    + "supply a value for each. Multiple columns form a composite key.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .build();

    static final PropertyDescriptor VALUE_COLUMNS = new PropertyDescriptor.Builder()
            .name("Value Columns")
            .description("Comma-separated columns to return, or * for every column in the table. "
                    + "Naming them explicitly keeps the cache smaller.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .defaultValue("*")
            .required(true)
            .build();

    static final PropertyDescriptor REFRESH_INTERVAL = new PropertyDescriptor.Builder()
            .name("Refresh Interval")
            .description("Delay between refreshes, measured after completion. Set to 0 to disable scheduled refresh. "
                    + "Refresh failures retain the previous data with no age limit. Incremental Refresh controls "
                    + "whether refreshes read changes or the full table.")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("1 hour")
            .required(true)
            .build();

    static final PropertyDescriptor INCREMENTAL_REFRESH = new PropertyDescriptor.Builder()
            .name("Incremental Refresh")
            .description("Refresh by reading only what changed since the last refresh, using "
                    + "Snowflake's CHANGES clause. Requires change tracking on the source: "
                    + "ALTER TABLE <table> SET CHANGE_TRACKING = TRUE. Inserts, updates and deletes "
                    + "are all applied. Every node keeps its own watermark, so this is safe on a "
                    + "cluster of any size. If a watermark falls outside the table's time travel "
                    + "retention - for example after a node has been down a long time - the next "
                    + "refresh automatically falls back to a full reload.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    static final AllowableValue STORAGE_HEAP = new AllowableValue("HEAP", "Heap",
            "Hold the table in JVM heap. Rebuilt on restart; size against the actual dataset and available heap.");

    static final AllowableValue STORAGE_EMBEDDED_KV = new AllowableValue("EMBEDDED_KV", "Embedded",
            "Hold the table in RocksDB on local disk. Can reuse existing data after restart when storage persists. "
                    + "Uses both native memory and disk; large incremental changes also use JVM heap.");

    static final PropertyDescriptor STORAGE_MODE = new PropertyDescriptor.Builder()
            .name("Storage Mode")
            .description("Where the materialized table is held. Choose based on row count.")
            .allowableValues(STORAGE_HEAP, STORAGE_EMBEDDED_KV)
            .defaultValue(STORAGE_HEAP.getValue())
            .required(true)
            .build();

    static final PropertyDescriptor STORAGE_DIRECTORY = new PropertyDescriptor.Builder()
            .name("Storage Directory")
            .description("Directory for the embedded database. Must be writable, and should be on "
                    + "persistent storage or the cache is rebuilt on every restart. Each service "
                    + "instance needs its own directory.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .dependsOn(STORAGE_MODE, STORAGE_EMBEDDED_KV)
            .required(true)
            .build();

    /** Composite key separator: not legal in a Snowflake identifier or a ITEM_ID. */
    private static final String KEY_SEPARATOR = "\u0000";

    /** Alias for the computed delete marker on an incremental read. */
    private static final String DELETE_FLAG = "__TABLE_CACHE_IS_DELETE";

    /**
     * A watermark is only ever a timestamp Snowflake itself produced, but it is interpolated into
     * SQL, so it is validated against this shape before use rather than trusted.
     */
    private static final java.util.regex.Pattern WATERMARK_PATTERN =
            java.util.regex.Pattern.compile("^[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9:.]{1,15}(\\s?[+-][0-9:]{2,5})?$");

    private static final List<PropertyDescriptor> PROPERTIES = List.of(
            SNOWFLAKE_CONNECTION_SERVICE, SOURCE_TABLE, KEY_COLUMNS, VALUE_COLUMNS,
            REFRESH_INTERVAL, INCREMENTAL_REFRESH, STORAGE_MODE, STORAGE_DIRECTORY);

    private volatile DBCPService dbcpService;
    private volatile String sourceTable;
    private volatile List<String> keyColumns;
    private volatile List<String> valueColumns;
    private volatile boolean selectAll;
    private volatile boolean incremental;
    private volatile TableCacheStore store;
    private volatile ScheduledExecutorService refreshExecutor;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws TableCacheException {
        dbcpService = context.getProperty(SNOWFLAKE_CONNECTION_SERVICE).asControllerService(DBCPService.class);
        sourceTable = context.getProperty(SOURCE_TABLE).evaluateAttributeExpressions().getValue().trim();
        keyColumns = splitColumns(context.getProperty(KEY_COLUMNS).getValue());
        final String rawValueColumns = context.getProperty(VALUE_COLUMNS).getValue().trim();
        selectAll = "*".equals(rawValueColumns);
        valueColumns = selectAll ? List.of() : splitColumns(rawValueColumns);
        incremental = context.getProperty(INCREMENTAL_REFRESH).asBoolean();
        store = createStore(context);

        final long intervalMillis = context.getProperty(REFRESH_INTERVAL)
                .asTimePeriod(TimeUnit.MILLISECONDS);

        if (store.isHydrated()) {
            // Embedded mode adopted an existing database, so a restarted node is already warm and
            // reads nothing. This is the point of on-disk storage.
            getLogger().info("Adopted existing cache of {} with {} rows from {}",
                    sourceTable, store.size(), store.describe());
        } else {
            try {
                final RefreshResult result = refresh();
                getLogger().info("Loaded {} with {} rows in {} ms into {}",
                        sourceTable, result.rowsUpserted(), result.durationMillis(), result.storeDescription());
            } catch (final TableCacheException e) {
                // Enable anyway rather than refusing to start. A controller service that fails to
                // enable stops the whole flow and needs manual intervention, so a transient source
                // outage during a node restart must not be fatal. Lookups fail closed until a
                // refresh succeeds, which is the safe answer, and the error raises a bulletin.
                getLogger().error("Initial load of {} failed; lookups will fail until a refresh "
                                + "succeeds{}", sourceTable,
                        intervalMillis > 0 ? "" : " (Refresh Interval is 0, so none is scheduled)", e);
            }
        }

        if (intervalMillis > 0) {
            startRefreshSchedule(intervalMillis);
        }
    }

    private void startRefreshSchedule(final long intervalMillis) {
        refreshExecutor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            final Thread thread = new Thread(runnable, "table-cache-refresh-" + getIdentifier());
            thread.setDaemon(true);
            return thread;
        });
        // Fixed DELAY, not fixed rate: the delay is measured from completion, so a reload that
        // overruns the interval simply postpones the next one instead of stacking refreshes.
        refreshExecutor.scheduleWithFixedDelay(() -> {
            try {
                final RefreshResult result = refresh();
                getLogger().debug("Refreshed {} ({}): {} upserted, {} deleted, {} ms",
                        sourceTable, result.mode(), result.rowsUpserted(), result.rowsDeleted(),
                        result.durationMillis());
            } catch (final Throwable t) {
                // Must not propagate: an exception escaping here cancels the schedule, which would
                // leave the cache frozen forever with no further attempts. Log a bulletin and retry
                // on the next tick. Previous contents remain visible in the meantime.
                getLogger().error("Failed to refresh cache for {}; previous contents retained",
                        sourceTable, t);
            }
        }, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
    }

    private TableCacheStore createStore(final ConfigurationContext context) throws TableCacheException {
        final String mode = context.getProperty(STORAGE_MODE).getValue();
        if (STORAGE_HEAP.getValue().equals(mode)) {
            return new HeapTableCacheStore();
        }
        final String directory = context.getProperty(STORAGE_DIRECTORY)
                .evaluateAttributeExpressions().getValue().trim();
        try {
            return new RocksDbTableCacheStore(Paths.get(directory));
        } catch (final IOException e) {
            throw new TableCacheException("Failed to open embedded store at " + directory
                    + ". The directory must be writable by the NiFi process.", e);
        }
    }

    @OnDisabled
    public void onDisabled() {
        final ScheduledExecutorService executor = refreshExecutor;
        refreshExecutor = null;
        if (executor != null) {
            executor.shutdownNow();
            try {
                executor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        final TableCacheStore current = store;
        store = null;
        if (current != null) {
            try {
                current.close();
            } catch (final Exception e) {
                getLogger().warn("Failed to close cache store", e);
            }
        }
    }

    // ---------- refresh ----------

    /**
     * Refresh the cache. Incremental when enabled and a usable watermark exists, otherwise a full
     * reload. On failure the previous contents remain visible.
     *
     * <p>Synchronized so a scheduled refresh and any other caller serialize. Two concurrent loads
     * would both claim the same generation number and collide.
     */
    synchronized RefreshResult refresh() throws TableCacheException {
        final TableCacheStore target = store;
        if (target == null) {
            throw new TableCacheException("Service is not enabled");
        }

        final String watermark = target.watermark();
        if (incremental && target.isHydrated() && watermark != null) {
            try {
                return refreshIncremental(target, watermark);
            } catch (final SQLException e) {
                // Most likely the watermark has aged out of the table's time travel retention, which
                // Snowflake reports as "Time travel data is not available". Falling back on any
                // failure is deliberate: a full reload is always correct, so self-healing beats
                // trying to classify the error and getting the classification wrong.
                getLogger().warn("Incremental refresh of {} failed, falling back to a full reload: {}",
                        sourceTable, e.getMessage());
            }
        }
        return refreshFull(target);
    }

    private RefreshResult refreshFull(final TableCacheStore target) throws TableCacheException {
        final long started = System.currentTimeMillis();
        final String sql = "SELECT " + projection() + " FROM " + sourceTable;
        long rows = 0;

        try (Connection connection = dbcpService.getConnection()) {
            // Capture the watermark BEFORE reading. Anything committed during the load is then
            // re-read by the next incremental pass, which is harmless because applies are
            // idempotent. Taking it afterwards could skip a change and never notice.
            final String watermark = serverTimestamp(connection);

            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(sql)) {

                final RecordSchema schema = schemaFor(resultSet.getMetaData());
                final List<String> fieldNames = schema.getFieldNames();

                try (TableCacheStore.BulkLoad load = target.beginBulkLoad(schema)) {
                    while (resultSet.next()) {
                        load.put(compositeKey(resultSet), readRecord(resultSet, schema, fieldNames));
                        rows++;
                    }
                    load.commit(watermark);
                }
            }
        } catch (final SQLException e) {
            throw new TableCacheException("Failed to load cache from " + sourceTable
                    + " using query [" + sql + "]", e);
        }

        return new RefreshResult(rows, 0L, System.currentTimeMillis() - started, false,
                target.describe());
    }

    private RefreshResult refreshIncremental(final TableCacheStore target, final String watermark)
            throws SQLException, TableCacheException {
        if (!WATERMARK_PATTERN.matcher(watermark).matches()) {
            throw new TableCacheException("Stored watermark is not a recognisable timestamp: " + watermark);
        }
        final RecordSchema schema = target.schema();
        if (schema == null) {
            throw new TableCacheException("Cache has no schema; a full load is required first");
        }
        final List<String> fieldNames = schema.getFieldNames();
        final long started = System.currentTimeMillis();

        // Project exactly the fields already stored, so the delta's schema matches the load's. On-disk
        // encoding is positional, so a differing column set would silently shift values.
        // The DELETE-half of an update pair is filtered out: it carries the OLD row state, and the
        // matching INSERT already carries the new one.
        final String sql = "SELECT " + String.join(", ", fieldNames)
                + ", METADATA$ACTION = 'DELETE' AS " + DELETE_FLAG
                + " FROM " + sourceTable
                + " CHANGES(INFORMATION => DEFAULT) AT(TIMESTAMP => '" + watermark + "'::TIMESTAMP_TZ)"
                + " WHERE NOT (METADATA$ACTION = 'DELETE' AND METADATA$ISUPDATE = TRUE)";

        final Map<String, Record> upserts = new HashMap<>();
        final Set<String> deletes = new LinkedHashSet<>();

        try (Connection connection = dbcpService.getConnection()) {
            final String nextWatermark = serverTimestamp(connection);

            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(sql)) {
                while (resultSet.next()) {
                    final String key = compositeKey(resultSet);
                    if (key == null) {
                        continue;
                    }
                    if (resultSet.getBoolean(DELETE_FLAG)) {
                        upserts.remove(key);
                        deletes.add(key);
                    } else {
                        deletes.remove(key);
                        upserts.put(key, readRecord(resultSet, schema, fieldNames));
                    }
                }
            }

            target.applyChanges(schema, upserts, deletes, nextWatermark);
        }

        return new RefreshResult(upserts.size(), deletes.size(),
                System.currentTimeMillis() - started, true, target.describe());
    }

    /** Both watermarks come from Snowflake, so client clock skew never enters the calculation. */
    private static String serverTimestamp(final Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT CURRENT_TIMESTAMP()")) {
            resultSet.next();
            return resultSet.getString(1);
        }
    }

    private Record readRecord(final ResultSet resultSet, final RecordSchema schema,
                              final List<String> fieldNames) throws SQLException {
        final Map<String, Object> values = new HashMap<>(fieldNames.size());
        for (final String fieldName : fieldNames) {
            values.put(fieldName, resultSet.getString(fieldName));
        }
        return new MapRecord(schema, values);
    }

    /** Entries currently visible to lookups. */
    long size() {
        final TableCacheStore current = store;
        return current == null ? 0L : current.size();
    }

    /** False until the first successful load. Lookups fail closed while false. */
    boolean isHydrated() {
        final TableCacheStore current = store;
        return current != null && current.isHydrated();
    }

    /** Visible for testing: the watermark the next incremental refresh would resume from. */
    String watermarkForTest() {
        final TableCacheStore current = store;
        return current == null ? null : current.watermark();
    }

    /** Visible for testing: the backing store. */
    TableCacheStore storeForTest() {
        return store;
    }

    // ---------- RecordLookupService ----------

    @Override
    public Optional<Record> lookup(final Map<String, Object> coordinates) throws LookupFailureException {
        final TableCacheStore current = store;
        if (current == null) {
            throw new LookupFailureException("Cache service is not enabled");
        }
        // Fail closed. An empty Optional would mean "no such key", which for an entitlement
        // dataset is a materially different answer from "the cache is not loaded yet".
        if (!current.isHydrated()) {
            throw new LookupFailureException("Cache for " + sourceTable + " is not loaded yet");
        }

        final StringBuilder key = new StringBuilder();
        for (int i = 0; i < keyColumns.size(); i++) {
            final String column = keyColumns.get(i);
            final Object value = coordinates.get(column);
            if (value == null) {
                throw new LookupFailureException("Lookup coordinate '" + column + "' is required but was not supplied");
            }
            if (i > 0) {
                key.append(KEY_SEPARATOR);
            }
            key.append(value);
        }
        return current.get(key.toString());
    }

    @Override
    public Set<String> getRequiredKeys() {
        final List<String> columns = keyColumns;
        return columns == null ? Collections.emptySet() : new LinkedHashSet<>(columns);
    }

    // ---------- internals ----------

    private String projection() {
        if (selectAll) {
            return "*";
        }
        final Set<String> columns = new LinkedHashSet<>(keyColumns);
        columns.addAll(valueColumns);
        return String.join(", ", columns);
    }

    private String compositeKey(final ResultSet resultSet) throws SQLException {
        if (keyColumns.size() == 1) {
            return resultSet.getString(keyColumns.get(0));
        }
        final List<String> parts = new ArrayList<>(keyColumns.size());
        for (final String column : keyColumns) {
            parts.add(resultSet.getString(column));
        }
        return String.join(KEY_SEPARATOR, parts);
    }

    /**
     * Every column is read as a string. Deliberate: it avoids a JDBC-to-record type mapping layer,
     * and string values are sufficient for the routing and entitlement cases this is aimed at.
     */
    private RecordSchema schemaFor(final ResultSetMetaData metaData) throws SQLException {
        final List<RecordField> fields = new ArrayList<>();
        final Set<String> seen = new LinkedHashSet<>();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            final String label = metaData.getColumnLabel(i);
            if (seen.add(label)) {
                fields.add(new RecordField(label, RecordFieldType.STRING.getDataType()));
            }
        }
        return new SimpleRecordSchema(fields);
    }

    private static List<String> splitColumns(final String raw) {
        return Arrays.stream(raw.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }
}
