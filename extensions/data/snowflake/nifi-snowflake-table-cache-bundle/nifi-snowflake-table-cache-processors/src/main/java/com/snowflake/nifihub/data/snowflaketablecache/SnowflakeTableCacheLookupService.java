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
import org.apache.nifi.serialization.record.Record;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
            .description("Fully qualified table or view to materialize, for example MY_DB.MY_SCHEMA.REFERENCE_DATA. "
                    + "Supports standard and hybrid tables, views, native dynamic tables in FULL/INCREMENTAL mode, and Snowflake-managed Iceberg v2/v3 tables. "
                    + "Temporary, external, externally managed Iceberg, dynamic Iceberg and materialized views are not qualified. "
                    + "Double-quote identifiers that require case preservation or contain punctuation.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(true)
            .build();

    static final PropertyDescriptor KEY_COLUMNS = new PropertyDescriptor.Builder()
            .name("Key Columns")
            .description("Comma-separated lookup key column(s). These become the required "
                    + "coordinate keys of the lookup service, so a LookupRecord Record Path must "
                    + "supply a value for each. Unquoted names are uppercase; double-quote mixed-case names. Multiple columns form a composite key.")
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
                    + "Transient refresh failures retain validated data with no age limit. Source changes block lookups until reload. Incremental Refresh controls "
                    + "whether refreshes read changes or the full table.")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("1 hour")
            .required(true)
            .build();

    static final PropertyDescriptor INCREMENTAL_REFRESH = new PropertyDescriptor.Builder()
            .name("Incremental Refresh")
            .description("Allow incremental reads on qualified tables using Snowflake's CHANGES clause. "
                    + "Views, hybrid tables and FULL-refresh dynamic tables always receive full reloads, even when true. "
                    + "False forces full reloads for every source. Incremental reads require owner-configured change tracking and retained history. Inserts, updates and deletes "
                    + "are all applied. Every node keeps its own query-ID checkpoint. If a checkpoint falls outside the table's time travel "
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
    private volatile boolean stopping;
    private volatile boolean serving;
    private boolean reloadRequired;
    private AnonymousTableRefresh reader;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @OnEnabled
    public synchronized void onEnabled(final ConfigurationContext context) throws TableCacheException {
        stopping = false;
        serving = false;
        reloadRequired = false;
        dbcpService = context.getProperty(SNOWFLAKE_CONNECTION_SERVICE).asControllerService(DBCPService.class);
        sourceTable = context.getProperty(SOURCE_TABLE).evaluateAttributeExpressions().getValue().trim();
        keyColumns = SnowflakeIdentifier.parse(context.getProperty(KEY_COLUMNS).getValue(), ',');
        final String rawValueColumns = context.getProperty(VALUE_COLUMNS).getValue().trim();
        selectAll = "*".equals(rawValueColumns);
        valueColumns = selectAll ? List.of() : SnowflakeIdentifier.parse(rawValueColumns, ',');
        incremental = context.getProperty(INCREMENTAL_REFRESH).asBoolean();
        reader = new AnonymousTableRefresh(sourceTable, keyColumns, valueColumns, selectAll, incremental);
        store = createStore(context);

        final long intervalMillis = context.getProperty(REFRESH_INTERVAL)
                .asTimePeriod(TimeUnit.MILLISECONDS);

        try {
            // A persistent cache may resume incrementally, but is not served before source validation.
            final RefreshResult result = refresh();
            getLogger().info("Loaded {} ({}) with {} rows in {} ms into {}",
                    sourceTable, result.mode(), result.rowsUpserted(), result.durationMillis(), result.storeDescription());
        } catch (final TableCacheException e) {
            getLogger().error("Initial validation of {} failed; lookups will fail until a refresh succeeds{}", sourceTable,
                    intervalMillis > 0 ? "" : " (Refresh Interval is 0, so none is scheduled)", e);
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
                getLogger().error("Failed to refresh cache for {}; previous contents retained, lookup availability: {}",
                        sourceTable, serving, t);
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
            final List<String> identity = new ArrayList<>(List.of("table-cache-v2", dbcpService.getIdentifier(), sourceTable));
            identity.add(Base64.getEncoder().encodeToString(RowCodec.encode(keyColumns)));
            identity.add(selectAll ? "*" : Base64.getEncoder().encodeToString(RowCodec.encode(valueColumns)));
            identity.add(Boolean.toString(incremental));
            return new RocksDbTableCacheStore(Paths.get(directory), RowCodec.encode(identity));
        } catch (final IOException e) {
            throw new TableCacheException("Cannot open embedded store: check permissions, exclusive use and configuration identity. "
                    + "Preserve existing files; use a fresh directory when changing the source.", e);
        }
    }

    @OnDisabled
    public void onDisabled() {
        stopping = true;
        serving = false;
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

        // A JDBC driver can ignore interruption. Never close JNI handles while refresh still uses them.
        synchronized (this) {
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
    }

    // ---------- refresh ----------

    /**
     * Refresh the cache. Transient errors preserve serving eligibility; detected source discontinuity
     * requires a full reload before any more lookups. Checkpoint and rows are published together.
     *
     * <p>Synchronized so a scheduled refresh and any other caller serialize. Two concurrent loads
     * would both claim the same generation number and collide.
     */
    synchronized RefreshResult refresh() throws TableCacheException {
        final TableCacheStore target = store;
        if (target == null || stopping) {
            throw new TableCacheException("Service is not enabled");
        }

        final boolean delta = incremental && target.isHydrated() && !reloadRequired;
        final boolean recovering = reloadRequired;
        try {
            RefreshResult result;
            try {
                result = reloadRequired ? reader.reload(dbcpService, target) : reader.refresh(dbcpService, target, delta);
            } catch (final SQLException exception) {
                final boolean changed = sourceChanged(exception);
                if (changed) {
                    serving = false;
                    reloadRequired = true;
                }
                if (recovering || !(changed || delta && historyUnavailable(exception))) {
                    throw exception;
                }
                getLogger().warn("Source continuity requires a full reload for {}", sourceTable);
                result = reader.reload(dbcpService, target);
            }
            reloadRequired = false;
            serving = !stopping;
            return result;
        } catch (final SQLException exception) {
            if (sourceChanged(exception)) {
                serving = false;
                reloadRequired = true;
            }
            throw new TableCacheException("Failed to refresh cache from %s".formatted(sourceTable), exception);
        } catch (final RuntimeException exception) {
            throw new TableCacheException("Failed to refresh cache from %s".formatted(sourceTable), exception);
        }
    }

    private static boolean sourceChanged(final SQLException exception) {
        for (SQLException current = exception; current != null; current = current.getNextException()) {
            final String message = String.valueOf(current.getMessage());
            if (Math.abs(current.getErrorCode()) == 20001 || Math.abs(current.getErrorCode()) == 20002
                    || message.contains("TABLE_CACHE_SOURCE_CHANGED") || message.contains("TABLE_CACHE_INVALID_SOURCE")) {
                return true;
            }
        }
        return false;
    }

    private static boolean historyUnavailable(final SQLException exception) {
        final String message = String.valueOf(exception.getMessage()).toLowerCase(Locale.ROOT);
        return message.contains("time travel data is not available")
                || message.contains("statement") && (message.contains("not found") || message.contains("does not exist"));
    }

    /** Entries currently visible to lookups. */
    long size() {
        final TableCacheStore current = store;
        return current == null ? 0L : current.size();
    }

    /** False until the first successful load. Lookups fail closed while false. */
    boolean isHydrated() {
        final TableCacheStore current = store;
        return !stopping && serving && current != null && current.isHydrated();
    }

    /** Visible for testing: the refresh marker, resumable only for effective incremental sources. */
    CacheCheckpoint checkpointForTest() {
        final TableCacheStore current = store;
        return current == null ? null : current.checkpoint();
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
        if (stopping || !serving || !current.isHydrated()) {
            throw new LookupFailureException("Cache for %s is not validated yet".formatted(sourceTable));
        }

        final List<String> parts = new ArrayList<>(keyColumns.size());
        for (final String column : keyColumns) {
            final Object value = coordinates.get(column);
            if (value == null) {
                throw new LookupFailureException("Lookup coordinate '" + column + "' is required but was not supplied");
            }
            parts.add(value.toString());
        }
        try {
            return current.get(encodeKey(parts));
        } catch (final RuntimeException e) {
            throw new LookupFailureException("Cache storage lookup failed", e);
        }
    }

    @Override
    public Set<String> getRequiredKeys() {
        final List<String> columns = keyColumns;
        return columns == null ? Collections.emptySet() : new LinkedHashSet<>(columns);
    }

    // ---------- internals ----------

    private static String encodeKey(final List<String> parts) {
        return parts.size() == 1 ? parts.getFirst() : Base64.getEncoder().encodeToString(RowCodec.encode(parts));
    }
}
