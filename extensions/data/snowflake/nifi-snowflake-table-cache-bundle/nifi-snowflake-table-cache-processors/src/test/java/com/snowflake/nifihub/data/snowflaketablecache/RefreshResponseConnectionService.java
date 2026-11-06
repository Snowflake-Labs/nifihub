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

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Scripted ordinary JDBC responses, not a Snowflake SQL interpreter. Unsupported JDBC operations
 * fail immediately so tests cannot accidentally pass through an unimplemented driver path.
 */
final class RefreshResponseConnectionService extends AbstractControllerService implements DBCPService {
    private static final List<String> PREFIX = List.of("KIND", "QUERY_ID", "SIGNATURE", "MODE", "EXPECTED_ROWS", "ACTION");
    private static final List<String> SOURCE_COLUMNS = List.of("TABLE_TYPE", "IS_TEMPORARY", "IS_ICEBERG", "IS_DYNAMIC", "IS_HYBRID", "RETENTION_TIME", "SIGNATURE");
    private static final List<String> CAPABILITY_COLUMNS = List.of("database_name", "schema_name", "name", "refresh_mode", "catalog_name", "iceberg_table_format_version");

    private final Queue<Step> steps = new ConcurrentLinkedQueue<>();
    private final List<Call> calls = new CopyOnWriteArrayList<>();
    private final List<Call> metadataCalls = new CopyOnWriteArrayList<>();
    private volatile String sourceType = "standard";
    private volatile List<Object[]> sourceRows;
    private volatile List<Object[]> capabilityRows;
    private volatile SQLException metadataFailure;
    private volatile SQLException capabilityFailure;
    private volatile String lastSignature = "source-signature";
    private final AtomicInteger openedConnections = new AtomicInteger();
    private final AtomicInteger closedConnections = new AtomicInteger();
    private final AtomicInteger openedStatements = new AtomicInteger();
    private final AtomicInteger closedStatements = new AtomicInteger();
    private final AtomicInteger openedResults = new AtomicInteger();
    private final AtomicInteger closedResults = new AtomicInteger();
    private final AtomicInteger activeExecutions = new AtomicInteger();
    private final AtomicInteger maximumExecutions = new AtomicInteger();
    private volatile String closeFailure;
    private boolean autoCommit = true;

    void autoCommit(final boolean enabled) {
        autoCommit = enabled;
    }

    void failOnClose(final String resource) {
        closeFailure = resource;
    }

    void sourceType(final String type) {
        sourceType = type;
    }

    void metadataRows(final Object[]... rows) {
        sourceRows = Arrays.stream(rows).map(Object[]::clone).toList();
    }

    void capabilityRows(final Object[]... rows) {
        capabilityRows = Arrays.stream(rows).map(Object[]::clone).toList();
    }

    void metadataFailure(final SQLException failure) {
        metadataFailure = failure;
    }

    void capabilityFailure(final SQLException failure) {
        capabilityFailure = failure;
    }

    List<Call> metadataCalls() {
        return List.copyOf(metadataCalls);
    }

    int connectionCount() {
        return openedConnections.get();
    }

    record Call(String sql, Map<Integer, String> parameters) {
        boolean incremental() {
            return sql.contains("CHANGES(INFORMATION => DEFAULT)");
        }
    }

    record Response(List<String> fields, List<Object[]> rows, int failAfterRows) {
        Response {
            fields = List.copyOf(fields);
            rows = rows.stream().map(Object[]::clone).toList();
            for (final Object[] row : rows) {
                if (row.length != PREFIX.size() + fields.size()) {
                    throw new IllegalArgumentException("Fixture row width must match the protocol and payload columns");
                }
            }
        }
    }

    private record Step(Response response, SQLException failure, CountDownLatch entered, CountDownLatch release) {
    }

    static Response response(final List<String> fields, final Object[]... rows) {
        return new Response(fields, Arrays.asList(rows), -1);
    }

    static Response truncated(final Response response, final int afterRows) {
        return new Response(response.fields(), response.rows(), afterRows);
    }

    static Object[] control(final int payloadColumns, final String queryId, final String signature,
                            final String mode, final Long expectedRows) {
        final Object[] row = new Object[PREFIX.size() + payloadColumns];
        row[0] = "CONTROL";
        row[1] = queryId;
        row[2] = signature;
        row[3] = mode;
        row[4] = expectedRows;
        return row;
    }

    static Object[] data(final String action, final String... values) {
        final Object[] row = new Object[PREFIX.size() + values.length];
        row[0] = "DATA";
        row[5] = action;
        System.arraycopy(values, 0, row, PREFIX.size(), values.length);
        return row;
    }

    void enqueue(final Response response) {
        steps.add(new Step(response, null, null, null));
    }

    void enqueueFailure(final SQLException failure) {
        steps.add(new Step(null, failure, null, null));
    }

    void enqueueBlocked(final Response response, final CountDownLatch entered, final CountDownLatch release) {
        steps.add(new Step(response, null, entered, release));
    }

    List<Call> calls() {
        return List.copyOf(calls);
    }

    int remainingSteps() {
        return steps.size();
    }

    int maximumExecutions() {
        return maximumExecutions.get();
    }

    boolean resourcesClosed() {
        return openedConnections.get() == closedConnections.get()
                && openedStatements.get() == closedStatements.get()
                && openedResults.get() == closedResults.get();
    }

    @Override
    public Connection getConnection() {
        openedConnections.incrementAndGet();
        final AtomicBoolean closed = new AtomicBoolean();
        return proxy(Connection.class, (instance, method, arguments) -> switch (method.getName()) {
            case "getAutoCommit" -> autoCommit;
            case "prepareStatement" -> statement((String) arguments[0]);
            case "createStatement" -> capabilityStatement();
            case "close" -> closeOnce(closed, closedConnections, "connection");
            case "isClosed" -> closed.get();
            default -> throw unsupported(method.getName());
        });
    }

    private PreparedStatement statement(final String sql) {
        openedStatements.incrementAndGet();
        final boolean metadata = sql.startsWith("SELECT TABLE_TYPE,");
        final Map<Integer, String> parameters = new HashMap<>();
        final AtomicBoolean closed = new AtomicBoolean();
        return proxy(PreparedStatement.class, (instance, method, arguments) -> switch (method.getName()) {
            case "setString" -> {
                parameters.put((Integer) arguments[0], (String) arguments[1]);
                yield null;
            }
            case "executeQuery" -> metadata ? readMetadata(sql, parameters) : execute(sql, parameters);
            case "close" -> closeOnce(closed, closedStatements, metadata ? "metadataStatement" : "statement");
            case "isClosed" -> closed.get();
            default -> throw unsupported(method.getName());
        });
    }

    private ResultSet readMetadata(final String sql, final Map<Integer, String> parameters) throws SQLException {
        if (!parameters.keySet().equals(java.util.Set.of(1, 2))) {
            throw new AssertionError("Source preflight must bind schema and table only");
        }
        metadataCalls.add(new Call(sql, Collections.unmodifiableMap(new HashMap<>(parameters))));
        if (metadataFailure != null) {
            throw metadataFailure;
        }
        if (sourceRows != null) {
            return resultSet(SOURCE_COLUMNS, sourceRows, -1, "metadataResultSet");
        }
        // Keep the last signature for scripted SQL failures: looking ahead would reject preflight before
        // the intended anonymous-call failure. Explicit metadataRows overrides model preflight changes.
        // Never consume a refresh step or mirror corrupt protocol signatures into valid metadata.
        final Step next = steps.peek();
        if (next != null && next.response() != null) {
            for (final Object[] row : next.response().rows()) {
                if ("CONTROL".equals(row[0]) && row[2] instanceof String signature && !signature.isBlank()) {
                    lastSignature = signature;
                    break;
                }
            }
        }
        final Object[] row = {"BASE TABLE", "NO", "NO", "NO", "NO", 1L, lastSignature};
        switch (sourceType) {
            case "standard", "transient" -> {
                // Both native table variants expose the same capability fields read by preflight.
            }
            case "view" -> row[0] = "VIEW";
            case "hybrid" -> row[4] = "YES";
            case "dynamicFull", "dynamicIncremental", "dynamicAuto" -> row[3] = "YES";
            case "managedIceberg", "externalIceberg" -> row[2] = "YES";
            case "dynamicIceberg" -> {
                row[2] = "YES";
                row[3] = "YES";
            }
            case "noRetention" -> row[5] = 0L;
            case "temporary" -> row[1] = "YES";
            case "invalid" -> row[2] = null;
            case "external" -> row[0] = "EXTERNAL TABLE";
            case "missing" -> {
                return resultSet(SOURCE_COLUMNS, List.of(), -1, "metadataResultSet");
            }
            case "ambiguous" -> {
                return resultSet(SOURCE_COLUMNS, List.of(row, row), -1, "metadataResultSet");
            }
            default -> throw new IllegalArgumentException("Unknown source fixture %s".formatted(sourceType));
        }
        return resultSet(SOURCE_COLUMNS, Collections.singletonList(row), -1, "metadataResultSet");
    }

    private Statement capabilityStatement() {
        openedStatements.incrementAndGet();
        final AtomicBoolean closed = new AtomicBoolean();
        return proxy(Statement.class, (instance, method, arguments) -> switch (method.getName()) {
            case "executeQuery" -> readCapability((String) arguments[0]);
            case "close" -> closeOnce(closed, closedStatements, "metadataStatement");
            case "isClosed" -> closed.get();
            default -> throw unsupported(method.getName());
        });
    }

    private ResultSet readCapability(final String sql) throws SQLException {
        if ((!sql.startsWith("SHOW DYNAMIC TABLES IN SCHEMA ") && !sql.startsWith("SHOW ICEBERG TABLES IN SCHEMA "))
                || !sql.contains(" STARTS WITH '") || !sql.endsWith(" LIMIT 1")) {
            throw new AssertionError("Unexpected capability statement %s".formatted(sql));
        }
        final Call preflight = metadataCalls.stream().filter(call -> call.sql().startsWith("SELECT TABLE_TYPE,")).reduce((previous, current) -> current).orElseThrow();
        metadataCalls.add(new Call(sql, Map.of()));
        if (capabilityFailure != null) {
            throw capabilityFailure;
        }
        if (capabilityRows != null) {
            return resultSet(CAPABILITY_COLUMNS, capabilityRows, -1, "metadataResultSet");
        }
        final String database = preflight.sql().substring(preflight.sql().indexOf("FROM ") + 5, preflight.sql().indexOf(".INFORMATION_SCHEMA.TABLES"));
        final String refreshMode = switch (sourceType) {
            case "dynamicFull" -> "FULL";
            case "dynamicAuto" -> "AUTO";
            default -> "INCREMENTAL";
        };
        final Object[] row = {SnowflakeIdentifier.parse(database, '.').getFirst(), preflight.parameters().get(1), preflight.parameters().get(2),
                refreshMode, "externalIceberg".equals(sourceType) ? "EXTERNAL" : "SNOWFLAKE", "2"};
        return resultSet(CAPABILITY_COLUMNS, Collections.singletonList(row), -1, "metadataResultSet");
    }

    private ResultSet execute(final String sql, final Map<Integer, String> parameters) throws SQLException {
        if (!sql.startsWith("WITH refresh_cache AS PROCEDURE") || !parameters.keySet().equals(java.util.Set.of(1, 2, 3, 4, 5))) {
            throw new AssertionError("Anonymous refresh must bind checkpoint, signature, schema, table and expected signature");
        }
        calls.add(new Call(sql, Collections.unmodifiableMap(new HashMap<>(parameters))));
        final Step step = steps.poll();
        if (step == null) {
            throw new SQLException("No scripted refresh response remains", "08006");
        }
        final int active = activeExecutions.incrementAndGet();
        maximumExecutions.accumulateAndGet(active, Math::max);
        try {
            if (step.entered() != null) {
                step.entered().countDown();
                try {
                    if (!step.release().await(10, TimeUnit.SECONDS)) {
                        throw new SQLException("Test did not release blocked refresh");
                    }
                } catch (final InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    throw new SQLException("Interrupted fixture refresh", exception);
                }
            }
            if (step.failure() != null) {
                throw step.failure();
            }
            final List<String> labels = new ArrayList<>(PREFIX);
            labels.addAll(step.response().fields());
            return resultSet(labels, step.response().rows(), step.response().failAfterRows(), "resultSet");
        } finally {
            activeExecutions.decrementAndGet();
        }
    }

    private ResultSet resultSet(final List<String> labels, final List<Object[]> rows, final int failAfterRows, final String resource) {
        openedResults.incrementAndGet();
        final AtomicInteger cursor = new AtomicInteger(-1);
        final AtomicBoolean wasNull = new AtomicBoolean();
        final AtomicBoolean closed = new AtomicBoolean();
        final ResultSetMetaData metadata = proxy(ResultSetMetaData.class, (instance, method, arguments) -> switch (method.getName()) {
            case "getColumnCount" -> labels.size();
            case "getColumnLabel", "getColumnName" -> labels.get((Integer) arguments[0] - 1);
            case "getColumnType" -> (Integer) arguments[0] == 5 ? Types.BIGINT : Types.VARCHAR;
            default -> throw unsupported(method.getName());
        });
        return proxy(ResultSet.class, (instance, method, arguments) -> switch (method.getName()) {
            case "next" -> {
                final int next = cursor.incrementAndGet();
                if (failAfterRows >= 0 && next >= failAfterRows) {
                    throw new SQLException("Truncated JDBC response", "08006");
                }
                yield next < rows.size();
            }
            case "getString", "getLong", "getObject", "getBigDecimal" -> {
                final int column = arguments[0] instanceof Integer index ? index - 1 : labels.indexOf((String) arguments[0]);
                if (column < 0 || column >= labels.size()) {
                    throw new SQLException("Unknown fixture column %s".formatted(arguments[0]));
                }
                final Object value = rows.get(cursor.get())[column];
                wasNull.set(value == null);
                if ("getObject".equals(method.getName())) {
                    yield value;
                }
                if ("getBigDecimal".equals(method.getName())) {
                    yield value == null ? null : new BigDecimal(value.toString());
                }
                if ("getLong".equals(method.getName())) {
                    yield value == null ? 0L : ((Number) value).longValue();
                }
                yield value == null ? null : value.toString();
            }
            case "wasNull" -> wasNull.get();
            case "getMetaData" -> metadata;
            case "close" -> closeOnce(closed, closedResults, resource);
            case "isClosed" -> closed.get();
            default -> throw unsupported(method.getName());
        });
    }

    private Object closeOnce(final AtomicBoolean closed, final AtomicInteger counter, final String resource) throws SQLException {
        if (closed.compareAndSet(false, true)) {
            counter.incrementAndGet();
            if (resource.equals(closeFailure)) {
                throw new SQLException("Scripted %s close failure".formatted(resource), "08006");
            }
        }
        return null;
    }

    private static UnsupportedOperationException unsupported(final String method) {
        return new UnsupportedOperationException("JDBC fixture does not implement %s".formatted(method));
    }

    private static <T> T proxy(final Class<T> type, final InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, (instance, method, arguments) -> {
            if (method.getDeclaringClass() == Object.class) {
                return switch (method.getName()) {
                    case "toString" -> "Scripted %s".formatted(type.getSimpleName());
                    case "hashCode" -> System.identityHashCode(instance);
                    case "equals" -> instance == arguments[0];
                    default -> throw unsupported(method.getName());
                };
            }
            return handler.invoke(instance, method, arguments);
        }));
    }
}
