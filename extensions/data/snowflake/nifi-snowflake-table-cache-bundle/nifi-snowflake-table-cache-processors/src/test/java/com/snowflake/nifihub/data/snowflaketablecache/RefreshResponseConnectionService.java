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

    private final Queue<Step> steps = new ConcurrentLinkedQueue<>();
    private final List<Call> calls = new CopyOnWriteArrayList<>();
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
            case "close" -> closeOnce(closed, closedConnections, "connection");
            case "isClosed" -> closed.get();
            default -> throw unsupported(method.getName());
        });
    }

    private PreparedStatement statement(final String sql) {
        openedStatements.incrementAndGet();
        final Map<Integer, String> parameters = new HashMap<>();
        final AtomicBoolean closed = new AtomicBoolean();
        return proxy(PreparedStatement.class, (instance, method, arguments) -> switch (method.getName()) {
            case "setString" -> {
                parameters.put((Integer) arguments[0], (String) arguments[1]);
                yield null;
            }
            case "executeQuery" -> execute(sql, parameters);
            case "close" -> closeOnce(closed, closedStatements, "statement");
            case "isClosed" -> closed.get();
            default -> throw unsupported(method.getName());
        });
    }

    private ResultSet execute(final String sql, final Map<Integer, String> parameters) throws SQLException {
        if (!parameters.keySet().equals(java.util.Set.of(1, 2, 3, 4))) {
            throw new AssertionError("Anonymous refresh must bind checkpoint, signature, schema and table");
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
            return resultSet(step.response());
        } finally {
            activeExecutions.decrementAndGet();
        }
    }

    private ResultSet resultSet(final Response response) {
        openedResults.incrementAndGet();
        final AtomicInteger cursor = new AtomicInteger(-1);
        final AtomicBoolean wasNull = new AtomicBoolean();
        final AtomicBoolean closed = new AtomicBoolean();
        final List<String> labels = new ArrayList<>(PREFIX);
        labels.addAll(response.fields());
        final ResultSetMetaData metadata = proxy(ResultSetMetaData.class, (instance, method, arguments) -> switch (method.getName()) {
            case "getColumnCount" -> labels.size();
            case "getColumnLabel", "getColumnName" -> labels.get((Integer) arguments[0] - 1);
            case "getColumnType" -> (Integer) arguments[0] == 5 ? Types.BIGINT : Types.VARCHAR;
            default -> throw unsupported(method.getName());
        });
        return proxy(ResultSet.class, (instance, method, arguments) -> switch (method.getName()) {
            case "next" -> {
                final int next = cursor.incrementAndGet();
                if (response.failAfterRows() >= 0 && next >= response.failAfterRows()) {
                    throw new SQLException("Truncated JDBC response", "08006");
                }
                yield next < response.rows().size();
            }
            case "getString", "getLong", "getObject", "getBigDecimal" -> {
                final Object value = response.rows().get(cursor.get())[(Integer) arguments[0] - 1];
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
            case "close" -> closeOnce(closed, closedResults, "resultSet");
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
