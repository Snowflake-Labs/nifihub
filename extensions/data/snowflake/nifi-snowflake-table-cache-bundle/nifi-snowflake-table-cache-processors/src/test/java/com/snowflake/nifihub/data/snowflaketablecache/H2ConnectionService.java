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
import org.apache.nifi.processor.exception.ProcessException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * DBCPService backed by an in-memory H2 database.
 *
 * <p>Lets the whole component be exercised against real JDBC and real SQL with no Snowflake
 * connection. The v1 refresh query is plain {@code SELECT ... FROM <table>}, which H2 executes
 * identically to Snowflake — so these are genuine tests of the refresh path, not stubs. Once
 * Snowflake-specific SQL arrives (stream consumption, {@code METADATA$ACTION}, stage snapshots)
 * those paths move to integration tests against a live account.
 */
class H2ConnectionService extends AbstractControllerService implements DBCPService {

    private final String jdbcUrl;

    H2ConnectionService(final String databaseName) {
        this.jdbcUrl = "jdbc:h2:mem:" + databaseName + ";DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=TRUE";
    }

    @Override
    public Connection getConnection() throws ProcessException {
        try {
            return DriverManager.getConnection(jdbcUrl, "sa", "");
        } catch (final SQLException e) {
            throw new ProcessException(e);
        }
    }

    /** Convenience for test setup: run DDL/DML outside the component. */
    void execute(final String... statements) throws SQLException {
        try (Connection connection = DriverManager.getConnection(jdbcUrl, "sa", "");
             Statement statement = connection.createStatement()) {
            for (final String sql : statements) {
                statement.execute(sql);
            }
        }
    }
}
