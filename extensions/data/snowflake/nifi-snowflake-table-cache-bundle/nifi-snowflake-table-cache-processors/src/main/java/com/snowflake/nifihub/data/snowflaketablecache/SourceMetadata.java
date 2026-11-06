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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/** Resolves qualified source variants without treating arbitrary SQL failures as full-only capability. */
record SourceMetadata(String signature, boolean incremental) {
    static final String SIGNATURE = """
            SHA2(TO_JSON(ARRAY_CONSTRUCT(
            CURRENT_ACCOUNT(), CURRENT_USER(), CURRENT_ROLE(), TABLE_TYPE, IS_ICEBERG, IS_DYNAMIC, IS_HYBRID,
            TO_CHAR(CONVERT_TIMEZONE('UTC', CREATED), 'YYYY-MM-DD HH24:MI:SS.FF9'),
            TO_CHAR(CONVERT_TIMEZONE('UTC', LAST_DDL), 'YYYY-MM-DD HH24:MI:SS.FF9'), RETENTION_TIME)), 256)
            """.strip();

    static SourceMetadata read(final Connection connection, final List<String> source, final boolean requested) throws SQLException {
        return read(connection, source, requested, null);
    }

    static SourceMetadata read(final Connection connection, final List<String> source, final boolean requested, final CacheCheckpoint previous) throws SQLException {
        validateSession(connection);
        final String sql = """
                SELECT TABLE_TYPE, IS_TEMPORARY, IS_ICEBERG, IS_DYNAMIC, IS_HYBRID, RETENTION_TIME,
                       IFF(CREATED IS NOT NULL AND LAST_DDL IS NOT NULL, %s, NULL)
                FROM %s.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                """.formatted(SIGNATURE, SnowflakeIdentifier.quote(source.getFirst()));
        final String tableType;
        final boolean iceberg;
        final boolean dynamic;
        final boolean hybrid;
        final long retention;
        final String signature;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, source.get(1));
            statement.setString(2, source.get(2));
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) {
                    throw invalid("Source is missing or its metadata is not visible");
                }
                tableType = rows.getString(1);
                final String temporary = rows.getString(2);
                iceberg = flag(rows.getString(3));
                dynamic = flag(rows.getString(4));
                hybrid = flag(rows.getString(5));
                retention = rows.getLong(6);
                signature = rows.getString(7);
                if (!"NO".equals(temporary) || rows.next() || signature == null) {
                    throw invalid("Source must be unambiguous, non-temporary and have creation/DDL metadata");
                }
                if (previous != null && !previous.sourceSignature().equals(signature)) {
                    throw new SQLException("TABLE_CACHE_SOURCE_CHANGED: Source changed before capability validation", "P0001", -20001);
                }
            }
        }
        final boolean supportedIncremental;
        if ("VIEW".equals(tableType)) {
            supportedIncremental = false;
        } else if (!"BASE TABLE".equals(tableType)) {
            throw invalid("Source type %s is not qualified".formatted(tableType));
        } else if (hybrid) {
            supportedIncremental = false;
        } else if (dynamic) {
            if (iceberg) {
                throw invalid("Dynamic Iceberg tables are not yet qualified");
            }
            final String mode = properties(connection, source, "DYNAMIC", List.of("refresh_mode")).getFirst();
            if (!"FULL".equals(mode) && !"INCREMENTAL".equals(mode)) {
                throw invalid("Dynamic table refresh mode %s is not qualified".formatted(mode));
            }
            supportedIncremental = "INCREMENTAL".equals(mode);
        } else if (iceberg) {
            final List<String> properties = properties(connection, source, "ICEBERG", List.of("catalog_name", "iceberg_table_format_version"));
            final String catalog = properties.getFirst();
            if (!"SNOWFLAKE".equals(catalog)) {
                throw invalid("Externally managed Iceberg tables are not yet qualified");
            }
            final String version = properties.get(1);
            if (!"2".equals(version) && !"3".equals(version)) {
                throw invalid("Iceberg version %s is not qualified".formatted(version));
            }
            supportedIncremental = true;
        } else {
            supportedIncremental = true;
        }
        if (requested && supportedIncremental && retention < 1) {
            throw invalid("Incremental refresh requires at least one day of retained history");
        }
        return new SourceMetadata(signature, requested && supportedIncremental);
    }

    private static void validateSession(final Connection connection) throws SQLException {
        // Metadata equality is case-sensitive even when quoted SQL identifiers are folded to uppercase.
        // Inspect the borrowed session instead of changing a setting owned by the connection pool.
        try (Statement statement = connection.createStatement();
                ResultSet rows = statement.executeQuery("SHOW PARAMETERS LIKE 'QUOTED_IDENTIFIERS_IGNORE_CASE' IN SESSION")) {
            int matches = 0;
            while (rows.next()) {
                if ("QUOTED_IDENTIFIERS_IGNORE_CASE".equals(rows.getString("key"))) {
                    if (!"false".equalsIgnoreCase(rows.getString("value"))) {
                        throw new SQLException("Cache refresh requires QUOTED_IDENTIFIERS_IGNORE_CASE=FALSE");
                    }
                    matches++;
                }
            }
            if (matches != 1) {
                throw new SQLException("Cannot verify QUOTED_IDENTIFIERS_IGNORE_CASE for the borrowed session");
            }
        }
    }

    private static boolean flag(final String value) throws SQLException {
        if (!"YES".equals(value) && !"NO".equals(value)) {
            throw invalid("Source type metadata is incomplete");
        }
        return "YES".equals(value);
    }

    private static List<String> properties(final Connection connection, final List<String> source, final String type, final List<String> names) throws SQLException {
        // The exact name sorts before every longer name sharing its prefix; LIMIT 1 avoids
        // SHOW's result limit and STARTS WITH treats percent/underscore characters literally.
        final String sql = "SHOW %s TABLES IN SCHEMA %s.%s STARTS WITH %s LIMIT 1".formatted(type,
                SnowflakeIdentifier.quote(source.get(0)), SnowflakeIdentifier.quote(source.get(1)),
                "'%s'".formatted(source.get(2).replace("\\", "\\\\").replace("'", "''")));
        try (Statement statement = connection.createStatement(); ResultSet rows = statement.executeQuery(sql)) {
            final List<String> values = new ArrayList<>(names.size());
            int count = 0;
            while (rows.next()) {
                if (source.get(0).equals(rows.getString("database_name")) && source.get(1).equals(rows.getString("schema_name"))
                        && source.get(2).equals(rows.getString("name"))) {
                    for (final String name : names) {
                        values.add(rows.getString(name));
                    }
                    count++;
                }
            }
            if (count != 1 || values.contains(null)) {
                throw invalid("Source capability metadata is missing or ambiguous");
            }
            return values;
        }
    }

    private static SQLException invalid(final String reason) {
        return new SQLException("TABLE_CACHE_INVALID_SOURCE: %s".formatted(reason), "P0001", -20002);
    }
}
