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
import org.tomlj.Toml;
import org.tomlj.TomlParseResult;
import org.tomlj.TomlTable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

/** Test-only JDBC service. Live tests require an explicit connection name. */
class SnowflakeConfigConnectionService extends AbstractControllerService implements DBCPService {
    static final String CONNECTION_ENV = "SNOWFLAKE_IT_CONNECTION";
    private final String jdbcUrl;
    private final Properties properties = new Properties();

    SnowflakeConfigConnectionService() {
        final Map<String, String> settings = resolveSettings();
        final String account = require(settings, "account");
        jdbcUrl = "jdbc:snowflake://%s/".formatted(settings.getOrDefault("host", account + ".snowflakecomputing.com"));
        properties.put("account", account);
        properties.put("user", require(settings, "user"));
        put("db", require(settings, "database"));
        put("schema", require(settings, "schema"));
        put("warehouse", require(settings, "warehouse"));
        put("role", require(settings, "role"));
        final String authenticator = settings.getOrDefault("authenticator", "SNOWFLAKE");
        put("authenticator", authenticator);
        if ("SNOWFLAKE_JWT".equalsIgnoreCase(authenticator)) {
            put("private_key_file", expandHome(require(settings, "private_key_file")));
            put("private_key_file_pwd", settings.get("private_key_file_pwd"));
        } else {
            put("password", settings.get("password"));
        }
    }

    @Override
    public Connection getConnection() throws ProcessException {
        try {
            return DriverManager.getConnection(jdbcUrl, properties);
        } catch (final SQLException exception) {
            throw new ProcessException("Failed to connect to the integration-test account", exception);
        }
    }

    private static Map<String, String> resolveSettings() {
        final Path config = Path.of(expandHome("~/.snowflake/config.toml"));
        final Map<String, String> settings = new HashMap<>();
        if (Files.exists(config)) {
            try {
                settings.putAll(readConnection(config, System.getenv(CONNECTION_ENV)));
            } catch (final IOException exception) {
                throw new IllegalStateException("Cannot read the named integration-test connection", exception);
            }
        }
        for (final String key : List.of("account", "user", "host", "authenticator", "password", "private_key_file",
                "private_key_file_pwd", "database", "schema", "warehouse", "role")) {
            final String value = System.getenv("SNOWFLAKE_IT_" + key.toUpperCase(Locale.ROOT));
            if (value != null && !value.isBlank()) {
                settings.put(key, value);
            }
        }
        return settings;
    }

    static Map<String, String> readConnection(final Path config, final String name) throws IOException {
        final TomlParseResult parsed = Toml.parse(config);
        if (parsed.hasErrors()) {
            // Parser diagnostics can contain the input, including secrets.
            throw new IllegalArgumentException("Invalid TOML in the integration-test connection file");
        }
        final TomlTable table = parsed.getTable(List.of("connections", name));
        final Map<String, String> values = new HashMap<>();
        if (table != null) {
            for (final String key : table.keySet()) {
                final Object value = table.get(List.of(key));
                if (value instanceof String stringValue) {
                    values.put(key, stringValue);
                }
            }
        }
        return values;
    }

    private static String require(final Map<String, String> values, final String key) {
        final String value = values.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Missing integration-test setting: " + key);
        }
        return value;
    }

    private void put(final String key, final String value) {
        if (value != null && !value.isBlank()) {
            properties.put(key, value);
        }
    }

    private static String expandHome(final String path) {
        return path.startsWith("~/") ? System.getProperty("user.home") + path.substring(1) : path;
    }
}
