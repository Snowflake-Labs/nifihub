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

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

/** Parses identifiers, never SQL expressions. Quoted identifiers retain their case. */
final class SnowflakeIdentifier {
    private static final Pattern UNQUOTED = Pattern.compile("[A-Za-z_][A-Za-z0-9_$]*");

    private SnowflakeIdentifier() {
    }

    static List<String> parse(final String text, final char separator) {
        final List<String> result = new ArrayList<>();
        final StringBuilder token = new StringBuilder();
        boolean quoted = false;
        for (int index = 0; index < text.length(); index++) {
            final char character = text.charAt(index);
            if (character == '"') {
                token.append(character);
                if (quoted && index + 1 < text.length() && text.charAt(index + 1) == '"') {
                    token.append(text.charAt(++index));
                } else {
                    quoted = !quoted;
                }
            } else if (character == separator && !quoted) {
                result.add(decode(token.toString()));
                token.setLength(0);
            } else {
                token.append(character);
            }
        }
        if (quoted) {
            throw new IllegalArgumentException("Unterminated quoted identifier");
        }
        result.add(decode(token.toString()));
        return List.copyOf(result);
    }

    static String quote(final String name) {
        return "\"%s\"".formatted(name.replace("\"", "\"\""));
    }

    private static String decode(final String token) {
        final String name = token.trim();
        if (UNQUOTED.matcher(name).matches()) {
            return name.toUpperCase(Locale.ROOT);
        }
        if (name.length() >= 2 && name.startsWith("\"") && name.endsWith("\"")) {
            final String contents = name.substring(1, name.length() - 1);
            if (!contents.isEmpty() && !contents.replace("\"\"", "").contains("\"") && contents.indexOf('\u0000') < 0) {
                return contents.replace("\"\"", "\"");
            }
        }
        throw new IllegalArgumentException("Expected a column or table identifier, not an SQL expression");
    }
}
