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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestSnowflakeIdentifier {
    @Test
    void fullyQualifiedSourceNormalizesUnquotedNames() {
        assertEquals(List.of("DB", "PUBLIC", "REF"), SnowflakeIdentifier.parse(" db . public . ref ", '.'));
        assertEquals(List.of("KEY", "VALUE$1", "_PRIVATE"), SnowflakeIdentifier.parse("key, value$1, _private", ','));
    }

    @Test
    void quotedSourcePreservesCaseDotsCommasAndDoubledQuotes() {
        assertEquals(List.of("Db.Name", "s,chema", "ta\"ble"),
                SnowflakeIdentifier.parse("\"Db.Name\" . \"s,chema\" . \"ta\"\"ble\"", '.'));
        assertEquals(List.of("KEY", "mixed.Case", "value,with,commas", "a\"b"),
                SnowflakeIdentifier.parse("key, \"mixed.Case\", \"value,with,commas\", \"a\"\"b\"", ','));
    }

    @Test
    void quotedWhitespaceAndExpressionLikeTextRemainLiteralNames() {
        assertEquals(List.of(" with spaces ", "x); DROP TABLE ref; --"),
                SnowflakeIdentifier.parse("\" with spaces \", \"x); DROP TABLE ref; --\"", ','));
        assertEquals(List.of("lower", "LOWER"), SnowflakeIdentifier.parse("\"lower\",lower", ','));
    }

    @Test
    void quoteEscapesEveryEmbeddedQuoteAndRoundTrips() {
        final String name = "a\"b\".c,d'\\";
        assertEquals("\"a\"\"b\"\".c,d'\\\"", SnowflakeIdentifier.quote(name));
        assertEquals(List.of(name), SnowflakeIdentifier.parse(SnowflakeIdentifier.quote(name), '.'));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "\"\"", "1key", "a b", "a,b", "db..ref", ".db", "db.", "\"a", "a\"",
            "\"a\"b", "a\"b\"", "\"a\" \"b\"", "COUNT(*)", "a + b", "a AS b", "a;DROP TABLE x", "a--comment", "a/*comment*/", "\"a\u0000b\""})
    void rejectsInvalidIdentifiersAndExpressions(final String input) {
        assertThrows(IllegalArgumentException.class, () -> SnowflakeIdentifier.parse(input, '.'));
    }

    @ParameterizedTest
    @ValueSource(strings = {"a,,b", ",a", "a,", "a, SUM(b)", "a, b c", "a, *", "a, \"\""})
    void columnListsRejectEmptyMembersAndExpressions(final String input) {
        assertThrows(IllegalArgumentException.class, () -> SnowflakeIdentifier.parse(input, ','));
    }

    @ParameterizedTest
    @ValueSource(strings = {"REF", "PUBLIC.REF", "A.B.C.D", "DB..REF", "DB.PUBLIC.REF WHERE 1=1"})
    void refreshRequiresExactlyThreeSourceParts(final String input) {
        assertThrows(IllegalArgumentException.class,
                () -> new AnonymousTableRefresh(input, List.of("K"), List.of("V"), false, true));
    }
}
