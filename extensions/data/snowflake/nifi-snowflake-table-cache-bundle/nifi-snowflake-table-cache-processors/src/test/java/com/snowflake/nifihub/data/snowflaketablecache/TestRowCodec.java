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
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestRowCodec {

    @Test
    void preservesV2EncodingIncludingEmptyAndNullValues() {
        final byte[] encoded = ByteBuffer.allocate(17).putInt(3).putInt(1).put((byte) 'a').putInt(0).putInt(-1).array();
        final List<String> values = Arrays.asList("a", "", null);
        assertArrayEquals(encoded, RowCodec.encode(values));
        assertEquals(values, RowCodec.decode(encoded));
        assertArrayEquals(new byte[4], RowCodec.encode(List.of()));
        assertEquals(List.of(), RowCodec.decode(new byte[4]));
    }

    @Test
    void roundTripsDelimitersAndUnicodeWithoutSplittingValues() {
        final List<String> values = Arrays.asList("\u0000|\n\t", "\u03bb\ud83c\udf0d", null, "");
        assertEquals(values, RowCodec.decode(RowCodec.encode(values)));
    }

    @ParameterizedTest
    @MethodSource("unexpectedCounts")
    void rejectsUnexpectedCountBeforeDecodingValues(final byte[] encoded) {
        final IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> RowCodec.decode(encoded, 2));
        assertEquals("Row value count does not match expected count", error.getMessage());
    }

    static Stream<Arguments> unexpectedCounts() {
        // Enough bytes for every length prefix; generic byte-size validation alone accepts this count.
        final byte[] padded = ByteBuffer.allocate(4 + 4096 * 4).putInt(4096).array();
        return Stream.of(Arguments.of((Object) padded), Arguments.of((Object) ByteBuffer.allocate(4).putInt(Integer.MAX_VALUE).array()));
    }

    @ParameterizedTest
    @MethodSource("invalidEncodings")
    void rejectsMalformedEncoding(final byte[] encoded) {
        assertThrows(IllegalArgumentException.class, () -> RowCodec.decode(encoded));
    }

    static Stream<Arguments> invalidEncodings() {
        final byte[] valid = RowCodec.encode(List.of("value"));
        return Stream.of(
                new byte[0],
                new byte[3],
                ByteBuffer.allocate(4).putInt(-1).array(),
                ByteBuffer.allocate(4).putInt(Integer.MAX_VALUE).array(),
                ByteBuffer.allocate(4).putInt(1).array(),
                ByteBuffer.allocate(8).putInt(2).putInt(0).array(),
                ByteBuffer.allocate(8).putInt(1).putInt(Integer.MAX_VALUE).array(),
                ByteBuffer.allocate(8).putInt(1).putInt(-2).array(),
                ByteBuffer.allocate(8).putInt(1).putInt(Integer.MIN_VALUE).array(),
                ByteBuffer.allocate(12).putInt(2).putInt(4).putInt(0).array(),
                ByteBuffer.allocate(9).putInt(1).putInt(1).put((byte) 0xff).array(),
                ByteBuffer.allocate(9).putInt(1).putInt(1).put((byte) 0xc2).array(),
                ByteBuffer.allocate(10).putInt(1).putInt(2).put((byte) 0xc0).put((byte) 0x80).array(),
                Arrays.copyOf(valid, valid.length - 1),
                Arrays.copyOf(valid, valid.length + 1),
                new byte[5]).map(encoded -> Arguments.of((Object) encoded));
    }
}
