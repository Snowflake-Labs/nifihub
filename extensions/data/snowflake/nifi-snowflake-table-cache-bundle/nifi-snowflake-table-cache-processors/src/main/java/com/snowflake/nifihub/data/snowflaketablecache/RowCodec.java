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

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Length-prefixed encoding for a row's values.
 *
 * <p>Format: {@code [int count]} then, per value, {@code [int length][utf8 bytes]} with a length of
 * {@code -1} meaning null.
 *
 * <p>Length-prefixed rather than delimiter-separated on purpose: a delimiter can appear inside a
 * value, and silently splitting a routing destination in half is the kind of bug that surfaces as
 * mysterious mis-routing months later.
 *
 * <p>Field <em>names</em> are not stored per row — they come from the schema held by the store, so
 * they are not repeated 40 million times.
 */
final class RowCodec {

    private RowCodec() {
    }

    static byte[] encode(final List<String> values) {
        final ByteArrayOutputStream buffer = new ByteArrayOutputStream(64);
        writeInt(buffer, values.size());
        for (final String value : values) {
            if (value == null) {
                writeInt(buffer, -1);
            } else {
                final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
                writeInt(buffer, bytes.length);
                buffer.write(bytes, 0, bytes.length);
            }
        }
        return buffer.toByteArray();
    }

    static List<String> decode(final byte[] encoded) {
        final ByteBuffer buffer = ByteBuffer.wrap(encoded);
        final int count = buffer.getInt();
        final List<String> values = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            final int length = buffer.getInt();
            if (length < 0) {
                values.add(null);
            } else {
                final byte[] bytes = new byte[length];
                buffer.get(bytes);
                values.add(new String(bytes, StandardCharsets.UTF_8));
            }
        }
        return values;
    }

    private static void writeInt(final ByteArrayOutputStream out, final int value) {
        out.write((value >>> 24) & 0xff);
        out.write((value >>> 16) & 0xff);
        out.write((value >>> 8) & 0xff);
        out.write(value & 0xff);
    }
}
