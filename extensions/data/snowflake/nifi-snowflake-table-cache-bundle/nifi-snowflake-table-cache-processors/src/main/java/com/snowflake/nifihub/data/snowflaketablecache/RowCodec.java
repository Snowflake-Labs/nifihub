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

import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Length-prefixed encoding and detached records for the cache's string/null values.
 *
 * <p>Format: {@code [int count]} then, per value, {@code [int length][utf8 bytes]} with a length of
 * {@code -1} meaning null.
 *
 * <p>Field names are stored once per schema, not per encoded row.
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
        return decode(encoded, -1);
    }

    static List<String> decode(final byte[] encoded, final int expectedCount) {
        final ByteBuffer buffer = ByteBuffer.wrap(encoded);
        if (buffer.remaining() < Integer.BYTES) {
            throw new IllegalArgumentException("Row value count is missing");
        }
        final int count = buffer.getInt();
        if (expectedCount >= 0 && count != expectedCount) {
            throw new IllegalArgumentException("Row value count does not match expected count");
        }
        if (count < 0 || count > buffer.remaining() / Integer.BYTES) {
            throw new IllegalArgumentException("Row value count is invalid");
        }
        final List<String> values = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            if (buffer.remaining() < Integer.BYTES) {
                throw new IllegalArgumentException("Row value length is missing");
            }
            final int length = buffer.getInt();
            if (length == -1) {
                values.add(null);
            } else {
                if (length < 0 || length > buffer.remaining()) {
                    throw new IllegalArgumentException("Row value length is invalid");
                }
                final ByteBuffer bytes = buffer.slice();
                bytes.limit(length);
                try {
                    values.add(StandardCharsets.UTF_8.newDecoder()
                            .onMalformedInput(CodingErrorAction.REPORT)
                            .onUnmappableCharacter(CodingErrorAction.REPORT)
                            .decode(bytes).toString());
                } catch (final CharacterCodingException e) {
                    throw new IllegalArgumentException("Row value is not valid UTF-8", e);
                }
                buffer.position(buffer.position() + length);
            }
        }
        if (buffer.hasRemaining()) {
            throw new IllegalArgumentException("Row has trailing bytes");
        }
        return values;
    }

    static List<String> fieldNames(final RecordSchema schema) {
        Objects.requireNonNull(schema, "Schema is required");
        for (final RecordField field : schema.getFields()) {
            if (!RecordFieldType.STRING.getDataType().equals(field.getDataType())) {
                throw new IllegalArgumentException("Cache schema fields must be strings");
            }
        }
        return fieldNames(schema.getFieldNames());
    }

    static List<String> fieldNames(final List<String> names) {
        if (names.isEmpty()) {
            throw new IllegalArgumentException("Cache schema must not be empty");
        }
        final Set<String> unique = new HashSet<>();
        for (final String name : names) {
            if (name == null || name.isEmpty() || !unique.add(name)) {
                throw new IllegalArgumentException("Cache field names must be nonempty and unique");
            }
        }
        return List.copyOf(names);
    }

    static RecordSchema schema(final List<String> fieldNames) {
        final List<RecordField> fields = new ArrayList<>(fieldNames.size());
        for (final String name : fieldNames) {
            fields.add(new RecordField(name, RecordFieldType.STRING.getDataType()));
        }
        return new SimpleRecordSchema(fields);
    }

    static List<String> values(final List<String> fieldNames, final Record record) {
        if (record == null || !fieldNames.equals(record.getSchema().getFieldNames())) {
            throw new IllegalArgumentException("Row schema does not match cache schema");
        }
        final List<String> values = new ArrayList<>(fieldNames.size());
        for (final String name : fieldNames) {
            final Object value = record.getValue(name);
            if (value != null && !(value instanceof String)) {
                throw new IllegalArgumentException("Cache values must be strings or null");
            }
            values.add((String) value);
        }
        // Unlike List.copyOf, this preserves SQL null values.
        return Collections.unmodifiableList(values);
    }

    static Record record(final List<String> fieldNames, final List<String> values) {
        if (fieldNames.size() != values.size()) {
            throw new IllegalArgumentException("Row value count does not match cache schema");
        }
        final Map<String, Object> asMap = new HashMap<>(fieldNames.size());
        for (int index = 0; index < fieldNames.size(); index++) {
            asMap.put(fieldNames.get(index), values.get(index));
        }
        return new MapRecord(schema(fieldNames), asMap);
    }

    private static void writeInt(final ByteArrayOutputStream out, final int value) {
        out.write((value >>> 24) & 0xff);
        out.write((value >>> 16) & 0xff);
        out.write((value >>> 8) & 0xff);
        out.write(value & 0xff);
    }
}
