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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestCacheCheckpointStores {

    private static final String QUERY_ID = "01bcdef0-1234-0000-abcd-0123456789ef";
    private static final CacheCheckpoint INITIAL = new CacheCheckpoint(QUERY_ID, "source-one");
    private static final CacheCheckpoint NEXT = new CacheCheckpoint("01bcdef1-1234-0000-abcd-0123456789ef", "source-one");
    private static final CacheCheckpoint REPLACEMENT = new CacheCheckpoint("01bcdef2-1234-0000-abcd-0123456789ef", "source-two");
    private static final RecordSchema SCHEMA = schema("VALUE");
    private static final RecordSchema REPLACEMENT_SCHEMA = schema("OTHER");
    private static final byte[] CHECKPOINT_KEY = metadataKey("checkpoint");

    @TempDir
    Path storageDirectory;

    @Test
    void acceptsHexadecimalQueryShapeWithoutRestrictingUuidVersion() {
        final String queryId = "ABCDEF01-FFFF-0000-0000-ABCDEF012345";
        final CacheCheckpoint checkpoint = new CacheCheckpoint(queryId, "scope\u0000with separators|and unicode \u03bb");
        assertEquals(queryId, checkpoint.queryId());
        assertEquals("scope\u0000with separators|and unicode \u03bb", checkpoint.sourceSignature());
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "\t\n", "query-id", "1-2-3-4-5", "01bcdef0-1234-0000-abcd-0123456789eg",
            "01bcdef0-1234-0000-abcd-0123456789ef ", " 01bcdef0-1234-0000-abcd-0123456789ef",
            "01bcdef0-1234-0000-abcd-0123456789ef\n"})
    void rejectsInvalidQueryId(final String queryId) {
        assertThrows(IllegalArgumentException.class, () -> new CacheCheckpoint(queryId, "source"));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "\t\n", "\u2003"})
    void rejectsBlankSourceSignature(final String sourceSignature) {
        assertThrows(IllegalArgumentException.class, () -> new CacheCheckpoint(QUERY_ID, sourceSignature));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void publishesRowsSchemaAndCheckpointTogether(final boolean embedded) throws Exception {
        try (final TableCacheStore store = store(embedded)) {
            assertFalse(store.isHydrated());
            assertNull(store.checkpoint());
            assertNull(store.schema());
            assertEquals(0, store.size());
            assertThrows(IllegalStateException.class, () -> store.applyChanges(SCHEMA, Map.of(), Set.of(), INITIAL));
            loadInitial(store);
            assertInitial(store);
            try (final TableCacheStore.BulkLoad load = store.beginBulkLoad(REPLACEMENT_SCHEMA)) {
                load.put("new", record(REPLACEMENT_SCHEMA, "replacement"));
                assertInitial(store);
                assertTrue(store.get("new").isEmpty());
                load.commit(REPLACEMENT);
                assertEquals(REPLACEMENT, store.checkpoint());
                assertEquals(REPLACEMENT_SCHEMA.getFieldNames(), store.schema().getFieldNames());
                assertEquals("replacement", store.get("new").orElseThrow().getAsString("OTHER"));
                assertTrue(store.get("old").isEmpty());
                assertEquals(1, store.size());
                assertThrows(IllegalStateException.class, () -> load.commit(NEXT));
                assertThrows(IllegalStateException.class, () -> load.put("later", record(SCHEMA, "later")));
            }
            assertEquals(REPLACEMENT, store.checkpoint());
            assertTrue(store.get("new").isPresent());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void failedOrAbandonedFullLoadPreservesCommittedPair(final boolean embedded) throws Exception {
        try (final TableCacheStore store = store(embedded)) {
            loadInitial(store);
            try (final TableCacheStore.BulkLoad load = store.beginBulkLoad(REPLACEMENT_SCHEMA)) {
                load.put("new", record(REPLACEMENT_SCHEMA, "replacement"));
                assertThrows(IllegalArgumentException.class, () -> load.put("new", record(REPLACEMENT_SCHEMA, "duplicate")));
            }
            assertInitial(store);
            try (final TableCacheStore.BulkLoad load = store.beginBulkLoad(REPLACEMENT_SCHEMA)) {
                load.put("new", record(REPLACEMENT_SCHEMA, "replacement"));
                assertThrows(NullPointerException.class, () -> load.commit(null));
                assertInitial(store);
            }
            try (final TableCacheStore.BulkLoad load = store.beginBulkLoad(REPLACEMENT_SCHEMA)) {
                load.put("abandoned", record(REPLACEMENT_SCHEMA, "abandoned"));
            }
            assertInitial(store);
        }
        if (embedded) {
            try (final TableCacheStore reopened = store(true)) {
                assertInitial(reopened);
            }
        }
    }

    @Test
    void failedFullLoadDiscardsAlreadyFlushedStagingRows() throws Exception {
        try (final TableCacheStore store = store(true)) {
            loadInitial(store);
            try (final TableCacheStore.BulkLoad load = store.beginBulkLoad(REPLACEMENT_SCHEMA)) {
                final Record value = record(REPLACEMENT_SCHEMA, "staged");
                for (int index = 0; index < 100_000; index++) {
                    load.put("staged-%s".formatted(index), value);
                }
                assertThrows(IllegalArgumentException.class, () -> load.put("staged-0", value));
                assertInitial(store);
                assertTrue(store.get("staged-0").isEmpty());
            }
            assertInitial(store);
        }
        try (final TableCacheStore reopened = store(true)) {
            assertInitial(reopened);
            assertTrue(reopened.get("staged-0").isEmpty());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void missingInitialCheckpointLeavesFreshCacheUnhydrated(final boolean embedded) throws Exception {
        try (final TableCacheStore store = store(embedded)) {
            try (final TableCacheStore.BulkLoad load = store.beginBulkLoad(SCHEMA)) {
                load.put("new", record(SCHEMA, "new"));
                assertThrows(NullPointerException.class, () -> load.commit(null));
            }
            assertFalse(store.isHydrated());
            assertNull(store.checkpoint());
            assertNull(store.schema());
            assertTrue(store.get("new").isEmpty());
            assertEquals(0, store.size());
        }
        if (embedded) {
            try (final TableCacheStore reopened = store(true)) {
                assertFalse(reopened.isHydrated());
                assertNull(reopened.checkpoint());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void deltaRequiresCheckpointAndPreservesPairOnFailure(final boolean embedded) throws Exception {
        try (final TableCacheStore store = store(embedded)) {
            loadInitial(store);
            final Map<String, Record> upserts = Map.of("new", record(SCHEMA, "new"));
            assertThrows(NullPointerException.class, () -> store.applyChanges(SCHEMA, upserts, Set.of("old"), null));
            assertInitial(store);
            assertThrows(IllegalStateException.class, () -> store.applyChanges(REPLACEMENT_SCHEMA, upserts, Set.of("old"), NEXT));
            assertInitial(store);
            assertTrue(store.get("new").isEmpty());
        }
        if (embedded) {
            try (final TableCacheStore reopened = store(true)) {
                assertInitial(reopened);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void failureAfterStagingDeltaRowPreservesCommittedPair(final boolean embedded) throws Exception {
        try (final TableCacheStore store = store(embedded)) {
            loadInitial(store);
            final Map<String, Record> failingUpserts = new AbstractMap<>() {
                @Override
                public Set<Entry<String, Record>> entrySet() {
                    return new AbstractSet<>() {
                        @Override
                        public int size() {
                            return 2;
                        }

                        @Override
                        public Iterator<Entry<String, Record>> iterator() {
                            return new Iterator<>() {
                                private boolean delivered;

                                @Override
                                public boolean hasNext() {
                                    return true;
                                }

                                @Override
                                public Entry<String, Record> next() {
                                    if (delivered) {
                                        throw new IllegalStateException("Injected failure after first delta row");
                                    }
                                    delivered = true;
                                    return Map.entry("new", record(SCHEMA, "new"));
                                }
                            };
                        }
                    };
                }
            };
            assertThrows(IllegalStateException.class, () -> store.applyChanges(SCHEMA, failingUpserts, Set.of("old"), NEXT));
            assertInitial(store);
            assertTrue(store.get("new").isEmpty());
        }
        if (embedded) {
            try (final TableCacheStore reopened = store(true)) {
                assertInitial(reopened);
                assertTrue(reopened.get("new").isEmpty());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void deltaPublishesUpsertsDeletesAndCheckpoint(final boolean embedded) throws Exception {
        try (final TableCacheStore store = store(embedded)) {
            loadInitial(store);
            store.applyChanges(SCHEMA, Map.of("new", record(SCHEMA, "new"), "kept", record(SCHEMA, "kept")), Set.of("old", "kept"), NEXT);
            assertEquals(NEXT, store.checkpoint());
            assertEquals(2, store.size());
            assertTrue(store.get("old").isEmpty());
            assertEquals("new", store.get("new").orElseThrow().getAsString("VALUE"));
            assertEquals("kept", store.get("kept").orElseThrow().getAsString("VALUE"));
            assertEquals(SCHEMA.getFieldNames(), store.schema().getFieldNames());
        }
        if (embedded) {
            try (final TableCacheStore reopened = store(true)) {
                assertEquals(NEXT, reopened.checkpoint());
                assertEquals(2, reopened.size());
                assertTrue(reopened.get("old").isEmpty());
                assertEquals("kept", reopened.get("kept").orElseThrow().getAsString("VALUE"));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void emptyDeltaAdvancesCheckpointWithoutChangingData(final boolean embedded) throws Exception {
        try (final TableCacheStore store = store(embedded)) {
            loadInitial(store);
            store.applyChanges(SCHEMA, Map.of(), Set.of(), NEXT);
            assertEquals(NEXT, store.checkpoint());
            assertEquals("original", store.get("old").orElseThrow().getAsString("VALUE"));
            assertEquals(1, store.size());
            assertEquals(SCHEMA.getFieldNames(), store.schema().getFieldNames());
        }
        if (embedded) {
            try (final TableCacheStore reopened = store(true)) {
                assertEquals(NEXT, reopened.checkpoint());
                assertEquals("original", reopened.get("old").orElseThrow().getAsString("VALUE"));
                assertEquals(1, reopened.size());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void emptyFullLoadIsHydratedAndReplacesOldContents(final boolean embedded) throws Exception {
        try (final TableCacheStore store = store(embedded)) {
            loadInitial(store);
            try (final TableCacheStore.BulkLoad load = store.beginBulkLoad(REPLACEMENT_SCHEMA)) {
                load.commit(REPLACEMENT);
            }
            assertTrue(store.isHydrated());
            assertEquals(REPLACEMENT, store.checkpoint());
            assertEquals(REPLACEMENT_SCHEMA.getFieldNames(), store.schema().getFieldNames());
            assertEquals(0, store.size());
            assertTrue(store.get("old").isEmpty());
        }
        if (embedded) {
            try (final TableCacheStore reopened = store(true)) {
                assertTrue(reopened.isHydrated());
                assertEquals(REPLACEMENT, reopened.checkpoint());
                assertEquals(REPLACEMENT_SCHEMA.getFieldNames(), reopened.schema().getFieldNames());
                assertEquals(0, reopened.size());
            }
        }
    }

    @Test
    void persistsCheckpointWithLengthPrefixedStrings() throws Exception {
        final CacheCheckpoint checkpoint = new CacheCheckpoint(QUERY_ID, "scope\u0000with|separators\n\u03bb");
        try (final TableCacheStore store = store(true);
             final TableCacheStore.BulkLoad load = store.beginBulkLoad(SCHEMA)) {
            load.put("old", record(SCHEMA, "original"));
            load.commit(checkpoint);
        }
        try (final Options options = new Options();
             final RocksDB database = RocksDB.open(options, generationPath().toString())) {
            assertArrayEquals(RowCodec.encode(List.of(checkpoint.queryId(), checkpoint.sourceSignature())), database.get(CHECKPOINT_KEY));
            assertNull(database.get(metadataKey("watermark")));
        }
        try (final TableCacheStore reopened = store(true)) {
            assertEquals(checkpoint, reopened.checkpoint());
            assertEquals("original", reopened.get("old").orElseThrow().getAsString("VALUE"));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"checkpoint", "schema", "count"})
    void missingMetadataFailsOpenWithoutDeletingCommittedData(final String metadata) throws Exception {
        createPersistedStore();
        final byte[] key = metadataKey(metadata);
        final byte[] previous;
        try (final Options options = new Options();
             final RocksDB database = RocksDB.open(options, generationPath().toString())) {
            previous = database.get(key);
            database.delete(key);
        }
        assertRejectedDirectoryPreserved();
        try (final Options options = new Options();
             final RocksDB database = RocksDB.open(options, generationPath().toString())) {
            assertNull(database.get(key));
            database.put(key, previous);
        }
        try (final TableCacheStore reopened = store(true)) {
            assertInitial(reopened);
        }
    }

    @ParameterizedTest
    @MethodSource("invalidCheckpointEncodings")
    void invalidCheckpointFailsOpenWithoutDeletingCommittedData(final byte[] encoded) throws Exception {
        createPersistedStore();
        try (final Options options = new Options();
             final RocksDB database = RocksDB.open(options, generationPath().toString())) {
            database.put(CHECKPOINT_KEY, encoded);
        }
        assertRejectedDirectoryPreserved();
        try (final Options options = new Options();
             final RocksDB database = RocksDB.open(options, generationPath().toString())) {
            assertArrayEquals(encoded, database.get(CHECKPOINT_KEY));
            database.put(CHECKPOINT_KEY, RowCodec.encode(List.of(INITIAL.queryId(), INITIAL.sourceSignature())));
        }
        try (final TableCacheStore reopened = store(true)) {
            assertInitial(reopened);
        }
    }

    static Stream<Arguments> invalidCheckpointEncodings() {
        final byte[] valid = RowCodec.encode(List.of(QUERY_ID, "source"));
        final byte[] invalidUtf8 = valid.clone();
        invalidUtf8[invalidUtf8.length - 1] = (byte) 0xff;
        return Stream.of(
                new byte[0],
                ByteBuffer.allocate(4).putInt(Integer.MAX_VALUE).array(),
                ByteBuffer.allocate(8).putInt(2).putInt(Integer.MAX_VALUE).array(),
                RowCodec.encode(List.of()),
                RowCodec.encode(List.of(QUERY_ID)),
                RowCodec.encode(List.of(QUERY_ID, "source", "extra")),
                RowCodec.encode(Arrays.asList(null, "source")),
                RowCodec.encode(Arrays.asList(QUERY_ID, null)),
                RowCodec.encode(List.of(" ", "source")),
                RowCodec.encode(List.of("not-a-query", "source")),
                RowCodec.encode(List.of(QUERY_ID, "")),
                RowCodec.encode(List.of(QUERY_ID, " \t\n")),
                Arrays.copyOf(valid, valid.length - 1),
                Arrays.copyOf(valid, valid.length + 1),
                invalidUtf8).map(encoded -> Arguments.of((Object) encoded));
    }

    @Test
    void defaultFormatIdentityIsV2AndOldIdentityIsPreservedOnRejection() throws Exception {
        createPersistedStore();
        final Path identityPath = storageDirectory.resolve("identity");
        assertArrayEquals(RowCodec.encode(List.of("table-cache-v2", "standalone")), Files.readAllBytes(identityPath));
        final byte[] oldIdentity = RowCodec.encode(List.of("table-cache-v1", "standalone"));
        Files.write(identityPath, oldIdentity);
        assertRejectedDirectoryPreserved();
        assertArrayEquals(oldIdentity, Files.readAllBytes(identityPath));
    }

    @Test
    void missingIdentityIsNotRecreatedForExistingGeneration() throws Exception {
        createPersistedStore();
        final Path identityPath = storageDirectory.resolve("identity");
        Files.delete(identityPath);
        assertRejectedDirectoryPreserved();
        assertFalse(Files.exists(identityPath));
    }

    @Test
    void secondOwnerCannotOpenDirectoryOrDisturbCommittedPair() throws Exception {
        try (final TableCacheStore store = store(true)) {
            loadInitial(store);
            assertThrows(IOException.class, () -> new RocksDbTableCacheStore(storageDirectory));
            assertInitial(store);
        }
        try (final TableCacheStore reopened = store(true)) {
            assertInitial(reopened);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void closedBulkLoadCannotUseDiscardedHandles(final boolean embedded) throws Exception {
        try (final TableCacheStore store = store(embedded)) {
            loadInitial(store);
            final TableCacheStore.BulkLoad load = store.beginBulkLoad(SCHEMA);
            load.close();
            assertThrows(IllegalStateException.class, () -> load.put("late", record(SCHEMA, "late")));
            assertThrows(IllegalStateException.class, () -> load.commit(NEXT));
            assertInitial(store);
        }
    }

    @Test
    void repeatedBulkCloseDoesNotDeleteReusedGeneration() throws Exception {
        try (final TableCacheStore store = store(true)) {
            loadInitial(store);
            final TableCacheStore.BulkLoad abandoned = store.beginBulkLoad(SCHEMA);
            abandoned.close();
            try (final TableCacheStore.BulkLoad replacement = store.beginBulkLoad(REPLACEMENT_SCHEMA)) {
                replacement.put("new", record(REPLACEMENT_SCHEMA, "replacement"));
                replacement.commit(REPLACEMENT);
            }
            abandoned.close();
            assertEquals(REPLACEMENT, store.checkpoint());
        }
        try (final TableCacheStore reopened = store(true)) {
            assertEquals(REPLACEMENT, reopened.checkpoint());
            assertEquals("replacement", reopened.get("new").orElseThrow().getAsString("OTHER"));
        }
    }

    @Test
    void closedRocksStoreCannotPublishStagedGeneration() throws Exception {
        final TableCacheStore store = store(true);
        try (store) {
            loadInitial(store);
            try (final TableCacheStore.BulkLoad load = store.beginBulkLoad(REPLACEMENT_SCHEMA)) {
                load.put("new", record(REPLACEMENT_SCHEMA, "replacement"));
                store.close();
                assertThrows(IllegalStateException.class, () -> load.commit(REPLACEMENT));
                assertThrows(IllegalStateException.class, () -> store.beginBulkLoad(SCHEMA));
            }
        }
        try (final TableCacheStore reopened = store(true)) {
            assertInitial(reopened);
        }
    }

    private TableCacheStore store(final boolean embedded) throws IOException {
        return embedded ? new RocksDbTableCacheStore(storageDirectory) : new HeapTableCacheStore();
    }

    private void createPersistedStore() throws Exception {
        try (final TableCacheStore store = store(true)) {
            loadInitial(store);
        }
    }

    private void assertRejectedDirectoryPreserved() throws Exception {
        final Path marker = storageDirectory.resolve("preserve.txt");
        Files.writeString(marker, "preserve");
        assertThrows(IOException.class, () -> new RocksDbTableCacheStore(storageDirectory));
        // A second failure also verifies that the failed constructor released the directory lock.
        assertThrows(IOException.class, () -> new RocksDbTableCacheStore(storageDirectory));
        assertEquals("preserve", Files.readString(marker));
        assertTrue(Files.isDirectory(generationPath()));
        try (final Options options = new Options();
             final RocksDB database = RocksDB.open(options, generationPath().toString())) {
            assertArrayEquals(RowCodec.encode(List.of("original")), database.get("\u0001old".getBytes(StandardCharsets.UTF_8)));
        }
    }

    private Path generationPath() {
        return storageDirectory.resolve("gen-1");
    }

    private static byte[] metadataKey(final String name) {
        return "\u0000__%s__".formatted(name).getBytes(StandardCharsets.UTF_8);
    }

    private static RecordSchema schema(final String name) {
        return new SimpleRecordSchema(List.of(new RecordField(name, RecordFieldType.STRING.getDataType())));
    }

    private static Record record(final RecordSchema schema, final String value) {
        return new MapRecord(schema, Map.of(schema.getFieldNames().getFirst(), value));
    }

    private static void loadInitial(final TableCacheStore store) {
        try (final TableCacheStore.BulkLoad load = store.beginBulkLoad(SCHEMA)) {
            load.put("old", record(SCHEMA, "original"));
            load.commit(INITIAL);
        }
    }

    private static void assertInitial(final TableCacheStore store) {
        assertTrue(store.isHydrated());
        assertEquals(INITIAL, store.checkpoint());
        assertEquals(SCHEMA.getFieldNames(), store.schema().getFieldNames());
        assertEquals(1, store.size());
        assertEquals("original", store.get("old").orElseThrow().getAsString("VALUE"));
    }
}
