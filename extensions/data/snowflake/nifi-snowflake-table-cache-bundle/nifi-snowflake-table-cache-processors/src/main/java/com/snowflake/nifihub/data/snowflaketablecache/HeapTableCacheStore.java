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

import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * On-heap store. Every write publishes by swapping one volatile reference, so readers never block
 * and never observe a partial write.
 *
 * <p>Full loads and incremental updates temporarily retain both old and new maps.
 *
 * <p>Not durable: contents and watermark are both rebuilt on restart. That pairing is deliberate —
 * a persisted watermark with an empty cache would skip changes it never applied.
 */
public class HeapTableCacheStore implements TableCacheStore {

    /** Contents, schema and watermark swapped together, so they can never disagree. */
    private record Snapshot(Map<String, Record> contents, RecordSchema schema, String watermark) {
    }

    private volatile Snapshot snapshot = null;

    @Override
    public BulkLoad beginBulkLoad(final RecordSchema schema) {
        return new HeapBulkLoad(schema);
    }

    @Override
    public void applyChanges(final RecordSchema schema, final Map<String, Record> upserts,
                            final Set<String> deletes, final String watermark) {
        final Snapshot current = snapshot;
        if (current == null) {
            throw new IllegalStateException("Cannot apply changes before an initial load");
        }
        // Copy-on-write. At heap-mode sizes this is cheap, and it makes the delta atomically
        // visible without any locking on the read path.
        final Map<String, Record> next = new HashMap<>(current.contents());
        next.keySet().removeAll(deletes);
        next.putAll(upserts);
        snapshot = new Snapshot(next, schema, watermark);
    }

    @Override
    public Optional<Record> get(final String key) {
        final Snapshot current = snapshot;
        if (current == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(current.contents().get(key));
    }

    @Override
    public long size() {
        final Snapshot current = snapshot;
        return current == null ? 0L : current.contents().size();
    }

    @Override
    public boolean isHydrated() {
        return snapshot != null;
    }

    @Override
    public String watermark() {
        final Snapshot current = snapshot;
        return current == null ? null : current.watermark();
    }

    @Override
    public RecordSchema schema() {
        final Snapshot current = snapshot;
        return current == null ? null : current.schema();
    }

    @Override
    public String describe() {
        return "heap";
    }

    @Override
    public void close() {
        snapshot = null;
    }

    private final class HeapBulkLoad implements BulkLoad {
        private final RecordSchema schema;
        private Map<String, Record> staged = new HashMap<>();

        HeapBulkLoad(final RecordSchema schema) {
            this.schema = schema;
        }

        @Override
        public void put(final String key, final Record value) {
            if (staged == null) {
                throw new IllegalStateException("Bulk load already completed");
            }
            if (key == null || value == null || staged.putIfAbsent(key, value) != null) {
                throw new IllegalArgumentException("Source keys must be unique and non-null");
            }
        }

        @Override
        public void commit(final String watermark) {
            if (staged == null) {
                throw new IllegalStateException("Bulk load already completed");
            }
            snapshot = new Snapshot(staged, schema, watermark);
            staged = null;
        }

        @Override
        public void close() {
            staged = null; // discard if not committed
        }
    }
}
