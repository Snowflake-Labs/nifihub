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

import java.io.Closeable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Storage backend for a materialized table cache.
 *
 * <p>Two write paths, and both must be <em>atomic</em>: readers concurrent with a write continue to
 * see the previous contents until it completes. That is what lets a refresh run while lookups are
 * being served, and what prevents a reader from observing a half-applied change — which for an
 * entitlement dataset would be a correctness bug, not a performance one.
 *
 * <ul>
 *   <li>{@link #beginBulkLoad} — replace everything. Used for the initial load and for recovery.</li>
 *   <li>{@link #applyChanges} — apply a delta. Used for incremental refresh.</li>
 * </ul>
 *
 * <p>Both write paths also carry the <b>watermark</b>, so it is stored atomically with the data it
 * describes. Crash and filesystem durability still depend on the backend and runtime.
 */
public interface TableCacheStore extends Closeable {

    /**
     * Start a full load. Not visible to readers until committed.
     *
     * @param schema schema of the records being loaded. On-disk backends store values positionally
     *               against this schema rather than repeating field names for every row.
     */
    BulkLoad beginBulkLoad(RecordSchema schema);

    /**
     * Atomically apply a delta and advance the watermark.
     *
     * @param schema    schema of the upserted records
     * @param upserts   keys to insert or replace
     * @param deletes   keys to remove; an upsert for the same key takes precedence
     * @param watermark new watermark, stored with the change
     */
    void applyChanges(RecordSchema schema, Map<String, Record> upserts, Set<String> deletes,
                      String watermark);

    /** Look up a single composite key. Empty if absent. */
    Optional<Record> get(String key);

    /** Number of entries currently visible to readers. */
    long size();

    /** False until the first successful write. */
    boolean isHydrated();

    /** Watermark stored by the last successful write, or null if there has never been one. */
    String watermark();

    /**
     * Schema the current contents were loaded with, or null if not hydrated. An incremental delta
     * must carry the same fields in the same order, because on-disk backends encode positionally.
     */
    RecordSchema schema();

    /** A short human-readable description of the backend, for logging. */
    String describe();

    /**
     * An in-flight full load. Closing without committing discards it, so try-with-resources is safe
     * and a failed load cannot partially apply.
     */
    interface BulkLoad extends AutoCloseable {

        void put(String key, Record value);

        /** Atomically replace the store's visible contents with everything put so far. */
        void commit(String watermark);

        @Override
        void close();
    }
}
