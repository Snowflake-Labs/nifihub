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
import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

/**
 * Embedded on-disk store backed by RocksDB, reusable when its directory survives restart.
 *
 * <p>Footprint depends on keys, values, compression and RocksDB's native buffers.
 *
 * <p>Each full load writes a fresh {@code gen-<n>} directory and commit swaps the open handle to
 * it under a write lock, while lookups hold a read lock — no reader can be using a handle that is
 * being closed. Both generations remain on disk until publication completes.
 *
 * <p>Incremental changes, by contrast, are applied <b>in place</b> to the live generation as a single
 * RocksDB {@code WriteBatch}, which is atomic.
 *
 * <p>On open, an existing newest generation is adopted, which is what makes a restart warm rather
 * than a rebuild.
 */
public class RocksDbTableCacheStore implements TableCacheStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(RocksDbTableCacheStore.class);

    private static final String GENERATION_PREFIX = "gen-";
    private static final byte[] SCHEMA_KEY = "\u0000__schema__".getBytes(StandardCharsets.UTF_8);
    private static final byte[] COUNT_KEY = "\u0000__count__".getBytes(StandardCharsets.UTF_8);
    private static final byte[] CHECKPOINT_KEY = "\u0000__checkpoint__".getBytes(StandardCharsets.UTF_8);

    static {
        RocksDB.loadLibrary();
    }

    private final Path baseDirectory;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final FileChannel ownershipChannel;
    private final FileLock ownershipLock;

    private Generation current;

    public RocksDbTableCacheStore(final Path baseDirectory) throws IOException {
        this(baseDirectory, RowCodec.encode(List.of("table-cache-v2", "standalone")));
    }

    public RocksDbTableCacheStore(final Path baseDirectory, final byte[] identity) throws IOException {
        this.baseDirectory = baseDirectory;
        Files.createDirectories(baseDirectory);
        ownershipChannel = FileChannel.open(baseDirectory.resolve("cache.lock"),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        try {
            ownershipLock = ownershipChannel.tryLock();
            if (ownershipLock == null) {
                throw new IOException("Cache directory is already in use");
            }
            final Path identityPath = baseDirectory.resolve("identity");
            if (Files.exists(identityPath)) {
                if (!Arrays.equals(identity, Files.readAllBytes(identityPath))) {
                    throw new IOException("Cache configuration differs; use a fresh storage directory");
                }
            } else {
                if (!generationNumbers().isEmpty()) {
                    throw new IOException("Cache has no format identity; use a fresh storage directory");
                }
                Files.write(identityPath, identity, StandardOpenOption.CREATE_NEW);
            }
            adoptNewestGeneration();
        } catch (final OverlappingFileLockException e) {
            ownershipChannel.close();
            throw new IOException("Cache directory is already in use", e);
        } catch (final IOException | RuntimeException e) {
            ownershipChannel.close();
            throw e;
        }
    }

    /** Adopt the newest existing generation, if any, so a restart does not re-hydrate. */
    private void adoptNewestGeneration() throws IOException {
        final long newest = generationNumbers().stream().max(Comparator.naturalOrder()).orElse(-1L);
        if (newest < 0) {
            return;
        }
        try {
            current = Generation.open(generationPath(newest), newest);
        } catch (final RocksDBException | RuntimeException e) {
            // Lock errors, corruption and incomplete generations must never trigger data deletion.
            throw new IOException("Cannot adopt cache generation; preserve it and use a fresh directory", e);
        }
    }

    @Override
    public BulkLoad beginBulkLoad(final RecordSchema schema) {
        lock.writeLock().lock();
        try {
            requireOpen();
            final long next = 1 + generationNumbers().stream().max(Comparator.naturalOrder()).orElse(0L);
            return new RocksBulkLoad(next, schema);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void requireOpen() {
        if (!ownershipLock.isValid()) {
            throw new IllegalStateException("Cache store is closed");
        }
    }

    @Override
    public void applyChanges(final RecordSchema schema, final Map<String, Record> upserts,
                            final Set<String> deletes, final CacheCheckpoint checkpoint) {
        Objects.requireNonNull(checkpoint, "Checkpoint is required");
        lock.writeLock().lock();
        try {
            if (current == null) {
                throw new IllegalStateException("Cannot apply changes before an initial load");
            }
            if (!current.fieldNames.equals(RowCodec.fieldNames(schema))) {
                throw new IllegalStateException("Schema changed since the last full load");
            }
            current.applyChanges(upserts, deletes, checkpoint);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Optional<Record> get(final String key) {
        lock.readLock().lock();
        try {
            return current == null ? Optional.empty() : current.get(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public long size() {
        lock.readLock().lock();
        try {
            return current == null ? 0L : current.rowCount;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public boolean isHydrated() {
        lock.readLock().lock();
        try {
            return current != null;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public CacheCheckpoint checkpoint() {
        lock.readLock().lock();
        try {
            return current == null ? null : current.checkpoint;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public RecordSchema schema() {
        lock.readLock().lock();
        try {
            return current == null ? null : RowCodec.schema(current.fieldNames);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public String describe() {
        lock.readLock().lock();
        try {
            return current == null
                    ? "rocksdb (empty) at %s".formatted(baseDirectory)
                    : "rocksdb gen-%s at %s".formatted(current.number, baseDirectory);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void close() {
        lock.writeLock().lock();
        try {
            if (current != null) {
                current.close();
                current = null;
            }
            try {
                if (ownershipLock.isValid()) {
                    ownershipLock.release();
                }
                ownershipChannel.close();
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private final class RocksBulkLoad implements BulkLoad {

        private static final int BATCH_ROWS = 100_000;

        private final long number;
        private final Path path;
        private final List<String> fieldNames;

        private Options options;
        private RocksDB db;
        private WriteOptions writeOptions;
        private WriteBatch batch;
        private final Set<String> pendingKeys = new HashSet<>();
        private long staged;
        private boolean committed;
        private boolean closed;

        RocksBulkLoad(final long number, final RecordSchema schema) {
            this.number = number;
            this.path = generationPath(number);
            this.fieldNames = RowCodec.fieldNames(schema);
            try {
                Files.createDirectory(path);
                options = new Options()
                        .setCreateIfMissing(true)
                        .setCompressionType(CompressionType.ZSTD_COMPRESSION)
                        .setWriteBufferSize(64L * 1024 * 1024)
                        .setMaxWriteBufferNumber(3);
                db = RocksDB.open(options, path.toString());
                writeOptions = new WriteOptions();
                batch = new WriteBatch();
            } catch (final IOException | RocksDBException e) {
                closeQuietly();
                throw new IllegalStateException("Failed to open RocksDB generation at %s".formatted(path), e);
            }
        }

        @Override
        public void put(final String key, final Record value) {
            if (committed || db == null) {
                throw new IllegalStateException("Bulk load already completed");
            }
            if (key == null || value == null) {
                throw new IllegalArgumentException("Source keys and records must be non-null");
            }
            final byte[] encodedValue = RowCodec.encode(RowCodec.values(fieldNames, value));
            try {
                final byte[] encodedKey = dataKey(key);
                if (!pendingKeys.add(key) || db.get(encodedKey) != null) {
                    throw new IllegalArgumentException("Source keys must be unique");
                }
                batch.put(encodedKey, encodedValue);
                staged++;
                if (staged % BATCH_ROWS == 0) {
                    flush();
                }
            } catch (final RocksDBException e) {
                throw new IllegalStateException("Failed to stage cache row", e);
            }
        }

        private void flush() throws RocksDBException {
            db.write(writeOptions, batch);
            batch.close();
            batch = new WriteBatch();
            pendingKeys.clear();
        }

        @Override
        public void commit(final CacheCheckpoint checkpoint) {
            if (committed || db == null) {
                throw new IllegalStateException("Bulk load already completed");
            }
            Objects.requireNonNull(checkpoint, "Checkpoint is required");
            final Generation previous;
            lock.writeLock().lock();
            try {
                requireOpen();
                batch.put(SCHEMA_KEY, RowCodec.encode(fieldNames));
                batch.put(COUNT_KEY, Long.toString(staged).getBytes(StandardCharsets.UTF_8));
                batch.put(CHECKPOINT_KEY, encodeCheckpoint(checkpoint));
                flush();
                db.flushWal(true);
                db.close();
                db = null;
                options.close();
                options = null;

                // Reopen and publish. Only now does anything become visible.
                final Generation next = Generation.open(path, number);
                previous = current;
                current = next;
                // Publication is final even if reclaiming the previous generation fails.
                committed = true;
            } catch (final RocksDBException e) {
                throw new IllegalStateException("Failed to commit RocksDB generation %s".formatted(number), e);
            } finally {
                lock.writeLock().unlock();
            }
            // Safe: the write lock guarantees no reader still holds this handle.
            if (previous != null) {
                try {
                    previous.close();
                    deleteRecursively(previous.path);
                } catch (final RuntimeException e) {
                    LOGGER.warn("Cache committed but previous generation cleanup failed at {}", previous.path, e);
                }
            }
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closeQuietly();
            if (!committed) {
                deleteRecursively(path);
            }
            closed = true;
        }

        private void closeQuietly() {
            if (batch != null) {
                batch.close();
                batch = null;
            }
            if (writeOptions != null) {
                writeOptions.close();
                writeOptions = null;
            }
            if (db != null) {
                db.close();
                db = null;
            }
            if (options != null) {
                options.close();
                options = null;
            }
        }
    }

    private static final class Generation {

        private final Path path;
        private final long number;
        private final Options options;
        private final RocksDB db;
        private final List<String> fieldNames;

        private volatile long rowCount;
        private volatile CacheCheckpoint checkpoint;

        private Generation(final Path path, final long number, final Options options,
                           final RocksDB db, final List<String> fieldNames, final long rowCount,
                           final CacheCheckpoint checkpoint) {
            this.path = path;
            this.number = number;
            this.options = options;
            this.db = db;
            this.fieldNames = RowCodec.fieldNames(fieldNames);
            if (rowCount < 0) {
                throw new IllegalArgumentException("Cache row count must not be negative");
            }
            this.rowCount = rowCount;
            this.checkpoint = checkpoint;
        }

        static Generation open(final Path path, final long number) throws RocksDBException {
            final Options options = new Options()
                    .setCreateIfMissing(false)
                    .setCompressionType(CompressionType.ZSTD_COMPRESSION);
            RocksDB db = null;
            try {
                db = RocksDB.open(options, path.toString());
                final byte[] schemaBytes = db.get(SCHEMA_KEY);
                final byte[] countBytes = db.get(COUNT_KEY);
                final byte[] checkpointBytes = db.get(CHECKPOINT_KEY);
                if (schemaBytes == null || countBytes == null || checkpointBytes == null) {
                    throw new RocksDBException("Generation at %s has no committed metadata".formatted(path));
                }
                return new Generation(path, number, options, db,
                        RowCodec.decode(schemaBytes),
                        Long.parseLong(new String(countBytes, StandardCharsets.UTF_8)),
                        decodeCheckpoint(checkpointBytes));
            } catch (final RocksDBException | RuntimeException e) {
                if (db != null) {
                    db.close();
                }
                options.close();
                throw e;
            }
        }

        /** One atomic WriteBatch: the delta, the row count, and the checkpoint together. */
        void applyChanges(final Map<String, Record> upserts, final Set<String> deletes,
                          final CacheCheckpoint newCheckpoint) {
            long delta = 0;
            try (WriteBatch batch = new WriteBatch();
                 WriteOptions writeOptions = new WriteOptions()) {

                for (final Map.Entry<String, Record> entry : upserts.entrySet()) {
                    final byte[] key = dataKey(entry.getKey());
                    if (db.get(key) == null) {
                        delta++;
                    }
                    batch.put(key, RowCodec.encode(RowCodec.values(fieldNames, entry.getValue())));
                }
                for (final String deleteKey : deletes) {
                    if (upserts.containsKey(deleteKey)) {
                        continue;
                    }
                    final byte[] key = dataKey(deleteKey);
                    if (db.get(key) != null) {
                        delta--;
                    }
                    batch.delete(key);
                }

                final long newCount = rowCount + delta;
                batch.put(COUNT_KEY, Long.toString(newCount).getBytes(StandardCharsets.UTF_8));
                batch.put(CHECKPOINT_KEY, encodeCheckpoint(newCheckpoint));

                db.write(writeOptions, batch);
                rowCount = newCount;
                checkpoint = newCheckpoint;
            } catch (final RocksDBException e) {
                throw new IllegalStateException("Failed to apply changes to generation %s".formatted(number), e);
            }
        }

        Optional<Record> get(final String key) {
            try {
                final byte[] encoded = db.get(dataKey(key));
                if (encoded == null) {
                    return Optional.empty();
                }
                return Optional.of(RowCodec.record(fieldNames, RowCodec.decode(encoded, fieldNames.size())));
            } catch (final RocksDBException | IllegalArgumentException e) {
                throw new IllegalStateException("RocksDB lookup failed", e);
            }
        }

        void close() {
            try {
                db.close();
            } finally {
                options.close();
            }
        }
    }

    private static byte[] encodeCheckpoint(final CacheCheckpoint checkpoint) {
        return RowCodec.encode(List.of(checkpoint.queryId(), checkpoint.sourceSignature()));
    }

    private static CacheCheckpoint decodeCheckpoint(final byte[] encoded) {
        final List<String> values = RowCodec.decode(encoded, 2);
        return new CacheCheckpoint(values.get(0), values.get(1));
    }

    private static byte[] dataKey(final String key) {
        // Separate arbitrary user keys from the metadata namespace, including embedded NUL bytes.
        Objects.requireNonNull(key, "Cache key must be non-null");
        return "\u0001%s".formatted(key).getBytes(StandardCharsets.UTF_8);
    }

    private Path generationPath(final long number) {
        return baseDirectory.resolve("%s%s".formatted(GENERATION_PREFIX, number));
    }

    private List<Long> generationNumbers() {
        try (Stream<Path> entries = Files.list(baseDirectory)) {
            return entries
                    .filter(Files::isDirectory)
                    .map(p -> p.getFileName().toString())
                    .filter(name -> name.startsWith(GENERATION_PREFIX))
                    .map(name -> {
                        try {
                            return Long.parseLong(name.substring(GENERATION_PREFIX.length()));
                        } catch (final NumberFormatException e) {
                            return null;
                        }
                    })
                    .filter(n -> n != null)
                    .toList();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void deleteRecursively(final Path path) {
        if (!Files.exists(path)) {
            return;
        }
        try (Stream<Path> entries = Files.walk(path)) {
            entries.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.delete(p);
                } catch (final IOException ignored) {
                    // best effort; a leftover generation directory is reclaimed on the next load
                }
            });
        } catch (final IOException ignored) {
            // as above
        }
    }
}
