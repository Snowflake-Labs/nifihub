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

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Account-free checks of the actual NAR after package, not just source resources. */
class NarPackagingIT {

    @Test
    void packagesReviewedLicensesAndExactNativeDependency() throws Exception {
        try (final ZipFile archive = new ZipFile(Path.of(System.getProperty("nar.path")).toFile())) {
            final Properties inventory = new Properties();
            try (final InputStream input = archive.getInputStream(required(archive, "META-INF/license-inventory.properties"))) {
                inventory.load(input);
            }
            final Set<String> expected = Set.of("apache", "leveldb", "rocks-xxhash", "rocks-xxhash-c", "rocks-xxph3",
                    "murmurhash", "zlib", "bzip2", "snappy", "snappy-c", "lz4", "zstd", "zstd-xxhash",
                    "divsufsort", "gcc-header", "gcc-gpl3", "gcc-exception");
            assertEquals(expected, Set.copyOf(Arrays.asList(inventory.getProperty("licenses").split(","))));
            for (final String license : expected) {
                final String path = "META-INF/" + inventory.getProperty(license + ".path");
                assertEquals(inventory.getProperty(license + ".sha256"), digest(archive, path), path);
                assertTrue(inventory.getProperty(license + ".source").startsWith("https://"));
            }
            assertTrue(required(archive, "META-INF/NOTICE").getSize() > 0);
            final String nativeJar = "META-INF/bundled-dependencies/rocksdbjni-"
                    + inventory.getProperty("rocksdb.version") + ".jar";
            assertEquals(inventory.getProperty("rocksdb.sha256"), digest(archive, nativeJar),
                    "RocksDB changed: re-review the native inventory, not just the checksum");
            final Set<String> jars = archive.stream().map(ZipEntry::getName)
                    .filter(name -> name.endsWith(".jar")).collect(Collectors.toSet());
            assertEquals(2, jars.size(), "Unexpected runtime dependency requires redistribution review");
            assertTrue(jars.stream().anyMatch(name -> name.startsWith("META-INF/bundled-dependencies/nifi-snowflake-table-cache-processors-")));
        }
    }

    private static ZipEntry required(final ZipFile archive, final String path) {
        final ZipEntry entry = archive.getEntry(path);
        assertNotNull(entry, "Missing packaged resource: " + path);
        return entry;
    }

    private static String digest(final ZipFile archive, final String path) throws Exception {
        final MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (final InputStream input = new DigestInputStream(archive.getInputStream(required(archive, path)), digest)) {
            input.transferTo(OutputStream.nullOutputStream());
        }
        return HexFormat.of().formatHex(digest.digest());
    }
}
