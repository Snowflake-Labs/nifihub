# Third-party redistribution inventory

Technical review dated 2026-10-07. This is not organizational source-release
authorization or legal advice. License choices below apply to third-party
components; adding an Apache header does not establish ownership of new source.

## Artifact scope

The NAR ships the component JAR and unmodified `org.rocksdb:rocksdbjni:11.1.1`.
The reviewed JNI JAR SHA-256 is
`71353febb7196261d6204d0f606e365e78009aaf014580a2f91d3a9a2d7d982e`.
It contains 15 native binaries and no embedded license/notice files. This bundle
supplies the notices at the NAR's `META-INF/NOTICE`, `META-INF/LICENSE` and
`META-INF/licenses/`. Do not distribute an extracted JNI JAR without those notices.

NiFi APIs and SLF4J are provided by the runtime, not bundled. H2, Snowflake JDBC,
TOML and test frameworks are test-only. Their licenses are not claimed to be
covered by this runtime inventory because their binaries are not distributed.

## Reviewed native sources

RocksDB tag `v11.1.1` resolves to source commit
`6cdeb9d9d0630763327f512e6255cab33f6834e7` (reviewed release source).
Its [Makefile](https://github.com/facebook/rocksdb/blob/6cdeb9d9d0630763327f512e6255cab33f6834e7/Makefile)
pins the compression versions below in `JAVA_COMPRESSIONS` and the corresponding
download checksums. These are confirmed source-build defaults, not a signed build
attestation for every native binary. Binary symbols corroborate the compression
families on Linux/macOS; Windows exposes LZ4 but does not establish the full set.
All reviewed notices are included conservatively for the cross-platform artifact.

| Material | Version/source | Selected terms | Packaged text under META-INF |
|---|---|---|---|
| RocksDB JNI | 11.1.1 | Apache-2.0 option | LICENSE |
| LevelDB-derived code | RocksDB LICENSE.leveldb | BSD-3-Clause | licenses/leveldb.md |
| xxHash / XXH3 preview | RocksDB util/xxhash.{h,cc}, util/xxph3.h | BSD-2-Clause | licenses/rocks-xxhash*.md, rocks-xxph3.md |
| MurmurHash | RocksDB util/murmurhash.cc | Upstream public-domain statement | licenses/murmurhash.md |
| zlib | 1.3.1 | Zlib | licenses/zlib.md |
| bzip2 | 1.0.8 | bzip2-1.0.6 terms as supplied in 1.0.8 | licenses/bzip2.md |
| Snappy / C wrapper | 1.2.2 | BSD-3-Clause | licenses/snappy.md, snappy-c.md |
| LZ4 library | 1.10.0 | BSD-2-Clause | licenses/lz4.md |
| Zstandard / adapted xxHash | 1.5.7 | BSD-3-Clause option | licenses/zstd.md, zstd-xxhash.md |
| libdivsufsort-lite | Zstandard 1.5.7 lib/dictBuilder | MIT | licenses/divsufsort.md |
| GCC PowerPC assembly definitions | RocksDB third-party/gcc/ppc-asm.h | GPL-3.0-or-later WITH GCC-exception-3.1 | licenses/gcc-header.md, gcc-gpl3.md, gcc-exception.md |

The upstream RocksDB README explicitly offers Apache 2.0 **or** GPLv2, so the
presence of COPYING does not by itself require choosing GPLv2 for RocksDB.
LevelDB and embedded third-party terms still apply. The PPC CRC source references
the bundled GCC header under Clang and the toolchain's header otherwise; both
PowerPC binaries contain the CRC symbol. The GCC exception text is included, but
eligibility of the publisher's exact compilation process is not established by
this inspection. Confirm it with the publisher/release approver before release.

ELF dependency tables for all 12 Linux variants reference external libc and GCC
runtime libraries. Both Mach-O variants reference system libc++/libSystem; the
Windows PE import table references Windows/MSVC runtime DLLs. Those runtime
libraries are not separate entries in this NAR. This does not prove absence of
all compiler-generated snippets, establish every transitive source version, or
replace a publisher SBOM/build attestation. Keep these provenance limitations in
the release review rather than asserting a complete legal clearance.

License files are copied without changing their terms; header excerpts retain
comment delimiters. Trailing spaces in bzip2's license were removed; final newlines
were normalized. `.md` filenames follow the repository's existing documentation
handling; no RAT exclusion or enforcement rule was added. The packaging test
checks each reviewed text against its recorded SHA-256 and checks the exact JNI
artifact hash. Update the inventory and re-review notices when changing RocksDB.
Source URLs are in `META-INF/license-inventory.properties`; the bzip2 archive hash
was verified against the RocksDB Makefile before extracting its LICENSE.

## Release checklist

- [x] Identify bundled artifacts, static compression defaults and embedded notices.
- [x] Include reviewed license/attribution texts and pin their hashes.
- [x] Verify the current NAR packaging test passes; repeat after any dependency change.
- [ ] Obtain organizational approval to release the component source under Apache 2.0.
- [ ] Resolve publisher build-provenance/GCC-exception eligibility with the release approver.
- [ ] Qualify the exact final artifact on the intended runtime separately.
