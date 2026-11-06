# Snowflake Table Cache Bundle

## Purpose and navigation

`SnowflakeTableCacheLookupService` is a community `RecordLookupService` used by
`LookupRecord`, not a processor or supported Snowflake product. Each node caches locally.

- [README.md](README.md): authoritative source modes, property defaults, operator requirements and failure behavior.
- [DESIGN.md](DESIGN.md): query checkpoints, source signatures, response validation and local publication.
- [CONTRIBUTING.md](CONTRIBUTING.md): account-free build, opt-in live tests and artifact qualification.
- [THIRD_PARTY.md](THIRD_PARTY.md): license inventory and release/provenance approvals.

## Change constraints

- Keep nodes independent; do not add a shared cursor or reuse another service's directory.
- Preserve exact statement boundaries and atomic data/schema/checkpoint publication.
  A full-only diagnostic marker must never resume changes.
- Preserve identifier parsing, quoting and binds. Lookup keys stay local; do not strip
  punctuation from valid identifiers or silently change borrowed-session settings.
- Keep returned records/schemas detached and validate persisted counts/lengths before
  allocation. Do not claim hostile-filesystem protection or immediate authorization revocation.
- Ordinary builds must remain account-free. Live tests require explicit opt-in and owned
  synthetic scope; do not run them as an incidental build step or silently skip failures.
- Keep older caches on fresh directories; no migration or compatibility layer.
- Update the authoritative document when behavior changes, then check linked summaries.
  Tests passing does not authorize deployment, public disclosure or release.
