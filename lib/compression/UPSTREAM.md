# Compression sources

`lz4/` holds LZ4 1.11.1 from `lz4/lz4` tag `v1.11.1` (`lib/`). `zstd/`
holds ZSTD 1.5.7 from `facebook/zstd` tag `v1.5.7` (`lib/`, plus the
repository's `LICENSE`). Only the sources `config.m4` and `config.w32`
compile, the headers they include, and each library's license are retained.
`.upstream/lz4.yml` and `.upstream/zstd.yml` record the pinned versions.

`scripts/check-vendored-patches.sh` reverses any documented local patch and
compares every retained codec source against those tags. Set
`CLICKHOUSE_LZ4_PRISTINE` and `CLICKHOUSE_ZSTD_PRISTINE` to local pristine
checkouts to avoid fetching them.
