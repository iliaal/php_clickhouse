# Compression sources

The `lz4/` and `zstd/` directories retain the sources shipped with
`ClickHouse/clickhouse-cpp` tag `v2.6.2`: LZ4 1.9.2 from `contrib/lz4/lz4/`
and ZSTD 1.5.5 from `contrib/zstd/zstd/`. Their license notices remain in each
directory. No C++ client code is compiled or required.

`scripts/check-vendored-patches.sh` reverses the documented local patch and
compares every retained codec source against that upstream tag. Set
`CLICKHOUSE_CODECS_PRISTINE` to a local pristine checkout to avoid fetching it.
