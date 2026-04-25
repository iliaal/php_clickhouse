# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.5.0] - 2026-04-25

This release renames the extension from `SeasClick` to `php_clickhouse`,
upgrades the vendored client library to ClickHouse/clickhouse-cpp v2.6.1,
and adds significant new functionality. The original SeasClick project
(SeasX/SeasClick on GitHub) appears unmaintained — its last accepted
external PR is from 2020. php_clickhouse is a soft fork that goes its
own way.

### Added

- New PHP class names: `ClickHouse` and `ClickHouseException`. The old
  `SeasClick` and `SeasClickException` names are kept as class aliases
  for this release cycle and removed in 1.0.
- New column types backed by clickhouse-cpp v2.6.1:
  - `Date32`
  - `Time` and `Time64(N)` (requires ClickHouse 25.x or later on the
    server)
  - `DateTime64(N[, timezone])`
  - `Int128`, `UInt128`, `Decimal128(P, S)` — round-trip as decimal
    strings since PHP integers are 64-bit
  - `LowCardinality(String)` and `LowCardinality(FixedString(N))`
  - `Map(K, V)` for `(String, String)`, `(String, Int64)`,
    `(String, UInt64)`, `(String, Float64)`, and `(Int64, String)`
- New connection / client options on the `__construct` config array:
  - `compression` now accepts `"lz4"`, `"zstd"`, `"none"` in addition
    to `true` / `false`. ZSTD is roughly 1.5-2x denser than LZ4 on
    typical row payloads.
  - `tcp_nodelay`, `tcp_keepalive`, `tcp_keepalive_idle`,
    `tcp_keepalive_intvl`, `tcp_keepalive_cnt`
  - `send_timeout` (companion to `receive_timeout` and
    `connect_timeout`)
  - `endpoints` — list of `[{host, port}, ...]` for round-robin
    failover. The lib walks the list in order on connect failure.
  - `max_compression_chunk_size`
- `query_id` accepted as a final optional argument on `select()`,
  `insert()`, `writeStart()`, and `execute()`. The id propagates to
  ClickHouse's `system.query_log` and is useful for `KILL QUERY` and
  trace correlation.
- TLS support: build with `--enable-clickhouse-openssl`. New config
  keys: `ssl`, `ssl_skip_verify`, `ssl_ca_files` (string or array),
  `ssl_ca_directory`, `ssl_use_default_ca`. Building without the flag
  and passing `ssl: true` raises `ClickHouseException` rather than
  silently downgrading to plaintext.
- Test suite: phpt coverage for Date32, Int128/UInt128/Decimal128,
  LowCardinality, Map (all five K/V combinations), DateTime64,
  Time/Time64, query_id propagation, ZSTD compression, multi-endpoint
  failover. Driven by `tests/_clickhouse.inc` which reads
  `CLICKHOUSE_HOST` / `CLICKHOUSE_PORT` / `CLICKHOUSE_USER` /
  `CLICKHOUSE_PASSWD` from the environment and skips cleanly when no
  server is reachable.
- README documents installation via [PIE](https://github.com/php/pie),
  a Docker recipe for a local ClickHouse server, the full configuration
  surface, and a benchmark table comparing against
  [smi2/phpClickHouse](https://github.com/smi2/phpClickHouse).

### Changed

- Vendored ClickHouse client library bumped from artpaul-fork v1.x to
  the official ClickHouse/clickhouse-cpp v2.6.1.
- Build now requires C++17 (clickhouse-cpp v2 hard requirement).
- Optional dependencies are vendored under `lib/clickhouse-cpp/contrib/`:
  cityhash, lz4, abseil int128, zstd. No external `pkg-config` lookups.
- Insert API rewired from the v1.x `InsertQuery` / `InsertData` /
  `InsertDataEnd` triplet to v2.x `BeginInsert` / `SendInsertBlock` /
  `EndInsert`. PHP-side `writeStart()` / `write()` / `writeEnd()`
  signatures unchanged.
- `ClientOptions` field renames carried through to the PHP-side
  config: `socket_receive_timeout` / `socket_connect_timeout` are now
  `connection_recv_timeout` / `connection_connect_timeout` internally.
  The old PHP-level keys `receive_timeout` / `connect_timeout` are
  preserved.
- PHP version floor: 7.1+ (drops 5.x and 7.0). PHP 7.1, 7.2, 7.4, 8.3,
  8.4, 8.5 all build and pass tests.
- Benchmarks moved from `tests/bench_mark/` to top-level `bench/`.
- Tests `006`-`008` now read connection settings from the environment
  via the shared `_clickhouse.inc` helper, drop their tables at the
  top, and (`008`) order results deterministically. The old
  `host=clickhouse` literal is still the default if no env is set.

### Fixed

- `Decimal` / `Decimal32` / `Decimal64` column reads previously
  downcast to `ColumnFloat32` / `ColumnFloat64` and SEGV'd on any
  pure `SELECT toDecimal128(42, 5)`. All four `Decimal*` variants now
  go through `ColumnDecimal` and round-trip as scaled-integer strings.
- `ZEND_ACC_DTOR` had no equivalent in PHP 8 (the bit value got
  reassigned to `ZEND_ACC_VARIADIC`); the old fallback was crashing
  `zend_register_functions` on extension load on every PHP 8.x build.
- `<cstdint>` now explicitly included in vendored `types.h` and
  `numeric.h`. Older GCC pulled it in transitively from `<atomic>` /
  `<vector>`; GCC 15 stopped doing that and refused to compile
  `uint8_t` / `int16_t` / `uint64_t` references. Backports the same
  fix as upstream's [PR #18](https://github.com/SeasX/SeasClick/pull/18).
- Two arg-info / zend_parse_parameters mismatches that PHP 8.3
  flagged as fatal: `select()` declared 3 required args while ZPP
  accepted 1, and `execute()` declared 2 while ZPP accepted 1. Both
  now declare 1 required argument.

### Removed

- PHP 5 support: branches, `TSRMLS_*` macros, `MAKE_STD_ZVAL`-style
  scaffolding deleted. Net -155 lines from the wrapper layer.
- `package.xml`: PECL is closed; PIE pulls metadata from `composer.json`.
- `EXPERIMENTAL`: PECL stability marker, empty file.
- `travis/`: Travis CI is no longer used. CI lives on GitHub Actions.
- `config.w32.without` Windows stub: replaced with a `config.w32` that
  emits a clear "unsupported" warning. Full Windows build of the
  vendored zstd + absl + lz4 + cityhash is a separate project.

[Unreleased]: https://github.com/iliaal/php_clickhouse/compare/0.5.0...HEAD
[0.5.0]: https://github.com/iliaal/php_clickhouse/releases/tag/0.5.0
