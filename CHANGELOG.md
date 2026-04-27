# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.8.0] - 2026-04-27

Architecture refactor that moves per-Client state from file-scope
`std::map` banks onto the `zend_object` itself. The change unblocks
ZTS support (no more global state to thread-isolate), plugs a
pre-existing leak on bailout (`free_obj` fires; the old userspace-only
`__destruct` did not), and fixes a refcount bug on the progress
callback. ZTS Linux builds are now first-class; a Windows
`config.w32` ships and is exercised in CI as a build-only smoke
test. Adds streaming via `ClickHouseRowIterator` plus a true
per-row callback path, four new Client knobs / introspection
methods, seven DDL helpers, transparent `LowCardinality(Nullable(T))`
and geo-type round-trips, and `query_id` echo through
`getStatistics()`.

### Added

- Streaming row iterator: `selectStream(string $sql, ...)` returns a
  `ClickHouseRowIterator` (`Iterator` + `Countable`) so large result
  sets walk lazily without materializing as a single PHP array. The
  iterator survives `unset($client)` since blocks own their column
  data via `shared_ptr`.
- True per-row callback path: `selectStreamCallback(string $sql,
  callable $cb, ...)` invokes the callback once per row as blocks
  arrive, never accumulating the full result. Use this for unbounded
  streams.
- Client knobs and introspection: `ping_before_query` constructor
  config key (round-trip ping before each query), `resetConnection()`
  method, `getServerInfo()` (name, version_major/minor/patch,
  revision, timezone, display_name), `getCurrentEndpoint()`
  (host/port of the active endpoint when an endpoints[] pool is in
  use), `setProfileCallback(?callable $cb)` for `Profile` packets
  (rows, blocks, bytes, rows_before_limit, applied_limit).
- `query_id` echoed through `getStatistics()` so callers can correlate
  a recorded statistics snapshot to a server-side query in
  `system.query_log`.
- DDL helpers: `isExists(string $database, string $table): bool`,
  `showDatabases(): array`, `showProcesslist(): array`,
  `getServerVersion(): string`, `tableSize(string $table): array`,
  `truncateTable(string $table): bool`,
  `dropPartition(string $table, string $partition): bool`. All
  identifier arguments validated; `dropPartition` SQL-escapes the
  partition value.
- Type coverage: `LowCardinality(Nullable(String))` and
  `LowCardinality(Nullable(FixedString))` round-trip on both read
  and write paths. Geo types Point, Ring, Polygon, MultiPolygon
  round-trip via `ColumnGeo` (Point as `[Float64, Float64]`, others
  as nested arrays). `SimpleAggregateFunction(f, T)` reads
  transparently as `T`.
- Map matrix expansion. The insert path now accepts any
  `Map(K, V)` over scalar K and V (String, all signed/unsigned
  integer widths, Float32/64, UUID) plus `LowCardinality(String)`
  keys and values. Read path mirrors the same matrix except for
  `LowCardinality` keys (vendor gap). Previously only five
  hardcoded combinations worked.
- `ClickHouseRowIterator` class registered alongside `ClickHouse`.
- smi2/phpClickHouse-style ergonomics:
  - `setSettings()` returns `$this` instead of `bool` so callers
    can chain. Truthy semantics preserved.
  - `setSetting(string $key, mixed $value): static` for single-key
    chainable sugar on top of the array form.
  - `setDatabase(string $database): static` issues `USE` on the
    server and updates the cached default used by helpers like
    `databaseSize()` and `showTables()`. Validates the identifier.
  - `ClickHouseException::getServerCode()`, `getServerName()`,
    `getQueryId()` getter aliases for the existing public
    `server_code`/`server_name`/`query_id` properties. Same data,
    smi2-compatible call shape.
- `selectStatement(string $sql, ...): ClickHouseStatement`
  result-wrapper variant of `select()`. The new
  `ClickHouseStatement` class implements Iterator, Countable,
  ArrayAccess, and JsonSerializable over the materialized rows,
  plus `fetchOne()` / `fetchKeyPair()` / `fetchColumn()` /
  `toArray()` / `statistics()`. Carries a per-call stats snapshot
  so it survives the client running other queries afterwards.
  Read-only: `offsetSet`/`offsetUnset` throw. Plain `select()` is
  unchanged and remains the faster path when you just need the
  array.

### Changed

- Per-Client state (Client*, insert Block, ClientStats, settings,
  progress/profile callbacks, log_enabled, query_log) lives on the
  `zend_object` itself via custom `create_object`/`free_obj`
  handlers. Replaces the seven file-scope `std::map<int, ...>` banks
  keyed on `Z_OBJ_HANDLE`.
- ZTS gate at MINIT removed. The extension loads under `--enable-zts`
  builds; per-object state means no thread-shared mutable state to
  protect.
- `config.w32` rewritten from a 9-line warning stub to a full Windows
  build script that mirrors `config.m4`'s source list, includes, and
  flags. Optional `--enable-clickhouse-openssl` plumbing is mirrored
  via `CHECK_LIB("libssl.lib", ...)`. CI exercises Windows as a build
  + extension-load smoke (no live server tests on Windows).
- CI matrix gains a `linux-zts` job (PHP 8.4 ZTS built from source)
  and a `windows` job (build-only).

### Fixed

- IPv4 / IPv6 read paths no longer crash. Vendored clickhouse-cpp
  v2.6.1 made `ColumnIPv4`/`ColumnIPv6` siblings of (not subclasses
  of) `ColumnUInt32`/`ColumnFixedString`, so the prior
  `As<ColumnUInt32>()` / `As<ColumnFixedString>()` calls returned
  null and segfaulted on dereference. Use `ColumnIPv*::AsString(row)`
  for canonical dotted-quad / `::1` form.
- Progress callback zval refcount: `setProgressCallback` now uses
  `ZVAL_COPY` instead of a struct copy, so the callable doesn't get
  freed out from under us when the caller goes out of scope.
- Connection / insert-block leaks on bailout: cleanup runs in the new
  `free_obj` handler, which fires unconditionally. Previously the
  userspace `__destruct` didn't run on fatal errors, leaking the
  underlying `Client*` and any half-open insert stream.

### Known limitations

- `SELECT ... WITH TOTALS` and `SETTINGS extremes=1` still throw
  `unimplemented 7` from the cpp layer. clickhouse-cpp v2.6.1 does
  not dispatch the Totals/Extremes packet types
  ([upstream issue #297](https://github.com/ClickHouse/clickhouse-cpp/issues/297));
  `getTotals()` / `getExtremes()` are deferred to a future release.
- `Map(LowCardinality(K), V)` read paths are not yet decoded by
  the vendored library (writes succeed). `showProcesslist()`
  selects a fixed projection of standard columns to avoid the
  unsupported map columns (`ProfileEvents`, `Settings`, `used_*`).

## [0.7.0] - 2026-04-26

Feature release closing the ergonomics gap with smi2/phpClickHouse.
Adds per-query and client-wide settings, server-side typed parameters,
a progress callback, a statistics getter, structured exception fields,
millisecond timeout precision, an associative-row insert helper, and a
small set of SQL helper methods. All additive; no BC breaks.

Method signatures, return types, and class properties are now declared
via a stub-driven arginfo workflow and visible to Reflection / IDEs /
static analyzers.

### Added

- `setSettings(array $settings)` for client-wide ClickHouse settings
  (e.g. `max_execution_time`, `max_memory_usage`, `async_insert`).
  Per-call settings take a 5th array argument on `select()`,
  `insert()`, `execute()`, `writeStart()`. Per-call overrides global.
- Server-side typed parameters via the `{name:Type}` placeholder
  syntax. Routed through `Query::SetParam` so the server quotes and
  parses according to the declared `Type`. Plain `{name}`
  placeholders keep their existing client-side identifier-substitution
  behavior. Arrays format as ClickHouse array literals so
  `Array(UInt32)`, `Array(String)`, etc. round-trip cleanly.
- `setProgressCallback(?callable $cb)` invokes the callable for every
  `Progress` packet during a query, receiving an associative array of
  `rows`, `bytes`, `total_rows`, `written_rows`, `written_bytes`.
- `getStatistics()` returns `rows_read`, `bytes_read`, `total_rows`,
  `written_rows`, `written_bytes`, `blocks`, `rows_before_limit`,
  `applied_limit`, `elapsed_ms` from the last completed query. Reset
  at the start of each `select` / `execute` / `insert` / `writeStart`.
- Structured `ClickHouseException` fields: `server_code` (server
  error code, e.g. 159 for TIMEOUT_EXCEEDED), `server_name` (e.g.
  `DB::Exception`), and `query_id`. Populated on server errors and on
  any throw that has a query-id context; unset on pure client errors.
- `insertAssoc(string $table, array $rows, string $query_id = "",
  array $settings = [])` derives the column list from the keys of
  the first row and forwards to `insert()`.
- SQL helper methods: `databaseSize(?string $database)`,
  `tablesSize(?string $database)`, `partitions(string $table)`,
  `showTables(?string $database, ?string $like)`,
  `showCreateTable(string $table)`, `getServerUptime()`. Each
  validates identifiers against the existing safe-character set.
- Config keys `connect_timeout_ms`, `receive_timeout_ms`, and
  `send_timeout_ms` for sub-second timeout precision. Override the
  existing seconds-based keys when present.
- `enableLogQueries(bool $enabled = true)` toggles a per-client query
  log accumulator; `getLogQueries()` returns the entries and clears
  the buffer. Each entry carries `sql`, `query_id`, `elapsed_ms`,
  `rows_read`, `bytes_read`, `error_code`, `error_message`. Errors
  are recorded with the ClickHouse server code (or `-1` for
  client/network failures).

### Changed

- `select()`, `insert()`, `execute()`, and `writeStart()` now build a
  full `clickhouse::Query` object internally so settings, server-side
  params, progress, and profile callbacks can attach. Behavior of
  existing call sites is unchanged.
- Vendored `clickhouse-cpp` patched to expose
  `Client::BeginInsert(const Query&)` so the streaming insert path
  honors per-query settings and progress callbacks. Documented in
  `lib/clickhouse-cpp/LOCAL_PATCHES.md`.
- Migrated to stub-driven arginfo (`clickhouse.stub.php` + generated
  `clickhouse_arginfo.h`). Method parameter and return types are now
  declared and visible to Reflection / IDEs / static analyzers;
  previously they were untyped at the engine boundary. Behavior is
  unchanged for correctly-typed callers; wrong-type callers now hit
  ZPP at the boundary instead of a custom thrown exception inside the
  method body.
- ClickHouse and ClickHouseException properties are now declared with
  types via the stub.
- Compat shims in `php7_wrapper.h` keep the generated arginfo header
  compiling unchanged across the entire build matrix (PHP 7.4 through
  8.5). On pre-8.0 builds the polyfills drop type information rather
  than emulate it, so reflection signatures revert to untyped on those
  versions; runtime behavior is unchanged.

### For contributors

- New phpt tests `034`–`041` cover structured exception fields,
  settings precedence, typed-param round-trip, progress callback
  firing, fast connect-timeout, `insertAssoc`, each SQL helper, and
  the query log accumulator.

## [0.6.0] - 2026-04-25

Hardening release on top of 0.5.0. Closes a SQL-injection class
through the `{placeholder}` substitution, fixes a handful of
lifecycle crashes around `__construct` failure and orphan
`writeStart`, and resolves several smaller correctness bugs in the
data path. Two upstream clickhouse-cpp v2.6.1 bugs are patched
locally and queued for upstream.

### Security

- `select()` and `execute()` placeholder substitution validates each
  value against an identifier-and-numerics character set; quotes,
  semicolons, backslashes, and other SQL meta-characters are
  rejected before the SQL is built. Closes an injection class that
  was reachable any time a caller fed user input through `$params`.
- `insert()` and `writeStart()` validate table and column
  identifiers against ClickHouse identifier syntax. Empty names,
  leading digits, and shell-meta characters throw before the INSERT
  is built.
- New `ssl_min_protocol_version` config knob (default `tls1.2`) so
  a server speaking only deprecated TLS versions fails closed
  without explicit config.
- `ClickHouseException` messages strip the SQL fragment that
  clickhouse-cpp appends to its errors, so a literal placed in a
  placeholder can't leak back to userland through `e.what()`.
- The `passwd` config key is consumed into `ClientOptions` and
  discarded; it's no longer stored as a PHP-object property, so
  `var_dump`, `serialize`, and reflection don't expose it.
- ZTS PHP builds refuse to load with an `E_CORE_ERROR`. Process-
  global Client state would race on shared handle space.

### Added

- 9 new phpt tests (025-033) covering `ping()`, BC aliases, TLS
  round-trip, `writeStart` query_id propagation, default-database
  config, error paths, placeholder rejection, identifier
  rejection, and object-lifecycle paths.

### Fixed

- `__construct` no longer `RETURN_TRUE`s on connection failure; the
  half-constructed object never reaches userland.
- Every `clientMap.at(key)` site routed through a `getClient()`
  helper that throws `ClickHouseException` on miss. `ping()`
  previously did the lookup outside its `try` block, so
  `std::out_of_range` escaped the PHP boundary unhandled.
- `__destruct` silently no-ops when no client was registered, and
  calls `EndInsert()` if a `writeStart()` was left dangling so the
  server doesn't see a half-open insert.
- `write()` and `writeEnd()` reject calls without a matching
  `writeStart()`. `writeEnd()` erases the in-progress flag only
  after `EndInsert()` returns.
- `Date` insert: dropped the `tm_gmtoff` shift so raw epoch ints
  round-trip TZ-independently.
- `Nullable(Enum8)` / `Nullable(Enum16)`: NULL rows no longer crash
  inside `ColumnEnum::Append`.
- `Decimal` reads apply column scale before formatting. A value
  inserted as `12345.6789012345` now reads back as
  `12345.6789012345`, not the unscaled storage integer.
- `Int128` / `UInt128` string parse rejects malformed input and
  detects overflow during accumulation.
- `to_time_t()` uses `timegm()` instead of `mktime()` so Date and
  DateTime string round-trips don't drift by the runner's TZ.
- `FETCH_ONE` select returns the first row of the result, not the
  first row of the last block.
- `Tuple` insert iterates by tuple arity and validates per-row
  arity, fixing an out-of-bounds read when row count differed
  from arity.
- Signed integer reads (Int8..Int64) cast through `zend_long`;
  negative values no longer surface as huge unsigned numbers.
- `tcp_keepalive_cnt`, `max_compression_chunk_size`, and endpoint
  port bounds-check before truncating.
- Unknown `compression` strings throw instead of silently
  disabling compression.
- HashTable leaks in `insert()` / `write()` error paths.
- Six tests previously TODO-skipped now run.

### Changed

- LICENSE replaced with the canonical PHP-3.01 text (the file
  previously held Apache 2.0, contradicting every source-file
  header, `composer.json`, and the README's license section).
- README documents `Tuple` insert as supported (was listed as
  read-only).
- README benchmark section dropped the unmaintained
  `lizhichao/one-ck` comparison column.

### For contributors

- Vendored `clickhouse-cpp` v2.6.1 patched for two upstream bugs:
  `Client::Impl::BeginInsert` was dropping `query_id` from the wire
  packet, and `ColumnStringBlock::AppendUnsafe` called `memcpy`
  with a NULL source on empty `string_view`. Both documented in
  `lib/clickhouse-cpp/LOCAL_PATCHES.md`. The empty-string fix has
  an upstream PR at clickhouse-cpp#489.
- `CONTRIBUTING.md` spells out the LOCAL_PATCHES.md re-apply step
  on lib bumps.
- ASan CI job is gating, not informational. Switched to
  `-shared-libasan` so `__cxa_throw` interception works across the
  PHP / extension dlopen boundary.
- Compile warnings cleared on PHP 7.4-8.5
  (`-Wunused-but-set-variable`, `-Wswitch`,
  `-Wmaybe-uninitialized`).
- typesToPhp.cpp: integer arms in `insertColumn` and
  `convertToZval`, the five `Map(K, V)` insert permutations, and
  the `DateTime` / `Date` / `Date32` read paths collapsed into
  templated helpers.
- Dead-code sweep: `FAST_ZPP` dead arms (never defined), unused
  macros in `php7_wrapper.h`, TSRM scaffolding in
  `php_clickhouse.h`, commented-out `clickhouse_version`, unused
  `<iostream>` includes, 18 unreachable `break;` after `return;`.
  Roughly 750 lines net removed.
- Migrated remaining `SC_HASHTABLE_FOREACH_START2` call sites to
  `ZEND_HASH_FOREACH_*` directly. The old macro silently dropped
  integer keys.
- Renamed `clientInsertBlack` to `clientInsertBlock` (Block typo).

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

[Unreleased]: https://github.com/iliaal/php_clickhouse/compare/0.7.0...HEAD
[0.7.0]: https://github.com/iliaal/php_clickhouse/releases/tag/0.7.0
[0.6.0]: https://github.com/iliaal/php_clickhouse/releases/tag/0.6.0
[0.5.0]: https://github.com/iliaal/php_clickhouse/releases/tag/0.5.0
