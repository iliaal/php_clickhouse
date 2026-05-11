# php_clickhouse

[![Tests](https://github.com/iliaal/php_clickhouse/actions/workflows/tests.yml/badge.svg)](https://github.com/iliaal/php_clickhouse/actions/workflows/tests.yml)
[![Version](https://img.shields.io/github/v/release/iliaal/php_clickhouse)](https://github.com/iliaal/php_clickhouse/releases)
[![License: PHP-3.01](https://img.shields.io/badge/License-PHP--3.01-green.svg)](http://www.php.net/license/3_01.txt)
[![Follow @iliaa](https://img.shields.io/badge/Follow-@iliaa-000000?style=flat&logo=x&logoColor=white)](https://x.com/intent/follow?screen_name=iliaa)

![php_clickhouse: native binary protocol vs HTTP](images/php_clickhouse-hero.jpg)

Native PHP extension for [ClickHouse](https://clickhouse.com/), built on the official [ClickHouse/clickhouse-cpp](https://github.com/ClickHouse/clickhouse-cpp) v2.6.1 client. Speaks the native binary TCP protocol with LZ4 / ZSTD compression and optional TLS, picking up where [SeasX/SeasClick](https://github.com/SeasX/SeasClick) left off in 2020. 30-40% faster than HTTP-based clients on heavy workloads, with modern types (Date32, Time64, Decimal128, LowCardinality, Map), multi-endpoint failover, and structured exceptions.

## Why this fork?

[SeasX/SeasClick](https://github.com/SeasX/SeasClick) was the canonical native PHP ClickHouse extension and stopped accepting PRs in 2020. Several follow-up PRs there have been pending for years. This fork:

- Renames the extension to `php_clickhouse` (module `clickhouse`, classes `ClickHouse` / `ClickHouseException`)
- Upgrades the vendored client from artpaul-fork v1.x to the official ClickHouse/clickhouse-cpp v2.6.1
- Adds Date32 / Time / Time64 / DateTime64 / Int128 / UInt128 / Decimal128 / LowCardinality / Map column types, multi-endpoint failover, ZSTD compression, query_id propagation, and TLS
- Ships an updated test suite, CI, PIE-based packaging, and benchmarks

The original `SeasClick` and `SeasClickException` class names continue to work as deprecated aliases.

Method signatures, return types, and class properties are declared with PHP types via a stub-driven arginfo workflow (`clickhouse.stub.php` → `clickhouse_arginfo.h`). Reflection, IDE completion, and static analyzers (PHPStan, Psalm) see the typed surface without manual stubs.

## 🚀 Install

Via [PIE](https://github.com/php/pie) (the PHP Foundation's PECL successor):

```sh
pie install iliaal/php_clickhouse
```

With TLS support:

```sh
pie install iliaal/php_clickhouse --enable-clickhouse-openssl
```

Building from source:

```sh
git clone https://github.com/iliaal/php_clickhouse.git
cd php_clickhouse
phpize
./configure                              # default build
./configure --enable-clickhouse-openssl  # with TLS, requires OpenSSL development headers
                                         #   (libssl-dev on Debian/Ubuntu, openssl-devel
                                         #    on RHEL/Fedora, openssl-dev on Alpine)
make && sudo make install
```

Add `extension=clickhouse.so` to your `php.ini`. The build needs a C++17-capable compiler (GCC 8+, Clang 7+, MSVC 2019+); LZ4, ZSTD, abseil-int128, and CityHash are vendored under `lib/clickhouse-cpp/contrib/`.

### Platforms

| Platform | Status | Notes |
|----------|--------|-------|
| Linux NTS | first-class | PHP 7.4 through 8.5, CI matrix |
| Linux ZTS | first-class | PHP 8.4 ZTS in CI |
| Windows (NTS, TS) | supported | PHP 8.4 x86 / x64 in CI; pre-built `.dll` released via PIE |
| macOS | unverified | should build (POSIX path); no CI lane |

Per-Client state lives on the `zend_object` itself (custom `create_object` / `free_obj` handlers), so ZTS works without locking. There is no module-global state to thread-isolate.

### Test server

For development and integration tests, the simplest path is the official ClickHouse server image:

```sh
docker run -d --name clickhouse-test \
    --ulimit nofile=262144:262144 \
    -p 9000:9000 -p 8123:8123 -p 9440:9440 \
    -e CLICKHOUSE_USER=test \
    -e CLICKHOUSE_PASSWORD=test \
    clickhouse/clickhouse-server:latest
```

Stop and clean up: `docker rm -f clickhouse-test`.

## 🛠️ Quick example

```php
<?php
$ch = new ClickHouse([
    "host"        => "127.0.0.1",
    "port"        => 9000,
    "database"    => "test",
    "user"        => "default",
    "passwd"      => "",
    "compression" => "lz4",   // or "zstd" / true / false
]);

$ch->execute("CREATE TABLE IF NOT EXISTS events (
    id UInt32, ts DateTime64(3), tag LowCardinality(String)
) ENGINE = Memory");

$ch->insert("events", ["id", "ts", "tag"], [
    [1, time(), "alpha"],
    [2, time(), "beta"],
]);

// Filter by an in-memory list of IDs without bloating the SQL — the
// server reads `ext_ids` as a named temp table for this query only.
$hits = $ch->selectWithExternalData(
    "SELECT id, tag FROM events WHERE id IN ext_ids",
    [["name" => "ext_ids",
      "columns" => ["id" => "UInt32"],
      "rows" => [[1], [42], [1337]]]]
);

foreach ($ch->select("SELECT id, ts, tag FROM events ORDER BY id",
                     [], ClickHouse::DATE_AS_STRINGS) as $row) {
    print_r($row);
}
```

## 📦 Supported data types

* `Array(T)` (single-level)
* `Date`, `Date32`, `DateTime`, `DateTime64(N[, timezone])`
* `Time`, `Time64(N)` (server side requires ClickHouse 25.x or later)
* `Decimal`, `Decimal32`, `Decimal64`, `Decimal128(P, S)` (read/write as scaled-integer strings)
* `Enum8`, `Enum16`
* `FixedString(N)`
* `Float32`, `Float64`
* `Int8` … `Int64`, `UInt8` … `UInt64`
* `Int128`, `UInt128` (round-trip as decimal strings; PHP integers are 64-bit)
* `IPv4`, `IPv6`
* `LowCardinality(String)`, `LowCardinality(FixedString(N))`, `LowCardinality(Nullable(String))`, `LowCardinality(Nullable(FixedString(N)))`
* `Map(K, V)` over scalar K and V (`String`, `Int8` through `Int64`, `UInt8` through `UInt64`, `Float32`, `Float64`, `UUID`) plus `LowCardinality(String)` keys and values. `Map(LowCardinality(K), V)` reads are not yet decoded by the vendored client; writes succeed and the data is queryable server side.
* `Point`, `Ring`, `Polygon`, `MultiPolygon` (geo)
* `Nullable(T)`
* `String`
* `Tuple` (read-only)
* `UUID`

## Configuration reference

All keys go in the array passed to `new ClickHouse([...])`.

### Connection

| Key | Type | Default | Description |
|---|---|---|---|
| `host` | string | `127.0.0.1` | Server host |
| `port` | int | `9000` | Native TCP port (or `9440` for TLS) |
| `database` | string | `default` | Default database |
| `user` | string | `default` | Username |
| `passwd` | string | (empty) | Password |
| `endpoints` | array | (none) | List of `[{host, port}, ...]` for round-robin failover. Tried in order on connect failure. |

### Compression

| Key | Type | Default | Description |
|---|---|---|---|
| `compression` | bool / string | `false` | `false`/`"none"` = uncompressed; `true`/`"lz4"` = LZ4 (fast); `"zstd"` = ZSTD (denser) |
| `max_compression_chunk_size` | int | `65535` | Block size used by the compressor |

### Timeouts and retry

| Key | Type | Default | Description |
|---|---|---|---|
| `connect_timeout` | int (sec) | `5` | TCP connect deadline |
| `connect_timeout_ms` | int (ms) | (none) | Sub-second connect deadline; overrides the seconds key when set |
| `receive_timeout` | int (sec) | `0` | Read deadline (0 = no timeout) |
| `receive_timeout_ms` | int (ms) | (none) | Sub-second read deadline; overrides the seconds key when set |
| `send_timeout` | int (sec) | `0` | Write deadline |
| `send_timeout_ms` | int (ms) | (none) | Sub-second write deadline; overrides the seconds key when set |
| `retry_count` | int | `1` | Send retries on transient failure |
| `retry_timeout` | int (sec) | `5` | Sleep between retries |
| `tcp_nodelay` | bool | `true` | TCP_NODELAY |
| `tcp_keepalive` | bool | `false` | TCP keepalive |
| `tcp_keepalive_idle` | int (sec) | `60` | Idle time before first keepalive probe |
| `tcp_keepalive_intvl` | int (sec) | `5` | Interval between probes |
| `tcp_keepalive_cnt` | int | `3` | Failed probes before declaring dead |

### TLS (build with `--enable-clickhouse-openssl`)

| Key | Type | Default | Description |
|---|---|---|---|
| `ssl` | bool | `false` | Enable TLS |
| `ssl_min_protocol_version` | string | `tls1.2` | Minimum protocol; one of `tls1.0`, `tls1.1`, `tls1.2`, `tls1.3` |
| `ssl_skip_verify` | bool | `false` | Skip cert validation; dev only |
| `ssl_use_default_ca` | bool | `true` | Trust the system CA bundle |
| `ssl_ca_files` | string \| array | (none) | PEM CA file path(s) |
| `ssl_ca_directory` | string | (none) | OpenSSL hashed-cert directory |

Building without `--enable-clickhouse-openssl` and passing `ssl => true` raises `ClickHouseException` at construct time.

## Methods

```php
$ch = new ClickHouse(array $config);

// Schema / DDL
$ch->execute(string $sql,
             array $params = [],
             string $query_id = "",
             array $settings = []);

// Read
$rows = $ch->select(string $sql,
                    array $params = [],
                    int $fetch_mode = 0,
                    string $query_id = "",
                    array $settings = []);

// Read with external in-memory tables sent alongside the query.
// Each entry: ['name' => 'ext_x', 'columns' => ['c' => 'Type', ...],
//              'rows' => [[...], [...], ...]]. The query body references
// the external table by name (e.g. `WHERE id IN ext_ids`). Keeps the
// SQL small when filtering by big lists; multiple externals per call.
$rows = $ch->selectWithExternalData(string $sql,
                                    array  $externals,
                                    array  $params = [],
                                    int    $fetch_mode = 0,
                                    string $query_id = "",
                                    array  $settings = []);

// Bulk insert (entire dataset in one call)
$ch->insert(string $table, array $columns, array $values,
            string $query_id = "",
            array $settings = []);

// Same as insert(), but rows are associative arrays and the column list
// is derived from the first row's keys.
$ch->insertAssoc(string $table, array $rows,
                 string $query_id = "",
                 array $settings = []);

// Streaming insert (open block, append, close)
$ch->writeStart(string $table, array $columns,
                string $query_id = "",
                array $settings = []);
$ch->write(array $values);
$ch->write(array $more_values);
$ch->writeEnd();

$ch->ping();          // returns true on success, throws on failure

// Streaming reads (no full-result PHP array)
$iter = $ch->selectStream(string $sql, array $params = [],
                          string $query_id = "", array $settings = []);
foreach ($iter as $row) { /* ... */ }   // ClickHouseRowIterator: Iterator+Countable

$ch->selectStreamCallback(string $sql, callable $cb,
                          array $params = [], string $query_id = "",
                          array $settings = []);  // true per-row stream

// Write rows straight to a PHP stream resource as TSV / CSV — bypasses
// per-row PHP array assembly and userland callbacks; cells are
// formatted block-by-block in C++ and flushed to the stream. Returns
// rows written. Formats: TabSeparated (alias TSV), TabSeparatedWithNames
// (alias TSVWithNames), CSV, CSVWithNames. Dates emit as ISO strings;
// Decimal / Int128 / UInt128 as decimal strings. NULL = `\N` (TSV) /
// empty (CSV). Array / Tuple / Map columns are rejected.
$f = fopen("/tmp/events.tsv", "wb");
$n = $ch->selectToStream(string $sql, array $params, mixed $stream,
                         string $format = "TabSeparated",
                         string $query_id = "", array $settings = []);
fclose($f);

// smi2-style result wrapper: returns a ClickHouseStatement (Iterator,
// Countable, ArrayAccess, JsonSerializable) carrying a per-call stats
// snapshot. Use when you want fetchOne / fetchKeyPair / fetchColumn /
// statistics() on the result, or want to keep stats around after
// running other queries on the client. Plain $ch->select() is faster
// when you just need the array.
$stmt = $ch->selectStatement(string $sql, array $params = [],
                             string $query_id = "", array $settings = []);
foreach ($stmt as $row) { /* ... */ }
$stmt[0]; count($stmt); json_encode($stmt);
$stmt->fetchOne(); $stmt->fetchKeyPair(); $stmt->fetchColumn();
$stmt->toArray(); $stmt->statistics();

// Settings, observability, helpers
$ch->setSettings(array $settings);     // client-wide; per-call overrides; chainable
$ch->setSetting(string $key, mixed $value);  // single-key sugar, chainable
$ch->setDatabase(string $database);    // USE on the server, updates default; chainable
$ch->setProgressCallback(?callable $cb);
$ch->setProfileCallback(?callable $cb);
$ch->setVerbose(true);                 // JSON lifecycle lines on STDERR
$ch->setVerbose(fn($e, $ctx) => ...);  // or custom sink: select_start /
                                       // data_block / select_finish /
                                       // execute_start / execute_finish /
                                       // server_exception
$ch->setVerbose(false);                // disable
$stats = $ch->getStatistics();         // last query: rows, bytes, elapsed_ms, query_id

$ch->resetConnection();
$info = $ch->getServerInfo();          // name, version_*, revision, timezone, display_name
$ep   = $ch->getCurrentEndpoint();     // {host, port} of the active endpoint, or null

$ch->enableLogQueries(bool $enabled = true);
$log = $ch->getLogQueries();           // returns and clears the buffer

// DDL / introspection helpers
$ch->isExists(string $database, string $table);
$ch->showDatabases();
$ch->showProcesslist();
$ch->getServerVersion();
$ch->databaseSize(?string $database = null);     // {bytes_on_disk, rows}
$ch->tablesSize(?string $database = null);
$ch->tableSize(string $table);                   // {rows, bytes_on_disk, partitions, modification_time}
$ch->partitions(string $table);
$ch->showTables(?string $database = null, ?string $like = null);
$ch->showCreateTable(string $table);
$ch->getServerUptime();                // seconds
$ch->truncateTable(string $table);
$ch->dropPartition(string $table, string $partition);
```

`fetch_mode` is a bitmask of `ClickHouse::FETCH_ONE`, `ClickHouse::FETCH_KEY_PAIR`, `ClickHouse::FETCH_COLUMN`, and `ClickHouse::DATE_AS_STRINGS`.

### Placeholders

Two placeholder syntaxes are supported in `select` / `execute`:

- `{name}` is client-side identifier substitution. Two value shapes:
  - **Scalar** (string / int / float): coerces to a single token, validated as either an identifier (`[A-Za-z_][A-Za-z0-9_]*`, optionally db-qualified by one dot like `db.tbl`) or a numeric literal. Whitespace, commas, quotes, semicolons, backslashes, and other SQL meta-characters are rejected.
  - **Array**: each element is validated as a single scalar token, then joined with `", "` for the SQL replacement. Use this for legitimate column lists; an element with internal whitespace or commas is still rejected. A scalar value containing commas is rejected — that ambiguity (single identifier vs. list) is the point of the array-shape API.
- `{name:Type}` is a server-side typed parameter. The SQL text is passed through unchanged; the value is bound via `Query::SetParam` and the server quotes and parses it according to `Type`. Pass PHP arrays for `Array(T)` types; `null` becomes a server `NULL`.

```php
// Single-identifier substitution.
$ch->select("SELECT * FROM {tbl}", ["tbl" => "users"]);

// Column-list substitution via array value.
$ch->select("SELECT {cols} FROM users",
            ["cols" => ["id", "name", "email"]]);

// Server-side typed parameters, no client-side quoting needed.
$ch->select("SELECT * FROM users WHERE id IN ({ids:Array(UInt32)})",
            ["ids" => [1, 2, 3]]);
```

### Settings

`setSettings()` applies client-wide. The 5th argument on `select` / `insert` / `execute` / `writeStart` overrides per call. Both accept plain `string => string` pairs; PHP scalars are stringified for you.

```php
$ch->setSettings(["max_execution_time" => "30"]);

// Per-call override.
$ch->select("SELECT * FROM big_table",
            [], 0, "",
            ["max_execution_time" => "5",
             "max_memory_usage"   => "1000000000"]);
```

### Statistics and progress

```php
$ch->setProgressCallback(function (array $p) {
    fprintf(STDERR, "rows=%d bytes=%d\n", $p["rows"], $p["bytes"]);
});

$ch->select("SELECT count() FROM big_table");

$stats = $ch->getStatistics();
// rows_read, bytes_read, total_rows, written_rows, written_bytes,
// blocks, rows_before_limit, applied_limit, elapsed_ms
```

### Query log

`enableLogQueries(true)` turns on a per-client buffer that records each completed `select` / `insert` / `execute` / `writeStart`. Each entry is `{sql, query_id, elapsed_ms, rows_read, bytes_read, error_code, error_message}`. `error_code` is `0` on success, the server error code on a `ServerException`, or `-1` on client/network failure. `getLogQueries()` returns the buffer and clears it.

```php
$ch->enableLogQueries(true);
$ch->select("SELECT count() FROM users");
$ch->insert("logins", ["user_id", "ts"], $batch);

foreach ($ch->getLogQueries() as $q) {
    fprintf(STDERR, "[%.1fms] %s\n", $q["elapsed_ms"], $q["sql"]);
}
```

### Structured exceptions

`ClickHouseException` carries three extra public properties:

- `server_code`: ClickHouse error code (e.g. 159 = `TIMEOUT_EXCEEDED`). `0` for client-side errors.
- `server_name`: server-reported exception name (e.g. `DB::Exception`). `null` for client-side errors.
- `query_id`: the query id associated with the failed call, when one was supplied. `null` otherwise.

## 📊 Benchmarks

PHP 8.4.22 / ClickHouse 26.3.9.8 / localhost loopback / `Memory` table (no disk).

Compared against [smi2/phpClickHouse](https://github.com/smi2/phpClickHouse), the most popular pure-PHP HTTP client.

Each cell is total wall-clock seconds for `selectCount` queries plus a single bulk insert of `dataCount` rows.

| dataCount × selectCount × limit | phpClickHouse (HTTP) | php_clickhouse (uncompressed) | php_clickhouse (LZ4) | php_clickhouse (ZSTD) |
|---|---:|---:|---:|---:|
| 10000 × 1 × 5000   | 0.112 | 0.085 | 0.074 | 0.023 |
| 10000 × 1 × 5000   | 0.104 | 0.030 | 0.024 | 0.081 |
| 10000 × 100 × 5000  | 0.298 | 0.263 | 0.209 | 0.218 |
| 10000 × 100 × 10000 | 0.303 | 0.210 | 0.265 | 0.215 |
| 1000 × 200 × 500   | 0.558 | 0.416 | 0.415 | 0.413 |
| 1000 × 200 × 1000  | 0.611 | 0.408 | 0.410 | 0.395 |
| 1000 × 500 × 500   | 1.428 | 1.063 | 0.976 | 0.982 |
| 1000 × 500 × 1000  | 1.383 | 0.959 | 1.025 | 1.030 |
| 1000 × 800 × 500   | 2.477 | 1.533 | 1.569 | 1.543 |
| 1000 × 800 × 1000  | 2.498 | 1.588 | 1.563 | 1.519 |

At high select counts the native binary protocol runs 30-40% faster than the HTTP client. On small bursts (`dataCount=10000, selectCount=1`), php_clickhouse with ZSTD or LZ4 is fastest. To reproduce, see [`bench/`](bench/).

## 🔗 PHP Performance Toolkit

Companion native PHP extensions for high-throughput PHP workloads:

- **[php_excel](https://github.com/iliaal/php_excel)**: native Excel I/O. 7-10× faster than PhpSpreadsheet, full XLS/XLSX with formulas, conditional formatting, and rich text. Powered by LibXL.
- **[mdparser](https://github.com/iliaal/mdparser)**: native CommonMark + GFM markdown parser. 15-30× faster than pure-PHP libraries (Parsedown, cebe, michelf). Powered by cmark-gfm.
- **[fastchart](https://github.com/iliaal/fastchart)**: native chart-rendering extension. 19 chart types behind one fluent OO API; composes with caller-owned `\GdImage` canvases.

## 📚 Read more

Full background, fork rationale, and benchmark methodology in the launch post: [php_clickhouse: A Native ClickHouse Client for PHP, Picking Up Where SeasClick Left Off](https://ilia.ws/blog/php-clickhouse-a-native-clickhouse-client-for-php-picking-up-where-seasclick-left-off).

## License

The PHP-side wrapper is licensed under [PHP-3.01](LICENSE).

The vendored client library at `lib/clickhouse-cpp/` is [ClickHouse/clickhouse-cpp](https://github.com/ClickHouse/clickhouse-cpp), licensed under the [Apache License 2.0](lib/clickhouse-cpp/LICENSE).

The vendored compression libraries (`lib/clickhouse-cpp/contrib/lz4/`, `contrib/zstd/`, `contrib/cityhash/`) carry BSD-style licenses; abseil int128 (`contrib/absl/`) is Apache 2.0. See each subdirectory for the exact text.

## Credits

`php_clickhouse` started as a fork of [SeasX/SeasClick](https://github.com/SeasX/SeasClick) by SeasX Group (`ahhhh.wang@gmail.com`). The original PR-4 work to add fetch modes landed in 2019 and the upstream maintainer hasn't accepted external PRs since. Independent re-vendoring, port to clickhouse-cpp v2.6.1, new types, TLS, and packaging are by Ilia Alshanetsky <ilia@ilia.ws>.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Security issues: [SECURITY.md](SECURITY.md).

---

[Follow @iliaa on X](https://x.com/iliaa) • [Blog](https://ilia.ws) • If this sped up your stack, ⭐ star it!
