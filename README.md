# php_clickhouse

[![Tests](https://github.com/iliaal/php_clickhouse/actions/workflows/tests.yml/badge.svg)](https://github.com/iliaal/php_clickhouse/actions/workflows/tests.yml)
[![Version](https://img.shields.io/github/v/release/iliaal/php_clickhouse)](https://github.com/iliaal/php_clickhouse/releases)
[![License: PHP-3.01](https://img.shields.io/badge/License-PHP--3.01-green.svg)](http://www.php.net/license/3_01.txt)
[![Follow @iliaa](https://img.shields.io/badge/Follow-@iliaa-000000?style=flat&logo=x&logoColor=white)](https://x.com/intent/follow?screen_name=iliaa)

![php_clickhouse: native binary protocol vs HTTP](images/php_clickhouse-hero.jpg)

Native PHP extension for [ClickHouse](https://clickhouse.com/), built on the official [ClickHouse/clickhouse-cpp](https://github.com/ClickHouse/clickhouse-cpp) v2.6.2 client. Speaks the native binary TCP protocol with LZ4 / ZSTD compression and optional TLS, picking up where [SeasX/SeasClick](https://github.com/SeasX/SeasClick) left off in 2020. 30-40% faster than HTTP-based clients on heavy workloads, with modern types (Date32, Time64, Decimal128, LowCardinality, Map, JSON), multi-endpoint failover, and structured exceptions.

## 📖 Documentation

**Full usage guide, data-type read/write reference, and configuration: [iliaal.github.io/php_clickhouse](https://iliaal.github.io/php_clickhouse/)**

The docs site covers every supported type (what `insert()` accepts and what `select()` returns), `fetch_mode` flags, CSV/TSV streaming, placeholders, settings, observability, and the complete method list. This README is the quick start.

## Why this fork?

[SeasX/SeasClick](https://github.com/SeasX/SeasClick) was the canonical native PHP ClickHouse extension and stopped accepting PRs in 2020. Several follow-up PRs there have been pending for years. This fork:

- Renames the extension to `php_clickhouse` (module `clickhouse`, classes `ClickHouse` / `ClickHouseException`)
- Upgrades the vendored client from artpaul-fork v1.x to the official ClickHouse/clickhouse-cpp v2.6.2
- Adds Date32 / Time / Time64 / DateTime64 / Int128 / UInt128 / Decimal128 / LowCardinality / Map / JSON column types, multi-endpoint failover, ZSTD compression, query_id propagation, and TLS
- Ships an updated test suite, CI, PIE-based packaging, and benchmarks

The original `SeasClick` and `SeasClickException` class names continue to work as deprecated aliases. Method signatures, return types, and class properties are declared with PHP types via a stub-driven arginfo workflow, so reflection, IDE completion, and static analyzers see the typed surface.

## 🚀 Install

Via [PIE](https://github.com/php/pie) (the PHP Foundation's PECL successor):

```sh
pie install iliaal/php_clickhouse
```

With TLS support:

```sh
pie install iliaal/php_clickhouse --enable-clickhouse-openssl
```

> Bare `php:X.Y-cli` Docker images lack `/usr/bin/unzip`, which composer needs to extract PIE's prebuilt `.so` zip. Run `apt-get install -y unzip` before `pie install`, otherwise composer falls back to PHP's ZipArchive and PIE fails with `ExtensionBinaryNotFound`. Host installs that already have `unzip` are fine.

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
| Linux ZTS | build-verified, not a release target | PHP 8.4 ZTS build canary in CI; `composer.json` sets `support-zts: false` |
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

foreach ($ch->select("SELECT id, ts, tag FROM events ORDER BY id",
                     [], ClickHouse::DATE_AS_STRINGS) as $row) {
    print_r($row);
}
```

Configuration keys, the full method list, per-type read/write rules, placeholders, settings, streaming, and observability all live in the **[documentation site](https://iliaal.github.io/php_clickhouse/)**.

## 📊 Benchmarks

PHP 8.4.22 / ClickHouse 26.3.9.8 / localhost loopback / `Memory` table (no disk).

Compared against [smi2/phpClickHouse](https://github.com/smi2/phpClickHouse), the most popular pure-PHP HTTP client. Each cell is total wall-clock seconds for `selectCount` queries plus a single bulk insert of `dataCount` rows.

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

## 🔗 Native PHP extensions

Companion native PHP extensions:

- **[php_excel](https://github.com/iliaal/php_excel)**: native Excel I/O via LibXL. 7-10× faster than PhpSpreadsheet, full XLS/XLSX with formulas, formatting, and styling.
- **[mdparser](https://github.com/iliaal/mdparser)**: native CommonMark + GFM markdown parser via md4c. 15-30× faster than pure-PHP libraries.
- **[pdo_duckdb](https://github.com/iliaal/pdo_duckdb)**: PDO driver for DuckDB, analytical SQL in your PHP stack.
- **[fastjson](https://github.com/iliaal/fastjson)**: drop-in faster `ext/json`, backed by yyjson. 6× encode, 2.7× decode, 5× validate.
- **[phpser](https://github.com/iliaal/phpser)**: decoder-optimized binary serializer for cache workloads. Faster than igbinary on packed numerics and DTO batches.
- **[fast_uuid](https://github.com/iliaal/fast_uuid)**: high-throughput UUID generation (v1/v4/v7), batched CSPRNG and SIMD hex formatter, ramsey-compatible API.
- **[fastchart](https://github.com/iliaal/fastchart)**: native chart-rendering extension. 38 chart types behind one fluent OO API, SVG-canonical with PNG/JPG/WebP and optional PDF output.
- **[statgrab](https://github.com/iliaal/statgrab)**: system statistics (CPU, memory, disk, network) via libstatgrab, no parsing /proc by hand.
- **[phonetic](https://github.com/iliaal/phonetic)**: native phonetic name matching (Double Metaphone, Beider-Morse, Daitch-Mokotoff, NYSIIS, Match Rating), the encoders PHP core lacks.

## 📚 Read more

Full background, fork rationale, and benchmark methodology in the launch post: [php_clickhouse: A Native ClickHouse Client for PHP, Picking Up Where SeasClick Left Off](https://ilia.ws/blog/php-clickhouse-a-native-clickhouse-client-for-php-picking-up-where-seasclick-left-off).

## License

The PHP-side wrapper is licensed under [PHP-3.01](LICENSE).

The vendored client library at `lib/clickhouse-cpp/` is [ClickHouse/clickhouse-cpp](https://github.com/ClickHouse/clickhouse-cpp), licensed under the [Apache License 2.0](lib/clickhouse-cpp/LICENSE).

The vendored compression libraries (`lib/clickhouse-cpp/contrib/lz4/`, `contrib/zstd/`, `contrib/cityhash/`) carry BSD-style licenses; abseil int128 (`contrib/absl/`) is Apache 2.0. See each subdirectory for the exact text.

## Credits

`php_clickhouse` started as a fork of [SeasX/SeasClick](https://github.com/SeasX/SeasClick) by SeasX Group (`ahhhh.wang@gmail.com`). The original PR-4 work to add fetch modes landed in 2019 and the upstream maintainer hasn't accepted external PRs since. Independent re-vendoring, port to clickhouse-cpp v2.6.2, new types, TLS, and packaging are by Ilia Alshanetsky <ilia@ilia.ws>.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Security issues: [SECURITY.md](SECURITY.md).

---

[Follow @iliaa on X](https://x.com/iliaa) • [Blog](https://ilia.ws) • If this sped up your stack, ⭐ star it!
