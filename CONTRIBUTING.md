# Contributing to php_clickhouse

## Requirements

- PHP 7.1 or later (8.x recommended; debug builds are useful for
  development: `--enable-debug`)
- C++17-capable compiler: GCC 8+, Clang 7+
- `phpize` and `php-config` (from `php-dev` / `php8.x-dev`)
- GNU Make

The vendored `lib/clickhouse-cpp/` includes its own copies of LZ4,
ZSTD, abseil's int128, and CityHash. No external libraries are
required to build the default extension. Building with TLS support
needs OpenSSL development headers (`libssl-dev` on Debian/Ubuntu).

## Bug reports

Use the [GitHub issue tracker](https://github.com/iliaal/php_clickhouse/issues).
Include:

- PHP version (`php -v`)
- php_clickhouse version (`php -r 'echo phpversion("clickhouse");'`)
- ClickHouse server version
- Operating system and compiler version
- Minimal reproducing code
- Expected vs actual behavior
- Any error messages, exceptions, or crash output

Before filing, try to reproduce against the latest `master` branch.

For security issues, do **not** file a public issue. See
[SECURITY.md](SECURITY.md).

## Pull requests

1. Fork and clone the repo
2. Create a topic branch off `master`
3. Build clean (`./configure --enable-clickhouse && make`) and run
   the test suite. Tests need a reachable ClickHouse server; the
   README has a one-liner Docker recipe.
4. Add `.phpt` coverage for any new behavior. Tests skip cleanly if
   no server is reachable, so adding new ones doesn't punish people
   without a local CH.
5. Open a PR. Include the rationale, the test you added, and any
   benchmark deltas if your change is in a hot path.

## Code style

- Two-space indent for PHP, four-space for C/C++.
- File headers carry the standard PHP-3.01 license block plus an
  `| Author: ` line. New files credit yourself there too.
- Prefer adding to `clickhouse.cpp` and `typesToPhp.cpp` over creating
  new translation units unless there's a structural reason. The build
  rebuilds everything in one shot anyway and the symbol surface is
  clearer when concentrated.
- C++ exceptions caught at the PHP boundary should go through
  `sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce, ...)`
  so users see `ClickHouseException` and not generic `Exception`.

## Vendored library updates

`lib/clickhouse-cpp/` tracks a specific tag of
[ClickHouse/clickhouse-cpp](https://github.com/ClickHouse/clickhouse-cpp).
When bumping it:

1. Drop the new release into `lib/clickhouse-cpp/clickhouse/` and
   `lib/clickhouse-cpp/contrib/` (keep cityhash, lz4, zstd, absl;
   skip gtest, ut, bench, tests).
2. Re-apply (or drop, if upstreamed) every patch listed in
   `lib/clickhouse-cpp/LOCAL_PATCHES.md`. Bumping without this step
   silently regresses the fixes the test suite depends on.
3. Update the source list in `config.m4`. The list there is
   alphabetical by directory; keep that order.
4. Run the full test suite against ClickHouse `latest` (the test
   server in CI is `clickhouse/clickhouse-server:latest`).
5. Note any breaking changes in `CHANGELOG.md` under the unreleased
   section.

## Releases

Maintainers run `/release` (see `.claude/commands/release.md` for the
playbook). The summary: bump version in `php_clickhouse.h`,
date-stamp the top section of `CHANGELOG.md`, tag, push.
