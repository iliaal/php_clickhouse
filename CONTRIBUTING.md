# Contributing to php_clickhouse

## Requirements

- PHP 7.4 or later (8.x recommended; debug builds are useful for
  development: `--enable-debug`). Matches `composer.json` and the CI matrix.
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
3. Follow the [source build instructions](README.md#-install), then
   [run the tests](#running-tests).
4. Add `.phpt` coverage for any new behavior. Tests skip cleanly if
   no server is reachable, so adding new ones doesn't punish people
   without a local CH.
5. Open a PR. Include the rationale, the test you added, and any
   benchmark deltas if your change is in a hot path.

## Running tests

After building and installing the extension using the
[README instructions](README.md#-install), start its
[local test server](README.md#test-server). Use a dedicated test server:
[the tests create and drop tables in the `test` database](tests/002.phpt).
The [CI test setup](.github/workflows/tests.yml) installs `clickhouse.so`
into PHP's `extension_dir` so the PHPT `--EXTENSIONS--` directive can find it.

Run this from the repository root with the README's Docker credentials:

```sh
export CLICKHOUSE_HOST=127.0.0.1
export CLICKHOUSE_PORT=9000
export CLICKHOUSE_USER=test
export CLICKHOUSE_PASSWD=test
export TEST_PHP_EXECUTABLE="$(command -v php)"
export NO_INTERACTION=1
php run-tests.php -j1 --show-diff -g FAIL,BORK,LEAK,XLEAK,SKIP tests/
```

These settings and runner options match the [Linux CI test step](.github/workflows/tests.yml).
The [test helper](tests/_clickhouse.inc) defaults to host `clickhouse`, not
`127.0.0.1`, and reads `CLICKHOUSE_PASSWD` (the Docker image uses
`CLICKHOUSE_PASSWORD`). Keep `-j1`: tests share tables, and the PHP 8.6
runner otherwise defaults to parallel execution, as noted in the CI step.
Review skipped tests as well as failures. The helper skips server-dependent
tests when the server is unreachable; CI uses a separate
[summary guard](scripts/assert-phpt-summary.sh) to reject an all-skip run
and unexpected skips.

## Code style

- Two-space indent for PHP, four-space for C/C++.
- File headers carry the standard PHP-3.01 license block plus an
  `| Author: ` line. New files credit yourself there too.
- Prefer adding to `clickhouse.cpp` and `typesToPhp.cpp` over creating
  new translation units unless there's a structural reason. The build
  rebuilds everything in one shot anyway and the symbol surface is
  clearer when concentrated.
- C++ exceptions caught at the PHP boundary must go through
  `throwClickHouseError(e[, query_id])` so users get a
  `ClickHouseException` with sanitized message and optional
  `server_code` / `server_name` / `query_id`. Static validation
  (bad config, empty columns, etc.) may call
  `zend_throw_exception(clickhouse_exception_ce, ...)` directly.
  Never throw with a NULL class entry for ClickHouse errors.

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
4. Run the full test suite against the pinned
   `clickhouse/clickhouse-server` digest in `.github/workflows/tests.yml`
   (the `services.clickhouse.image` pin), not `:latest`.
5. Note any breaking changes in `CHANGELOG.md` under the unreleased
   section.

## Releases

Maintainers use `/release-ext` with the repository's
[`.release-config`](.release-config). The command's playbook lives in the
maintainer's `~/.claude/commands/release-ext.md`; it is not included in this
repository.

The [Linux/macOS](.github/workflows/release-linux.yml) and
[Windows](.github/workflows/release-windows.yml) binary workflows run when
a GitHub Release is published. See those workflow files for their build
matrices and manual validation options.
