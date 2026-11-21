# Contributing to php_clickhouse

## Requirements

- PHP 7.4 or later (8.x recommended; debug builds are useful for
  development: `--enable-debug`). Matches `composer.json` and the CI matrix.
- C11-capable compiler: GCC or Clang on Unix; the MSVC toolset matching your PHP build on Windows
- `phpize` and `php-config` (from `php-dev` / `php8.x-dev`)
- GNU Make

The protocol client lives in `lib/clickhouse-c/`; LZ4 and ZSTD live in
`lib/compression/`. Portable 128-bit arithmetic is implemented in `ch_wide.c`.
The default build needs no external client or compression library, C++
compiler, or Abseil. TLS builds need OpenSSL development headers and libraries
(`libssl-dev` on Debian/Ubuntu), but not PHP's OpenSSL extension.

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

### Threaded ZTS request regression

The PHP 8.4 ZTS CI cell also builds the shared embed SAPI and runs
`scripts/test-zts-request-thread.sh`. Unlike CLI PHPTs, this starts module
initialization on the main thread and constructs `ClickHouse` on fresh
worker threads. It protects the per-request TSRM cache initialization
without requiring a ClickHouse server.

To run locally on Linux, build PHP with `--enable-zts --enable-embed=shared`,
build the extension with that PHP's `phpize` and `php-config`, then run:

```sh
PHP_CONFIG=/path/to/php-zts/bin/php-config \
  ./scripts/test-zts-request-thread.sh modules/clickhouse.so
```

The same cell runs `scripts/test-zts-fatal-state.sh`, which needs the
ClickHouse server from the CLICKHOUSE_* variables. It runs two requests on
one worker thread: the first ends in a fatal error inside `insert()`
conversion, and the second checks that no conversion state carried over.
Run it locally the same way, with the server variables set.

### Fatal-error leak regressions

Some tests guard against native memory leaked by a fatal error. Their output
matches either way, so the ASan CI job also runs them through
`scripts/test-fatal-leaks.sh` with LeakSanitizer enabled:

```sh
./scripts/test-fatal-leaks.sh /path/to/asan/php modules/clickhouse.so \
  tests/277_select_memory_limit_deferred_oom.phpt tests/278_fatal_in_row_callback.phpt \
  tests/285_select_memory_limit_fatal_client_usable.phpt \
  tests/286_map_memory_limit_fatal_client_usable.phpt \
  tests/287_memory_limit_fatal_any_allocation.phpt \
  tests/288_memory_limit_fatal_fiber_interleaved.phpt
```

Add a test to that list in `.github/workflows/tests.yml` when the leak it
guards against happens in the test process itself. Tests that run their
fatal errors in child processes through `clickhouse_run_memory_limit_children()`
(287 through 293) start each child with LeakSanitizer enabled, so a regular
`run-tests.php --asan` run already fails on a child's leak report. Of those,
287 and 288 are also on the list, so `test-fatal-leaks.sh` checks them with
a plain CLI parent process as well.

## Code style

- Two-space indent for PHP, four-space for C.
- File headers carry the standard PHP-3.01 license block plus an
  `| Author: ` line. New files credit yourself there too.
- Keep changes in the component that owns the behavior: `clickhouse.c`
  for module/configuration code, `ch_backend.c` for transport,
  `ch_query.c` for query execution, `ch_insert.c` for insertion,
  `ch_results.c` for results, and `ch_types_read.c` / `ch_types_write.c`
  for conversion. Shared contracts live in `clickhouse_c_internal.h`,
  `ch_backend.h`, and `ch_types.h`.
- Return `CHC_*` status codes from the transport/protocol boundary.
  Translate them with `ch_throw_error()` so PHP receives sanitized
  `ClickHouseException` metadata. Preserve pending user exceptions.
  When a PHP boundary creates a local validation error, preserve its
  origin so `ch_annotate_local_exception()` can attach the query ID and
  sanitize the message without changing callback exceptions.
- Follow the ownership contracts in the shared headers. Decoded blocks may
  outlive their connection; block builders borrow their column data until
  sending completes. Never throw with a NULL class entry for ClickHouse errors.

## Vendored library updates

`lib/clickhouse-c/` tracks the commit recorded in
[.upstream/clickhouse-c.yml](.upstream/clickhouse-c.yml). The current pin is
`084498e63a8e814d8f8ab9a32b923d7d6c3c7d9f`. When updating it:

1. Import the complete upstream source tree, including its license and tests,
   from the chosen commit. Update the manifest's revision.
2. Reapply or explicitly retire every patch documented in
   `lib/clickhouse-c/LOCAL_PATCHES.md`. Keep the patch files under
   `patches/clickhouse-c/` reproducible against the pinned source and apply them
   in filename order. The patch set omits upstream-only workflow copies after
   import; the extension's active workflows remain in the repository-root
   `.github/workflows/` directory.
3. Check the PHP compatibility options in `ch_protocol.c`, the transport
   contract in `ch_backend.h`, and the source lists in `config.m4` and
   `config.w32`. Preserve legacy-server revision gates, deferred external
   data, decoding limits, error metadata, and compression configuration.
4. Run `scripts/test-vendored-patches.sh` and
   `scripts/check-vendored-patches.sh`. The latter reverses local patches and
   compares the complete client tree with pristine upstream. Set
   `CLICKHOUSE_C_PRISTINE` to a local pristine checkout for an offline check.
5. Run the upstream tests and the full PHPT suite against the pinned server
   digest in `.github/workflows/tests.yml`, plus the supported PHP, TLS,
   Windows, and 32-bit lanes. Verify older-server compatibility separately;
   upstream's default ClickHouse 23.3 minimum is not the PHP extension's
   server-support contract.
6. Record user-visible changes in `CHANGELOG.md` under Unreleased.

The retained C compression sources have a separate provenance record in
[lib/compression/UPSTREAM.md](lib/compression/UPSTREAM.md) and patch ledger in
[lib/compression/LOCAL_PATCHES.md](lib/compression/LOCAL_PATCHES.md). The guard
compares them with the `lz4/lz4` and `facebook/zstd` tags pinned in
`.upstream/lz4.yml` and `.upstream/zstd.yml`; those checkouts are
verification inputs, not build dependencies. Use `CLICKHOUSE_LZ4_PRISTINE`
and `CLICKHOUSE_ZSTD_PRISTINE` to supply them locally.

## Releases

Maintainers use `/release-ext` with the repository's
[`.release-config`](.release-config). The command's playbook lives in the
maintainer's `~/.claude/commands/release-ext.md`; it is not included in this
repository.

The [Linux/macOS](.github/workflows/release-linux.yml) and
[Windows](.github/workflows/release-windows.yml) binary workflows run when
a GitHub Release is published. See those workflow files for their build
matrices and manual validation options.
