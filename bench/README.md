# php_clickhouse benchmarks

Compares `php_clickhouse` (LZ4, ZSTD, uncompressed) against
[smi2/phpClickHouse](https://github.com/smi2/phpClickHouse), a
pure-PHP HTTP client.

## Run

```sh
cd bench
composer update --no-dev          # installs phpClickHouse
CLICKHOUSE_HOST=127.0.0.1 \
CLICKHOUSE_PORT=9000 \
CLICKHOUSE_HTTP_PORT=8123 \
CLICKHOUSE_USER=test \
CLICKHOUSE_PASSWD=test \
php -d extension=../modules/clickhouse.so bench_mark.php
```

The runner uses one `Memory` table per client. Table creation, truncation,
and an initial warm-up are outside the measured interval. Each result is
the median of four runs; client order rotates on every run so each client
occupies every order position once. A measured run contains one bulk
insert followed by the stated number of selects.

Set `BENCH_REPETITIONS` to change the sample count. Multiples of four
preserve equal client-order exposure. `BENCH_SMOKE=1` runs a small
two-sample connectivity and runner check.
The published C-backend results use `BENCH_REPETITIONS=8`.

The composer dependencies need `ext-curl`, `ext-mbstring`, `ext-phar`,
`ext-tokenizer`. Use a stock distro PHP for the benchmark run if your
dev PHP is built with `--disable-all`.

`tests/` runs the functional suite; this directory is performance-only
and deliberately separate.

## Results

Latest run lives in the top-level [README.md](../README.md) under
"Benchmarks". To update it after a code change, run the default matrix
on an otherwise idle host and replace the table with the emitted Markdown.

## Known limitations

- The HTTP client runs without compression (its default), while two of the
  three extension columns are compressed. A `phpClickHouse (HTTP, gzip)`
  column would make the compression axis symmetric; until it exists, read
  the compressed columns as protocol + compression, not compression alone.
- Native binary TCP (9000) against JSON over HTTP (8123) is a protocol
  comparison as much as a library comparison.
- smi2 sets `CURLOPT_FORBID_REUSE`, so every HTTP query pays a fresh TCP
  handshake that the extension's persistent connection does not. That is
  the client's own behaviour, but it is where much of the fixed per-query
  gap lives.
- Only medians are published; run-to-run spread is not shown.
- ClickHouse 26.9 enables asynchronous inserts by default. Its adaptive
  batching delay depends on queue history, and table UUIDs affect queue
  assignment. This runner recreates its tables between invocations, so
  comparing short-workload medians from separate runs can measure different
  queue delays. For backend A/B measurements, create the tables once, retain
  their UUIDs across both builds, and keep the existing per-sample truncation,
  warm-up, workload, and checks unchanged.
