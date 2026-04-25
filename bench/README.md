# php_clickhouse benchmarks

Compares `php_clickhouse` (LZ4, ZSTD, uncompressed) against:

- [smi2/phpClickHouse](https://github.com/smi2/phpClickHouse) — pure-PHP HTTP client
- [lizhichao/one-ck](https://github.com/lizhichao/one-ck) — pure-PHP TCP client

## Run

```sh
cd bench
composer update --no-dev          # installs phpClickHouse + one-ck
CLICKHOUSE_HOST=127.0.0.1 \
CLICKHOUSE_PORT=9000 \
CLICKHOUSE_HTTP_PORT=8123 \
CLICKHOUSE_USER=test \
CLICKHOUSE_PASSWD=test \
php -d extension=../modules/clickhouse.so bench_mark.php
```

The composer dependencies need `ext-curl`, `ext-mbstring`, `ext-phar`,
`ext-tokenizer`. Use a stock distro PHP for the benchmark run if your
dev PHP is built with `--disable-all`.

`tests/` runs the functional suite; this directory is performance-only
and deliberately separate.

## Results

Latest run lives in the top-level [README.md](../README.md) under
"Benchmarks". To update it after a code change, re-run with the same
`dataCount × selectCount × limit` matrix and replace the table.
