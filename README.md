SeasClick
=====
[![Build Status](https://travis-ci.org/SeasX/SeasClick.svg?branch=master)](https://travis-ci.org/SeasX/SeasClick)

PHP client for [ClickHouse](https://clickhouse.com/), built on the official [ClickHouse/clickhouse-cpp](https://github.com/ClickHouse/clickhouse-cpp) library (vendored at v2.6.1).

## ClickHouse
* [What is ClickHouse](https://clickhouse.com/docs)
* [ClickHouse Performance](https://clickhouse.com/docs/en/introduction/performance/)

## Supported data types

* Array(T) (single-level only)
* Date, DateTime, DateTime64(N)
* Decimal, Decimal32, Decimal64, Decimal128
* Enum8, Enum16
* FixedString(N)
* Float32, Float64
* IPv4, IPv6
* LowCardinality(String) and LowCardinality(FixedString(N))
* Map (column creation only; row read/write through the new column API is a TODO)
* Nullable(T)
* String
* Tuple (read-only)
* UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64
* UUID

## Supported PHP version
PHP 7.1+ (PHP 8.x supported and tested)

## Performance
![image](https://github.com/SeasX/SeasClick/raw/master/tests/bench_mark/bench_mark.png)

This performance test [demo](https://github.com/SeasX/SeasClick/blob/master/tests/bench_mark/bench_mark.php) is compared to [phpclickhouse](https://github.com/smi2/phpClickHouse)

## Install
```sh
git clone https://github.com/SeasX/SeasClick.git
cd SeasClick
phpize
./configure
make && sudo make install
```

The vendored `lib/clickhouse-cpp/` requires a C++17 compiler (set automatically by `config.m4`). All optional dependencies (LZ4, ZSTD, abseil-int128) are vendored under `lib/clickhouse-cpp/contrib/`.

### TLS / SSL

To build with TLS support, install OpenSSL development headers (`libssl-dev` on Debian/Ubuntu) and configure with the opt-in flag:

```sh
phpize
./configure --enable-SeasClick-openssl
make && sudo make install
```

Connect over TLS by passing `"ssl" => true` in the constructor config:

```php
$c = new SeasClick([
    "host"             => "ch.example.com",
    "port"             => 9440,                   // tcp_port_secure on the server
    "user"             => "default",
    "passwd"           => "secret",
    "ssl"              => true,
    "ssl_ca_files"     => "/etc/ssl/certs/ca.crt", // string or array of paths
    "ssl_ca_directory" => "/etc/ssl/certs",        // optional
    "ssl_use_default_ca" => true,                  // default true; set false to lock to the explicit CA only
    "ssl_skip_verify"  => false,                   // true only for dev / self-signed
]);
```

Building without `--enable-SeasClick-openssl` leaves the extension SSL-free; passing `"ssl" => true` in that case throws `SeasClickException("SeasClick was built without TLS support...")` so misconfiguration is loud, not silent.

## Testing against a local ClickHouse server

The fastest way to spin up a server for integration tests is the official server image:

```sh
docker run -d --name seasclick-ch \
    --ulimit nofile=262144:262144 \
    -p 9000:9000 -p 8123:8123 \
    -e CLICKHOUSE_USER=test \
    -e CLICKHOUSE_PASSWORD=test \
    -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
    clickhouse/clickhouse-server:24.8
```

Then connect with `host=127.0.0.1 port=9000 user=test passwd=test`. To stop and clean up: `docker rm -f seasclick-ch`.

For a minimal showcase image, see [`ClickHouse/ClickHouse/docker/bare`](https://github.com/ClickHouse/ClickHouse/tree/master/docker/bare). That recipe builds a `FROM scratch` image around a pre-built `clickhouse` binary; it is a reference for understanding ClickHouse's runtime dependency surface, not a daily-driver test image.

## Example

```php
<?php
$config = [
    "host" => "clickhouse",
    "port" => 9000,
    "compression" => true
];

clientTest($config);

function clientTest($config)
{
    $deleteTable = true;
    $client = new SeasClick($config);

    $client->execute("CREATE DATABASE IF NOT EXISTS test");

    testArray($client, $deleteTable);
}

function testArray($client, $deleteTable = false) {
    $client->execute("CREATE TABLE IF NOT EXISTS test.array_test (string_c String, array_c Array(Int8), arraynull_c Array(Nullable(String))) ENGINE = Memory");

    $client->insert("test.array_test", [
        'string_c', 'array_c', 'arraynull_c'
    ], [
        ['string_c1', [1, 2, 3], ['string']],
        ['string_c2', [4, 5, 6], [null]]
    ]);

    $result = $client->select("SELECT {select} FROM {table}", [
        'select' => 'string_c, array_c, arraynull_c',
        'table' => 'test.array_test'
    ]);
    var_dump($result);

    if ($deleteTable) {
        $client->execute("DROP TABLE {table}", [
            'table' => 'test.array_test'
        ]);
    }
}
```
#### [More examples](https://github.com/SeasX/SeasClick/blob/master/tests/test.php)

## Support
SeasX Group
