--TEST--
ClickHouse multi-endpoint failover
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$base = clickhouse_test_config();

// First endpoint is intentionally dead; second is the live test server.
$config = $base + [
    "endpoints" => [
        ["host" => "127.0.0.1", "port" => 1],   // closed port
        ["host" => $base["host"], "port" => $base["port"]],
    ],
];

$c = new ClickHouse($config);
echo "select via failover: ", $c->select("SELECT 1", [], ClickHouse::FETCH_ONE), "\n";
?>
--EXPECT--
select via failover: 1
