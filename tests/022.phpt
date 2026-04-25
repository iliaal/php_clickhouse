--TEST--
SeasClick multi-endpoint failover
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; seasclick_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$base = seasclick_test_config();

// First endpoint is intentionally dead; second is the live test server.
$config = $base + [
    "endpoints" => [
        ["host" => "127.0.0.1", "port" => 1],   // closed port
        ["host" => $base["host"], "port" => $base["port"]],
    ],
];

$c = new SeasClick($config);
echo "select via failover: ", $c->select("SELECT 1", [], SeasClick::FETCH_ONE), "\n";
?>
--EXPECT--
select via failover: 1
