--TEST--
ClickHouse setSettings + per-call settings precedence
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";
$c = new ClickHouse(clickhouse_test_config());

// Global setting: limit to 1ms — any non-trivial query trips it.
$c->setSettings(["max_execution_time" => "0.001"]);

try {
    $c->select("SELECT count() FROM numbers(100000000)");
    echo "global: no throw\n";
} catch (ClickHouseException $e) {
    // Code 159 = TIMEOUT_EXCEEDED.
    echo "global code: ", ($e->server_code === 159 ? "159" : "other:" . $e->server_code), "\n";
}

// Per-call override: relax the timeout. Should succeed.
$res = $c->select("SELECT 1 AS x", [], 0, "", ["max_execution_time" => "30"]);
echo "per-call x: ", $res[0]["x"], "\n";

// Per-call setting also wins on a tiny value.
try {
    $c->select("SELECT count() FROM numbers(100000000)", [], 0, "",
               ["max_execution_time" => "0.001"]);
    echo "per-call: no throw\n";
} catch (ClickHouseException $e) {
    echo "per-call code: ", ($e->server_code === 159 ? "159" : "other:" . $e->server_code), "\n";
}

// Clear globals; without a per-call setting the query runs.
$c->setSettings([]);
$res = $c->select("SELECT 2 AS x");
echo "cleared x: ", $res[0]["x"], "\n";
?>
--EXPECT--
global code: 159
per-call x: 1
per-call code: 159
cleared x: 2
