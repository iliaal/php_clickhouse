--TEST--
ClickHouse Date32 round-trip
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.date32_t");
$c->execute("CREATE TABLE test.date32_t (id UInt32, d Date32) ENGINE = Memory");

// Pass epoch seconds (int) only; "Y-m-d" string input goes through
// mktime which is host-TZ-dependent and would make the test flaky
// across CI runners and dev hosts.
$c->insert("test.date32_t", ["id", "d"], [
    [1, 1745539200],   // 2025-04-25 UTC
    [2, 0],            // 1970-01-01 UTC
    [3, -2208988800],  // 1900-01-01 UTC
]);

$rows = $c->select("SELECT id, d FROM test.date32_t ORDER BY id", [], ClickHouse::DATE_AS_STRINGS);
foreach ($rows as $r) {
    echo $r["id"], " => ", $r["d"], "\n";
}

$c->execute("DROP TABLE test.date32_t");
?>
--EXPECT--
1 => 2025-04-25
2 => 1970-01-01
3 => 1900-01-01
