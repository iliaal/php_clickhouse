--TEST--
ClickHouse Date32 round-trip
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.date32_t");
$c->execute("CREATE TABLE test.date32_t (id UInt32, d Date32) ENGINE = Memory");

$c->insert("test.date32_t", ["id", "d"], [
    [1, "2026-04-25"],
    [2, "1900-01-01"],
    [3, "1970-01-01"],
]);

$rows = $c->select("SELECT id, d FROM test.date32_t ORDER BY id", [], ClickHouse::DATE_AS_STRINGS);
foreach ($rows as $r) {
    echo $r["id"], " => ", $r["d"], "\n";
}

$c->execute("DROP TABLE test.date32_t");
?>
--EXPECT--
1 => 2026-04-25
2 => 1900-01-02
3 => 1970-01-01
