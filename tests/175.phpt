--TEST--
DR-004: DateTime64 string insert guards the tick multiply against int64 overflow
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// The DateTime64 string path did whole*scale + frac with no overflow guard
// (the integer path already guarded), so a far-future timestamp at high
// precision wrapped silently (2262-04-12 at precision 9 -> 1900-01-01...).

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");

function try_insert($c, $label, $type, $val) {
    $c->execute("DROP TABLE IF EXISTS test.dr004");
    $c->execute("CREATE TABLE test.dr004 (v $type) ENGINE = Memory");
    try {
        $c->insert("test.dr004", array('v'), array(array($val)));
        $r = $c->select("SELECT toString(v) s FROM test.dr004");
        echo "$label: stored ", $r[0]['s'], "\n";
    } catch (ClickHouseException $e) {
        echo "$label: REJECTED\n";
    }
}

// overflow at precision 9 -> rejected
try_insert($c, "DT64(9) 2262-04-12", "DateTime64(9)", "2262-04-12 00:00:00.000000000");
// near-max in-range still works
try_insert($c, "DT64(9) 2262-04-11", "DateTime64(9)", "2262-04-11 23:47:16.000000000");
try_insert($c, "DT64(3) 2024",       "DateTime64(3)", "2024-01-15 10:30:00.123");

$c->execute("DROP TABLE IF EXISTS test.dr004");
?>
--EXPECT--
DT64(9) 2262-04-12: REJECTED
DT64(9) 2262-04-11: stored 2262-04-11 23:47:16.000000000
DT64(3) 2024: stored 2024-01-15 10:30:00.123
