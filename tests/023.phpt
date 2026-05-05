--TEST--
ClickHouse Time and Time64 round-trip
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
require __DIR__ . "/_clickhouse.inc";
clickhouse_skip_if_no_server();
$c = new ClickHouse(clickhouse_test_config());
try {
    $c->execute("DROP TABLE IF EXISTS test._time_probe");
    $c->execute("CREATE DATABASE IF NOT EXISTS test");
    $c->execute("CREATE TABLE test._time_probe (t Time) ENGINE = Memory");
    $c->execute("DROP TABLE test._time_probe");
} catch (ClickHouseException $e) {
    print "skip ClickHouse build does not support Time / Time64";
    exit;
}
?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.time_t");
$c->execute("CREATE TABLE test.time_t (id UInt32, t Time, t3 Time64(3)) ENGINE = Memory");

// Time / Time64 inserts accept seconds-since-midnight (int) and Time64
// internally scales by 10^precision.
$c->insert("test.time_t", ["id", "t", "t3"], [
    [1, 3661,  3661],   // 01:01:01 / 01:01:01.000
    [2, 0,     0],      // 00:00:00 / 00:00:00.000
    [3, -3600, -3600],  // -01:00:00 / -01:00:00.000
]);

$rows = $c->select("SELECT id, t, t3 FROM test.time_t ORDER BY id", [], ClickHouse::DATE_AS_STRINGS);
foreach ($rows as $r) {
    echo $r["id"], "|", $r["t"], "|", $r["t3"], "\n";
}

$c->execute("DROP TABLE test.time_t");
?>
--EXPECT--
1|01:01:01|01:01:01.000
2|00:00:00|00:00:00.000
3|-01:00:00|-01:00:00.000
