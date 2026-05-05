--TEST--
ClickHouse DateTime64 fractional parser rejects bare dot, non-digits, and zero-precision suffixes
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// Regression for round-9-followup CR-003: to_time_t_with_frac
// (a) silently dropped any fractional suffix when precision==0,
// (b) accepted a bare dot ("12:34:56.") with no digits after,
// (c) didn't validate that the first character after the dot was a
// digit. All three combinations now throw.

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.dt64_p0");
$c->execute("DROP TABLE IF EXISTS test.dt64_p3");
$c->execute("CREATE TABLE test.dt64_p0 (dt DateTime64(0)) ENGINE=Memory");
$c->execute("CREATE TABLE test.dt64_p3 (dt DateTime64(3)) ENGINE=Memory");

$probes = [
    "p0 with .digits"     => ['dt64_p0', '2024-01-01 00:00:00.123'],
    "p0 with .garbage"    => ['dt64_p0', '2024-01-01 00:00:00.abc'],
    "p0 bare dot"         => ['dt64_p0', '2024-01-01 00:00:00.'],
    "p3 bare dot"         => ['dt64_p3', '2024-01-01 00:00:00.'],
    "p3 non-digit start"  => ['dt64_p3', '2024-01-01 00:00:00.a23'],
    "p3 trailing garbage" => ['dt64_p3', '2024-01-01 00:00:00.123abc'],
];
foreach ($probes as $label => [$tbl, $val]) {
    try { $c->insert("test.$tbl", ["dt"], [[$val]]); echo "$label: NO THROW\n"; }
    catch (ClickHouseException $e) { echo "$label: REJECTED\n"; }
}

// Sanity: well-formed values still land.
$c->insert("test.dt64_p0", ["dt"], [["2024-01-15 12:00:00"]]);
$c->insert("test.dt64_p3", ["dt"], [["2024-01-15 12:00:00.789"]]);
$rows0 = $c->select("SELECT toString(dt) AS dt FROM test.dt64_p0", [], ClickHouse::FETCH_ONE);
$rows3 = $c->select("SELECT toString(dt) AS dt FROM test.dt64_p3", [], ClickHouse::FETCH_ONE);
echo "p0: $rows0\n";
echo "p3: $rows3\n";

$c->execute("DROP TABLE test.dt64_p0");
$c->execute("DROP TABLE test.dt64_p3");
?>
--EXPECT--
p0 with .digits: REJECTED
p0 with .garbage: REJECTED
p0 bare dot: REJECTED
p3 bare dot: REJECTED
p3 non-digit start: REJECTED
p3 trailing garbage: REJECTED
p0: 2024-01-15 12:00:00
p3: 2024-01-15 12:00:00.789
