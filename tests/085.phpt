--TEST--
ClickHouse insert() rejects rows with extra positional or named cells
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// Regression for round-11-followup CR-001: buildColumnMajorRows looped
// only `i < columns_count` and silently ignored any cells past that
// position. A row like `[1, 99]` against a single-column table landed
// as `1` with `99` dropped — quiet data loss with a successful insert
// return. Pre-flight per-row count check now rejects extras up front.

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.shape");
$c->execute("CREATE TABLE test.shape (a UInt8, b UInt8) ENGINE=Memory");

$probes = [
    "positional extra"           => [['a','b'], [[1, 2, 99]]],
    "named extra"                => [['a','b'], [["a"=>1, "b"=>2, "c"=>99]]],
    "later row extra positional" => [['a','b'], [[1, 2], [3, 4, 99]]],
    "later row extra named"      => [['a','b'], [["a"=>1, "b"=>2], ["a"=>3, "b"=>4, "extra"=>99]]],
    "single-column extra"        => [['a'],     [[1, 99]]],
];
foreach ($probes as $label => [$cols, $vals]) {
    try { $c->insert("test.shape", $cols, $vals); echo "$label: NO THROW\n"; }
    catch (ClickHouseException $e) { echo "$label: REJECTED\n"; }
}

// Sanity: well-formed rows still land cleanly.
$c->insert("test.shape", ["a","b"], [[10, 20], [11, 21]]);
$cnt = $c->select("SELECT count() FROM test.shape", [], ClickHouse::FETCH_ONE);
echo "rowcount: $cnt\n";

// Mixed positional + named where the row maps to declared columns
// without extras still works.
$c->insert("test.shape", ["a","b"], [["a"=>30, "b"=>40]]);
$cnt = $c->select("SELECT count() FROM test.shape", [], ClickHouse::FETCH_ONE);
echo "rowcount after named: $cnt\n";

$c->execute("DROP TABLE test.shape");
?>
--EXPECT--
positional extra: REJECTED
named extra: REJECTED
later row extra positional: REJECTED
later row extra named: REJECTED
single-column extra: REJECTED
rowcount: 2
rowcount after named: 3
