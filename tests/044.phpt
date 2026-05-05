--TEST--
ClickHouse SimpleAggregateFunction(f, T) reads transparently as T
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.saf_t");
$c->execute("CREATE TABLE test.saf_t (
    k UInt32,
    s SimpleAggregateFunction(sum, UInt64),
    m SimpleAggregateFunction(max, Float64),
    n SimpleAggregateFunction(any, String)
) ENGINE = AggregatingMergeTree() ORDER BY k");

$c->insert("test.saf_t", ["k", "s", "m", "n"], [
    [1, 10, 1.5, "a"],
    [1,  5, 2.5, "b"],
    [2,  7, 0.5, "c"],
]);
$c->execute("OPTIMIZE TABLE test.saf_t FINAL");

$rows = $c->select("SELECT k, sum(s) AS sum_s, max(m) AS max_m, any(n) AS any_n FROM test.saf_t GROUP BY k ORDER BY k");
foreach ($rows as $r) {
    echo $r["k"], "|", $r["sum_s"], "|", $r["max_m"], "|", $r["any_n"], "\n";
}

$c->execute("DROP TABLE test.saf_t");
?>
--EXPECTREGEX--
1\|15\|2\.5\|[ab]
2\|7\|0\.5\|c
