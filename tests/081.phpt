--TEST--
ClickHouse {name} placeholder requires single-token strings; arrays accepted as identifier lists
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// Regression for round-9-followup CR-001: the structural validator
// briefly accepted comma-separated lists in a single-string {name},
// which let `{tbl}` with value "a, b" turn `FROM {tbl}` into a cross
// join. The contract is now: string = exactly one identifier or
// numeric literal; arrays are the explicit list shape.

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
foreach (["fr1_a","fr1_b"] as $t) $c->execute("DROP TABLE IF EXISTS test.$t");
$c->execute("CREATE TABLE test.fr1_a (x Int32) ENGINE=Memory");
$c->execute("CREATE TABLE test.fr1_b (x Int32) ENGINE=Memory");
$c->insert("test.fr1_a", ["x"], [[1], [2]]);
$c->insert("test.fr1_b", ["x"], [[10], [20], [30]]);

// Single-string with commas: rejected.
try {
    $c->select("SELECT count() FROM {tbl}",
        ["tbl" => "test.fr1_a, test.fr1_b"], ClickHouse::FETCH_ONE);
    echo "string comma list: NO THROW\n";
} catch (ClickHouseException $e) { echo "string comma list: REJECTED\n"; }

// Array value with single element: works (single identifier per element).
$one = $c->select("SELECT count() FROM {tbl}",
    ["tbl" => ["test.fr1_a"]], ClickHouse::FETCH_ONE);
echo "array single: $one\n";

// Array value with two identifiers used as a column list.
$rows = $c->select("SELECT {cols} FROM test.fr1_a ORDER BY x",
    ["cols" => ["x", "x"]]);
echo "array col list rowcount: ", count($rows), "\n";

// Each array element is a SINGLE token; an element containing whitespace
// or commas is rejected per the same single-identifier rule.
try {
    $c->select("SELECT {cols} FROM test.fr1_a",
        ["cols" => ["x", "x AS y"]], ClickHouse::FETCH_ONE);
    echo "array element with space: NO THROW\n";
} catch (ClickHouseException $e) { echo "array element with space: REJECTED\n"; }

// Empty array is rejected.
try {
    $c->select("SELECT {cols} FROM test.fr1_a", ["cols" => []], ClickHouse::FETCH_ONE);
    echo "empty array: NO THROW\n";
} catch (ClickHouseException $e) { echo "empty array: REJECTED\n"; }

foreach (["fr1_a","fr1_b"] as $t) $c->execute("DROP TABLE test.$t");
?>
--EXPECT--
string comma list: REJECTED
array single: 2
array col list rowcount: 2
array element with space: REJECTED
empty array: REJECTED
