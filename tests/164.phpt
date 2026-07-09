--TEST--
DR-010: numeric Array(T) typed parameters reject non-numeric string elements
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// Array(Int*/UInt*/Float*/Decimal*/Bool) typed params splice elements into
// the SQL array literal unquoted. A PHP string element used to be spliced
// raw, so ["1,2,3"] became three values (arity corruption) and punctuation
// could inject into the literal. String elements must now be single numeric
// literals; ints/floats and clean numeric strings still work.

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.dr010");
$c->execute("CREATE TABLE test.dr010 (x Int32) ENGINE = Memory");
$c->insert("test.dr010", ['x'], [[1], [2], [3]]);

$q = "SELECT count() c FROM test.dr010 WHERE x IN {ids:Array(Int32)}";

// comma-in-string: arity corruption -> now rejected
try { $c->select($q, ["ids" => ["1,2,3"]]); echo "comma string: NO THROW\n"; }
catch (ClickHouseException $e) { echo "comma string: REJECTED\n"; }

// injection-ish punctuation -> rejected
try { $c->select($q, ["ids" => ["1),(2"]]); echo "punct string: NO THROW\n"; }
catch (ClickHouseException $e) { echo "punct string: REJECTED\n"; }

// legit int elements
$r = $c->select($q, ["ids" => [1, 2]]);
echo "int elems count: ", $r[0]['c'], "\n";

// legit numeric-string elements still accepted
$r = $c->select($q, ["ids" => ["1", "3"]]);
echo "numeric-string elems count: ", $r[0]['c'], "\n";

$c->execute("DROP TABLE test.dr010");
?>
--EXPECT--
comma string: REJECTED
punct string: REJECTED
int elems count: 2
numeric-string elems count: 2
