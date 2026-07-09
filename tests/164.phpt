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

// A Stringable object coerces via __toString to the same raw payload and
// must not bypass the gate that a literal string hits.
class ArityBomb { public function __toString() { return "1,2,3"; } }
try { $c->select($q, ["ids" => [new ArityBomb()]]); echo "comma object: NO THROW\n"; }
catch (ClickHouseException $e) { echo "comma object: REJECTED\n"; }

// A Stringable returning a clean numeric literal still works.
class NumStr { public function __toString() { return "2"; } }
$r = $c->select($q, ["ids" => [new NumStr()]]);
echo "numeric-object elems count: ", $r[0]['c'], "\n";

// legit int elements
$r = $c->select($q, ["ids" => [1, 2]]);
echo "int elems count: ", $r[0]['c'], "\n";

// legit numeric-string elements still accepted
$r = $c->select($q, ["ids" => ["1", "3"]]);
echo "numeric-string elems count: ", $r[0]['c'], "\n";

// The special float words inf / nan (optionally signed) are valid ClickHouse
// numeric literals and must pass the gate; the server accepts them for a
// Float column. Verify the whole array binds (length 4) rather than a count.
$r = $c->select("SELECT length({v:Array(Float64)}) AS n",
                ["v" => ["inf", "-inf", "nan", "1.5"]]);
echo "float specials len: ", $r[0]['n'], "\n";

$c->execute("DROP TABLE test.dr010");
?>
--EXPECT--
comma string: REJECTED
punct string: REJECTED
comma object: REJECTED
numeric-object elems count: 1
int elems count: 2
numeric-string elems count: 2
float specials len: 4
