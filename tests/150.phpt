--TEST--
ClickHouse by-reference values: nested composites, settings, and config dereference correctly
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");

// Each case puts a by-reference cell inside a nested composite; without a
// dereference the type branch sees IS_REFERENCE and either errors or, worse,
// silently writes a placeholder (null -> 0 / "").

$c->execute("DROP TABLE IF EXISTS test.r_an");
$c->execute("CREATE TABLE test.r_an (a Array(Nullable(Int32))) ENGINE = Memory");
$n = null;
$c->insert("test.r_an", ["a"], [[[1, &$n, 3]]]);
echo "Array(Nullable(Int32)): ", json_encode($c->select("SELECT a FROM test.r_an", [], ClickHouse::FETCH_ONE)), "\n";

$c->execute("DROP TABLE IF EXISTS test.r_lc");
$c->execute("CREATE TABLE test.r_lc (a Array(LowCardinality(Nullable(String)))) ENGINE = Memory");
$ln = null;
$c->insert("test.r_lc", ["a"], [[["x", &$ln, "y"]]]);
echo "Array(LC(Nullable(String))): ", json_encode($c->select("SELECT a FROM test.r_lc", [], ClickHouse::FETCH_ONE)), "\n";

$c->execute("DROP TABLE IF EXISTS test.r_tp");
$c->execute("CREATE TABLE test.r_tp (t Tuple(Int32, String)) ENGINE = Memory");
$iv = 42;
$c->insert("test.r_tp", ["t"], [[[&$iv, "hi"]]]);
echo "Tuple field by-ref: ", json_encode($c->select("SELECT t FROM test.r_tp", [], ClickHouse::FETCH_ONE)), "\n";

$c->execute("DROP TABLE IF EXISTS test.r_en");
$c->execute("CREATE TABLE test.r_en (a Array(Enum8('a' = 1, 'b' = 2))) ENGINE = Memory");
$ev = 2;
$c->insert("test.r_en", ["a"], [[[1, &$ev]]]);
echo "Array(Enum8) by-ref int: ", json_encode($c->select("SELECT a FROM test.r_en", [], ClickHouse::FETCH_ONE)), "\n";

$c->execute("DROP TABLE IF EXISTS test.r_pt");
$c->execute("CREATE TABLE test.r_pt (p Point) ENGINE = Memory");
$pt = [1.5, 2.5];
$c->insert("test.r_pt", ["p"], [[&$pt]]);
echo "Point by-ref: ", json_encode($c->select("SELECT p FROM test.r_pt", [], ClickHouse::FETCH_ONE)), "\n";

// By-reference setting value (formatScalarParam).
$flag = false;
$one = $c->select("SELECT {n} AS x FROM system.one", ["n" => "1"], ClickHouse::FETCH_ONE, "",
    ["output_format_json_quote_64bit_integers" => &$flag]);
echo "by-ref setting: ", $one, "\n";

// By-reference constructor config: compression name and endpoints array.
$z = "zstd";
$cfg = clickhouse_test_config();
$cfg["compression"] = &$z;
$c2 = new ClickHouse($cfg);
// Read the protected property via a bound closure: portable across 7.4-8.5
// (Reflection's setAccessible is required on 7.4 but deprecated in 8.5).
$readComp = Closure::bind(function () { return $this->compression; }, $c2, ClickHouse::class);
echo "by-ref compression: ", $readComp(), "\n";

foreach (["r_an","r_lc","r_tp","r_en","r_pt"] as $t) $c->execute("DROP TABLE test.$t");
?>
--EXPECT--
Array(Nullable(Int32)): [1,null,3]
Array(LC(Nullable(String))): ["x",null,"y"]
Tuple field by-ref: [42,"hi"]
Array(Enum8) by-ref int: ["a","b"]
Point by-ref: [1.5,2.5]
by-ref setting: 1
by-ref compression: 2
