--TEST--
ClickHouse strict numeric validation extended to Map values, Int128/UInt128 non-strings, and geo coords
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// Regression for FR-002: CR-003 routed top-level scalar Int*/UInt*/
// Float* through strict_zval_long / strict_zval_double, but Map values,
// non-string Int128/UInt128 cells, and geo Point coordinates kept the
// permissive zval_get_long / zval_get_double path. Same data-corruption
// class: "abc" → 0, [] → 1, NaN → 0, etc. The strict helpers now flow
// through every numeric extractor.

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
foreach (["fr2_map","fr2_pt","fr2_i128","fr2_u128"] as $t) $c->execute("DROP TABLE IF EXISTS test.$t");
$c->execute("CREATE TABLE test.fr2_map (mi Map(String,Int64), mu Map(String,UInt8), mf Map(String,Float64)) ENGINE=Memory");
$c->execute("CREATE TABLE test.fr2_pt (p Point) ENGINE=Memory");
$c->execute("CREATE TABLE test.fr2_i128 (v Int128) ENGINE=Memory");
$c->execute("CREATE TABLE test.fr2_u128 (v UInt128) ENGINE=Memory");

$probes = [
    "Map Int64 abc" => function () use ($c) {
        $c->insert("test.fr2_map", ["mi","mu","mf"], [[["k"=>"abc"], [], []]]);
    },
    "Map UInt8 abc" => function () use ($c) {
        $c->insert("test.fr2_map", ["mi","mu","mf"], [[[], ["k"=>"abc"], []]]);
    },
    "Map Float64 abc" => function () use ($c) {
        $c->insert("test.fr2_map", ["mi","mu","mf"], [[[], [], ["k"=>"abc"]]]);
    },
    "Map UInt8 narrow overflow" => function () use ($c) {
        $c->insert("test.fr2_map", ["mi","mu","mf"], [[[], ["k"=>300], []]]);
    },
    "Int128 from array" => function () use ($c) {
        $c->insert("test.fr2_i128", ["v"], [[[1,2,3]]]);
    },
    "UInt128 from array" => function () use ($c) {
        $c->insert("test.fr2_u128", ["v"], [[[1,2,3]]]);
    },
    "Point abc abc" => function () use ($c) {
        $c->insert("test.fr2_pt", ["p"], [[["abc","def"]]]);
    },
    "Point with NaN" => function () use ($c) {
        $c->insert("test.fr2_pt", ["p"], [[[NAN, 1.0]]]);
    },
];
foreach ($probes as $label => $fn) {
    try { $fn(); echo "$label: NO THROW\n"; }
    catch (ClickHouseException $e) { echo "$label: REJECTED\n"; }
}

// Sanity: well-formed Map / Point / Int128 / UInt128 still land.
$c->insert("test.fr2_map", ["mi","mu","mf"],
    [[["a"=>-1, "b"=>2], ["a"=>0, "b"=>255], ["a"=>1.5, "b"=>2.5]]]);
$c->insert("test.fr2_pt",   ["p"],   [[[1.5, 2.5]]]);
$c->insert("test.fr2_i128", ["v"],   [[42]]);
$c->insert("test.fr2_u128", ["v"],   [[42]]);
echo "ok\n";

foreach (["fr2_map","fr2_pt","fr2_i128","fr2_u128"] as $t) $c->execute("DROP TABLE test.$t");
?>
--EXPECT--
Map Int64 abc: REJECTED
Map UInt8 abc: REJECTED
Map Float64 abc: REJECTED
Map UInt8 narrow overflow: REJECTED
Int128 from array: REJECTED
UInt128 from array: REJECTED
Point abc abc: REJECTED
Point with NaN: REJECTED
ok
