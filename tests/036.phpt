--TEST--
ClickHouse server-side typed parameters {name:Type}
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";
$c = new ClickHouse(clickhouse_test_config());

// Scalar typed params.
$res = $c->select(
    "SELECT {n:UInt32} AS n, {s:String} AS s",
    ["n" => 42, "s" => "hello world"]
);
echo "scalar n: ",  $res[0]["n"], "\n";
echo "scalar s: ",  $res[0]["s"], "\n";

// Array typed param: numeric Array.
$res = $c->select(
    "SELECT arrayJoin({ids:Array(UInt32)}) AS id ORDER BY id",
    ["ids" => [3, 1, 2]]
);
echo "array ids: ",  implode(",", array_column($res, "id")), "\n";

// Array typed param: string Array (server quotes per element).
$res = $c->select(
    "SELECT arrayJoin({xs:Array(String)}) AS s",
    ["xs" => ["a", "b", "c"]]
);
echo "array xs: ", implode(",", array_column($res, "s")), "\n";

// Mix: typed param plus identifier substitution in same SQL.
$res = $c->select(
    "SELECT {n:UInt32} AS {col}",
    ["n" => 7, "col" => "answer"]
);
echo "mix: ",  $res[0]["answer"], "\n";

// String containing single quote: server unescapes.
$res = $c->select("SELECT {s:String} AS s", ["s" => "it's fine"]);
echo "quote: ",  $res[0]["s"], "\n";

// Null routed via SetParam(nullopt).
$res = $c->select("SELECT {x:Nullable(UInt32)} AS x", ["x" => null]);
var_dump($res[0]["x"]);
?>
--EXPECT--
scalar n: 42
scalar s: hello world
array ids: 1,2,3
array xs: a,b,c
mix: 7
quote: it's fine
NULL
