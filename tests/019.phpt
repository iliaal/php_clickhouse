--TEST--
ClickHouse Map row I/O across supported K/V combinations
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.maps_t");
$c->execute("CREATE TABLE test.maps_t (
    id UInt32,
    str_str Map(String, String),
    str_int Map(String, Int64),
    str_uint Map(String, UInt64),
    str_flt Map(String, Float64),
    int_str Map(Int64, String)
) ENGINE = Memory");

$c->insert("test.maps_t", ["id", "str_str", "str_int", "str_uint", "str_flt", "int_str"], [
    [1,
     ["env" => "prod", "tier" => "blue"],
     ["count" => 42, "size" => -7],
     ["bytes" => 1024],
     ["pi" => 3.14],
     [10 => "ten", 20 => "twenty"]],
    [2, [], ["x" => 0], [], ["zero" => 0.0], [-1 => "neg"]],
]);

foreach ($c->select("SELECT id, str_str, str_int, str_uint, str_flt, int_str FROM test.maps_t ORDER BY id") as $r) {
    ksort($r["str_str"]);
    ksort($r["str_int"]);
    ksort($r["str_uint"]);
    ksort($r["str_flt"]);
    ksort($r["int_str"]);
    echo "id=", $r["id"], "\n";
    echo "  str_str=",  json_encode($r["str_str"]),  "\n";
    echo "  str_int=",  json_encode($r["str_int"]),  "\n";
    echo "  str_uint=", json_encode($r["str_uint"]), "\n";
    echo "  str_flt=",  json_encode($r["str_flt"]),  "\n";
    echo "  int_str=",  json_encode($r["int_str"]),  "\n";
}

$c->execute("DROP TABLE test.maps_t");
?>
--EXPECT--
id=1
  str_str={"env":"prod","tier":"blue"}
  str_int={"count":42,"size":-7}
  str_uint={"bytes":1024}
  str_flt={"pi":3.14}
  int_str={"10":"ten","20":"twenty"}
id=2
  str_str=[]
  str_int={"x":0}
  str_uint=[]
  str_flt={"zero":0}
  int_str={"-1":"neg"}
