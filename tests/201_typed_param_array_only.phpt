--TEST--
Typed parameters: PHP arrays only for Array(T), not Map
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";
$c = new ClickHouse(clickhouse_test_config());

try {
    $c->select("SELECT {m:Map(String, UInt8)} AS x", ["m" => ["a" => 1, "b" => 2]]);
    echo "map array: no throw\n";
} catch (ClickHouseException $e) {
    echo (strpos($e->getMessage(), "Array(") !== false || strpos($e->getMessage(), "Map") !== false)
        ? "map array: rejected\n" : ("map array: other: " . $e->getMessage() . "\n");
}

$r = $c->select("SELECT {a:Array(UInt8)} AS x", ["a" => [1, 2, 3]]);
echo "array ok: ", json_encode($r[0]["x"]), "\n";
?>
--EXPECT--
map array: rejected
array ok: [1,2,3]
