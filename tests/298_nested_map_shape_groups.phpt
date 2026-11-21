--TEST--
Nested Map shape validation follows Array cells and ordered Map rows
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$assoc = ["a" => "one"];
$pairs = [["b", "two"]];

function groupedMaps($c, $type, $rows) {
    return $c->selectWithExternalData("SELECT id, m FROM grouped_maps ORDER BY id", [[
        "name" => "grouped_maps",
        "columns" => ["id" => "UInt8", "m" => $type],
        "rows" => $rows,
    ]], [], ClickHouse::MAP_AS_PAIRS);
}

$valid = [
    "Array(Map)" => ["Array(Map(String, String))", [[1, [$assoc]], [2, [$pairs]]]],
    "Array(Tuple(Map))" => ["Array(Tuple(Map(String, String)))", [[1, [[$assoc]]], [2, [[$pairs]]]]],
    "Map(Map)" => ["Map(String, Map(String, String))", [[1, [["outer", $assoc]]], [2, [["outer", $pairs]]]]],
    "Tuple(Array(Map))" => ["Tuple(Array(Map(String, String)))", [[1, [[$assoc]]], [2, [[$pairs]]]]],
    "Map(Tuple(Map))" => ["Map(String, Tuple(Map(String, String)))", [[1, [["outer", [$assoc]]]], [2, [["outer", [$pairs]]]]]],
    "Array(Tuple(Array(Map)))" => ["Array(Tuple(Array(Map(String, String))))", [[1, [[[$assoc]], [[$pairs]]]]]],
    "empty groups" => ["Array(Map(String, String))", [[1, []], [2, [$assoc, []]], [3, [$pairs, []]], [4, []]]],
    "UInt8 shape conversions" => ["Array(Map(UInt8, UInt8))", [[1, [[1 => "0xff"]]], [2, [[["+2", "+3"]]]]]],
];
foreach ($valid as $label => $case) {
    echo $label, ": ", json_encode(groupedMaps($c, $case[0], $case[1])), "\n";
}

$invalid = [
    "top Map" => ["Map(String, String)", [[1, $assoc], [2, $pairs]]],
    "Tuple(Map)" => ["Tuple(Map(String, String))", [[1, [$assoc]], [2, [$pairs]]]],
    "one Array cell" => ["Array(Map(String, String))", [[1, [$assoc, $pairs]]]],
    "one Array Tuple group" => ["Array(Tuple(Map(String, String)))", [[1, [[$assoc], [$pairs]]]]],
    "one Map row" => ["Map(String, Map(String, String))", [[1, [["first", $assoc], ["second", $pairs]]]]],
    "one Map Tuple group" => ["Map(String, Tuple(Map(String, String)))", [[1, [["first", [$assoc]], ["second", [$pairs]]]]]],
    "Nullable preserves group" => ["Array(Nullable(Map(String, String)))", [[1, [$assoc, $pairs]]]],
    "pair UInt8 stays strict" => ["Array(Map(UInt8, UInt8))", [[1, [[1 => "0xff"]]], [2, [[[2, "0xff"]]]]]],
    "empty Map group restrictions" => ["Array(Map(String, Tuple(String)))", [[1, []]]],
];
foreach ($invalid as $label => $case) {
    try {
        groupedMaps($c, $case[0], $case[1]);
        echo $label, ": accepted\n";
    } catch (ClickHouseException $e) {
        echo $label, ": ", $e->getMessage(), "\n";
    }
    echo "recovered: ", $c->ping() ? "yes" : "no", "\n";
}

$c->execute("CREATE TEMPORARY TABLE grouped_map_insert (id UInt8, m Array(Map(String, String)))");
$c->insert("grouped_map_insert", ["id", "m"], [[1, [$assoc]], [2, [$pairs]]]);
$c->writeStart("grouped_map_insert", ["id", "m"]);
$c->write([[3, [$pairs]], [4, [$assoc]]]);
$c->writeEnd();
echo "insert/write: ", json_encode($c->select("SELECT id, m FROM grouped_map_insert ORDER BY id", [], ClickHouse::MAP_AS_PAIRS)), "\n";
?>
--EXPECT--
Array(Map): [{"id":1,"m":[[["a","one"]]]},{"id":2,"m":[[["b","two"]]]}]
Array(Tuple(Map)): [{"id":1,"m":[[[["a","one"]]]]},{"id":2,"m":[[[["b","two"]]]]}]
Map(Map): [{"id":1,"m":[["outer",[["a","one"]]]]},{"id":2,"m":[["outer",[["b","two"]]]]}]
Tuple(Array(Map)): [{"id":1,"m":[[[["a","one"]]]]},{"id":2,"m":[[[["b","two"]]]]}]
Map(Tuple(Map)): [{"id":1,"m":[["outer",[[["a","one"]]]]]},{"id":2,"m":[["outer",[[["b","two"]]]]]}]
Array(Tuple(Array(Map))): [{"id":1,"m":[[[[["a","one"]]]],[[[["b","two"]]]]]}]
empty groups: [{"id":1,"m":[]},{"id":2,"m":[[["a","one"]],[]]},{"id":3,"m":[[["b","two"]],[]]},{"id":4,"m":[]}]
UInt8 shape conversions: [{"id":1,"m":[[[1,255]]]},{"id":2,"m":[[[2,3]]]}]
top Map: Map rows must consistently use associative arrays or ordered key/value pairs
recovered: yes
Tuple(Map): Map rows must consistently use associative arrays or ordered key/value pairs
recovered: yes
one Array cell: Map rows must consistently use associative arrays or ordered key/value pairs
recovered: yes
one Array Tuple group: Map rows must consistently use associative arrays or ordered key/value pairs
recovered: yes
one Map row: Map rows must consistently use associative arrays or ordered key/value pairs
recovered: yes
one Map Tuple group: Map rows must consistently use associative arrays or ordered key/value pairs
recovered: yes
Nullable preserves group: Map rows must consistently use associative arrays or ordered key/value pairs
recovered: yes
pair UInt8 stays strict: invalid integer string for UInt8
recovered: yes
empty Map group restrictions: Unsupported Map value type: Tuple(String)
recovered: yes
insert/write: [{"id":1,"m":[[["a","one"]]]},{"id":2,"m":[[["b","two"]]]},{"id":3,"m":[[["b","two"]]]},{"id":4,"m":[[["a","one"]]]}]
