--TEST--
External data rejects empty Tuple types without rejecting named tuples or matching Enum labels
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";
$c = new ClickHouse(clickhouse_test_config());
foreach ([
    "Tuple()",
    "Tuple( )",
    "Array(Tuple())",
    "Tuple(UInt8, Tuple())",
    "Nullable(Tuple())",
    "Map(String, Tuple())",
    "LowCardinality(Nullable(Tuple()))",
] as $type) {
    try {
        $c->selectWithExternalData("SELECT count() FROM empty_tuple_input", [[
            "name" => "empty_tuple_input",
            "columns" => ["x" => $type],
            "rows" => [],
        ]]);
        echo $type, ": accepted\n";
    } catch (ClickHouseException $error) {
        echo $type, ":", $error->getServerCode(), ":", $error->getMessage(), "\n";
    }
}
echo "recovery=", $c->ping() ? "ping" : "failed", ":",
    $c->select("SELECT 9 AS n", [], ClickHouse::FETCH_ONE), "\n";

echo "named tuple:", json_encode($c->selectWithExternalData(
    "SELECT tupleElement(x, 1) AS label, tupleElement(x, 2) AS n FROM named_tuple_input",
    [[
        "name" => "named_tuple_input",
        "columns" => ["x" => "Tuple(label String, value UInt8)"],
        "rows" => [[["ok", 7]]],
    ]]
)), "\n";
echo "enum label:", json_encode($c->selectWithExternalData(
    "SELECT x FROM enum_tuple_input",
    [[
        "name" => "enum_tuple_input",
        "columns" => ["x" => "Enum8('Tuple()'=1)"],
        "rows" => [["Tuple()"]],
    ]]
)), "\n";
?>
--EXPECT--
Tuple():0:AST child element index out of bounds: 0
Tuple( ):0:AST child element index out of bounds: 0
Array(Tuple()):0:AST child element index out of bounds: 0
Tuple(UInt8, Tuple()):0:AST child element index out of bounds: 0
Nullable(Tuple()):0:AST child element index out of bounds: 0
Map(String, Tuple()):0:AST child element index out of bounds: 0
LowCardinality(Nullable(Tuple())):0:AST child element index out of bounds: 0
recovery=ping:9
named tuple:[{"label":"ok","n":7}]
enum label:[{"x":"Tuple()"}]
