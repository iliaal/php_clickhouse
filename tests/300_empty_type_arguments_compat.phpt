--TEST--
Empty type arguments retain native precision defaults and required-argument errors
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
require __DIR__ . "/_clickhouse.inc";
clickhouse_skip_if_no_server();
$c = new ClickHouse(clickhouse_test_config());
try {
    $c->select("SELECT CAST(0 AS Time64(0)) AS x");
} catch (ClickHouseException $e) {
    print "skip server has no Time64 support";
}
?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";
$c = new ClickHouse(clickhouse_test_config());
function empty_arguments_query($c, $type, $value) {
    return $c->selectWithExternalData(
        "SELECT toTypeName(x) AS t, toString(x) AS s FROM empty_arguments_input",
        [["name" => "empty_arguments_input", "columns" => ["x" => $type], "rows" => [[$value]]]]
    );
}
foreach ([
    ["DateTime()", 1],
    ["DateTime64()", 1.25],
    ["DateTime64( )", 1.25],
    ["Time64()", 1.25],
    ["Time64( )", 1.25],
    ["Decimal32()", 1],
    ["Decimal64()", 1],
    ["Decimal128()", 1],
    ["Array(DateTime64())", [1.25]],
    ["Array(Time64())", [1.25]],
    ["Array(Decimal32())", [1]],
    ["Nullable(Decimal64())", 1],
    ["Tuple(n Decimal128())", [1]],
    ["DateTime64(3)", 1.25],
    ["Time64(3)", 1.25],
    ["Decimal32(2)", "1.25"],
] as $case) {
    list($type, $value) = $case;
    echo $type, ":", json_encode(empty_arguments_query($c, $type, $value)), "\n";
}
foreach ([
    ["DateTime64", 1],
    ["Time64", 1],
    ["Decimal32", 1],
    ["Decimal64", 1],
    ["Decimal128", 1],
    ["Array(Decimal32)", [1]],
    ["Decimal()", 1],
    ["FixedString()", "x"],
    ["Enum8()", 1],
    ["Enum16()", 1],
    ["Array()", []],
    ["Nullable()", null],
    ["Map()", []],
    ["SimpleAggregateFunction()", 1],
    ["LowCardinality(Array())", []],
    ["SimpleAggregateFunction(sum, Array())", []],
    ["Map(String, Decimal32())", ["one" => 1]],
    ["Decimal32()", 1.25],
    ["Decimal64()", 1.25],
    ["Decimal128()", 1.25],
] as $case) {
    list($type, $value) = $case;
    try {
        empty_arguments_query($c, $type, $value);
        echo $type, ":accepted\n";
    } catch (ClickHouseException $error) {
        echo $type, ":", $error->getServerCode(), ":", $error->getMessage(), "\n";
    }
}
echo "recovery=", $c->ping() ? "ping" : "failed", ":",
    $c->select("SELECT 9 AS n", [], ClickHouse::FETCH_ONE), "\n";
?>
--EXPECT--
DateTime():[{"t":"DateTime","s":"1970-01-01 00:00:01"}]
DateTime64():[{"t":"DateTime64(0)","s":"1970-01-01 00:00:01"}]
DateTime64( ):[{"t":"DateTime64(0)","s":"1970-01-01 00:00:01"}]
Time64():[{"t":"Time64(0)","s":"00:00:01"}]
Time64( ):[{"t":"Time64(0)","s":"00:00:01"}]
Decimal32():[{"t":"Decimal(9, 0)","s":"1"}]
Decimal64():[{"t":"Decimal(18, 0)","s":"1"}]
Decimal128():[{"t":"Decimal(38, 0)","s":"1"}]
Array(DateTime64()):[{"t":"Array(DateTime64(0))","s":"['1970-01-01 00:00:01']"}]
Array(Time64()):[{"t":"Array(Time64(0))","s":"['00:00:01']"}]
Array(Decimal32()):[{"t":"Array(Decimal(9, 0))","s":"[1]"}]
Nullable(Decimal64()):[{"t":"Nullable(Decimal(18, 0))","s":"1"}]
Tuple(n Decimal128()):[{"t":"Tuple(Decimal(38, 0))","s":"(1)"}]
DateTime64(3):[{"t":"DateTime64(3)","s":"1970-01-01 00:00:01.250"}]
Time64(3):[{"t":"Time64(3)","s":"00:00:01.250"}]
Decimal32(2):[{"t":"Decimal(9, 2)","s":"1.25"}]
DateTime64:0:external table 'empty_arguments_input' column 'x' has unsupported type 'DateTime64'
Time64:0:external table 'empty_arguments_input' column 'x' has unsupported type 'Time64'
Decimal32:0:AST child element index out of bounds: 0
Decimal64:0:AST child element index out of bounds: 0
Decimal128:0:AST child element index out of bounds: 0
Array(Decimal32):0:AST child element index out of bounds: 0
Decimal():0:AST child element index out of bounds: -1
FixedString():0:FixedString size must be between 1 and 16777215, got 0
Enum8():0:Enum8 content is not correct
Enum16():0:Enum16 content is not correct
Array():0:AST child element index out of bounds: 0
Nullable():0:AST child element index out of bounds: 0
Map():0:Map content is not correct
SimpleAggregateFunction():0:AST child element index out of bounds: -1
LowCardinality(Array()):0:LowCardinality(Array) is not supported
SimpleAggregateFunction(sum, Array()):0:external table 'empty_arguments_input' column 'x' has unsupported type 'SimpleAggregateFunction(sum, Array())'
Map(String, Decimal32()):0:Unsupported Map value type: Decimal(9,0)
Decimal32():0:Decimal value has more fractional digits than the column scale allows
Decimal64():0:Decimal value has more fractional digits than the column scale allows
Decimal128():0:Decimal value has more fractional digits than the column scale allows
recovery=ping:9
