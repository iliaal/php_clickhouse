--TEST--
LowCardinality nullable strings preserve top-level and sliced container representations
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";
$c = new ClickHouse(clickhouse_test_config());
$queries = [
    "top-string" => "SELECT toLowCardinality(CAST('' AS Nullable(String))) AS x",
    "top-tuple-string" => "SELECT tuple(toLowCardinality(CAST('' AS Nullable(String)))) AS x",
    "array-string" => "SELECT [toLowCardinality(CAST('a' AS Nullable(String))), toLowCardinality(CAST(NULL AS Nullable(String))), toLowCardinality(CAST('' AS Nullable(String)))] AS x",
    "array-tuple-string" => "SELECT [tuple(toLowCardinality(CAST('' AS Nullable(String))))] AS x",
    "array-array-string" => "SELECT [[toLowCardinality(CAST('' AS Nullable(String)))]] AS x",
    "array-plain-string" => "SELECT [toLowCardinality('')] AS x",
    "top-fixed" => "SELECT toLowCardinality(CAST('' AS Nullable(FixedString(4)))) AS x",
    "array-fixed" => "SELECT [toLowCardinality(CAST('' AS Nullable(FixedString(4))))] AS x",
    "array-fixed-nul" => "SELECT [toLowCardinality(CAST(unhex('00000000') AS Nullable(FixedString(4))))] AS x",
    "array-fixed-a" => "SELECT [toLowCardinality(CAST('a' AS Nullable(FixedString(4))))] AS x",
];
foreach ($queries as $name => $sql) {
    foreach ([0, ClickHouse::FIXEDSTRING_BINARY] as $flags) {
        echo $name, "/", $flags, ":", json_encode($c->select($sql, [], $flags)), "\n";
    }
}
$map = "SELECT map('a', toLowCardinality(CAST('' AS Nullable(String)))) AS x";
try {
    $c->select($map);
    echo "map default: accepted\n";
} catch (ClickHouseException $error) {
    echo "map default: ", $error->getMessage(), "\n";
}
echo "map pairs:", json_encode($c->select($map, [], ClickHouse::MAP_AS_PAIRS)), "\n";
echo "recovery=", $c->ping() ? "ok" : "failed", "\n";
?>
--EXPECT--
top-string/0:[{"x":""}]
top-string/128:[{"x":""}]
top-tuple-string/0:[{"x":[""]}]
top-tuple-string/128:[{"x":[""]}]
array-string/0:[{"x":["a",null,null]}]
array-string/128:[{"x":["a",null,null]}]
array-tuple-string/0:[{"x":[[null]]}]
array-tuple-string/128:[{"x":[[null]]}]
array-array-string/0:[{"x":[[null]]}]
array-array-string/128:[{"x":[[null]]}]
array-plain-string/0:[{"x":[""]}]
array-plain-string/128:[{"x":[""]}]
top-fixed/0:[{"x":""}]
top-fixed/128:[{"x":"\u0000\u0000\u0000\u0000"}]
array-fixed/0:[{"x":[""]}]
array-fixed/128:[{"x":["\u0000\u0000\u0000\u0000"]}]
array-fixed-nul/0:[{"x":[""]}]
array-fixed-nul/128:[{"x":["\u0000\u0000\u0000\u0000"]}]
array-fixed-a/0:[{"x":["a"]}]
array-fixed-a/128:[{"x":["a\u0000\u0000\u0000"]}]
map default: Map read: unsupported value type LowCardinality(Nullable(String))
map pairs:[{"x":[["a",null]]}]
recovery=ok
