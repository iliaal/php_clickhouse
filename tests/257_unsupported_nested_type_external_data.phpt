--TEST--
ClickHouse rejects external-data column types whose nested type cannot be created; valid nested types still work
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());

foreach ([
    "Array(5)",
    "Array(Null)",
    "Array(DateTime64)",
    "Nullable(5)",
    "Nullable(DateTime64)",
    "LowCardinality(Nullable(5))",
] as $type) {
    try {
        $c->selectWithExternalData("SELECT count() FROM ext_bad", [[
            "name" => "ext_bad",
            "columns" => ["v" => $type],
            "rows" => [[null]],
        ]]);
        echo "$type: accepted\n";
    } catch (ClickHouseException $e) {
        echo "$type: ", get_class($e), ": ", $e->getMessage(), "\n";
    }
}

$rows = $c->selectWithExternalData(
    "SELECT a, n FROM ext_ok ORDER BY length(a)",
    [[
        "name" => "ext_ok",
        "columns" => ["a" => "Array(String)", "n" => "Nullable(UInt8)"],
        "rows" => [[["x", "y"], null], [["z"], 7]],
    ]]
);
echo json_encode($rows), "\n";

$rows = $c->select(
    "SELECT toNullable(toUInt8(42)) AS n, ['x', 'y'] AS a,
            toLowCardinality(toNullable('lc')) AS lc,
            CAST([toDateTime64('2026-01-02 03:04:05.678', 3, 'UTC'), NULL] AS Array(Nullable(DateTime64(3, 'UTC')))) AS d",
    [], ClickHouse::DATE_AS_STRINGS
);
echo json_encode($rows), "\n";

$rows = $c->select("SELECT CAST([NULL] AS Array(Nullable(DateTime64(3)))) AS d");
echo json_encode($rows), "\n";
?>
--EXPECT--
Array(5): ClickHouseException: external table 'ext_bad' column 'v' has unsupported type 'Array(5)'
Array(Null): ClickHouseException: external table 'ext_bad' column 'v' has unsupported type 'Array(Null)'
Array(DateTime64): ClickHouseException: external table 'ext_bad' column 'v' has unsupported type 'Array(DateTime64)'
Nullable(5): ClickHouseException: external table 'ext_bad' column 'v' has unsupported type 'Nullable(5)'
Nullable(DateTime64): ClickHouseException: external table 'ext_bad' column 'v' has unsupported type 'Nullable(DateTime64)'
LowCardinality(Nullable(5)): ClickHouseException: LowCardinality(Nullable()) is not supported
[{"a":["z"],"n":7},{"a":["x","y"],"n":null}]
[{"n":42,"a":["x","y"],"lc":"lc","d":["2026-01-02 03:04:05.678",null]}]
[{"d":[null]}]
