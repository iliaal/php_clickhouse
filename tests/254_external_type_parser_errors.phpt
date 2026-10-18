--TEST--
selectWithExternalData rejects unbalanced column types and keeps balanced nested types
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());

foreach (["))", "UInt8))", "),", ")))", "UInt8)(", "UInt8, String", "Array(UInt8)),"] as $type) {
    try {
        $c->selectWithExternalData("SELECT x FROM ext_eyn5", [[
            "name" => "ext_eyn5",
            "columns" => ["x" => $type],
            "rows" => [[1]],
        ]]);
        echo "$type: accepted\n";
    } catch (ClickHouseException $e) {
        echo "$type: ", $e->getMessage(), "\n";
    }
}

$rows = $c->selectWithExternalData(
    "SELECT a, t, m FROM ext_eyn5 ORDER BY t.1",
    [[
        "name" => "ext_eyn5",
        "columns" => [
            "a" => "Array(Nullable(UInt8))",
            "t" => "Tuple(UInt8, String)",
            "m" => "Map(String, UInt8)",
        ],
        "rows" => [
            [[1, null], [5, "hi"], ["k" => 9]],
            [[], [6, ""], []],
        ],
    ]]
);
echo json_encode($rows), "\n";
echo json_encode($c->select("SELECT 1 AS ok")), "\n";
?>
--EXPECT--
)): external table 'ext_eyn5' column 'x' has unsupported type '))'
UInt8)): external table 'ext_eyn5' column 'x' has unsupported type 'UInt8))'
),: external table 'ext_eyn5' column 'x' has unsupported type '),'
))): external table 'ext_eyn5' column 'x' has unsupported type ')))'
UInt8)(: external table 'ext_eyn5' column 'x' has unsupported type 'UInt8)('
UInt8, String: external table 'ext_eyn5' column 'x' has unsupported type 'UInt8, String'
Array(UInt8)),: external table 'ext_eyn5' column 'x' has unsupported type 'Array(UInt8)),'
[{"a":[1,null],"t":[5,"hi"],"m":{"k":9}},{"a":[],"t":[6,""],"m":[]}]
[{"ok":1}]
