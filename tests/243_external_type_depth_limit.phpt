--TEST--
selectWithExternalData bounds caller type-name nesting and length before parsing
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());

function nest($wrapper, $depth, $leaf) {
    return str_repeat("$wrapper(", $depth) . $leaf . str_repeat(")", $depth);
}
function nestValue($depth, $leaf) {
    for ($i = 0; $i < $depth; $i++) $leaf = [$leaf];
    return $leaf;
}
function external($c, $label, $type, $value, $sql = "SELECT count() AS v FROM e") {
    try {
        $r = $c->selectWithExternalData($sql, [
            ["name" => "e", "columns" => ["v" => $type], "rows" => [[$value]]],
        ]);
        echo "$label: ok ", substr(json_encode($r), 0, 120), "\n";
    } catch (ClickHouseException $e) {
        $m = $e->getMessage();
        if (strpos($m, "type name nesting exceeds 64 levels") !== false) $m = "nesting limit";
        elseif (strpos($m, "type name exceeds 1048576 bytes") !== false) $m = "length limit";
        elseif (strpos($m, "nested-type depth exceeds limit") !== false) $m = "conversion depth limit";
        echo "$label: rejected ($m)\n";
    }
}

external($c, "array-5000", nest("Array", 5000, "UInt8"), []);
external($c, "array-100000", nest("Array", 100000, "UInt8"), []);
external($c, "unbalanced-100000", str_repeat("Array(", 100000) . "UInt8", []);
external($c, "tuple-64", nest("Tuple", 64, "UInt8"), nestValue(64, 1));
external($c, "tuple-65", nest("Tuple", 65, "UInt8"), nestValue(65, 1));
external($c, "tuple-31", nest("Tuple", 31, "UInt8"), nestValue(31, 7));
external($c, "tuple-31-lc", nest("Tuple", 31, "LowCardinality(Nullable(FixedString(2)))"), nestValue(31, "ab"));

/* Parentheses inside an enum label are not nesting. */
$label = str_repeat("(", 100);
$show = "SELECT toString(v) AS v FROM e";
external($c, "enum-label", "Enum8('$label' = 1, 'b' = 2)", 1, $show);
external($c, "enum-label-close", "Enum8('" . str_repeat(")", 100) . "' = 1)", 1, $show);

external($c, "length-cap", str_repeat(" ", 1048576) . "UInt8", 1);
external($c, "length-under-cap", str_repeat(" ", 1000000) . "UInt8", 1, $show);
?>
--EXPECT--
array-5000: rejected (nesting limit)
array-100000: rejected (nesting limit)
unbalanced-100000: rejected (nesting limit)
tuple-64: rejected (conversion depth limit)
tuple-65: rejected (nesting limit)
tuple-31: ok [{"v":1}]
tuple-31-lc: ok [{"v":1}]
enum-label: ok [{"v":"(((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((("}]
enum-label-close: ok [{"v":"))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))"}]
length-cap: rejected (length limit)
length-under-cap: ok [{"v":"1"}]
