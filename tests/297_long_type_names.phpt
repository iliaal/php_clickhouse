--TEST--
Type names longer than 255 bytes round-trip through external data, insert, and incremental writes
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$entries = [];
for ($i = 1; $i <= 64; $i++) {
    $entries[] = "'" . sprintf("label_%02d", $i) . "'=" . $i;
}
$type = "Enum16(" . implode(",", $entries) . ")";
echo "input type longer than 255: ", strlen($type) > 255 ? "yes" : "no", "\n";

function showLongTypeRows($label, array $rows) {
    $longTypes = count($rows) === 3;
    foreach ($rows as $row) {
        $longTypes = $longTypes && strlen($row["type"]) > 255;
    }
    echo $label, " labels: ", implode(",", array_column($rows, "value")), "\n";
    echo $label, " returned types longer than 255: ", $longTypes ? "yes" : "no", "\n";
}

$c = new ClickHouse(clickhouse_test_config());
$rows = [["label_64"], ["label_01"], ["label_32"]];
showLongTypeRows("external", $c->selectWithExternalData(
    "SELECT value, toTypeName(value) AS type FROM long_enum_external ORDER BY toInt16(value)",
    [[
        "name" => "long_enum_external",
        "columns" => ["value" => $type],
        "rows" => $rows,
    ]]
));

$c->execute("CREATE TEMPORARY TABLE long_enum_roundtrip (value " . $type . ")");
$c->insert("long_enum_roundtrip", ["value"], [["label_64"], ["label_01"]]);
$c->writeStart("long_enum_roundtrip", ["value"]);
$c->write([["label_32"]]);
$c->writeEnd();
showLongTypeRows("insert/write", $c->select(
    "SELECT value, toTypeName(value) AS type FROM long_enum_roundtrip ORDER BY toInt16(value)"
));
?>
--EXPECT--
input type longer than 255: yes
external labels: label_01,label_32,label_64
external returned types longer than 255: yes
insert/write labels: label_01,label_32,label_64
insert/write returned types longer than 255: yes
