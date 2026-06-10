--TEST--
ClickHouseStatement iteration after toArray() does not touch the shared array's cursor
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());

$stmt = $c->selectStatement("SELECT number AS n FROM system.numbers LIMIT 3");

/* Hand the rows array out to userland, then iterate the statement.
 * The statement's cursor must not move the internal pointer of the
 * array the caller now holds. */
$rows = $stmt->toArray();
$sum = 0;
$cnt = 0;
foreach ($stmt as $r) {
    $sum += $r["n"];
    $cnt++;
}
echo "iterated cnt=", $cnt, " sum=", $sum, "\n";
echo "copy current=", json_encode(current($rows)), "\n";

/* A second pass rewinds cleanly. */
$cnt2 = 0;
foreach ($stmt as $r) {
    $cnt2++;
}
echo "second pass=", $cnt2, "\n";

/* jsonSerialize() shares the same array; same guarantee. */
$json = $stmt->jsonSerialize();
foreach ($stmt as $r) {
}
echo "json current=", json_encode(current($json)), "\n";
?>
--EXPECT--
iterated cnt=3 sum=3
copy current={"n":0}
second pass=3
json current={"n":0}
