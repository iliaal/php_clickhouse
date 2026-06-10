--TEST--
insert / insertAssoc / write accept rows left as references by foreach-by-ref
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.byref_rows");
$c->execute(
    "CREATE TABLE test.byref_rows (a UInt32, b String, arr Array(UInt32)) ENGINE=Memory"
);

/* foreach-by-ref leaves every visited element wrapped in IS_REFERENCE,
 * and unset() does not unwrap the buckets. */
$rows = [
    ["a" => 1, "b" => "x", "arr" => [1]],
    ["a" => 2, "b" => "y", "arr" => [2, 3]],
];
foreach ($rows as &$row) {
}
unset($row);
var_dump($c->insertAssoc("test.byref_rows", $rows));

$rows2 = [[3, "z", [4]], [4, "w", []]];
foreach ($rows2 as &$r2) {
}
unset($r2);
var_dump($c->insert("test.byref_rows", ["a", "b", "arr"], $rows2));

/* Nested foreach-by-ref: the row AND every cell become references. */
$rows3 = [[5, "v", [5, 6]]];
foreach ($rows3 as &$r3) {
    foreach ($r3 as &$cell) {
    }
    unset($cell);
}
unset($r3);
var_dump($c->insert("test.byref_rows", ["a", "b", "arr"], $rows3));

/* Streaming write() shares the same row validation. */
$rows4 = [[6, "s", [7]], [7, "t", [8]]];
foreach ($rows4 as &$r4) {
}
unset($r4);
$c->writeStart("test.byref_rows", ["a", "b", "arr"]);
var_dump($c->write($rows4));
var_dump($c->writeEnd());

/* Column-name lists can be reference-wrapped too. */
$cols = ["a", "b", "arr"];
foreach ($cols as &$col) {
}
unset($col);
var_dump($c->insert("test.byref_rows", $cols, [[8, "u", [9]]]));

foreach ($c->select("SELECT a, b, arr FROM test.byref_rows ORDER BY a") as $r) {
    echo $r["a"], " ", $r["b"], " [", implode(",", $r["arr"]), "]\n";
}
$c->execute("DROP TABLE test.byref_rows");
?>
--EXPECT--
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
1 x [1]
2 y [2,3]
3 z [4]
4 w []
5 v [5,6]
6 s [7]
7 t [8]
8 u [9]
