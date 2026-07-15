--TEST--
ClickHouse Array(Map) writes preserve map shape across multiple rows
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.array_map_rows");
$c->execute("CREATE TABLE test.array_map_rows (
    id UInt32,
    v Array(Map(String, Int64))
) ENGINE=Memory");
$c->insert("test.array_map_rows", ["id", "v"], [
    [1, [["a" => 1], ["b" => 2]]],
    [2, [["c" => 3]]],
]);

foreach ($c->select("SELECT id, v FROM test.array_map_rows ORDER BY id") as $row) {
    echo $row["id"], ":", json_encode($row["v"]), "\n";
}
$c->execute("DROP TABLE test.array_map_rows");
?>
--EXPECT--
1:[{"a":1},{"b":2}]
2:[{"c":3}]
