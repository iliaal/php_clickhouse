--TEST--
ClickHouse Array of complex inner types: Array(Tuple), Array(Nullable), Array(Array)
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");

// Array(Nullable(String)) read path -- the inner null mask must round-trip.
$rows = $c->select("SELECT [1, NULL, 3, NULL] AS arr_n");
echo "arr_n=", json_encode($rows[0]["arr_n"]), "\n";

// Array(Tuple(...)) read path.
$rows = $c->select("SELECT [(1, 'a'), (2, 'b')] AS arr_t");
echo "arr_t=", json_encode($rows[0]["arr_t"]), "\n";

// Array(Array(...)) -- two-dim, server-built.
$rows = $c->select("SELECT [[1, 2], [3], []] AS arr_a");
echo "arr_a=", json_encode($rows[0]["arr_a"]), "\n";

?>
--EXPECT--
arr_n=[1,null,3,null]
arr_t=[[1,"a"],[2,"b"]]
arr_a=[[1,2],[3],[]]
