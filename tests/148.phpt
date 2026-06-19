--TEST--
ClickHouse Array(Tuple) write and read round-trip
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.atup_t");
$c->execute("CREATE TABLE test.atup_t (id UInt32, a Array(Tuple(String, Int32))) ENGINE = Memory");

$c->insert("test.atup_t", ["id", "a"], [
    [1, [["x", 10], ["y", 20]]],
    [2, []],
    [3, [["z", 99]]],
]);

foreach ($c->select("SELECT id, a FROM test.atup_t ORDER BY id") as $r) {
    $parts = array_map(fn($t) => $t[0] . ":" . $t[1], $r["a"]);
    echo $r["id"], " [", implode(",", $parts), "]\n";
}

// A plain Array(Int32) still round-trips after the shared Array write path changed.
$c->execute("DROP TABLE IF EXISTS test.aint_t");
$c->execute("CREATE TABLE test.aint_t (id UInt32, a Array(Int32)) ENGINE = Memory");
$c->insert("test.aint_t", ["id", "a"], [[1, [1, 2, 3]], [2, []]]);
foreach ($c->select("SELECT id, a FROM test.aint_t ORDER BY id") as $r) {
    echo $r["id"], " [", implode(",", $r["a"]), "]\n";
}

$c->execute("DROP TABLE test.atup_t");
$c->execute("DROP TABLE test.aint_t");
?>
--EXPECT--
1 [x:10,y:20]
2 []
3 [z:99]
1 [1,2,3]
2 []
