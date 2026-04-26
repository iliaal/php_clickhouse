--TEST--
ClickHouse insertAssoc derives column order from first row
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";
$c = new ClickHouse(clickhouse_test_config());

$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.assoc_round_trip");
$c->execute(
    "CREATE TABLE test.assoc_round_trip (id UInt32, name String, score Float32) ".
    "ENGINE = Memory"
);

$rows = [
    ["id" => 1, "name" => "alice", "score" => 1.5],
    ["id" => 2, "name" => "bob",   "score" => 2.5],
    ["id" => 3, "name" => "carol", "score" => 3.5],
];
$c->insertAssoc("test.assoc_round_trip", $rows);

$res = $c->select("SELECT id, name, score FROM test.assoc_round_trip ORDER BY id");
foreach ($res as $row) {
    echo $row["id"], " ", $row["name"], " ", $row["score"], "\n";
}

// Reordered keys on later rows still align via key lookup.
$c->execute("TRUNCATE TABLE test.assoc_round_trip");
$rows = [
    ["id" => 10, "name" => "x", "score" => 0.1],
    ["score" => 0.2, "id" => 20, "name" => "y"],
];
$c->insertAssoc("test.assoc_round_trip", $rows);
$res = $c->select("SELECT id, name FROM test.assoc_round_trip ORDER BY id");
echo $res[0]["id"], " ", $res[0]["name"], "\n";
echo $res[1]["id"], " ", $res[1]["name"], "\n";

// Missing key triggers a clear error.
try {
    $c->insertAssoc("test.assoc_round_trip", [
        ["id" => 99, "name" => "ok", "score" => 0.0],
        ["id" => 100, "name" => "missing"],
    ]);
    echo "missing: no throw\n";
} catch (ClickHouseException $e) {
    echo "missing: throw\n";
}

$c->execute("DROP TABLE test.assoc_round_trip");
?>
--EXPECT--
1 alice 1.5
2 bob 2.5
3 carol 3.5
10 x
20 y
missing: throw
