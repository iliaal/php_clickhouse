--TEST--
ClickHouseStatement preserves wide unique and duplicate column results
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";
$ch = new ClickHouse(clickhouse_test_config());

$columns = ["number", "number + 10 AS value"];
for ($i = 0; $i < 1024; $i++) {
    $columns[] = "$i AS column_$i";
}
$sql = "SELECT " . implode(", ", $columns) . " FROM numbers(3) ORDER BY number";
$stmt = $ch->selectStatement($sql);
$rows = $stmt->toArray();
var_dump(count($rows), count($rows[0]));
var_dump($rows[2]['column_1023']);
var_dump($stmt->fetchColumn() === [0, 1, 2]);
var_dump($stmt->fetchKeyPair() === [10, 11, 12]);
var_dump($stmt->fetchOne() === $rows[0]);

// A duplicate at the end must still enable the positional representation.
$columns[] = "number";
$dup = $ch->selectStatement("SELECT " . implode(", ", $columns) .
    " FROM numbers(3) ORDER BY number");
var_dump($dup->toArray() === $rows);
var_dump($dup->fetchColumn() === [0, 1, 2]);
var_dump($dup->fetchKeyPair() === [10, 11, 12]);
var_dump($dup->fetchOne() === $rows[0]);

// String identity remains case-sensitive, including numeric-looking names.
$names = $ch->selectStatement('SELECT 1 AS `01`, 2 AS `1`, 3 AS `Name`, 4 AS `name`');
var_dump(count($names->fetchOne()));
var_dump($names->fetchColumn() === [1]);
var_dump($names->fetchKeyPair() === [1 => 2]);
?>
--EXPECT--
int(3)
int(1026)
int(1023)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
int(4)
bool(true)
bool(true)
