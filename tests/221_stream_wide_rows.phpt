--TEST--
ClickHouse streaming preserves wide rows, duplicate names, value flags, and cancellation
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";
$ch = new ClickHouse(clickhouse_test_config());

$columns = ["number", "[number, number + 1] AS nested",
            "toDate('2026-01-02') AS day",
            "10 AS `01`", "11 AS `1`", "12 AS `Name`", "13 AS `name`"];
for ($i = 0; $i < 250; $i++) {
    $columns[] = "$i AS column_$i";
}
$sql = "SELECT " . implode(", ", $columns) . " FROM numbers(3) ORDER BY number";
$flags = ClickHouse::DATE_AS_STRINGS;
$expected = $ch->select($sql, [], $flags);
var_dump(count($expected[0]) === 257);
var_dump($expected[2]['nested'] === [2, 3], $expected[0]['day'] === '2026-01-02');

$iter = $ch->selectStream($sql, [], "", [], $flags);
var_dump(iterator_to_array($iter) === $expected);
$iter->rewind();
$row = $iter->current();
$row['nested'][0] = 999;
var_dump($iter->current() === $expected[0]);
var_dump(iterator_to_array($iter) === $expected);

$rows = [];
$ch->selectStreamCallback($sql, function (array $row) use (&$rows) {
    $rows[] = $row;
}, [], "", [], $flags);
var_dump($rows === $expected);

// Duplicate columns still overwrite in place, preserving associative key order.
$columns[] = 'number';
$duplicateSql = "SELECT " . implode(", ", $columns) . " FROM numbers(3) ORDER BY number";
var_dump(iterator_to_array($ch->selectStream($duplicateSql, [], "", [], $flags)) === $expected);
$rows = [];
$ch->selectStreamCallback($duplicateSql, function (array $row) use (&$rows) {
    $rows[] = $row;
}, [], "", [], $flags);
var_dump($rows === $expected);

$seen = 0;
$ch->selectStreamCallback($sql, function (array $row) use (&$seen) {
    ++$seen;
    return false;
});
var_dump($seen === 1);
var_dump($ch->select('SELECT 1', [], ClickHouse::FETCH_ONE) === 1);

$emptySql = "SELECT " . implode(", ", $columns) . " FROM numbers(0)";
var_dump(iterator_to_array($ch->selectStream($emptySql)) === []);
$seen = 0;
$ch->selectStreamCallback($emptySql, function (array $row) use (&$seen) { ++$seen; });
var_dump($seen === 0);
?>
--EXPECT--
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
