--TEST--
DR-015: selectStatement() accepts a fetch_mode and honors value-shaping flags
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// selectStatement() used to hardcode fetch_mode = 0, so a Statement user
// (smi2-style) could not request DATE_AS_STRINGS / UUID_WITH_DASHES etc.
// It now accepts an optional trailing $fetch_mode; value flags apply while
// row-shape flags (FETCH_ONE/KEY_PAIR/COLUMN) are ignored (a Statement
// always materializes full rows for array/iterator access).

$c = new ClickHouse(clickhouse_test_config());

echo "-- default (raw) --\n";
$st = $c->selectStatement("SELECT toDate('2024-01-15') AS d");
echo json_encode($st->toArray()), "\n";

echo "-- DATE_AS_STRINGS --\n";
$st = $c->selectStatement("SELECT toDate('2024-01-15') AS d", [], "", [], ClickHouse::DATE_AS_STRINGS);
echo json_encode($st->toArray()), "\n";

echo "-- UUID_WITH_DASHES --\n";
$st = $c->selectStatement(
    "SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') AS u",
    [], "", [], ClickHouse::UUID_WITH_DASHES);
echo json_encode($st->toArray()), "\n";

echo "-- shape flag FETCH_ONE is ignored (still full rows) --\n";
$st = $c->selectStatement("SELECT 1 AS a, 2 AS b", [], "", [], ClickHouse::FETCH_ONE);
echo json_encode($st->toArray()), "\n";
?>
--EXPECT--
-- default (raw) --
[{"d":1705276800}]
-- DATE_AS_STRINGS --
[{"d":"2024-01-15"}]
-- UUID_WITH_DASHES --
[{"u":"61f0c404-5cb3-11e7-907b-a6006ad3dba0"}]
-- shape flag FETCH_ONE is ignored (still full rows) --
[{"a":1,"b":2}]
