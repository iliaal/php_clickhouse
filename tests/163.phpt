--TEST--
DR-003: value-shaping fetch flags propagate into nested Array / Tuple cells
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// Nested Array/Tuple reads used to hardcode fetch_mode = 0 for their
// elements, so DATE_AS_STRINGS / UUID_WITH_DASHES (and JSON_AS_*,
// FIXEDSTRING_BINARY) were honored at the top level but silently dropped
// one level down. They must now propagate to nested cells.

$c = new ClickHouse(clickhouse_test_config());

echo "-- Array(Date), no flag --\n";
$r = $c->select("SELECT [toDate('2024-01-15'), toDate('2024-02-20')] AS a");
echo json_encode($r[0]['a']), "\n";

echo "-- Array(Date), DATE_AS_STRINGS --\n";
$r = $c->select("SELECT [toDate('2024-01-15'), toDate('2024-02-20')] AS a", [], ClickHouse::DATE_AS_STRINGS);
echo json_encode($r[0]['a']), "\n";

echo "-- Tuple(Date, Int32), DATE_AS_STRINGS --\n";
$r = $c->select("SELECT tuple(toDate('2024-03-10'), toInt32(7)) AS t", [], ClickHouse::DATE_AS_STRINGS);
echo json_encode($r[0]['t']), "\n";

echo "-- Array(UUID), UUID_WITH_DASHES --\n";
$r = $c->select("SELECT [toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')] AS u", [], ClickHouse::UUID_WITH_DASHES);
echo json_encode($r[0]['u']), "\n";

echo "-- Array(UUID), no flag (bare hex) --\n";
$r = $c->select("SELECT [toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')] AS u");
echo json_encode($r[0]['u']), "\n";
?>
--EXPECT--
-- Array(Date), no flag --
[1705276800,1708387200]
-- Array(Date), DATE_AS_STRINGS --
["2024-01-15","2024-02-20"]
-- Tuple(Date, Int32), DATE_AS_STRINGS --
["2024-03-10",7]
-- Array(UUID), UUID_WITH_DASHES --
["61f0c404-5cb3-11e7-907b-a6006ad3dba0"]
-- Array(UUID), no flag (bare hex) --
["61f0c4045cb311e7907ba6006ad3dba0"]
