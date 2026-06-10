--TEST--
DateTime64 and Time64 render pre-epoch / sub-second-negative values with floor semantics
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());

/* Integer division truncates toward zero, so a negative sub-second raw
 * value split wrong: DateTime64(-0.5) rendered "1970-01-01 00:00:00.5"
 * (a second ahead) instead of flooring to "1969-12-31 23:59:59.5", and
 * Time64(-0.5) dropped its leading '-' because the whole-seconds part was
 * 0. The extension's DATE_AS_STRINGS rendering must match the server's
 * own toString(). */
$mode = ClickHouse::FETCH_ONE | ClickHouse::DATE_AS_STRINGS;

$dtServer = $c->select("SELECT toString(toDateTime64(-0.5, 1)) AS x", [], ClickHouse::FETCH_ONE);
$dtExt    = $c->select("SELECT toDateTime64(-0.5, 1) AS x", [], $mode);
echo "dt64 match=", ($dtServer === $dtExt ? "yes" : "no"), " ($dtExt)\n";

$tmServer = $c->select("SELECT toString(toTime64(-0.5, 1)) AS x", [], ClickHouse::FETCH_ONE);
$tmExt    = $c->select("SELECT toTime64(-0.5, 1) AS x", [], $mode);
echo "time64 match=", ($tmServer === $tmExt ? "yes" : "no"), " ($tmExt)\n";
?>
--EXPECT--
dt64 match=yes (1969-12-31 23:59:59.5)
time64 match=yes (-00:00:00.5)
