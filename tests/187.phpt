--TEST--
ClickHouse Time and Time64 string rendering handles minimum signed values
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$mode = ClickHouse::FETCH_ONE | ClickHouse::DATE_AS_STRINGS;

echo $c->select("SELECT CAST(-2147483648 AS Time) AS x", [], $mode), "\n";
echo $c->select("SELECT CAST(-9223372036854775808 AS Time64(0)) AS x", [], $mode), "\n";
?>
--EXPECT--
-596523:14:08
-2562047788015215:30:08
