--TEST--
ClickHouse DateTime64 accepts the valid second at Unix epoch minus one
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.datetime64_epoch_minus_one");
$c->execute("CREATE TABLE test.datetime64_epoch_minus_one (
    id UInt8,
    value DateTime64(3, 'UTC')
) ENGINE=Memory");

$c->insert("test.datetime64_epoch_minus_one", ["id", "value"], [
    [1, "1969-12-31 23:59:58.999"],
    [2, "1969-12-31 23:59:59.000"],
    [3, "1969-12-31 23:59:59.500"],
]);

$rows = $c->select(
    "SELECT id, value FROM test.datetime64_epoch_minus_one ORDER BY id",
    [],
    ClickHouse::DATE_AS_STRINGS
);
foreach ($rows as $row) {
    echo $row["value"], "\n";
}

$c->execute("DROP TABLE test.datetime64_epoch_minus_one");
?>
--EXPECT--
1969-12-31 23:59:58.999
1969-12-31 23:59:59.000
1969-12-31 23:59:59.500
