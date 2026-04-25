--TEST--
ClickHouse Exception test
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";
$config = clickhouse_test_config();

$deleteTable = true;
$client = new ClickHouse($config);
try {
    $client->execute('FOO');
} catch (ClickHouseException $e) {
    var_dump(get_class($e));
}
?>
--EXPECT--
string(19) "ClickHouseException"
