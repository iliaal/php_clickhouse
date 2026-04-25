--TEST--
ClickHouse SeasClick / SeasClickException BC aliases
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";
$c = new SeasClick(clickhouse_test_config());
var_dump($c instanceof ClickHouse);
var_dump(get_class($c));
try {
    $c->execute("FOO");
} catch (SeasClickException $e) {
    var_dump($e instanceof ClickHouseException);
    var_dump(get_class($e));
}
?>
--EXPECT--
bool(true)
string(10) "ClickHouse"
bool(true)
string(19) "ClickHouseException"
