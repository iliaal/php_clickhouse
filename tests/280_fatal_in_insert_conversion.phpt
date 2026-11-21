--TEST--
A fatal error in __toString during insert() conversion leaves the client usable
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";
class Fatal280
{
    public function __toString(): string
    {
        eval('function clickhouse_280_dup() {} function clickhouse_280_dup() {}');
        return "x";
    }
}

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.fatal_conv_280");
$c->execute("CREATE TABLE test.fatal_conv_280 (s String) ENGINE=Memory");
register_shutdown_function(function () use ($c) {
    try {
        echo "rows: ", $c->select("SELECT count() FROM test.fatal_conv_280", [], ClickHouse::FETCH_ONE), "\n";
        $c->insert("test.fatal_conv_280", ["s"], [["after"]]);
        echo "rows after reuse: ", $c->select("SELECT count() FROM test.fatal_conv_280", [], ClickHouse::FETCH_ONE), "\n";
    } catch (ClickHouseException $e) {
        echo get_class($e), ": ", $e->getMessage(), "\n";
    }
});
$rows = [];
for ($i = 0; $i < 1000; $i++) {
    $rows[] = [$i === 500 ? new Fatal280 : "value-$i"];
}
$c->insert("test.fatal_conv_280", ["s"], $rows);
echo "unreachable\n";
?>
--CLEAN--
<?php
require __DIR__ . "/_clickhouse.inc";
$c = new ClickHouse(clickhouse_test_config());
$c->execute("DROP TABLE IF EXISTS test.fatal_conv_280");
?>
--EXPECTF--
Fatal error: Cannot redeclare %A
rows: 0
rows after reuse: 1
