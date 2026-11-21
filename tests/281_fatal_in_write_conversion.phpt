--TEST--
A fatal error in __toString during write() conversion leaves the client usable
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";
class Fatal281
{
    public function __toString(): string
    {
        eval('function clickhouse_281_dup() {} function clickhouse_281_dup() {}');
        return "x";
    }
}

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.fatal_write_281");
$c->execute("CREATE TABLE test.fatal_write_281 (s String) ENGINE=Memory");
register_shutdown_function(function () use ($c) {
    try {
        echo "rows: ", $c->select("SELECT count() FROM test.fatal_write_281", [], ClickHouse::FETCH_ONE), "\n";
        $c->writeStart("test.fatal_write_281", ["s"]);
        $c->write([["after"]]);
        $c->writeEnd();
        echo "rows after reuse: ", $c->select("SELECT count() FROM test.fatal_write_281", [], ClickHouse::FETCH_ONE), "\n";
    } catch (ClickHouseException $e) {
        echo get_class($e), ": ", $e->getMessage(), "\n";
    }
});
$c->writeStart("test.fatal_write_281", ["s"]);
$c->write([["first"]]);
$c->write([["a"], [new Fatal281]]);
echo "unreachable\n";
?>
--CLEAN--
<?php
require __DIR__ . "/_clickhouse.inc";
$c = new ClickHouse(clickhouse_test_config());
$c->execute("DROP TABLE IF EXISTS test.fatal_write_281");
?>
--EXPECTF--
Fatal error: Cannot redeclare %A
rows: 1
rows after reuse: 2
