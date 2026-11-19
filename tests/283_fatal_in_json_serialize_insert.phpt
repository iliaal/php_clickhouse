--TEST--
A fatal error in jsonSerialize() during a JSON column insert leaves the client usable
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";
class Fatal283 implements JsonSerializable
{
    #[\ReturnTypeWillChange]
    public function jsonSerialize()
    {
        eval('function clickhouse_283_dup() {} function clickhouse_283_dup() {}');
        return [];
    }
}

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.fatal_json_283");
$c->execute("CREATE TABLE test.fatal_json_283 (j JSON) ENGINE=Memory");
register_shutdown_function(function () use ($c) {
    try {
        $c->insert("test.fatal_json_283", ["j"], [[["a" => 1]]]);
        echo "rows after reuse: ", $c->select("SELECT count() FROM test.fatal_json_283", [], ClickHouse::FETCH_ONE), "\n";
    } catch (ClickHouseException $e) {
        echo get_class($e), ": ", $e->getMessage(), "\n";
    }
});
$c->insert("test.fatal_json_283", ["j"], [[["a" => 0]], [new Fatal283]]);
echo "unreachable\n";
?>
--CLEAN--
<?php
require __DIR__ . "/_clickhouse.inc";
$c = new ClickHouse(clickhouse_test_config());
$c->execute("DROP TABLE IF EXISTS test.fatal_json_283");
?>
--EXPECTF--
Fatal error: Cannot redeclare %A
rows after reuse: 1
