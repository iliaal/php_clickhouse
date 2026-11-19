--TEST--
A fatal error inside a row callback unwinds the query so the client stays usable
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";
$c = new ClickHouse(clickhouse_test_config());
register_shutdown_function(function () use ($c) {
    try {
        echo json_encode($c->select("SELECT 1 AS x")), "\n";
    } catch (ClickHouseException $e) {
        echo get_class($e), ": ", $e->getMessage(), "\n";
    }
});
$n = 0;
$c->selectStreamCallback(
    "SELECT number FROM numbers(300000) SETTINGS max_block_size = 65536",
    function ($row) use (&$n) {
        if (++$n === 1000) {
            eval('function clickhouse_278_dup() {} function clickhouse_278_dup() {}');
        }
    }
);
echo "unreachable\n";
?>
--EXPECTF--
Fatal error: Cannot redeclare %A
[{"x":1}]
