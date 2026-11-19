--TEST--
A fatal error in the verbose callback during insert() setup leaves the client usable
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";
$c = new ClickHouse(clickhouse_test_config());
register_shutdown_function(function () use ($c) {
    $c->setVerbose(null);
    try {
        echo json_encode($c->select("SELECT 1 AS x")), "\n";
    } catch (ClickHouseException $e) {
        echo get_class($e), ": ", $e->getMessage(), "\n";
    }
});
/* The server rejects the INSERT before sending the block header; the client
 * reports that through the verbose server_exception event. */
$c->setVerbose(function ($event, $ctx) {
    if ($event === "server_exception") {
        eval('function clickhouse_279_dup() {} function clickhouse_279_dup() {}');
    }
});
$c->insert("test.clickhouse_279_missing_table", ["v"], [[1]]);
echo "unreachable\n";
?>
--EXPECTF--
Fatal error: Cannot redeclare %A
[{"x":1}]
