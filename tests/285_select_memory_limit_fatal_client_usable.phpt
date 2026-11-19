--TEST--
A memory_limit fatal during select() conversion leaves the client usable
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--INI--
memory_limit=32M
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";
$c = new ClickHouse(clickhouse_test_config());
/* The partial result holds its memory until the request ends; releasing the
 * ballast gives the shutdown function room to run. */
$ballast = str_repeat("x", 4 << 20);
register_shutdown_function(function () use ($c, &$ballast) {
    $ballast = null;
    try {
        echo json_encode($c->select("SELECT 1 AS x")), "\n";
    } catch (ClickHouseException $e) {
        echo get_class($e), ": ", $e->getMessage(), "\n";
    }
});
$rows = $c->select("SELECT number, toString(number) AS s, [number, number] AS a
                    FROM numbers(300000) SETTINGS max_block_size = 65536");
echo "unreachable ", count($rows), "\n";
?>
--EXPECTF--
Fatal error: Allowed memory size of %d bytes exhausted%A
[{"x":1}]
