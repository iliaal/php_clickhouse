<?php
/* Child of tests/287: runs out of memory_limit during select() conversion
 * after a ballast of 1 MiB plus argv[1] bytes, then queries again from a
 * shutdown function. */
require __DIR__ . "/_clickhouse.inc";
$c = new ClickHouse(clickhouse_test_config());
$ballast = str_repeat("x", (1 << 20) + (int)$argv[1]);
register_shutdown_function(function () use ($c, &$ballast) {
    $ballast = null;
    try {
        echo json_encode($c->select("SELECT 1 AS x")), "\n";
    } catch (ClickHouseException $e) {
        echo get_class($e), ": ", $e->getMessage(), "\n";
    }
});
$rows = $c->select("SELECT map(concat(repeat('k', 64), toString(number)), number,
                               concat(repeat('j', 64), toString(number)), number) AS m
                    FROM numbers(1000000) SETTINGS max_block_size = 65536");
echo "unreachable ", count($rows), "\n";
