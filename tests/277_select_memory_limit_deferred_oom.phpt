--TEST--
select() past memory_limit raises the memory error instead of reaching the hard limit
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
require __DIR__ . "/_clickhouse.inc";
clickhouse_skip_if_no_server();
clickhouse_skip_unless_shutdown_after_oom();
?>
--INI--
memory_limit=32M
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";
$c = new ClickHouse(clickhouse_test_config());
register_shutdown_function(function () {
    echo "shutdown reached\n";
});
$rows = $c->select("SELECT number, toString(number) AS s, [number, number] AS a
                    FROM numbers(300000) SETTINGS max_block_size = 65536");
echo "unreachable ", count($rows), "\n";
?>
--EXPECTF--
Fatal error: Allowed memory size of %d bytes exhausted%A
shutdown reached
