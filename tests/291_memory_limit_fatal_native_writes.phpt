--TEST--
A memory_limit fatal while inserts or the constructor hold native state leaves no leak
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
require __DIR__ . "/_clickhouse.inc";
clickhouse_skip_if_no_server();
clickhouse_skip_unless_shutdown_after_oom();
clickhouse_skip_unless_tracked_allocator();
if (!function_exists("proc_open")) {
    print "skip proc_open unavailable";
} elseif (!is_readable("/proc/self/maps")) {
    print "skip /proc/self/maps is needed to locate the extension";
}
?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";
$c = new ClickHouse(clickhouse_test_config());
$c->execute("DROP TABLE IF EXISTS test.memory_limit_native");
$c->execute("CREATE TABLE test.memory_limit_native (a UInt64, s String, n Nullable(String),
             m Map(String, UInt64), t Tuple(UInt8, String)) ENGINE = Null");
/* Each mode is one API call. The shifts step memory_limit in 32-byte
 * increments through the allocations that call makes while it holds insert
 * blocks, external tables, or client options.
 * Under LeakSanitizer, a child that unwinds natively over that state reports
 * the leak. The modes are split across tests 289 to 291 so that each test
 * stays within the runner's per-test timeout. */
clickhouse_run_memory_limit_children([
    "insert" => range(0, 1280, 32),
    "insert_assoc" => range(0, 1728, 32),
    "write" => range(0, 576, 32),
    "from_stream" => range(0, 1056, 32),
    "external" => range(0, 1056, 32),
    "construct" => range(0, 4768, 32),
    "metadata" => range(0, 800, 32),
], ["insert", "insert_assoc", "write", "from_stream", "external", "construct", "metadata"]);
$c->execute("DROP TABLE test.memory_limit_native");
?>
--EXPECT--
insert: 41 children, 0 failures
insert_assoc: 55 children, 0 failures
write: 19 children, 0 failures
from_stream: 34 children, 0 failures
external: 34 children, 0 failures
construct: 150 children, 0 failures
metadata: 26 children, 0 failures
