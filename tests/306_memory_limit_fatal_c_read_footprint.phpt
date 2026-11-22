--TEST--
A memory_limit fatal anywhere in a geo, JSON, metadata, or constructor call leaves the client usable
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
/* The C backend charges its protocol buffers to memory_limit, so a call
 * needs several times the headroom that tests 287 to 291 step through, and
 * those tests reach the limit at the first native buffer. These shifts span
 * each call's whole footprint in coarser steps, so the limit also lands on
 * the allocations after the buffers and some children complete the call. */
clickhouse_run_memory_limit_children([
    "geo" => range(0, 21504, 256),
    "json" => range(0, 21504, 256),
    "metadata" => range(0, 21504, 256),
    "construct" => range(0, 21504, 256),
    "construct_options" => range(0, 21504, 256),
], ["geo", "json", "metadata", "construct", "construct_options"]);
$c->execute("DROP TABLE test.memory_limit_native");
?>
--EXPECT--
geo: 85 children, 0 failures
json: 85 children, 0 failures
metadata: 85 children, 0 failures
construct: 85 children, 0 failures
construct_options: 85 children, 0 failures
