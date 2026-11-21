--TEST--
A memory_limit fatal anywhere in a query call leaves the client usable
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
/* The C backend charges its protocol buffers to memory_limit, so a call
 * needs several times the headroom that tests 287 to 291 step through, and
 * those tests reach the limit at the first native buffer. These shifts span
 * each call's whole footprint in coarser steps, so the limit also lands on
 * the allocations after the buffers and some children complete the call. */
clickhouse_run_memory_limit_children([
    "prepare" => range(0, 21504, 256),
    "execute" => range(0, 21504, 256),
    "verbose" => range(0, 21504, 256),
    "statement_params" => range(0, 21504, 256),
], ["prepare", "execute", "verbose", "statement_params"]);
?>
--EXPECT--
prepare: 85 children, 0 failures
execute: 85 children, 0 failures
verbose: 85 children, 0 failures
statement_params: 85 children, 0 failures
