--TEST--
A memory_limit fatal at any allocation of a ClickHouse call leaves the client usable
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
/* Which allocation fails depends on the heap layout, so each child shifts
 * where it reaches memory_limit, stepping the failure through more than one
 * row's worth of conversion allocations (under 1 KiB per row), or through
 * the allocations select() makes before converting rows. */
clickhouse_run_memory_limit_children([
    "select" => range(0, 1104, 16),
    "statement" => range(0, 1104, 16),
    "iterator" => range(0, 1104, 16),
    "prepare" => range(0, 1600, 8),
], ["prepare"]);
?>
--EXPECT--
select: 70 children, 0 failures
statement: 70 children, 0 failures
iterator: 70 children, 0 failures
prepare: 201 children, 0 failures
