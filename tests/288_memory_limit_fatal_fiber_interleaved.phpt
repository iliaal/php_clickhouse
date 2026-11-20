--TEST--
A memory_limit fatal after a Fiber interleaves two clients' queries leaves the client usable
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
require __DIR__ . "/_clickhouse.inc";
clickhouse_skip_if_no_server();
clickhouse_skip_unless_shutdown_after_oom();
clickhouse_skip_unless_tracked_allocator();
if (PHP_VERSION_ID < 80100) {
    print "skip Fibers need PHP 8.1";
} elseif (!function_exists("proc_open")) {
    print "skip proc_open unavailable";
} elseif (!is_readable("/proc/self/maps")) {
    print "skip /proc/self/maps is needed to locate the extension";
}
?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";
/* A Fiber leaves one client's query suspended in its row callback while
 * another client's query resumes it, then runs out of memory outside row
 * conversion; each child shifts where the limit is reached. */
clickhouse_run_memory_limit_children([
    "fiber" => range(0, 1600, 8),
], ["fiber"]);
?>
--EXPECT--
fiber: 201 children, 0 failures
