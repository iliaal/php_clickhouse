--TEST--
A memory_limit fatal in a ClickHouseStatement method ends the script while a Fiber is suspended in a query
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
}
if (!function_exists("proc_open")) {
    print "skip proc_open unavailable";
} elseif (!is_readable("/proc/self/maps")) {
    print "skip /proc/self/maps is needed to locate the extension";
}
?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";
/* ClickHouseStatement::__construct(), offsetSet(), offsetUnset(), and
 * fetchKeyPair() throw without running a query. Each mode runs one of them
 * while a Fiber is suspended inside another client's query, stepping
 * memory_limit in 32-byte increments through the throw. The script must end
 * at the fatal error: only the shutdown function may print after it. */
clickhouse_run_memory_limit_children([
    "plain_construct_fiber" => range(0, 2432, 32),
    "plain_offsetset_fiber" => range(0, 2432, 32),
    "plain_offsetunset_fiber" => range(0, 2432, 32),
    "plain_fetchkeypair_fiber" => range(0, 2432, 32),
], ["plain_construct_fiber", "plain_offsetset_fiber", "plain_offsetunset_fiber", "plain_fetchkeypair_fiber"]);
?>
--EXPECT--
plain_construct_fiber: 77 children, 0 failures
plain_offsetset_fiber: 77 children, 0 failures
plain_offsetunset_fiber: 77 children, 0 failures
plain_fetchkeypair_fiber: 77 children, 0 failures
