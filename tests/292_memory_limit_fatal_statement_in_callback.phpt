--TEST--
A memory_limit fatal in a ClickHouseStatement method called from a ClickHouse callback ends the script
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
/* ClickHouseStatement::__construct(), offsetSet(), offsetUnset(), and
 * fetchKeyPair() throw without running a query. Each mode runs one of them
 * inside a selectStreamCallback() row callback, stepping
 * memory_limit in 32-byte increments through the throw. The script must end
 * at the fatal error: only the shutdown function may print after it. */
clickhouse_run_memory_limit_children([
    "plain_construct_callback" => range(0, 3680, 32),
    "plain_offsetset_callback" => range(0, 3680, 32),
    "plain_offsetunset_callback" => range(0, 3680, 32),
    "plain_fetchkeypair_callback" => range(0, 3680, 32),
], ["plain_construct_callback", "plain_offsetset_callback", "plain_offsetunset_callback", "plain_fetchkeypair_callback"]);
?>
--EXPECT--
plain_construct_callback: 116 children, 0 failures
plain_offsetset_callback: 116 children, 0 failures
plain_offsetunset_callback: 116 children, 0 failures
plain_fetchkeypair_callback: 116 children, 0 failures
