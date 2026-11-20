--TEST--
A memory_limit fatal around a query's native state leaves no leak and the client usable
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
/* Each mode is one API call. The shifts step memory_limit in 32-byte
 * increments through the allocations that call makes around its SQL, bound
 * parameters, settings, verbose payloads, result objects, and error objects.
 * Under LeakSanitizer, a child that unwinds natively over that state reports
 * the leak. The modes are split across tests 289 to 291 so that each test
 * stays within the runner's per-test timeout. */
clickhouse_run_memory_limit_children([
    "execute" => range(0, 128, 32),
    "verbose" => range(0, 3264, 32),
    "statement_params" => range(0, 4352, 32),
    "stream" => range(0, 1856, 32),
    "callback" => range(0, 1216, 32),
    "server_error" => range(0, 2272, 32),
], ["execute", "verbose", "statement_params", "stream", "callback", "server_error"]);
?>
--EXPECT--
execute: 5 children, 0 failures
verbose: 103 children, 0 failures
statement_params: 137 children, 0 failures
stream: 59 children, 0 failures
callback: 39 children, 0 failures
server_error: 72 children, 0 failures
