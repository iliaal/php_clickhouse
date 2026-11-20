--TEST--
A memory_limit fatal while selectToStream() or geo and JSON reads hold native state leaves no leak
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
 * increments through the allocations that call makes while it holds column
 * data or stream buffers.
 * Under LeakSanitizer, a child that unwinds natively over that state reports
 * the leak. The modes are split across tests 289 to 291 so that each test
 * stays within the runner's per-test timeout. */
clickhouse_run_memory_limit_children([
    "to_stream" => range(0, 5952, 32),
    "geo" => range(0, 4032, 32),
    "json" => range(0, 1632, 32),
], ["to_stream", "geo", "json"]);
?>
--EXPECT--
to_stream: 187 children, 0 failures
geo: 127 children, 0 failures
json: 52 children, 0 failures
