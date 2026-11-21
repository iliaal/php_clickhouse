--TEST--
A memory_limit fatal anywhere in a callback, stream output, or error call leaves the client usable
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
 * the allocations after the buffers and some children complete the call.
 * selectStream() is left to test 289: it reports a refused buffering
 * allocation as a catchable ClickHouseException, as tests 211 and 302 assert. */
clickhouse_run_memory_limit_children([
    "callback" => range(0, 21504, 256),
    "server_error" => range(0, 21504, 256),
    "to_stream" => range(0, 21504, 256),
    "to_stream_nullable" => range(0, 43008, 512),
], ["callback", "server_error", "to_stream", "to_stream_nullable"]);
?>
--EXPECT--
callback: 85 children, 0 failures
server_error: 85 children, 0 failures
to_stream: 85 children, 0 failures
to_stream_nullable: 85 children, 0 failures
