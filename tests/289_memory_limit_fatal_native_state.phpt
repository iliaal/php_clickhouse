--TEST--
A memory_limit fatal at any allocation made while native query state is live leaves no leak and the client usable
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
 * increments through the allocations that call makes around its SQL, bound
 * parameters, settings, client options, insert blocks, verbose payloads, and
 * error objects.
 * Under LeakSanitizer, a child that unwinds natively over that state
 * reports the leak. */
clickhouse_run_memory_limit_children([
    "execute" => range(0, 128, 32),
    "verbose" => range(0, 3264, 32),
    "statement_params" => range(0, 4352, 32),
    "stream" => range(0, 1856, 32),
    "to_stream" => range(0, 5952, 32),
    "callback" => range(0, 1216, 32),
    "server_error" => range(0, 2272, 32),
    "insert" => range(0, 1280, 32),
    "insert_assoc" => range(0, 1728, 32),
    "write" => range(0, 576, 32),
    "from_stream" => range(0, 1056, 32),
    "external" => range(0, 1056, 32),
    "construct" => range(0, 4768, 32),
    "metadata" => range(0, 800, 32),
    "geo" => range(0, 4032, 32),
    "json" => range(0, 1632, 32),
], ["execute", "verbose", "statement_params", "stream", "to_stream", "callback", "server_error", "insert", "insert_assoc", "write", "from_stream", "external", "construct", "metadata", "geo", "json"]);
$c->execute("DROP TABLE test.memory_limit_native");
?>
--EXPECT--
execute: 5 children, 0 failures
verbose: 103 children, 0 failures
statement_params: 137 children, 0 failures
stream: 59 children, 0 failures
to_stream: 187 children, 0 failures
callback: 39 children, 0 failures
server_error: 72 children, 0 failures
insert: 41 children, 0 failures
insert_assoc: 55 children, 0 failures
write: 19 children, 0 failures
from_stream: 34 children, 0 failures
external: 34 children, 0 failures
construct: 150 children, 0 failures
metadata: 26 children, 0 failures
geo: 127 children, 0 failures
json: 52 children, 0 failures
