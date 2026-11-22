--TEST--
Progress packets keep a long query within receive_timeout; operation_timeout bounds its total wait
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
if (!function_exists("proc_open")) print "skip proc_open unavailable";
?>
--FILE--
<?php
require __DIR__ . "/_paced_mock.inc";
/* The peer sends a Progress packet every 500 ms for three seconds, three
 * times the one-second receive_timeout, then the result. */
$run = ["packet" => "030000000000", "count" => 6, "interval" => 500000,
    "tail" => bin2hex(wire_select_response([["x", "UInt8", "\x2a"]], 1))];
$select = function ($client) {
    return json_encode($client->select("SELECT x FROM t"));
};
list($result) = paced_run($run, ["receive_timeout_ms" => 1000], $select);
echo "receive_timeout 1 s: $result\n";
list($result) = paced_run($run, ["receive_timeout_ms" => 1000, "operation_timeout_ms" => 1500], $select);
echo "operation_timeout 1.5 s: $result\n";
list($result) = paced_run($run, ["receive_timeout_ms" => 1000, "operation_timeout" => 10], $select);
echo "operation_timeout 10 s: $result\n";
?>
--EXPECT--
receive_timeout 1 s: [{"x":42}]
operation_timeout 1.5 s: exception read timed out
operation_timeout 10 s: [{"x":42}]
