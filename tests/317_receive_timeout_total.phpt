--TEST--
receive_timeout bounds each packet as a whole, and abandoning an insert waits at most five seconds
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
if (!function_exists("proc_open")) print "skip proc_open unavailable";
?>
--FILE--
<?php
require __DIR__ . "/_paced_mock.inc";
$defaults = (new ReflectionClass("ClickHouse"))->getDefaultProperties();
echo "default receive_timeout: ", $defaults["receive_timeout"], "\n";

/* One result block sent a byte every 300 ms, about nine seconds in all,
 * against a one-second timeout: every read finishes within it, the packet
 * does not. The failed query's reconnect then waits out one more second for
 * the busy peer's hello. */
$block = bin2hex(wire_select_response([["x", "UInt8", "\x2a"]], 1));
list($result, $elapsed) = paced_run(["packet" => $block, "interval" => 300000, "split" => true],
    ["receive_timeout_ms" => 1000], function ($client) {
        return json_encode($client->select("SELECT 1"));
    });
echo "trickled block: $result", $elapsed < 4 ? ", within the timeout" : ", after {$elapsed} s", "\n";

/* writeStart() gets its schema; destroying the client then finalizes the
 * INSERT while the peer sends a Progress packet every second for ten
 * seconds. The configured timeouts are unlimited. */
$schema = bin2hex(wire_data_block([["a", "UInt8", ""]], 0));
list($result) = paced_run(["first" => $schema, "packet" => "030000000000", "count" => 10, "interval" => 1000000],
    ["receive_timeout_ms" => 0], function ($client) {
        $client->writeStart("t", ["a"]);
        $started = microtime(true);
        unset($client);
        return microtime(true) - $started < 8 ? "abandoned within the cap" : "abandon outlived the cap";
    });
echo "abandoned insert: $result\n";
?>
--EXPECT--
default receive_timeout: 300
trickled block: exception read timed out, within the timeout
abandoned insert: abandoned within the cap
