--TEST--
Finalizing an insert rejects Data packets, and operation_timeout bounds inserts the server stops reading
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
if (!function_exists("proc_open")) print "skip proc_open unavailable";
?>
--INI--
memory_limit=512M
--FILE--
<?php
require __DIR__ . "/_paced_mock.inc";
/* After the end of INSERT data the peer sends 34-byte Data packets whose
 * sparse UInt64 column declares 2^20 default rows, for about 20 seconds.
 * Before the fix each one was decoded and dropped: they carry rows, so they
 * never counted toward the cap on packets without rows, and the decoding
 * never counted toward the five-second abandon cap. */
$schema = bin2hex(wire_data_block([["a", "UInt64", ""]], 0));
$sparse = bin2hex("\x01" . wire_str("") . "\x01\x00\x02\xff\xff\xff\xff\x00" . "\x01" . "\x80\x80\x40"
    . wire_str("a") . wire_str("UInt64") . "\x01\x01" . "\x80\x80\xc0\x80\x80\x80\x80\x80\x40");
$flood = ["first" => $schema, "packet" => $sparse, "count" => 200000, "interval" => 100];
list($result) = paced_run($flood, ["receive_timeout_ms" => 0], function ($client) {
    $client->writeStart("t", ["a"]);
    $started = microtime(true);
    unset($client);
    return microtime(true) - $started < 8 ? "abandoned within the cap" : "abandon outlived the cap";
});
echo "abandoned insert: $result\n";
list($result, $elapsed) = paced_run($flood, ["operation_timeout_ms" => 2000], function ($client) {
    $client->writeStart("t", ["a"]);
    $client->writeEnd();
    return "writeEnd returned";
});
echo "writeEnd: $result", $elapsed < 6 ? "" : " after {$elapsed} s", "\n";

/* The peer sends the schema and then stops reading for twelve seconds while
 * insert() sends 64 MiB. send_timeout is unlimited; operation_timeout is not.
 * The failed insert's reconnect then waits out operation_timeout once more
 * for the stalled peer's hello. */
$rows = array_fill(0, 64, [str_repeat("x", 1 << 20)]);
list($result, $elapsed) = paced_run(["first" => bin2hex(wire_data_block([["a", "String", ""]], 0)), "stall" => 12],
    ["send_timeout_ms" => 0, "operation_timeout_ms" => 1500], function ($client) use ($rows) {
        $client->insert("t", ["a"], $rows);
        return "insert returned";
    });
echo "stalled insert: $result", $elapsed < 6 ? ", within operation_timeout" : ", after {$elapsed} s", "\n";
?>
--EXPECT--
abandoned insert: abandoned within the cap
writeEnd: exception server sent a Data packet after the end of INSERT data
stalled insert: exception write timed out, within operation_timeout
