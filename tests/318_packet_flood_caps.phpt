--TEST--
Packets and compressed frames that carry no rows are capped per operation
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
if (!function_exists("proc_open")) print "skip proc_open unavailable";
?>
--FILE--
<?php
require __DIR__ . "/_wire_mock.inc";
/* A flood never leaves the client waiting, so receive_timeout cannot end it.
 * Before the fix both responses below were consumed to the end. */
$progress = "\x03\x00\x00\x00\x00\x00";
$flood = wire_response_file(str_repeat($progress, 1000001) . "\x05");
$result = wire_run(wire_default_hello_hex(), $flood, wire_select_op());
echo "progress flood: ", $result[0], " ", is_string($result[1]) ? $result[1] : json_encode($result[1]), "\n";

/* Valid empty LZ4 frames ahead of one frame holding UInt8 column c0 = 42. */
$empty = "12b83a602bd7de6df44915041a40db2b820a0000000000000000";
$data = "ef9bb5a6cbee933fa46332429aaa790e822000000015000000f006010002ffffffff0001010263300555496e7438002a";
foreach ([64, 65] as $count) {
    $mock = wire_mock_start(wire_default_hello_hex(), "0100" . str_repeat($empty, $count) . $data . "05");
    try {
        $client = new ClickHouse(["compression" => "lz4"] + wire_mock_config($mock[2]));
        $rows = json_encode($client->select("SELECT c0 FROM t"));
    } catch (ClickHouseException $e) {
        $rows = "exception " . $e->getMessage();
    }
    unset($client);
    echo "$count empty frames: $rows", wire_mock_stop($mock), "\n";
}
?>
--EXPECT--
progress flood: exception server sent more than 1000000 packets without rows
64 empty frames: [{"c0":42}]
65 empty frames: exception more than 64 consecutive empty compressed frames
