--TEST--
A Data packet that ends before its block is an error, not an empty block
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
if (!function_exists("proc_open")) print "skip proc_open unavailable";
?>
--FILE--
<?php
require __DIR__ . "/_wire_mock.inc";
/* The peer sends the Data packet type and its empty table name, then closes.
 * Before the fix writeStart() took the missing block as the INSERT schema
 * and returned. */
$ops = [
    "writeStart" => function ($client) {
        $client->writeStart("t", ["a"]);
        return "returned";
    },
    "select" => wire_select_op(),
];
foreach ($ops as $label => $op) {
    $result = wire_run(wire_default_hello_hex(), "0100", $op);
    echo $label, ": ", $result[0], " ", is_string($result[1]) ? $result[1] : json_encode($result[1]), "\n";
}
?>
--EXPECT--
writeStart: exception connection closed before the packet's block
select: exception connection closed before the packet's block
