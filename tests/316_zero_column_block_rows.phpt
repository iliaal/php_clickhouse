--TEST--
A block that declares rows without any column is rejected
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
if (!function_exists("proc_open")) print "skip proc_open unavailable";
?>
--INI--
memory_limit=128M
--FILE--
<?php
require __DIR__ . "/_wire_mock.inc";
/* 2^31 rows and no columns: before the fix select() built an empty row for
 * each declared row until memory_limit ended the request. */
$block = "\x01" . wire_str("") . "\x01\x00\x02\xff\xff\xff\xff\x00" . "\x00" . "\x80\x80\x80\x80\x08";
$modes = ["rows" => 0, "column" => ClickHouse::FETCH_COLUMN];
foreach ($modes as $label => $flags) {
    $result = wire_run(wire_default_hello_hex(), bin2hex($block . "\x05"), wire_select_op($flags));
    echo $label, ": ", $result[0], " ", is_string($result[1]) ? $result[1] : json_encode($result[1]), "\n";
}
?>
--EXPECT--
rows: exception block declares 2147483648 rows but no columns
column: exception block declares 2147483648 rows but no columns
