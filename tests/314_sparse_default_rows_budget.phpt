--TEST--
Sparse columns expand no more default rows than a block allows
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
require __DIR__ . "/_wire_mock.inc";
/* One gap group can declare every row of a sparse column default, so a few
 * bytes declare any row count. Before the fix the client expanded 2^28 such
 * rows into a dense column and then a PHP row each. The varints are literal
 * bytes because 32-bit PHP cannot encode the end-of-gaps flag, 2^62. */
const ROWS_2_28 = "\x80\x80\x80\x80\x01";
const ALL_DEFAULT_2_28 = "\x80\x80\x80\x80\x81\x80\x80\x80\x40";
const ROWS_2_26 = "\x80\x80\x80\x20";
const ALL_DEFAULT_2_26 = "\x80\x80\x80\xa0\x80\x80\x80\x80\x40";
/* Gaps of one row before rows 1 and 3, then the last row and the end flag. */
const ROWS_1_AND_3 = "\x01\x01\x81\x80\x80\x80\x80\x80\x80\x80\x40" . "\x07\x09";

function sparse_response(string $rows, array $columns): string
{
    $header = [];
    $block = "\x01" . wire_str("") . "\x01\x00\x02\xff\xff\xff\xff\x00" . wire_varint(count($columns)) . $rows;
    foreach ($columns as $name => $body) {
        $header[] = [$name, "UInt8", ""];
        $block .= wire_str($name) . wire_str("UInt8") . "\x01\x01" . $body;
    }
    return bin2hex(wire_data_block($header, 0) . $block . "\x05");
}

$cases = [
    "one column of 2^28 default rows" => sparse_response(ROWS_2_28, ["c" => ALL_DEFAULT_2_28]),
    "three columns of 2^26 default rows" => sparse_response(ROWS_2_26,
        ["a" => ALL_DEFAULT_2_26, "b" => ALL_DEFAULT_2_26, "c" => ALL_DEFAULT_2_26]),
    "rows 1 and 3 set" => sparse_response(wire_varint(5), ["c" => ROWS_1_AND_3]),
];
foreach ($cases as $label => $response) {
    $result = wire_run(wire_default_hello_hex(), $response, function ($client) {
        return json_encode($client->select("SELECT c FROM t", [], ClickHouse::FETCH_COLUMN));
    });
    echo $label, ": ", implode(" ", $result), "\n";
}
?>
--EXPECT--
one column of 2^28 default rows: exception sparse columns declare more default rows than a block may expand (268435456 rows of 268435456)
three columns of 2^26 default rows: exception sparse columns declare more default rows than a block may expand (67108864 rows of 67108864)
rows 1 and 3 set: ok [0,7,0,9,0]
