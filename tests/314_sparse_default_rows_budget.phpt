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
 * bytes declare any row count. The client accepts 2^20 default rows per
 * column and 64 MiB of expanded default values per block, and rejects one
 * row or one column more. Before the fix it accepted up to 1 GiB, such as
 * 2^27 UInt64 rows from about 75 bytes. The varints are literal bytes because
 * 32-bit PHP cannot encode the end-of-gaps flag, 2^62. */
const ROWS_2_20 = "\x80\x80\x40";
const ALL_DEFAULT_2_20 = "\x80\x80\xc0\x80\x80\x80\x80\x80\x40";
const ROWS_2_20_1 = "\x81\x80\x40";
const ALL_DEFAULT_2_20_1 = "\x81\x80\xc0\x80\x80\x80\x80\x80\x40";
const ROWS_2_28 = "\x80\x80\x80\x80\x01";
const ALL_DEFAULT_2_28 = "\x80\x80\x80\x80\x81\x80\x80\x80\x40";
/* Gaps of one row before rows 1 and 3, then the last row and the end flag. */
const ROWS_1_AND_3 = "\x01\x01\x81\x80\x80\x80\x80\x80\x80\x80\x40" . "\x07\x00\x00\x00\x00\x00\x00\x00"
    . "\x09\x00\x00\x00\x00\x00\x00\x00";

function sparse_response(string $rows, string $body, int $columns): string
{
    $header = [];
    $block = "\x01" . wire_str("") . "\x01\x00\x02\xff\xff\xff\xff\x00" . wire_varint($columns) . $rows;
    for ($i = 0; $i < $columns; $i++) {
        $header[] = ["c$i", "UInt64", ""];
        $block .= wire_str("c$i") . wire_str("UInt64") . "\x01\x01" . $body;
    }
    return bin2hex(wire_data_block($header, 0) . $block . "\x05");
}

$cases = [
    "one column of 2^20 default rows" => sparse_response(ROWS_2_20, ALL_DEFAULT_2_20, 1),
    "one column of 2^20+1 default rows" => sparse_response(ROWS_2_20_1, ALL_DEFAULT_2_20_1, 1),
    "eight columns of 2^20 default rows" => sparse_response(ROWS_2_20, ALL_DEFAULT_2_20, 8),
    "nine columns of 2^20 default rows" => sparse_response(ROWS_2_20, ALL_DEFAULT_2_20, 9),
    "one column of 2^28 default rows" => sparse_response(ROWS_2_28, ALL_DEFAULT_2_28, 1),
    "rows 1 and 3 set" => sparse_response(wire_varint(5), ROWS_1_AND_3, 1),
];
foreach ($cases as $label => $response) {
    $result = wire_run(wire_default_hello_hex(), $response, function ($client) {
        $column = $client->select("SELECT c0 FROM t", [], ClickHouse::FETCH_COLUMN);
        return count($column) > 5 ? count($column) . " rows, sum " . array_sum($column) : json_encode($column);
    });
    echo $label, ": ", implode(" ", $result), "\n";
}
?>
--EXPECT--
one column of 2^20 default rows: ok 1048576 rows, sum 0
one column of 2^20+1 default rows: exception sparse column declares 1048577 default rows, more than 1048576
eight columns of 2^20 default rows: ok 1048576 rows, sum 0
nine columns of 2^20 default rows: exception sparse columns of one block expand past 67108864 bytes of default rows
one column of 2^28 default rows: exception sparse column declares 268435456 default rows, more than 1048576
rows 1 and 3 set: ok [0,7,0,9,0]
