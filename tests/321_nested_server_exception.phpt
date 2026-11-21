--TEST--
A nested server exception is consumed with the exception that carries it
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
if (!function_exists("proc_open")) print "skip proc_open unavailable";
?>
--FILE--
<?php
require __DIR__ . "/_wire_mock.inc";
function exception_body(int $code, string $text, bool $nested): string
{
    return pack("V", $code) . wire_str("DB::Exception") . wire_str($text) . wire_str("") . ($nested ? "\x01" : "\x00");
}
/* The nested exception's code, 5, reads as an EndOfStream packet when it is
 * left in the buffer: before the fix the next query returned no rows from it
 * instead of reading its own response. */
$response = "\x02" . exception_body(60, "outer", true) . exception_body(5, "inner", false)
    . wire_select_response([["x", "UInt8", "\x2a"]], 1);
$result = wire_run(wire_default_hello_hex(), bin2hex($response), function ($client) {
    $out = [];
    try {
        $client->select("SELECT x FROM missing");
        $out[] = "first query returned";
    } catch (ClickHouseException $e) {
        $out[] = "first: " . $e->getServerCode() . " " . $e->getMessage();
    }
    try {
        $out[] = "second: " . json_encode($client->select("SELECT x FROM t"));
    } catch (ClickHouseException $e) {
        $out[] = "second: exception " . $e->getMessage();
    }
    return $out;
});
echo $result[0], "\n", implode("\n", (array)$result[1]), "\n";
if (isset($result[2])) echo $result[2], "\n";
?>
--EXPECT--
ok
first: 60 outer
second: [{"x":42}]
