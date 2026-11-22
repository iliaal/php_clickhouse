--TEST--
A compressed frame that declares more output than its codec can produce is rejected before its buffer is reserved
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
if (!function_exists("proc_open")) print "skip proc_open unavailable";
?>
--FILE--
<?php
require __DIR__ . "/_wire_mock.inc";
/* Each frame has a valid checksum and a one-row UInt8 block, but declares
 * 256 MiB of output: in the frame header for LZ4 and ZSTD, and also in the
 * ZSTD frame's own content size. Neither codec reaches that from 40 bytes. */
$hello = "00044d6f636b0101bba90303555443044d6f636b00";
$frames = [
    "lz4" => ["lz4", "010050f219dc7a3399dfa0916fcc28ed2d6f822000000000000010f006010002ffffffff0001010263300555496e74380007"],
    "zstd" => ["zstd", "01008967bac15e438c2f2dbeae7c6b34391d902e0000000000001028b52ffde01500000000000000a90000010002ffffffff0001010263300555496e74380007"],
    "zstd content size" => ["zstd", "0100eb9a6f7b0aa27db8c6b38cabe0c6ecc4902e0000000000001028b52ffde00000001000000000a90000010002ffffffff0001010263300555496e74380007"],
];
foreach ($frames as $label => list($codec, $response)) {
    $mock = wire_mock_start($hello, $response);
    try {
        $c = new ClickHouse(["compression" => $codec] + wire_mock_config($mock[2]));
        $rows = $c->select("SELECT c0", [], ClickHouse::FETCH_COLUMN);
        echo $label, ": accepted ", json_encode($rows), "\n";
    } catch (ClickHouseException $e) {
        echo $label, ": ", $e->getMessage(), "\n";
    }
    $status = wire_mock_stop($mock);
    if ($status !== "") echo $label, ": ", $status, "\n";
}
?>
--EXPECT--
lz4: decompressed block size 268435456 exceeds what 32 compressed bytes can hold, data is likely corrupted
zstd: decompressed block size 268435456 exceeds what 46 compressed bytes can hold, data is likely corrupted
zstd content size: decompressed block size 268435456 exceeds what 46 compressed bytes can hold, data is likely corrupted
