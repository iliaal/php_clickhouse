--TEST--
ClickHouse rejects compressed frames whose declared size is smaller than the frame header
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
if (!function_exists("proc_open")) {
    print "skip proc_open unavailable";
}
?>
--FILE--
<?php
$mockCode = <<<'PHP'
$response = hex2bin($argv[1]);
$server = stream_socket_server("tcp://127.0.0.1:0", $errno, $error);
if (!$server) {
    fwrite(STDERR, "$errno: $error\n");
    exit(1);
}
$address = stream_socket_get_name($server, false);
fwrite(STDOUT, substr(strrchr($address, ":"), 1) . "\n");
fflush(STDOUT);
$hello = hex2bin("000a436c69636b486f7573651a09dca90303555443" . "0c3563666363653339323563630d");

function readByte($peer) {
    $data = fread($peer, 1);
    return $data === false || $data === "" ? false : ord($data);
}

$peer = stream_socket_accept($server, 5);
fclose($server);
if ($peer === false) {
    fwrite(STDERR, "accept failed\n");
    exit(2);
}
if (readByte($peer) !== 0) {
    fwrite(STDERR, "handshake failed\n");
    exit(2);
}
fwrite($peer, $hello);
$sawQuery = false;
for ($i = 0; $i < 4096 && !$sawQuery; $i++) {
    $byte = readByte($peer);
    if ($byte === false) break;
    $sawQuery = $byte === 1;
}
if (!$sawQuery) {
    fwrite(STDERR, "query not received\n");
    exit(2);
}
fwrite($peer, $response);
stream_socket_shutdown($peer, STREAM_SHUT_WR);
stream_set_timeout($peer, 2);
while (!feof($peer)) {
    $chunk = fread($peer, 8192);
    if ($chunk === false || $chunk === "") break;
}
fclose($peer);
exit(0);
PHP;

$mockFile = tempnam(sys_get_temp_dir(), "clickhouse-mock-");
if ($mockFile === false) throw new RuntimeException("mock file creation failed");
register_shutdown_function(function () use ($mockFile) {
    @unlink($mockFile);
});
if (file_put_contents($mockFile, "<?php\n" . $mockCode) === false) {
    throw new RuntimeException("mock file write failed");
}

function runCase($mockFile, $compression, $response) {
    $process = proc_open([PHP_BINARY, "-n", "-f", $mockFile, bin2hex($response)], [
        0 => ["pipe", "r"],
        1 => ["pipe", "w"],
        2 => ["pipe", "w"],
    ], $pipes);
    if (!is_resource($process)) throw new RuntimeException("mock start failed");
    fclose($pipes[0]);
    $line = fgets($pipes[1]);
    $port = $line === false ? 0 : (int)trim($line);
    if ($port < 1 || $port > 65535) {
        proc_terminate($process);
        fclose($pipes[1]);
        $error = trim(stream_get_contents($pipes[2]));
        fclose($pipes[2]);
        proc_close($process);
        throw new RuntimeException("mock did not publish a valid port: " . $error);
    }
    try {
        $client = new ClickHouse([
            "host" => "127.0.0.1",
            "port" => $port,
            "compression" => $compression,
            "retry_count" => 0,
            "retry_timeout" => 0,
            "connect_timeout_ms" => 500,
            "receive_timeout_ms" => 2000,
            "send_timeout_ms" => 2000,
        ]);
        $result = "rows=" . json_encode($client->select("SELECT x"));
    } catch (ClickHouseException $e) {
        $result = get_class($e) . ": " . $e->getMessage();
    }
    unset($client);
    fclose($pipes[1]);
    $stderr = trim(stream_get_contents($pipes[2]));
    fclose($pipes[2]);
    $status = proc_close($process);
    if ($status !== 0 || $stderr !== "") $result .= " [mock failed: $status $stderr]";
    return $result;
}

/* Data packet (code 1, empty table name) wrapping one compressed frame:
 * CityHash128, method byte, declared compressed size, declared original
 * size, payload. The block inside the valid frames is one UInt8 column
 * "x" holding 7. */
$data = "\x01\x00";
$eos = "\x05";
$w1 = hex2bin("52b6501e8b800113d85dec7ddd536062") . "\x90" . pack("V", 0) . pack("V", 0x02000100) . "\xff\xff";
$w1bTrailer = str_repeat("A", 4096);
$frames = [
    "lz4" => [
        "method" => "\x82",
        "valid" => "adc2ac817d00899ab66c366ee9b2d026821f00000014000000f0050100020000000000010101780555496e74380007",
        "empty" => "452713af4617af8949e2873f47daf0ee820900000014000000",
    ],
    "zstd" => [
        "method" => "\x90",
        "valid" => "7f62ba24ae47c69953017ea395a82747902d0000001400000028b52ffde01400000000000000a100000100020000000000010101780555496e74380007",
        "empty" => "9dd51c1e2414b26ff83a15d69a3edcd8900900000014000000",
    ],
];

echo "zstd w1: ", runCase($mockFile, "zstd", $data . $w1), "\n";
foreach ($frames as $name => $frame) {
    foreach ([0, 1, 8] as $size) {
        $header = str_repeat("\xaa", 16) . $frame["method"] . pack("V", $size) . pack("V", 0x40);
        echo "$name size=$size: ", runCase($mockFile, $name, $data . $header . $w1bTrailer), "\n";
    }
    /* A 9-byte frame carries an empty payload. LZ4 fails to decompress it.
     * ZSTD decodes it to zero bytes without an error, so the frame is
     * rejected only by the unread-bytes check in ~CompressedInput. */
    echo "$name size=9: ", runCase($mockFile, $name, $data . hex2bin($frame["empty"]) . $eos), "\n";
    echo "$name valid: ", runCase($mockFile, $name, $data . hex2bin($frame["valid"]) . $eos), "\n";
}
?>
--EXPECT--
zstd w1: ClickHouseException: compressed block size 0 is smaller than header, data is likely corrupted
lz4 size=0: ClickHouseException: compressed block size 0 is smaller than header, data is likely corrupted
lz4 size=1: ClickHouseException: compressed block size 1 is smaller than header, data is likely corrupted
lz4 size=8: ClickHouseException: compressed block size 8 is smaller than header, data is likely corrupted
lz4 size=9: ClickHouseException: can't decompress LZ4-encoded data
lz4 valid: rows=[{"x":7}]
zstd size=0: ClickHouseException: compressed block size 0 is smaller than header, data is likely corrupted
zstd size=1: ClickHouseException: compressed block size 1 is smaller than header, data is likely corrupted
zstd size=8: ClickHouseException: compressed block size 8 is smaller than header, data is likely corrupted
zstd size=9: ClickHouseException: some data was not read
zstd valid: rows=[{"x":7}]
