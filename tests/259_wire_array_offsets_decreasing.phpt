--TEST--
Server-sent Array offsets that decrease are rejected
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
require __DIR__ . "/_wire_mock.inc";
$mockCode = <<<'PHP'
$hello = hex2bin($argv[1]);
$response = hex2bin($argv[2]);
$server = stream_socket_server("tcp://127.0.0.1:0", $errno, $error);
if (!$server) {
    fwrite(STDERR, "$errno: $error\n");
    exit(1);
}
$address = stream_socket_get_name($server, false);
fwrite(STDOUT, substr(strrchr($address, ":"), 1) . "\n");
fflush(STDOUT);

function readByte($peer) {
    $data = fread($peer, 1);
    return $data === false || $data === "" ? false : ord($data);
}

$peer = stream_socket_accept($server, 5);
if ($peer === false) {
    fwrite(STDERR, "accept failed\n");
    exit(2);
}
if (readByte($peer) !== 0) {
    fwrite(STDERR, "handshake failed\n");
    exit(2);
}
fwrite($peer, $hello);
$query = false;
for ($i = 0; $i < 128; $i++) {
    $byte = readByte($peer);
    if ($byte === false) break;
    if ($byte === 1) {
        $query = true;
        break;
    }
}
if (!$query) {
    fwrite(STDERR, "query not received\n");
    exit(2);
}
fwrite($peer, $response);
stream_socket_shutdown($peer, STREAM_SHUT_WR);
stream_set_timeout($peer, 1);
while (!feof($peer)) {
    $chunk = fread($peer, 8192);
    if ($chunk === false || $chunk === "") break;
}
fclose($peer);
fclose($server);
PHP;

$mockFile = tempnam(sys_get_temp_dir(), "clickhouse-mock-");
if ($mockFile === false) throw new RuntimeException("mock file creation failed");
register_shutdown_function(function () use ($mockFile) {
    @unlink($mockFile);
});
if (file_put_contents($mockFile, "<?php\n" . $mockCode) === false) {
    throw new RuntimeException("mock file write failed");
}

$hello = "000a436c69636b486f7573651a09dca903035554430c3563666363653339323563630d";

/* Header block with no rows, then a data block, then EndOfStream. */
function response($type, $rows, $body) {
    $block = function ($rows, $data) use ($type) {
        return "\x01\x00\x01\x00\x02\xff\xff\xff\xff\x00\x01" . chr($rows) .
            "\x02c0" . chr(strlen($type)) . $type . "\x00" . $data;
    };
    return bin2hex($block(0, "") . $block($rows, $body) . "\x05");
}

/* 127-row Array(Nothing) block whose offsets rise and then fall back to 0. */
$decreasing = "0100010002ffffffff00020001650e4172726179284e6f7468696e672900016e114e756c6c61626c65284e6f7468696e6729000100010002ffffffff" .
    "00027f01650e4172726179284e6f7468696e6729000000000000000000016e114e756c6c61626c65284e6f7468696e67290001300601019040000001" .
    "0301010100000e00010002ffffffff00062409686f73745f6e616d6506537472696e67000c3563666363653339323563630c35636663636533393235" .
    "63630c3563666363653339323563630c3563666363653339323563630c3563666363653339323563630c3563666363653339323563630c3563666363" .
    "653339323563630c3563666363653339323563630c3563666363653339323563630c3563666363653339323563630c3563666363653339323563630c" .
    "3563666363653339323563630c3563666363653339323563630c3563666363653339323563630c3563666363653339323563630c3563666363653339" .
    "323563630c3563666363653339323563630c3563666363653339323563630c3563666363653339323563630c3563666363653339323563630c356366" .
    "6363653339323563630c3563666363653339323563630c3563666363653339323563630c3563666363653339323563630c3563666363653339323563" .
    "630c3563666363653339323563630c3563666363653339323563630c3563666363653339323563630c3563666363653339323563630c356366636365" .
    "3339323563630c3563666363653339323563630c3563666363653339323563630c3563666363653339323563630c3563666363653339323563630c35" .
    "63666363653339323563630c3563666363653339323563630c63757272656e745f74696d65084461746554696d6500b791d26ab791d26ab791d26ab7" .
    "91d26ab791d26ab791d26ab791d26ab791d26ab791d26ab791d26ab791d26ab791d26ab791d26ab791d26ab791d26ab791d26ab791d26ab791d26ab7" .
    "91d26ab791d26ab791d26ab791d26ab791d26ab791d26ab791d26ab791d26ab791d26ab791d26ab791d26ab791d26ab791d26ab791d26ab791d26ab7" .
    "91d26ab791d26ab791d26a097468726561645f69640655496e7436340000000000000000000000000000000000000000000000000000000000000000" .
    "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" .
    "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" .
    "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" .
    "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" .
    "0000000000000000000000000000000000047479706523456e756d382827696e6372656d656e7427203d20312c2027676175676527203d2032290001" .
    "01010101010101010101010101010101010165627567184c6f67676572456c61707365644e616e6f7365636f6e647318496e746572666163654e6174" .
    "69766553656e6442797465731e436f6e63757272656e6379436f6e74726f6c536c6f74734772616e7465642b436f6e63757272656e6379436f6e7472" .
    "6f6c536c6f747341637175697265644e6f6e436f";

$cases = [
    "decreasing" => [$decreasing, ClickHouse::DATE_AS_STRINGS, true],
    /* Map is Array(Tuple(K, V)), so its offsets go through the same check. */
    "map-backwards" => [response("Map(String, UInt8)", 2, wire_u64(3, 1) . "\x01a" . "\x05"), 0, true],
    "map-overlap" => [response("Map(String, UInt8)", 3, wire_u64(2, 1, 3) . "\x01a\x01b\x01c" . "\x01\x02\x03"), 0, true],
    "map-overlap-pairs" => [response("Map(String, UInt8)", 3, wire_u64(2, 1, 3) . "\x01a\x01b\x01c" . "\x01\x02\x03"), ClickHouse::MAP_AS_PAIRS, true],
    "control-uint8" => [response("Array(UInt8)", 3, wire_u64(2, 2, 3) . "\x01\x02\x03"), 0, false],
    "control-empty" => [response("Array(Nothing)", 2, wire_u64(0, 0)), 0, false],
];

foreach ($cases as $name => $case) {
    list($responseHex, $flags, $expectOffsetError) = $case;
    $process = proc_open([PHP_BINARY, "-n", "-f", $mockFile, $hello, $responseHex], [
        0 => ["pipe", "r"],
        1 => ["pipe", "w"],
        2 => ["pipe", "w"],
    ], $pipes);
    if (!is_resource($process)) throw new RuntimeException("mock start failed");
    fclose($pipes[0]);
    $port = (int)trim((string)fgets($pipes[1]));
    try {
        $client = new ClickHouse([
            "host" => "127.0.0.1",
            "port" => $port,
            "compression" => 0,
            "retry_count" => 0,
            "retry_timeout" => 0,
            "connect_timeout_ms" => 1000,
            "receive_timeout_ms" => 2000,
            "send_timeout_ms" => 2000,
        ]);
        $rows = $client->select("SELECT * FROM t", [], $flags);
        echo "$name=rows:", json_encode($rows), "\n";
    } catch (ClickHouseException $e) {
        $offsetError = strpos($e->getMessage(), "Array offsets must be non-decreasing") !== false;
        echo "$name=", get_class($e), $offsetError === $expectOffsetError ? "" : ":unexpected-message:" . $e->getMessage(), "\n";
    }
    unset($client);
    fclose($pipes[1]);
    $stderr = stream_get_contents($pipes[2]);
    fclose($pipes[2]);
    $status = proc_close($process);
    if ($status !== 0 || $stderr !== "") echo "$name=mock-failed:$status:$stderr\n";
}
?>
--EXPECT--
decreasing=ClickHouseException
map-backwards=ClickHouseException
map-overlap=ClickHouseException
map-overlap-pairs=ClickHouseException
control-uint8=rows:[{"c0":[1,2]},{"c0":[]},{"c0":[3]}]
control-empty=rows:[{"c0":[]},{"c0":[]}]
