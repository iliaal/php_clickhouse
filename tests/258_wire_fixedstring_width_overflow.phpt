--TEST--
Server-declared FixedString widths whose row total overflows are rejected
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

/* Header block with no rows, then a 4-row data block, then EndOfStream. */
function response($type, $body) {
    $block = function ($rows, $data) use ($type) {
        return "\x01\x00\x01\x00\x02\xff\xff\xff\xff\x00\x01" . chr($rows) .
            "\x02c0" . chr(strlen($type)) . $type . "\x00" . $data;
    };
    return bin2hex($block(0, "") . $block(4, $body) . "\x05");
}

$cases = [
    "tuple" => [
        "0100010002ffffffff0001000263302e5475706c652855496e74382c204669786564537472696e6728343631313638363031383432373338373930352929000100010002ffffffff0001040263302e5475706c652855496e74382c204669786564537472696e672834363131363836303138343237333837393035292900010203044142434405",
        true,
    ],
    "nullable" => [
        "0100010002ffffffff0001000263302a4e756c6c61626c65284669786564537472696e6728343631313638363031383432373338373930352929000100010002ffffffff0001040263302a4e756c6c61626c65284669786564537472696e672834363131363836303138343237333837393035292900000000004142434405",
        true,
    ],
    "plain" => [
        "0100010002ffffffff000100026330204669786564537472696e67283436313136383630313834323733383739303529000100010002ffffffff000104026330204669786564537472696e67283436313136383630313834323733383739303529004142434405",
        true,
    ],
    "lowcardinality" => [
        "0100010002ffffffff000100026330304c6f7743617264696e616c697479284669786564537472696e6728343631313638363031383432373338373930352929000100010002ffffffff000104026330304c6f7743617264696e616c697479284669786564537472696e6728343631313638363031383432373338373930352929000100000000000000000200000000000004000000000000004142434404000000000000000000000005",
        false,
    ],
    "control-plain" => [response("FixedString(1)", "ABCD"), false],
    "control-tuple" => [response("Tuple(UInt8, FixedString(1))", "\x01\x02\x03\x04ABCD"), false],
    "control-nullable" => [response("Nullable(FixedString(1))", "\x00\x01\x00\x00ABCD"), false],
];

foreach ($cases as $name => $case) {
    list($responseHex, $expectWidthError) = $case;
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
        $rows = $client->select("SELECT * FROM t");
        echo "$name=rows:", json_encode($rows), "\n";
    } catch (ClickHouseException $e) {
        $widthError = strpos($e->getMessage(), "exceeds the addressable size") !== false;
        echo "$name=", get_class($e), $widthError === $expectWidthError ? "" : ":unexpected-message:" . $e->getMessage(), "\n";
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
tuple=ClickHouseException
nullable=ClickHouseException
plain=ClickHouseException
lowcardinality=ClickHouseException
control-plain=rows:[{"c0":"A"},{"c0":"B"},{"c0":"C"},{"c0":"D"}]
control-tuple=rows:[{"c0":[1,"A"]},{"c0":[2,"B"]},{"c0":[3,"C"]},{"c0":[4,"D"]}]
control-nullable=rows:[{"c0":"A"},{"c0":null},{"c0":"C"},{"c0":"D"}]
