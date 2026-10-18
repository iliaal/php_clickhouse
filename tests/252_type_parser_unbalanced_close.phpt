--TEST--
ClickHouse rejects server column types with surplus closing parentheses
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
$response = file_get_contents($argv[2]);
$server = stream_socket_server("tcp://127.0.0.1:0", $errno, $error);
if (!$server) {
    fwrite(STDERR, "$errno: $error\n");
    exit(1);
}
$address = stream_socket_get_name($server, false);
fwrite(STDOUT, substr(strrchr($address, ":"), 1) . "\n");
fflush(STDOUT);
$peer = stream_socket_accept($server, 5);
if ($peer === false) {
    fwrite(STDERR, "accept failed\n");
    exit(2);
}
if (fread($peer, 1) !== "\x00") {
    fwrite(STDERR, "client hello not received\n");
    exit(2);
}
fwrite($peer, $hello);
fwrite($peer, $response);
stream_socket_shutdown($peer, STREAM_SHUT_WR);
stream_set_timeout($peer, 2);
while (!feof($peer)) {
    $chunk = fread($peer, 8192);
    if ($chunk === false || $chunk === "") break;
}
fclose($peer);
fclose($server);
PHP;

$mockFile = tempnam(sys_get_temp_dir(), "clickhouse-mock-");
$responseFile = tempnam(sys_get_temp_dir(), "clickhouse-response-");
if ($mockFile === false || $responseFile === false) throw new RuntimeException("temp file creation failed");
register_shutdown_function(function () use ($mockFile, $responseFile) {
    @unlink($mockFile);
    @unlink($responseFile);
});
file_put_contents($mockFile, "<?php\n" . $mockCode);

function replay($mockFile, $responseFile, $hello, $response, $compression, $query) {
    file_put_contents($responseFile, $response);
    $process = proc_open([PHP_BINARY, "-n", "-f", $mockFile, bin2hex($hello), $responseFile], [
        0 => ["pipe", "r"],
        1 => ["pipe", "w"],
        2 => ["pipe", "w"],
    ], $pipes);
    if (!is_resource($process)) throw new RuntimeException("mock start failed");
    fclose($pipes[0]);
    $port = (int)trim((string)fgets($pipes[1]));
    $result = "mock-failed";
    if ($port > 0) {
        try {
            $client = new ClickHouse([
                "host" => "127.0.0.1",
                "port" => $port,
                "compression" => $compression,
                "retry_count" => 0,
                "retry_timeout" => 0,
                "connect_timeout_ms" => 2000,
                "receive_timeout_ms" => 2000,
                "send_timeout_ms" => 2000,
            ]);
            $result = "accepted:" . json_encode($query($client));
        } catch (ClickHouseException $e) {
            $result = "exception:" . $e->getMessage();
        }
        unset($client);
    }
    fclose($pipes[1]);
    $stderr = trim(stream_get_contents($pipes[2]));
    fclose($pipes[2]);
    $status = proc_close($process);
    if ($status !== 0 || $stderr !== "") $result .= " mock-status=$status $stderr";
    return $result;
}

$hello = hex2bin("000a436c69636b486f7573651a09dca903035554430c3563666363653339323563630d");
$select = function ($client) {
    return $client->select("SELECT * FROM t");
};
$stream = function ($client) {
    $rows = [];
    foreach ($client->selectStream("SELECT * FROM t", [], "", [], 468) as $row) $rows[] = $row;
    return $rows;
};

function dataBlock($type, $body) {
    return "\x01\x00\x01\x00\x02\xff\xff\xff\xff\x00\x01" . ($body === "" ? "\x00" : "\x01")
        . "\x02c0" . chr(strlen($type)) . $type . "\x00" . $body . "\x05";
}

/* W-5b: plain Data block declaring column type UInt8)) */
echo "W-5b: ", replay($mockFile, $responseFile, $hello,
    hex2bin("0100010002ffffffff0001000263300755496e743829290005"), "none", $select), "\n";

/* W-5: LZ4 frame (zstd negotiated) whose block declares column type UInt8)) */
echo "W-5: ", replay($mockFile, $responseFile, $hello, hex2bin(
    "01003345bf3397cbe3d1f7eaabd97a1b1f6f824e00000043000000f034010002ffffffff00050001610755496e7438"
    . "2929000261611341727261792841727261792855496e743829290002616e174172726179284e756c6c61626c6528537472"),
    "zstd", $stream), "\n";

foreach (["),", ")))", "Array(UInt8)),", "UInt8)("] as $type) {
    echo "$type: ", replay($mockFile, $responseFile, $hello, dataBlock($type, ""), "none", $select), "\n";
}

echo "control: ", replay($mockFile, $responseFile, $hello,
    dataBlock("Array(Nullable(UInt8))", "\x02\x00\x00\x00\x00\x00\x00\x00\x00\x01\x07\x00"), "none", $select), "\n";
echo "control-tuple: ", replay($mockFile, $responseFile, $hello,
    dataBlock("Tuple(a UInt8, b String)", "\x05\x02hi"), "none", $select), "\n";
?>
--EXPECT--
W-5b: exception:unsupported column type: UInt8))
W-5: exception:unsupported column type: UInt8))
),: exception:unsupported column type: ),
))): exception:unsupported column type: )))
Array(UInt8)),: exception:unsupported column type: Array(UInt8)),
UInt8)(: exception:unsupported column type: UInt8)(
control: accepted:[{"c0":[7,null]}]
control-tuple: accepted:[{"c0":[5,"hi"]}]
