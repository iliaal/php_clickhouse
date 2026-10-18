--TEST--
ClickHouse rejects server column types nested beyond the parser depth limit
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

function varint($value) {
    $out = "";
    while ($value >= 0x80) {
        $out .= chr(($value & 0x7f) | 0x80);
        $value >>= 7;
    }
    return $out . chr($value);
}

function dataBlock($type, $body) {
    return "\x01\x00\x01\x00\x02\xff\xff\xff\xff\x00\x01" . ($body === "" ? "\x00" : "\x01")
        . "\x02c0" . varint(strlen($type)) . $type . "\x00" . $body . "\x05";
}

function nestedArray($depth, $closed) {
    return str_repeat("Array(", $depth) . ($closed ? "UInt8" . str_repeat(")", $depth) : "");
}

function summary($result) {
    return strlen($result) > 80 ? substr($result, 0, 80) . "...(" . strlen($result) . ")" : $result;
}

/* W-11: 100000 closed Array( levels; W-11b: 100000 unclosed Array( levels */
$w11 = dataBlock(nestedArray(100000, true), "");
$w11b = dataBlock(nestedArray(100000, false), "");
echo "W-11 bytes: ", strlen($w11), " ", md5($w11), "\n";
echo "W-11b bytes: ", strlen($w11b), " ", md5($w11b), "\n";
echo "W-11: ", summary(replay($mockFile, $responseFile, $hello, $w11, "none", $select)), "\n";
echo "W-11b: ", summary(replay($mockFile, $responseFile, $hello, $w11b, "none", $select)), "\n";

foreach ([1001, 5000] as $depth) {
    echo "depth $depth: ", summary(replay($mockFile, $responseFile, $hello,
        dataBlock(nestedArray($depth, true), ""), "none", $select)), "\n";
}
echo "depth 1000: ", replay($mockFile, $responseFile, $hello,
    dataBlock(nestedArray(1000, true), ""), "none", $select), "\n";
echo "depth 8: ", replay($mockFile, $responseFile, $hello,
    dataBlock(nestedArray(8, true), str_repeat("\x01\x00\x00\x00\x00\x00\x00\x00", 8) . "\x07"), "none", $select), "\n";
echo "map: ", replay($mockFile, $responseFile, $hello,
    dataBlock("Map(String, UInt8)", "\x01\x00\x00\x00\x00\x00\x00\x00" . "\x01k" . "\x09"),
    "none", $select), "\n";
?>
--EXPECT--
W-11 bytes: 700025 77bd0738ed682a90f6062bda821ad9bd
W-11b bytes: 600020 c9877e6644bd25673f0e642834436511
W-11: exception:unsupported column type: Array(Array(Array(Array(Array(Array(Array(Arr...(4106)
W-11b: exception:unsupported column type: Array(Array(Array(Array(Array(Array(Array(Arr...(4106)
depth 1001: exception:unsupported column type: Array(Array(Array(Array(Array(Array(Array(Arr...(4106)
depth 5000: exception:unsupported column type: Array(Array(Array(Array(Array(Array(Array(Arr...(4106)
depth 1000: accepted:[]
depth 8: accepted:[{"c0":[[[[[[[[7]]]]]]]]}]
map: accepted:[{"c0":{"k":9}}]
