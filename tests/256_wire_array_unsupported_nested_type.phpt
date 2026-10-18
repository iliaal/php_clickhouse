--TEST--
ClickHouse rejects server-sent Array types whose element type cannot be created
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
    fwrite(STDERR, "hello not received\n");
    exit(2);
}
fwrite($peer, $hello);
$code = false;
for ($i = 0; $i < 128; $i++) {
    $code = readByte($peer);
    if ($code === false || $code === 1) break;
}
if ($code !== 1) {
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

/* Revision 54459: the client expects a custom-serialization byte per column. */
$hello = "00044d6f636b0101bba90303555443044d6f636b00";

function dataBlock($type, $rows, $payload) {
    return "\x01\x00" . "\x01\x00\x02\xff\xff\xff\xff\x00" . "\x01" . chr($rows)
        . "\x01c" . chr(strlen($type)) . $type . "\x00" . $payload;
}

function runCase($mockFile, $label, $helloHex, $responseHex, $method) {
    $process = proc_open([PHP_BINARY, "-n", "-f", $mockFile, $helloHex, $responseHex], [
        0 => ["pipe", "r"],
        1 => ["pipe", "w"],
        2 => ["pipe", "w"],
    ], $pipes);
    if (!is_resource($process)) throw new RuntimeException("mock start failed");
    fclose($pipes[0]);
    $line = fgets($pipes[1]);
    $port = $line === false ? 0 : (int)trim($line);
    if ($port < 1 || $port > 65535) throw new RuntimeException("mock did not publish a port");

    try {
        $client = new ClickHouse([
            "host" => "127.0.0.1",
            "port" => $port,
            "compression" => 0,
            "retry_count" => 0,
            "retry_timeout" => 0,
            "connect_timeout_ms" => 500,
            "receive_timeout_ms" => 500,
            "send_timeout_ms" => 500,
        ]);
        $result = $client->$method("SELECT c");
        $rows = $method === "selectStatement" ? $result->toArray() : $result;
        echo "$label: ", json_encode($rows), "\n";
    } catch (ClickHouseException $e) {
        echo "$label: ", get_class($e), ": ", $e->getMessage(), "\n";
    }
    unset($client);
    fclose($pipes[1]);
    $stderr = stream_get_contents($pipes[2]);
    fclose($pipes[2]);
    $status = proc_close($process);
    if ($status !== 0 || $stderr !== "") echo "$label: mock-failed $status $stderr\n";
}

/* Minimized W-4 response: a Data block with an Array(Null) column. */
runCase($mockFile, "W-4 data block",
    "000a436c69636b486f7573651a09dca903035554430c3563666363653339323563630d",
    "0100010002ffffffff0001000263300b4172726179284e756c6c2900",
    "select");

foreach ([
    "Array(Null)",
    "Array(DateTime64)",
    "Array(5)",
    "Array(Time64)",
    "Array(Array(5))",
    "Tuple(Array(5))",
    "Map(String, Array(5))",
] as $type) {
    runCase($mockFile, $type, $hello, bin2hex(dataBlock($type, 1, "\x00\x07") . "\x05"), "select");
}

runCase($mockFile, "Array(String) control", $hello,
    bin2hex(dataBlock("Array(String)", 2, wire_u64(2, 3) . "\x01x\x01y\x01z") . "\x05"), "select");
?>
--EXPECT--
W-4 data block: ClickHouseException: unsupported column type: Array(Null)
Array(Null): ClickHouseException: unsupported column type: Array(Null)
Array(DateTime64): ClickHouseException: unsupported column type: Array(DateTime64)
Array(5): ClickHouseException: unsupported column type: Array(5)
Array(Time64): ClickHouseException: unsupported column type: Array(Time64)
Array(Array(5)): ClickHouseException: unsupported column type: Array(Array(5))
Tuple(Array(5)): ClickHouseException: unsupported column type: Tuple(Array(5))
Map(String, Array(5)): ClickHouseException: unsupported column type: Map(String, Array(5))
Array(String) control: [{"c":["x","y"]},{"c":["z"]}]
