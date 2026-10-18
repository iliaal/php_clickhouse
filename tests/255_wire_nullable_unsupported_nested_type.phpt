--TEST--
ClickHouse rejects server-sent Nullable types whose nested type cannot be created
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

/* Minimized W-3 response: a ProfileEvents block with an Array(Nullable(5)) column. */
runCase($mockFile, "W-3 profile events",
    "000a436c69636b486f7573651a09dca903035554430c3563666363653339323563630d",
    "0100010002ffffffff000200066e756d6265720655496e74363400017306537472696e6700060c04020000"
    . "01030c610c00000e00010002ffffffff00062609686f73745f6e616d65124172726179284e756c6c61626c"
    . "652835292900",
    "selectStatement");

foreach ([
    "Nullable(5)",
    "Nullable(DateTime64)",
    "Nullable(Time64)",
    "Array(Nullable(5))",
    "LowCardinality(Nullable(5))",
    "LowCardinality(Nullable(DateTime64))",
] as $type) {
    runCase($mockFile, $type, $hello, bin2hex(dataBlock($type, 1, "\x00\x07") . "\x05"), "select");
}

runCase($mockFile, "Nullable(UInt8) control", $hello,
    bin2hex(dataBlock("Nullable(UInt8)", 2, "\x00\x01" . "\x2a\x00") . "\x05"), "select");
?>
--EXPECT--
W-3 profile events: ClickHouseException: unsupported column type: Array(Nullable(5))
Nullable(5): ClickHouseException: unsupported column type: Nullable(5)
Nullable(DateTime64): ClickHouseException: unsupported column type: Nullable(DateTime64)
Nullable(Time64): ClickHouseException: unsupported column type: Nullable(Time64)
Array(Nullable(5)): ClickHouseException: unsupported column type: Array(Nullable(5))
LowCardinality(Nullable(5)): ClickHouseException: LowCardinality(Nullable()) is not supported
LowCardinality(Nullable(DateTime64)): ClickHouseException: LowCardinality(Nullable(DateTime64)) is not supported
Nullable(UInt8) control: [{"c":42},{"c":null}]
