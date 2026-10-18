--TEST--
ClickHouse rejects a malformed FixedString width without caching it
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
$server = stream_socket_server("tcp://127.0.0.1:0", $errno, $error);
if (!$server) {
    fwrite(STDERR, "$errno: $error\n");
    exit(1);
}
$address = stream_socket_get_name($server, false);
fwrite(STDOUT, substr(strrchr($address, ":"), 1) . "\n");
fflush(STDOUT);
$hello = hex2bin("000a436c69636b486f7573651a09dca903035554430c3563666363653339323563630d");
$response = hex2bin($argv[1]);

function readByte($peer) {
    $data = fread($peer, 1);
    return $data === false || $data === "" ? false : ord($data);
}

$peer = stream_socket_accept($server, 5);
if ($peer === false) {
    fwrite(STDERR, "accept failed\n");
    exit(2);
}
if (readByte($peer) !== 0) exit(3);
fwrite($peer, $hello);
for ($i = 0; $i < 128; $i++) {
    $byte = readByte($peer);
    if ($byte === false) exit(4);
    if ($byte === 1) break;
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

function selectFromMock($file, $responseHex) {
    $process = proc_open([PHP_BINARY, "-n", "-f", $file, $responseHex], [
        0 => ["pipe", "r"],
        1 => ["pipe", "w"],
        2 => ["pipe", "w"],
    ], $pipes);
    if (!is_resource($process)) throw new RuntimeException("mock start failed");
    fclose($pipes[0]);
    $port = (int)trim((string)fgets($pipes[1]));
    if ($port < 1 || $port > 65535) throw new RuntimeException("mock did not publish a port");
    try {
        $client = new ClickHouse([
            "host" => "127.0.0.1",
            "port" => $port,
            "compression" => 0,
            "retry_count" => 0,
            "retry_timeout" => 0,
            "connect_timeout_ms" => 1000,
            "receive_timeout_ms" => 1000,
            "send_timeout_ms" => 1000,
        ]);
        $result = json_encode($client->select("SELECT c0 FROM t"));
    } catch (ClickHouseException $e) {
        $result = get_class($e) . ": " . $e->getMessage();
    }
    unset($client);
    fclose($pipes[1]);
    $stderr = stream_get_contents($pipes[2]);
    fclose($pipes[2]);
    if (proc_close($process) !== 0 || $stderr !== "") $result .= " (mock failed)";
    return $result;
}

/* pocs/wire/min/W-2b.bin: "FixedString(-)"; the identical query runs twice so a
 * cached partial parse from the first attempt would be reused by the second. */
$malformed = "0100010002ffffffff0001000263300e4669786564537472696e67282d290005";
echo selectFromMock($mockFile, $malformed), "\n";
echo selectFromMock($mockFile, $malformed), "\n";

$overflow = "FixedString(99999999999999999999)";
$overflowHex = "0100010002ffffffff00010002633021" . bin2hex($overflow) . "0005";
echo selectFromMock($mockFile, $overflowHex), "\n";
echo selectFromMock($mockFile, $overflowHex), "\n";

echo selectFromMock($mockFile, "0100010002ffffffff0001010263300f4669786564537472696e672831362900"
    . bin2hex("0123456789abcdef") . "05"), "\n";
?>
--EXPECT--
ClickHouseException: unsupported column type: FixedString(-)
ClickHouseException: unsupported column type: FixedString(-)
ClickHouseException: unsupported column type: FixedString(99999999999999999999)
ClickHouseException: unsupported column type: FixedString(99999999999999999999)
[{"c0":"0123456789abcdef"}]
