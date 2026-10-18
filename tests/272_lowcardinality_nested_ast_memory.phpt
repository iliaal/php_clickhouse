--TEST--
ClickHouse rejects deeply nested LowCardinality types without copying the type tree per level
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
if (!function_exists("proc_open")) print "skip proc_open unavailable";
elseif (PHP_OS_FAMILY !== "Linux") print "skip ru_maxrss is reported in KiB only on Linux";
?>
--INI--
memory_limit=64M
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

function varint($value) {
    $out = "";
    while ($value >= 0x80) {
        $out .= chr(($value & 0x7f) | 0x80);
        $value >>= 7;
    }
    return $out . chr($value);
}

function dataBlock($type) {
    return "\x01\x00\x01\x00\x02\xff\xff\xff\xff\x00\x01\x00\x02c0" . varint(strlen($type)) . $type . "\x00\x05";
}

function peakRssKiB() {
    $usage = getrusage();
    return (int)$usage["ru_maxrss"];
}

/* 255 LowCardinality(Nullable( levels over a 4000-element Tuple: copying the
 * subtree at every level would need about 150 MiB before the type is rejected. */
$type = str_repeat("LowCardinality(Nullable(", 255) . "Tuple(UInt8" . str_repeat(", UInt8", 3999) . ")"
    . str_repeat(")", 510);
$before = peakRssKiB();
$result = replay($mockFile, $responseFile, $hello, dataBlock($type), "none", $select);
$growth = peakRssKiB() - $before;
echo "nested: ", strpos($result, "exception:") === 0 ? "exception" : $result, "\n";
echo "peak-rss-growth-under-32MiB: ", $growth < 32 * 1024 ? "yes" : "no ($growth KiB)", "\n";

echo "control: ", replay($mockFile, $responseFile, $hello,
    dataBlock("LowCardinality(Nullable(String))"), "none", $select), "\n";
?>
--EXPECT--
nested: exception
peak-rss-growth-under-32MiB: yes
control: accepted:[]
