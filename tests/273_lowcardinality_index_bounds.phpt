--TEST--
LowCardinality dictionary indexes past the loaded dictionary are rejected
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

function response($type, $body, $rows) {
    $block = function ($rows, $data) use ($type) {
        return "\x01\x00\x01\x00\x02\xff\xff\xff\xff\x00\x01" . chr($rows) .
            "\x02c0" . chr(strlen($type)) . $type . "\x00" . $data;
    };
    return bin2hex($block(0, "") . $block($rows, $body) . "\x05");
}

/* Key version, index type (0 UInt8 .. 3 UInt64) with HasAdditionalKeys,
 * dictionary, row count, indexes already packed at that width. */
function lcBody($keys, $dictionary, array $indexes, $indexType = 3) {
    $out = wire_u64(1, 0x200 | $indexType, $keys) . $dictionary . wire_u64(count($indexes));
    foreach ($indexes as $index) $out .= $index;
    return $out;
}

$dict = str_repeat("A", 1001) . str_repeat("B", 1001);
$fs = "LowCardinality(FixedString(1001))";
$cases = [
    /* 1001 * index wraps modulo 2^64 to byte 1500 of the 2002-byte dictionary. */
    "fixedstring-wrapping-index" => [response($fs, lcBody(2, $dict, [wire_u64(1), hex2bin("7c79ddee4a2306a8")]), 2), ClickHouse::FIXEDSTRING_BINARY],
    "fixedstring-index-past-end" => [response($fs, lcBody(2, $dict, [wire_u64(1), wire_u64(2)]), 2), 0],
    "fixedstring-uint8-index-past-end" => [response($fs, lcBody(2, $dict, ["\x01", "\x02"], 0), 2), 0],
    "string-index-past-end" => [response("LowCardinality(String)", lcBody(2, "\x00\x03foo", [wire_u64(1), wire_u64(5)]), 2), 0],
    "control-fixedstring" => [response($fs, lcBody(2, $dict, [wire_u64(1), wire_u64(0)]), 2), ClickHouse::FIXEDSTRING_BINARY],
    "control-fixedstring-uint8" => [response($fs, lcBody(2, $dict, ["\x01", "\x00"], 0), 2), 0],
    "control-nullable-fixedstring" => [response("LowCardinality(Nullable(FixedString(3)))", lcBody(2, "\0\0\0xyz", [wire_u64(0), wire_u64(1)]), 2), 0],
];

foreach ($cases as $name => $case) {
    list($responseHex, $flags) = $case;
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
        $cells = [];
        foreach ($rows as $row) {
            $cells[] = $row["c0"] === null ? "null" : strlen($row["c0"]) . ":" . substr($row["c0"], 0, 3);
        }
        echo "$name=rows:", implode(",", $cells), "\n";
    } catch (ClickHouseException $e) {
        $expected = strpos($e->getMessage(), "can't load column 'c0'") !== false;
        echo "$name=", get_class($e), $expected ? "" : ":unexpected-message:" . $e->getMessage(), "\n";
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
fixedstring-wrapping-index=ClickHouseException
fixedstring-index-past-end=ClickHouseException
fixedstring-uint8-index-past-end=ClickHouseException
string-index-past-end=ClickHouseException
control-fixedstring=rows:1001:BBB,1001:AAA
control-fixedstring-uint8=rows:1001:BBB,1001:AAA
control-nullable-fixedstring=rows:null,3:xyz
