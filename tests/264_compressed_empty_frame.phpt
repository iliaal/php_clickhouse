--TEST--
Compressed frames declaring an original size of 0 decode cleanly
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
$peers = [];
foreach (array_slice($argv, 1) as $reply) {
    $peer = stream_socket_accept($server, 5);
    if ($peer === false) {
        fwrite(STDERR, "accept failed\n");
        exit(2);
    }
    /* The client hello starts with packet type 0; its remaining bytes stay
     * buffered and are drained below. */
    if (fread($peer, 1) !== "\x00") {
        fwrite(STDERR, "no client hello\n");
        exit(3);
    }
    fwrite($peer, hex2bin($reply));
    stream_socket_shutdown($peer, STREAM_SHUT_WR);
    $peers[] = $peer;
}
foreach ($peers as $peer) {
    stream_set_timeout($peer, 2);
    while (!feof($peer)) {
        $chunk = fread($peer, 8192);
        if ($chunk === false || $chunk === "") break;
    }
    fclose($peer);
}
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

function startMock($file, array $replies) {
    $command = array_merge([PHP_BINARY, "-n", "-f", $file], $replies);
    $process = proc_open($command, [
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
        throw new RuntimeException("mock did not publish a valid port: " . stream_get_contents($pipes[2]));
    }
    return [$process, $pipes, $port];
}

function stopMock(array $mock) {
    list($process, $pipes) = $mock;
    fclose($pipes[1]);
    $stderr = stream_get_contents($pipes[2]);
    fclose($pipes[2]);
    $status = proc_close($process);
    if ($status !== 0 || $stderr !== "") echo "mock-failed:$status:", trim($stderr), "\n";
}

function mockConfig($port, $compression = 0) {
    return [
        "host" => "127.0.0.1",
        "port" => $port,
        "compression" => $compression,
        "retry_count" => 0,
        "retry_timeout" => 0,
        "connect_timeout_ms" => 1000,
        "receive_timeout_ms" => 1000,
        "send_timeout_ms" => 1000,
    ];
}

$hello = "000a436c69636b486f7573651a09dca903035554430c3563666363653339323563630d";
/* Data packet whose LZ4 frame declares an original size of 0 but carries a
 * 21-byte literal run. */
$emptyWithPayload = "01006af409e7bea8626d9cbe04a233a8d506822000000000000000f006010002ffffffff00010001640744796e616d696300";
/* LZ4 frame holding one block: UInt8 column "c0" with the single value 42. */
$dataFrame = "ef9bb5a6cbee933fa46332429aaa790e822000000015000000f006010002ffffffff0001010263300555496e7438002a";
$emptyLz4 = "12b83a602bd7de6df44915041a40db2b820a0000000000000000";
$emptyZstd = "5f66888d3eecd757a15a751936df6f8690120000000000000028b52ffd2000010000";

$cases = [
    "empty-with-payload" => $emptyWithPayload,
    "data" => "0100" . $dataFrame . "05",
    "empty-lz4-then-data" => "0100" . $emptyLz4 . $dataFrame . "05",
    "empty-zstd-then-data" => "0100" . $emptyZstd . $dataFrame . "05",
];
foreach ($cases as $name => $reply) {
    $mock = startMock($mockFile, [$hello . $reply]);
    $client = new ClickHouse(mockConfig($mock[2], "lz4"));
    try {
        $result = json_encode($client->select("SELECT c0 FROM t"));
    } catch (ClickHouseException $e) {
        $result = get_class($e) . " " . $e->getMessage();
    }
    echo "$name: $result\n";
    unset($client);
    stopMock($mock);
}
echo "done\n";
?>
--EXPECT--
empty-with-payload: ClickHouseException can't decompress LZ4-encoded data
data: [{"c0":42}]
empty-lz4-then-data: [{"c0":42}]
empty-zstd-then-data: [{"c0":42}]
done
