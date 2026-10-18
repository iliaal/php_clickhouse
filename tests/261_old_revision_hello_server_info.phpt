--TEST--
getServerInfo() reports zero for Hello fields an old-revision server omits
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

function showInfo($label, array $info) {
    printf("%s: name=%s revision=%d version=%d.%d.%d timezone=%s display_name=%s\n", $label,
        $info["name"], $info["revision"], $info["version_major"], $info["version_minor"],
        $info["version_patch"], var_export($info["timezone"], true), var_export($info["display_name"], true));
}

$oldHello = "000a436c69636b486f7573651a09dc05";
$newHello = "00024348020381a90303555443016407";

$mock = startMock($mockFile, [$oldHello]);
$client = new ClickHouse(mockConfig($mock[2]));
showInfo("old", $client->getServerInfo());
unset($client);
stopMock($mock);

$mock = startMock($mockFile, [$newHello, $oldHello]);
$client = new ClickHouse(mockConfig($mock[2]));
showInfo("new", $client->getServerInfo());
$client->resetConnection();
showInfo("reconnected-old", $client->getServerInfo());
unset($client);
stopMock($mock);
echo "done\n";
?>
--EXPECT--
old: name=ClickHouse revision=732 version=26.9.0 timezone='' display_name=''
new: name=CH revision=54401 version=2.3.7 timezone='UTC' display_name='d'
reconnected-old: name=ClickHouse revision=732 version=26.9.0 timezone='' display_name=''
done
