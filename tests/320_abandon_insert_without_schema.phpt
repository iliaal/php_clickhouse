--TEST--
Destroying a client whose INSERT never received its schema does not finalize it
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
if (!function_exists("proc_open")) print "skip proc_open unavailable";
elseif (!is_readable("/proc/self/maps")) print "skip needs /proc";
?>
--FILE--
<?php
require __DIR__ . "/_wire_mock.inc";
/* The peer answers the INSERT with a Progress packet. The child's progress
 * callback raises a fatal error there, before the schema arrives, and the
 * client is destroyed at shutdown. Before the fix it then sent the INSERT's
 * end-of-data block to a server that had not accepted the INSERT. */
$mockCode = <<<'PHP'
$server = stream_socket_server("tcp://127.0.0.1:0", $errno, $error);
if (!$server) {
    fwrite(STDERR, "$errno: $error\n");
    exit(1);
}
$address = stream_socket_get_name($server, false);
fwrite(STDOUT, substr(strrchr($address, ":"), 1) . "\n");
fflush(STDOUT);
function idle($peer, $wait) {
    $got = "";
    for (;;) {
        $read = [$peer];
        $write = $except = null;
        if (@stream_select($read, $write, $except, 0, $got === "" ? $wait : 300000) < 1) return $got;
        $chunk = fread($peer, 65536);
        if ($chunk === false || $chunk === "") return $got;
        $got .= $chunk;
    }
}
$peer = stream_socket_accept($server, 10);
if ($peer === false) exit(2);
idle($peer, 5000000);
fwrite($peer, hex2bin($argv[1]));
idle($peer, 5000000);
fwrite($peer, "\x03\x00\x00\x00\x00\x00");
$after = idle($peer, 5000000);
echo $after === "" ? "client sent nothing more" : "client sent " . strlen($after) . " more bytes, starting " . bin2hex($after[0]), "\n";
fclose($peer);
fclose($server);
PHP;
$childCode = <<<'PHP'
$client = new ClickHouse([
    "host" => "127.0.0.1", "port" => (int)$argv[1], "compression" => 0, "retry_count" => 0,
    "connect_timeout_ms" => 2000, "receive_timeout_ms" => 5000, "send_timeout_ms" => 2000,
]);
$client->setProgressCallback(function () {
    eval('function clickhouse_320_dup() {} function clickhouse_320_dup() {}');
});
$client->writeStart("t", ["a"]);
echo "unreachable\n";
PHP;
$dir = sys_get_temp_dir();
$mockFile = tempnam($dir, "clickhouse-mock-");
$childFile = tempnam($dir, "clickhouse-child-");
file_put_contents($mockFile, "<?php\n" . $mockCode);
file_put_contents($childFile, "<?php\n" . $childCode);
register_shutdown_function(function () use ($mockFile, $childFile) {
    @unlink($mockFile);
    @unlink($childFile);
});

$mock = proc_open([PHP_BINARY, "-n", "-f", $mockFile, wire_default_hello_hex()],
    [0 => ["pipe", "r"], 1 => ["pipe", "w"], 2 => ["pipe", "w"]], $mockPipes);
fclose($mockPipes[0]);
$port = (int)trim((string)fgets($mockPipes[1]));
$child = proc_open(array_merge(wire_child_php(["display_errors" => "1"]), [$childFile, (string)$port]),
    [0 => ["pipe", "r"], 1 => ["pipe", "w"], 2 => ["redirect", 1]], $childPipes);
fclose($childPipes[0]);
$output = stream_get_contents($childPipes[1]);
fclose($childPipes[1]);
$status = proc_close($child);
echo "child: exit $status, ", strpos($output, "Cannot redeclare") !== false ? "fatal error in the callback" : trim($output), "\n";
echo "peer: ", trim(stream_get_contents($mockPipes[1])), "\n";
fclose($mockPipes[1]);
$stderr = stream_get_contents($mockPipes[2]);
fclose($mockPipes[2]);
if (proc_close($mock) !== 0 || $stderr !== "") echo "mock failed: ", trim($stderr), "\n";
?>
--EXPECT--
child: exit 255, fatal error in the callback
peer: client sent nothing more
