--TEST--
receive_timeout bounds an operation's total wait, and abandoning an insert waits at most five seconds
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
if (!function_exists("proc_open")) print "skip proc_open unavailable";
?>
--FILE--
<?php
require __DIR__ . "/_wire_mock.inc";
$defaults = (new ReflectionClass("ClickHouse"))->getDefaultProperties();
echo "default receive_timeout: ", $defaults["receive_timeout"], "\n";

/* The peer answers the first request after the hello with $first, then sends
 * $count copies of $packet $interval microseconds apart and EndOfStream.
 * Each gap is shorter than the client's timeout; their sum is longer. */
$mockCode = <<<'PHP'
list(, $hello, $first, $packet, $count, $interval) = $argv;
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
        if (@stream_select($read, $write, $except, 0, $got === "" ? $wait : 100000) < 1) return $got;
        $chunk = fread($peer, 65536);
        if ($chunk === false || $chunk === "") return $got;
        $got .= $chunk;
    }
}
$peer = stream_socket_accept($server, 10);
if ($peer === false) exit(2);
idle($peer, 5000000);
fwrite($peer, hex2bin($hello));
idle($peer, 5000000);
if ($first !== "") fwrite($peer, hex2bin($first));
if ($first !== "") idle($peer, 5000000);
for ($i = 0; $i < (int)$count; $i++) {
    if (@fwrite($peer, hex2bin($packet)) === false) break;
    usleep((int)$interval);
}
@fwrite($peer, "\x05");
idle($peer, 2000000);
fclose($peer);
fclose($server);
PHP;
$mockFile = tempnam(sys_get_temp_dir(), "clickhouse-mock-");
file_put_contents($mockFile, "<?php\n" . $mockCode);
register_shutdown_function(function () use ($mockFile) {
    @unlink($mockFile);
});

function trickle(array $args, array $config, callable $op) {
    global $mockFile;
    $process = proc_open(array_merge([PHP_BINARY, "-n", "-f", $mockFile, wire_default_hello_hex()], $args),
        [0 => ["pipe", "r"], 1 => ["pipe", "w"], 2 => ["pipe", "w"]], $pipes);
    $port = (int)trim((string)fgets($pipes[1]));
    $started = microtime(true);
    try {
        $result = $op(new ClickHouse($config + wire_mock_config($port)));
    } catch (ClickHouseException $e) {
        $result = "exception " . $e->getMessage();
    }
    $elapsed = microtime(true) - $started;
    fclose($pipes[0]);
    fclose($pipes[1]);
    $stderr = stream_get_contents($pipes[2]);
    fclose($pipes[2]);
    $status = proc_close($process);
    if ($status !== 0 || $stderr !== "") $result .= " mock-failed:$status:" . trim($stderr);
    return [$result, $elapsed];
}

$progress = "030000000000";
/* Twelve Progress packets 250 ms apart against a one-second timeout. */
list($result) = trickle(["", $progress, "12", "250000"], ["receive_timeout_ms" => 1000],
    function ($client) {
        return json_encode($client->select("SELECT 1"));
    });
echo "trickled select: $result\n";

/* writeStart() gets its schema; destroying the client then finalizes the
 * INSERT while the peer sends a Progress packet every second for ten
 * seconds. The configured timeout is unlimited. */
$schema = bin2hex(wire_data_block([["a", "UInt8", ""]], 0));
list($result, $elapsed) = trickle([$schema, $progress, "10", "1000000"], ["receive_timeout_ms" => 0],
    function ($client) {
        $client->writeStart("t", ["a"]);
        $started = microtime(true);
        unset($client);
        return microtime(true) - $started < 8 ? "abandoned within the cap" : "abandon outlived the cap";
    });
echo "abandoned insert: $result\n";
?>
--EXPECT--
default receive_timeout: 300
trickled select: exception read timed out
abandoned insert: abandoned within the cap
