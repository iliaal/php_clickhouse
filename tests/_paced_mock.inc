<?php
/* Scripted peer that paces its reply. After the hello it answers the first
 * request with $first and waits for the client's next bytes when $first is
 * not empty. It then sends $packet $count times, $interval microseconds apart,
 * or with $split each byte of $packet that far apart, and finally $tail. It
 * stops early once the client hangs up. */
require_once __DIR__ . "/_wire_mock.inc";

function paced_mock_code(): string
{
    return <<<'PHP'
list(, $hello, $first, $packet, $count, $interval, $split, $tail) = $argv;
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
if ($first !== "") {
    fwrite($peer, hex2bin($first));
    idle($peer, 5000000);
}
$pieces = $split ? str_split(hex2bin($packet)) : array_fill(0, (int)$count, hex2bin($packet));
foreach ($pieces as $piece) {
    if (@fwrite($peer, $piece) === false) break;
    $read = [$peer];
    $write = $except = null;
    if (@stream_select($read, $write, $except, 0, (int)$interval) > 0 && feof($peer)) break;
}
@fwrite($peer, hex2bin($tail));
idle($peer, 2000000);
fclose($peer);
fclose($server);
PHP;
}

/* Runs $op on a client of the paced peer; returns [result, seconds $op took]. */
function paced_run(array $args, array $config, callable $op): array
{
    static $file = null;
    if ($file === null) {
        $file = tempnam(sys_get_temp_dir(), "clickhouse-paced-mock-");
        file_put_contents($file, "<?php\n" . paced_mock_code());
        register_shutdown_function(function () use ($file) {
            @unlink($file);
        });
    }
    $args += ["first" => "", "packet" => "", "count" => 0, "interval" => 0, "split" => false, "tail" => "05"];
    $process = proc_open([PHP_BINARY, "-n", "-f", $file, wire_default_hello_hex(), $args["first"], $args["packet"],
        (string)$args["count"], (string)$args["interval"], $args["split"] ? "1" : "", $args["tail"]],
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
