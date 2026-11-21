--TEST--
A reconnect that fails while recovering from a fatal error in insert() reports the connection error, not an insert in progress
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
require __DIR__ . "/_clickhouse.inc";
clickhouse_skip_if_no_server();
if (!function_exists("proc_open")) {
    print "skip proc_open unavailable";
} elseif (!is_readable("/proc/self/maps")) {
    print "skip /proc/self/maps is needed to locate the extension";
}
?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";
/* A TCP relay to the server stands in for the network: the client inserts
 * through it, a __toString() fatal error interrupts the insert, and the
 * first shutdown function stops the relay so that the recovery reconnect
 * fails. A relay on the same port then lets the next call reconnect. */
$relay = <<<'PHP'
$config = ["host" => $argv[1], "port" => $argv[3]];
$server = stream_socket_server("tcp://127.0.0.1:" . $argv[2], $errno, $error);
if (!$server) { fwrite(STDERR, "$errno: $error\n"); exit(1); }
echo substr(strrchr(stream_socket_get_name($server, false), ":"), 1), "\n";
fflush(STDOUT);
$pairs = [];
for (;;) {
    $read = [$server];
    foreach ($pairs as $pair) { $read[] = $pair[0]; $read[] = $pair[1]; }
    $write = $except = null;
    if (stream_select($read, $write, $except, 30) < 1) exit(0);
    foreach ($read as $socket) {
        if ($socket === $server) {
            $peer = stream_socket_accept($server);
            $upstream = stream_socket_client("tcp://" . $config["host"] . ":" . $config["port"]);
            $pairs[] = [$peer, $upstream];
            continue;
        }
        foreach ($pairs as $key => $pair) {
            if ($socket !== $pair[0] && $socket !== $pair[1]) continue;
            $data = fread($socket, 65536);
            if ($data === "" || $data === false) {
                fclose($pair[0]);
                fclose($pair[1]);
                unset($pairs[$key]);
            } else {
                fwrite($socket === $pair[0] ? $pair[1] : $pair[0], $data);
            }
        }
    }
}
PHP;
$child = <<<'PHP'
require $argv[1];
$relayFile = $argv[2];
function relay_start($relayFile, $port) {
    $config = clickhouse_test_config();
    $process = proc_open([PHP_BINARY, "-n", "-f", $relayFile, $config["host"], (string)$port, (string)$config["port"]],
        [1 => ["pipe", "w"], 2 => ["pipe", "w"]], $pipes);
    return [$process, (int)trim((string)fgets($pipes[1])), $pipes];
}
list($relay, $port) = relay_start($relayFile, 0);
$c = new ClickHouse(["port" => $port, "host" => "127.0.0.1", "retry_count" => 0, "retry_timeout" => 0,
                     "connect_timeout_ms" => 1000] + clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("CREATE TABLE IF NOT EXISTS test.failed_reconnect_312 (s String) ENGINE = Memory");
register_shutdown_function(function () use ($relay) {
    proc_terminate($relay, 9);
    proc_close($relay);
});
register_shutdown_function(function () use ($c) {
    try {
        $result = json_encode($c->select("SELECT 1 AS x"));
    } catch (ClickHouseException $e) {
        $result = strpos($e->getMessage(), "insert operation is now in progress") !== false
            ? $e->getMessage() : "connection error";
    }
    echo "while the relay is down: ", $result, "\n";
});
register_shutdown_function(function () use ($c, $relayFile, $port) {
    list($relay) = relay_start($relayFile, $port);
    try {
        $result = json_encode($c->select("SELECT 1 AS x"));
    } catch (ClickHouseException $e) {
        $result = $e->getMessage();
    }
    echo "after the relay is back: ", $result, "\n";
    proc_terminate($relay, 9);
    proc_close($relay);
});
class Fatal312
{
    public function __toString(): string
    {
        eval('function clickhouse_312_dup() {} function clickhouse_312_dup() {}');
        return "x";
    }
}
$c->insert("test.failed_reconnect_312", ["s"], [["a"], [new Fatal312()]]);
echo "unreachable\n";
PHP;
$dir = sys_get_temp_dir();
$relayFile = tempnam($dir, "clickhouse-312-relay-");
$childFile = tempnam($dir, "clickhouse-312-child-");
file_put_contents($relayFile, "<?php\n" . $relay);
file_put_contents($childFile, "<?php\n" . $child);
/* PHP 7.4 may build json, which the extension requires, as a shared module. */
$args = [getenv("TEST_PHP_EXECUTABLE") ?: PHP_BINARY, "-n"];
preg_match_all('~\s(/\S*/(json|clickhouse)\.so)$~m', file_get_contents("/proc/self/maps"), $m, PREG_SET_ORDER);
$loaded = array_column($m, 1, 2);
foreach (["json", "clickhouse"] as $name) {
    if (isset($loaded[$name])) array_push($args, "-d", "extension=" . $loaded[$name]);
}
$proc = proc_open(array_merge($args, ["-d", "display_errors=1",
                   $childFile, __DIR__ . "/_clickhouse.inc", $relayFile]),
                  [1 => ["pipe", "w"], 2 => ["redirect", 1]], $pipes);
$output = stream_get_contents($pipes[1]);
fclose($pipes[1]);
proc_close($proc);
echo preg_match('~Fatal error: Cannot redeclare~', $output) ? "fatal: Cannot redeclare" : $output, "\n";
preg_match_all('~^(while|after) the relay .*$~m', $output, $lines);
echo implode("\n", $lines[0]), "\n";
unlink($relayFile);
unlink($childFile);
$c = new ClickHouse(clickhouse_test_config());
$c->execute("DROP TABLE IF EXISTS test.failed_reconnect_312");
?>
--EXPECT--
fatal: Cannot redeclare
while the relay is down: connection error
after the relay is back: [{"x":1}]
