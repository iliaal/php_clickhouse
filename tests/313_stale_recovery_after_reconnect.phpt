--TEST--
A recovery reconnect that fails, followed by a reconnect in the same call, does not end the next writeStart() session
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
/* As in test 312, a relay stands in for the network and a fatal error
 * interrupts insert(). The shutdown function stops the relay, so the
 * recovery reconnect in writeStart() fails, and restarts it a second later.
 * With ping_before_query, writeStart() then reconnects through its retry;
 * the write() after it must continue that session, not recover the
 * connection again. Without ping_before_query, writeStart() reports the
 * closed connection and the next writeStart() reconnects. */
$relay = <<<'PHP'
$config = ["host" => $argv[1], "port" => $argv[3]];
if (isset($argv[4])) usleep((int)$argv[4]);
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
$ping = $argv[3] === "ping";
function relay_start($relayFile, $port, $delay = 0) {
    $config = clickhouse_test_config();
    $process = proc_open([PHP_BINARY, "-n", "-f", $relayFile, $config["host"], (string)$port, (string)$config["port"], (string)$delay],
        [1 => ["pipe", "w"], 2 => ["pipe", "w"]], $pipes);
    return [$process, $delay ? $port : (int)trim((string)fgets($pipes[1])), $pipes];
}
$relay = relay_start($relayFile, 0);
$port = $relay[1];
$c = new ClickHouse(["port" => $port, "host" => "127.0.0.1", "retry_count" => 1, "retry_timeout" => 2,
                     "ping_before_query" => $ping, "connect_timeout_ms" => 1000] + clickhouse_test_config());
register_shutdown_function(function () use ($c, &$relay, $relayFile, $port, $ping) {
    proc_terminate($relay[0], 9);
    proc_close($relay[0]);
    $relay = relay_start($relayFile, $port, 1000000);
    $table = "test.stale_recovery_313";
    $step = "writeStart";
    try {
        if (!$ping) {
            try {
                $c->writeStart($table, ["s"]);
                echo "writeStart before the relay is back: ok\n";
            } catch (ClickHouseException $e) {
                echo "writeStart before the relay is back: ", get_class($e), "\n";
            }
            usleep(1500000);
        }
        $c->writeStart($table, ["s"]);
        echo "writeStart: ok\n";
        $step = "write";
        $c->write([["w1"]]);
        echo "write: ok\n";
        $step = "writeEnd";
        $c->writeEnd();
        echo "writeEnd: ok\n";
    } catch (ClickHouseException $e) {
        echo "$step: ", $e->getMessage(), "\n";
    }
    try {
        echo "rows: ", $c->select("SELECT count() FROM $table", [], ClickHouse::FETCH_ONE), "\n";
    } catch (ClickHouseException $e) {
        echo "rows: ", $e->getMessage(), "\n";
    }
    proc_terminate($relay[0], 9);
    proc_close($relay[0]);
});
class Fatal313
{
    public function __toString(): string
    {
        eval('function clickhouse_313_dup() {} function clickhouse_313_dup() {}');
        return "x";
    }
}
$c->insert("test.stale_recovery_313", ["s"], [["a"], [new Fatal313()]]);
echo "unreachable\n";
PHP;
$dir = sys_get_temp_dir();
$relayFile = tempnam($dir, "clickhouse-313-relay-");
$childFile = tempnam($dir, "clickhouse-313-child-");
file_put_contents($relayFile, "<?php\n" . $relay);
file_put_contents($childFile, "<?php\n" . $child);
/* PHP 7.4 may build json, which the extension requires, as a shared module. */
$args = [getenv("TEST_PHP_EXECUTABLE") ?: PHP_BINARY, "-n"];
preg_match_all('~\s(/\S*/(json|clickhouse)\.so)$~m', file_get_contents("/proc/self/maps"), $m, PREG_SET_ORDER);
$loaded = array_column($m, 1, 2);
foreach (["json", "clickhouse"] as $name) {
    if (isset($loaded[$name])) array_push($args, "-d", "extension=" . $loaded[$name]);
}
$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
foreach (["ping", "default"] as $mode) {
    $c->execute("DROP TABLE IF EXISTS test.stale_recovery_313");
    $c->execute("CREATE TABLE test.stale_recovery_313 (s String) ENGINE = Memory");
    $proc = proc_open(array_merge($args, ["-d", "display_errors=1",
                       $childFile, __DIR__ . "/_clickhouse.inc", $relayFile, $mode]),
                      [1 => ["pipe", "w"], 2 => ["redirect", 1]], $pipes);
    $output = stream_get_contents($pipes[1]);
    fclose($pipes[1]);
    proc_close($proc);
    echo "$mode: ", preg_match('~Fatal error: Cannot redeclare~', $output) ? "fatal" : $output, "\n";
    preg_match_all('~^(writeStart|write|writeEnd|rows)[ :].*$~m', $output, $lines);
    echo implode("\n", $lines[0]), "\n";
}
unlink($relayFile);
unlink($childFile);
$c->execute("DROP TABLE IF EXISTS test.stale_recovery_313");
?>
--EXPECT--
ping: fatal
writeStart: ok
write: ok
writeEnd: ok
rows: 1
default: fatal
writeStart before the relay is back: ClickHouseException
writeStart: ok
write: ok
writeEnd: ok
rows: 1
