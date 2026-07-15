--TEST--
resetConnection recovers after retry exhausts and clears the current endpoint
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
require __DIR__ . "/_clickhouse.inc";
clickhouse_skip_if_no_server();
if (!function_exists("proc_open")) print "skip proc_open unavailable";
?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$proxyScript = tempnam(sys_get_temp_dir(), "clickhouse-proxy-");
file_put_contents($proxyScript, <<<'PROXY'
<?php
$listenPort = isset($argv[1]) ? (int)$argv[1] : 0;
$upstreamHost = isset($argv[2]) ? $argv[2] : "127.0.0.1";
$upstreamPort = isset($argv[3]) ? (int)$argv[3] : 9000;
$server = stream_socket_server(
    "tcp://127.0.0.1:" . $listenPort,
    $errno,
    $error,
    STREAM_SERVER_BIND | STREAM_SERVER_LISTEN
);
if (!$server) {
    fwrite(STDERR, $error . "\n");
    exit(1);
}
stream_set_blocking($server, false);
$address = stream_socket_get_name($server, false);
$separator = strrpos($address, ":");
fwrite(STDOUT, substr($address, $separator + 1) . "\n");
fflush(STDOUT);
$pairs = [];
while (true) {
    $read = [$server];
    foreach ($pairs as $pair) {
        $read[] = $pair[0];
        $read[] = $pair[1];
    }
    $write = null;
    $except = null;
    if (@stream_select($read, $write, $except, null) === false) {
        break;
    }
    foreach ($read as $ready) {
        if ($ready === $server) {
            $downstream = @stream_socket_accept($server, 0);
            if (!$downstream) continue;
            $upstream = @stream_socket_client(
                "tcp://" . $upstreamHost . ":" . $upstreamPort,
                $connectErrno,
                $connectError,
                2
            );
            if (!$upstream) {
                fclose($downstream);
                continue;
            }
            stream_set_blocking($downstream, false);
            stream_set_blocking($upstream, false);
            $pairs[] = [$downstream, $upstream];
            continue;
        }
        foreach ($pairs as $index => $pair) {
            if ($ready !== $pair[0] && $ready !== $pair[1]) continue;
            $peer = $ready === $pair[0] ? $pair[1] : $pair[0];
            $data = @fread($ready, 65536);
            if ($data === "") {
                if (feof($ready)) {
                    fclose($pair[0]);
                    fclose($pair[1]);
                    unset($pairs[$index]);
                }
                continue 2;
            }
            $offset = 0;
            $length = strlen($data);
            while ($offset < $length) {
                $written = @fwrite($peer, substr($data, $offset));
                if ($written === false || $written === 0) {
                    fclose($pair[0]);
                    fclose($pair[1]);
                    unset($pairs[$index]);
                    continue 4;
                }
                $offset += $written;
            }
            continue 2;
        }
    }
}
PROXY
);

function startProxy($script, $port, $upstreamHost, $upstreamPort) {
    $command = "exec " . escapeshellarg(PHP_BINARY) . " " .
        escapeshellarg($script) . " " .
        (int)$port . " " . escapeshellarg($upstreamHost) . " " . (int)$upstreamPort;
    $pipes = [];
    $process = proc_open($command, [
        0 => ["pipe", "r"],
        1 => ["pipe", "w"],
        2 => ["pipe", "w"],
    ], $pipes);
    if (!is_resource($process)) {
        throw new RuntimeException("failed to start TCP proxy");
    }
    $line = fgets($pipes[1]);
    if ($line === false) {
        $error = stream_get_contents($pipes[2]);
        throw new RuntimeException("TCP proxy failed: " . $error);
    }
    $actualPort = (int)trim($line);
    if ($actualPort < 1 || $actualPort > 65535) {
        throw new RuntimeException("TCP proxy returned an invalid port");
    }
    return [$process, $pipes, $actualPort];
}

function stopProxy($process, $pipes) {
    proc_terminate($process);
    foreach ($pipes as $pipe) fclose($pipe);
    proc_close($process);
}

$server = clickhouse_test_config();
list($proxy, $proxyPipes, $proxyPort) = startProxy(
    $proxyScript, 0, $server["host"], $server["port"]
);

$config = $server;
$config["host"] = "127.0.0.1";
$config["port"] = $proxyPort;
$config["connect_timeout_ms"] = 250;
$config["send_retries"] = 0;
$config["retry_timeout"] = 0;
$c = new ClickHouse($config);

stopProxy($proxy, $proxyPipes);
try {
    $c->select("SELECT 1");
    echo "outage=accepted\n";
} catch (Throwable $e) {
    echo "outage=rejected\n";
}

list($proxy, $proxyPipes) = startProxy(
    $proxyScript, $proxyPort,
    $server["host"],
    $server["port"]
);
try {
    $c->resetConnection();
    echo "reset=", $c->ping() ? "ok" : "fail", "\n";
} catch (Throwable $e) {
    echo "reset=", get_class($e), ":", $e->getMessage(), "\n";
}
stopProxy($proxy, $proxyPipes);
unlink($proxyScript);
?>
--EXPECT--
outage=rejected
reset=ok
