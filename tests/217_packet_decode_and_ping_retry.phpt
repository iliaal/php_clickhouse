--TEST--
ClickHouse rejects malformed native packets and bounds automatic ping recovery
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
$mode = $argv[1];
$server = stream_socket_server("tcp://127.0.0.1:0", $errno, $error);
if (!$server) {
    fwrite(STDERR, "$errno: $error\n");
    exit(1);
}
$address = stream_socket_get_name($server, false);
fwrite(STDOUT, substr(strrchr($address, ":"), 1) . "\n");
fflush(STDOUT);
$hello = hex2bin("00044d6f636b0101bba90303555443044d6f636b00");
$data = hex2bin("01000100020000000000010101780555496e74380007");
$overlong = str_repeat("\x80", 10);

function readByte($peer) {
    $data = fread($peer, 1);
    return $data === false || $data === "" ? false : ord($data);
}

function readVarint($peer) {
    $value = 0;
    for ($i = 0; $i < 10; $i++) {
        $byte = readByte($peer);
        if ($byte === false) return false;
        $value |= ($byte & 0x7f) << (7 * $i);
        if (!($byte & 0x80)) return $value;
    }
    return false;
}

function readString($peer) {
    $length = readVarint($peer);
    if ($length === false) return false;
    $value = "";
    while (strlen($value) < $length) {
        $part = fread($peer, $length - strlen($value));
        if ($part === false || $part === "") return false;
        $value .= $part;
    }
    return $value;
}

function handshake($peer, $hello) {
    /* The client hello starts with packet type 0. The remaining fields are
     * buffered by the kernel and need not be consumed before Hello. */
    if (readByte($peer) !== 0) return false;
    fwrite($peer, $hello);
    return true;
}

function readClientCode($peer) {
    for ($i = 0; $i < 128; $i++) {
        $byte = readByte($peer);
        if ($byte === false) return false;
        if ($byte === 1 || $byte === 4) return $byte;
    }
    return false;
}

if (strpos($mode, "packet:") === 0) {
    $packet = substr($mode, 7);
    $peer = stream_socket_accept($server, 5);
    if ($peer === false) {
        fwrite(STDERR, "accept failed\n");
        exit(2);
    }
    if (!handshake($peer, $hello)) {
        fwrite(STDERR, "handshake failed\n");
        exit(2);
    }
    if ($packet === "initial") {
        fwrite($peer, $overlong);
        usleep(200000);
    } elseif ($packet === "progress") {
        fwrite($peer, $data . "\x03" . $overlong);
        usleep(200000);
    } elseif ($packet === "profile") {
        fwrite($peer, $data . "\x06" . $overlong);
        usleep(200000);
    } elseif ($packet === "log") {
        fwrite($peer, $data . "\x0a" . $overlong);
        usleep(200000);
    } elseif ($packet === "columns") {
        fwrite($peer, $data . "\x0b" . $overlong);
        usleep(200000);
    } elseif ($packet === "events") {
        fwrite($peer, $data . "\x0e" . $overlong);
        usleep(200000);
    } elseif ($packet === "exception") {
        fwrite($peer, "\x02\x01\x00\x00\x00" . $overlong);
        usleep(200000);
    } elseif ($packet === "eos") {
        fwrite($peer, $data . "\x05");
    } elseif ($packet === "truncated") {
        fwrite($peer, $data . "\x03\x01");
    } else {
        exit(4);
    }
    fclose($peer);
    fclose($server);
    exit(0);
}

$retryCount = (int)$argv[2];
$healthy = isset($argv[3]) && $argv[3] === "healthy";
$limit = $healthy ? 1 : ($retryCount > 0 ? 3 : 1);
$pings = 0;
$queries = 0;
$deadline = microtime(true) + 1.0;
stream_set_blocking($server, false);
while ($pings < $limit && microtime(true) < $deadline) {
    $read = [$server];
    $write = null;
    $except = null;
    if (@stream_select($read, $write, $except, 0, 200000) < 1) continue;
    $peer = @stream_socket_accept($server);
    if ($peer === false) continue;
    if (!handshake($peer, $hello)) exit(5);
    $code = readClientCode($peer);
    if ($code === 4) {
        $pings++;
        if ($healthy || ($pings === $limit && $retryCount > 0)) {
            fwrite($peer, "\x04");
            $code = readClientCode($peer);
            if ($code === 1) {
                $queries++;
                fwrite($peer, "\x05");
            }
        } else {
            fwrite($peer, "\x05");
        }
    } elseif ($code === 1) {
        $queries++;
        fwrite($peer, "\x05");
    }
    if ($retryCount > 0 && !$healthy && $pings >= 2) break;
    if ($retryCount === 0) {
        stream_set_timeout($peer, 0, 200000);
        $code = fread($peer, 1);
        if ($code === "\x01") {
            $queries++;
            fwrite($peer, "\x05");
        }
    }
    fclose($peer);
    if ($retryCount === 0) break;
}
fclose($server);
fwrite(STDERR, "pings=$pings queries=$queries\n");
PHP;

function startMock($code, $mode, $arg = null, $variant = null) {
    $command = escapeshellarg(PHP_BINARY) . " -n -r " . escapeshellarg($code) .
        " " . escapeshellarg($mode);
    if ($arg !== null) $command .= " " . (int)$arg;
    if (isset($variant)) $command .= " " . escapeshellarg($variant);
    $process = proc_open($command, [
        0 => ["pipe", "r"],
        1 => ["pipe", "w"],
        2 => ["pipe", "w"],
    ], $pipes);
    if (!is_resource($process)) throw new RuntimeException("mock start failed");
    fclose($pipes[0]);
    $line = fgets($pipes[1]);
    if ($line === false) throw new RuntimeException("mock did not publish a port");
    $port = (int)trim($line);
    if ($port < 1 || $port > 65535) throw new RuntimeException("mock published an invalid port");
    return [$process, $pipes, $port];
}

function configForPort($port) {
    return [
        "host" => "127.0.0.1",
        "port" => $port,
        "compression" => 0,
        "retry_count" => 0,
        "retry_timeout" => 0,
        "connect_timeout_ms" => 250,
        "receive_timeout_ms" => 250,
        "send_timeout_ms" => 250,
    ];
}

foreach (["initial", "progress", "profile", "log", "columns", "events", "exception", "eos", "truncated"] as $packet) {
    list($process, $pipes, $port) = startMock($mockCode, "packet:$packet");
    try {
        $client = new ClickHouse(configForPort($port));
    } catch (ClickHouseException $e) {
        fclose($pipes[1]);
        $mockError = trim(stream_get_contents($pipes[2]));
        fclose($pipes[2]);
        $status = proc_close($process);
        echo "$packet=mock-connect-failed:$mockError:$status\n";
        continue;
    }
    $serverExceptionEvents = 0;
    if ($packet === "exception") {
        $client->setVerbose(function ($event, $context) use (&$serverExceptionEvents) {
            if ($event === "server_exception") $serverExceptionEvents++;
        });
    }
    try {
        $rows = $client->select("SELECT 1");
        echo "$packet=", count($rows) === ($packet === "eos" ? 1 : 0) ? "accepted" : "wrong", "\n";
    } catch (ClickHouseException $e) {
        $expectedProtocolError = $packet !== "exception" ||
            (get_class($e) === "ClickHouseException" &&
             strpos($e->getMessage(), "exception packet could not be decoded") !== false);
        echo "$packet=", $expectedProtocolError ? "rejected" : "wrong", "\n";
    }
    if ($packet === "exception") echo "exception-server-events=$serverExceptionEvents\n";
    fclose($pipes[1]);
    $stderr = stream_get_contents($pipes[2]);
    fclose($pipes[2]);
    $status = proc_close($process);
    if ($status !== 0 || $stderr !== "") echo "$packet=mock-failed\n";
}

foreach ([
    [1, 2, 0],
    [0, 1, 0],
    [1, 1, 1],
] as $case) {
    list($retryCount, $expectedPings, $expectedQueries) = $case;
    list($process, $pipes, $port) = startMock(
        $mockCode, "ping", $retryCount, $expectedQueries ? "healthy" : "reject"
    );
    $config = configForPort($port);
    $config["retry_count"] = $retryCount;
    $config["ping_before_query"] = true;
    $label = $retryCount === 0 ? "zero" : ($expectedQueries ? "healthy" : "bounded");
    try {
        $client = new ClickHouse($config);
    } catch (ClickHouseException $e) {
        fclose($pipes[1]);
        fclose($pipes[2]);
        proc_terminate($process);
        proc_close($process);
        echo "$label=mock-connect-failed\n";
        continue;
    }
    try {
        $client->select("SELECT 1");
        echo "$label=accepted\n";
    } catch (ClickHouseException $e) {
        echo "$label=rejected\n";
    }
    fclose($pipes[1]);
    stream_set_blocking($pipes[2], false);
    $summary = "";
    $reportDeadline = microtime(true) + 2.0;
    while (microtime(true) < $reportDeadline) {
        $chunk = fread($pipes[2], 4096);
        if ($chunk !== false && $chunk !== "") $summary .= $chunk;
        $status = proc_get_status($process);
        if (!$status["running"]) {
            $chunk = fread($pipes[2], 4096);
            if ($chunk !== false && $chunk !== "") $summary .= $chunk;
            break;
        }
        usleep(10000);
    }
    fclose($pipes[2]);
    if (proc_get_status($process)["running"]) proc_terminate($process);
    proc_close($process);
    $summary = trim($summary);
    echo "$label=$summary\n";
    if ($summary !== "pings=$expectedPings queries=$expectedQueries") {
        echo "$label=wrong-counters\n";
    }
}
?>
--EXPECT--
initial=rejected
progress=rejected
profile=rejected
log=rejected
columns=rejected
events=rejected
exception=rejected
exception-server-events=0
eos=accepted
truncated=rejected
bounded=rejected
bounded=pings=2 queries=0
zero=rejected
zero=pings=1 queries=0
healthy=accepted
healthy=pings=1 queries=1
