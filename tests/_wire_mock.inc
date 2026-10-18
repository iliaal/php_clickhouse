<?php
/* In-test hostile native-protocol peer. Each accepted connection reads the
 * client hello, sends the scripted server hello, waits for the request to go
 * idle, sends the scripted response and half-closes. The peer keeps serving
 * reconnects until wire_mock_stop() connects and sends the stop token. Stdin
 * cannot be the stop signal: on Windows stream_select() reports a pipe as
 * always readable, so the peer would block in fgets(STDIN) and never accept. */

function wire_mock_code(): string
{
    return <<<'PHP'
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

function drain_idle($peer, $firstUsec, $idleUsec, $maxSec) {
    $deadline = microtime(true) + $maxSec;
    $got = "";
    while (microtime(true) < $deadline) {
        $read = [$peer];
        $write = null;
        $except = null;
        $wait = $got === "" ? $firstUsec : $idleUsec;
        if (@stream_select($read, $write, $except, (int)($wait / 1000000), $wait % 1000000) < 1) break;
        $chunk = fread($peer, 65536);
        if ($chunk === false || $chunk === "") break;
        $got .= $chunk;
    }
    return $got;
}

$deadline = microtime(true) + 30.0;
while (microtime(true) < $deadline) {
    $read = [$server];
    $write = null;
    $except = null;
    if (@stream_select($read, $write, $except, 0, 200000) < 1) continue;
    $peer = @stream_socket_accept($server, 1);
    if ($peer === false) continue;
    stream_set_blocking($peer, true);
    $request = drain_idle($peer, 2000000, 50000, 2.0);
    if ($request === "wire-mock-stop") {
        fclose($peer);
        break;
    }
    if ($request === "") {
        fclose($peer);
        continue;
    }
    fwrite($peer, $hello);
    drain_idle($peer, 150000, 50000, 2.0);
    if ($response !== "") fwrite($peer, $response);
    stream_socket_shutdown($peer, STREAM_SHUT_WR);
    drain_idle($peer, 3000000, 3000000, 3.0);
    fclose($peer);
}
fclose($server);
PHP;
}

function wire_mock_start(string $helloHex, string $responseHex): array
{
    static $file = null;
    if ($file === null) {
        $file = tempnam(sys_get_temp_dir(), "clickhouse-wire-mock-");
        if ($file === false || file_put_contents($file, "<?php\n" . wire_mock_code()) === false) {
            throw new RuntimeException("mock file creation failed");
        }
        register_shutdown_function(function () use ($file) {
            @unlink($file);
        });
    }
    $process = proc_open([PHP_BINARY, "-n", "-f", $file, $helloHex, $responseHex], [
        0 => ["pipe", "r"],
        1 => ["pipe", "w"],
        2 => ["pipe", "w"],
    ], $pipes);
    if (!is_resource($process)) throw new RuntimeException("mock start failed");
    $line = fgets($pipes[1]);
    $port = $line === false ? 0 : (int)trim($line);
    if ($port < 1 || $port > 65535) {
        proc_terminate($process);
        $error = trim(stream_get_contents($pipes[2]));
        foreach ($pipes as $pipe) fclose($pipe);
        proc_close($process);
        throw new RuntimeException("mock did not publish a valid port: " . $error);
    }
    return [$process, $pipes, $port];
}

function wire_mock_stop(array $mock): string
{
    list($process, $pipes, $port) = $mock;
    $control = @stream_socket_client("tcp://127.0.0.1:$port", $errno, $error, 2);
    if ($control !== false) {
        fwrite($control, "wire-mock-stop");
        fclose($control);
    }
    fclose($pipes[0]);
    fclose($pipes[1]);
    $stderr = stream_get_contents($pipes[2]);
    fclose($pipes[2]);
    $status = proc_close($process);
    return $status === 0 && $stderr === "" ? "" : "mock-failed:$status:" . trim($stderr);
}

function wire_mock_config(int $port): array
{
    return [
        "host" => "127.0.0.1",
        "port" => $port,
        "user" => "u",
        "passwd" => "p",
        "compression" => 0,
        "retry_count" => 0,
        "retry_timeout" => 0,
        "connect_timeout_ms" => 2000,
        "receive_timeout_ms" => 4000,
        "send_timeout_ms" => 2000,
    ];
}

/* Native-protocol encoders for building Data packets in tests. */
function wire_varint(int $v): string
{
    $out = "";
    do {
        $byte = $v & 0x7f;
        $v = ($v >> 7) & (PHP_INT_MAX >> 6);
        if ($v) $byte |= 0x80;
        $out .= chr($byte);
    } while ($v);
    return $out;
}

function wire_str(string $s): string
{
    return wire_varint(strlen($s)) . $s;
}

/* Little-endian UInt64 values. pack("P") needs a 64-bit PHP; limiting values
 * to 31 bits keeps the high word zero and the output the same on both. One
 * pack() call, because appending per value is quadratic when realloc always
 * moves the string, as it does under ASan with USE_ZEND_ALLOC=0. */
function wire_u64(int ...$values): string
{
    $words = [];
    foreach ($values as $v) {
        if ($v < 0 || $v > 0x7fffffff) {
            throw new InvalidArgumentException("wire_u64 value out of range: $v");
        }
        $words[] = $v;
        $words[] = 0;
    }
    return pack("V*", ...$words);
}

/* $columns: list of [name, type, encoded column body]; empty bodies give a header block. */
function wire_data_block(array $columns, int $rows): string
{
    $out = "\x01" . wire_str("") . "\x01\x00\x02\xff\xff\xff\xff\x00"
        . wire_varint(count($columns)) . wire_varint($rows);
    foreach ($columns as $column) {
        $out .= wire_str($column[0]) . wire_str($column[1]) . "\x00" . $column[2];
    }
    return $out;
}

/* Header block, one data block, end of stream. */
function wire_select_response(array $columns, int $rows): string
{
    $header = [];
    foreach ($columns as $column) $header[] = [$column[0], $column[1], ""];
    return wire_data_block($header, 0) . wire_data_block($columns, $rows) . "\x05";
}

/* Server hello for revision 54492 (the scan's recorded peer). */
function wire_default_hello_hex(): string
{
    return "000a436c69636b486f7573651a09dca903035554430c3563666363653339323563630d";
}

/* Run $op(ClickHouse) against one scripted peer; returns ["ok", value] or
 * ["exception", message], plus a mock status entry if the peer misbehaved. */
function wire_run(string $helloHex, string $responseHex, callable $op): array
{
    $mock = wire_mock_start($helloHex, $responseHex);
    try {
        $client = new ClickHouse(wire_mock_config($mock[2]));
        try {
            $result = ["ok", $op($client)];
        } catch (ClickHouseException $e) {
            $result = ["exception", $e->getMessage()];
            /* The trace holds $client as the $op argument; keeping it alive
             * leaves the peer waiting out its final drain. */
            $e = null;
        }
        unset($client);
    } catch (ClickHouseException $e) {
        $result = ["connect-exception", $e->getMessage()];
    }
    $status = wire_mock_stop($mock);
    if ($status !== "") $result[] = $status;
    return $result;
}

function wire_select_op(int $flags = 0): callable
{
    return function ($client) use ($flags) {
        return $client->select("SELECT * FROM t", [], $flags);
    };
}
