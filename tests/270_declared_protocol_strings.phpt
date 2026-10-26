--TEST--
ClickHouse does not allocate server-declared protocol string lengths before the data arrives
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
if (!function_exists("proc_open")) print "skip proc_open unavailable";
elseif (!is_readable("/proc/self/status") || !is_readable("/proc/self/maps")) print "skip needs /proc";
?>
--FILE--
<?php
/* Exception names and column names are length-prefixed strings capped at
 * 16 MiB. Each response declares the cap but carries three bytes. Before the
 * fix the client zero-filled the declared length before reading it.
 *
 * The first connect, select, and exception in a process fault in several MiB
 * of extension and runtime code, and that file-backed RSS varies by kernel
 * and page cache (10-13 MiB on GitHub's ASan runners). Each client therefore
 * first runs the same truncated response with a declared length of 4 and only
 * measures the second connection. */
const BOUND_KB = 8192;

$mockCode = <<<'PHP'
/* One listener per response, closed once its connection is accepted, so the
 * client's reconnect after a failed query is refused instead of consuming the
 * next response. */
$servers = $ports = [];
foreach (array_slice($argv, 1) as $responseFile) {
    $server = stream_socket_server("tcp://127.0.0.1:0", $errno, $error);
    if (!$server) {
        fwrite(STDERR, "$errno: $error\n");
        exit(1);
    }
    $servers[$responseFile] = $server;
    $ports[] = substr(strrchr(stream_socket_get_name($server, false), ":"), 1);
}
fwrite(STDOUT, implode(" ", $ports) . "\n");
fflush(STDOUT);
foreach ($servers as $responseFile => $server) {
    $peer = stream_socket_accept($server, 5);
    fclose($server);
    if ($peer === false) exit(2);
    if (fread($peer, 1) !== "\x00") exit(3);
    fwrite($peer, hex2bin("00044d6f636b0101bba90303555443044d6f636b00"));
    for ($i = 0; $i < 256; $i++) {
        $byte = fread($peer, 1);
        if ($byte === false || $byte === "") exit(4);
        if ($byte === "\x01") break;
    }
    fwrite($peer, file_get_contents($responseFile));
    stream_socket_shutdown($peer, STREAM_SHUT_WR);
    stream_set_timeout($peer, 2);
    while (!feof($peer)) {
        $chunk = fread($peer, 65536);
        if ($chunk === false || $chunk === "") break;
    }
    fclose($peer);
}
PHP;

/* Each case runs in a fresh client process: VmPeak cannot be reset, so a
 * process that already held a large mapping would hide the next one. */
$clientCode = <<<'PHP'
function memory() {
    $status = file_get_contents("/proc/self/status");
    preg_match('/^VmPeak:\s+(\d+)/m', $status, $peak);
    preg_match('/^VmHWM:\s+(\d+)/m', $status, $hwm);
    return [(int)$peak[1], (int)$hwm[1]];
}
function query($port) {
    global $argv;
    try {
        $client = new ClickHouse([
            "host" => "127.0.0.1", "port" => $port, "compression" => $argv[2] === "none" ? false : $argv[2],
            "retry_count" => 0, "connect_timeout_ms" => 1000,
            "receive_timeout_ms" => 5000, "send_timeout_ms" => 1000,
        ]);
        return [md5(serialize($client->select("SELECT c0", [], ClickHouse::FETCH_COLUMN))), "-"];
    } catch (ClickHouseException $e) {
        return ["exception", base64_encode(json_encode([$e->getServerCode(), $e->getServerName(), substr($e->getMessage(), 0, 1024)]))];
    }
}
if ($argv[3] !== "-") {
    list($warmResult) = query((int)$argv[3]);
    if ($warmResult !== "exception") {
        echo "warm-up returned $warmResult\n";
        exit(1);
    }
}
@file_put_contents("/proc/self/clear_refs", "5");
list($peak, $hwm) = memory();
list($result, $detail) = query((int)$argv[1]);
list($peakAfter, $hwmAfter) = memory();
echo $result, " ", $peakAfter - $peak, " ", $hwmAfter - $hwm, " ", $detail, "\n";
PHP;

function clientArgs() {
    $args = [PHP_BINARY, "-n", "-d", "memory_limit=256M"];
    $loaded = [];
    foreach (file("/proc/self/maps") as $line) {
        if (preg_match('#\s(/\S+/(json|clickhouse)\.so)$#', trim($line), $m)) $loaded[$m[2]] = $m[1];
    }
    foreach (["json", "clickhouse"] as $name) {
        if (isset($loaded[$name])) {
            $args[] = "-d";
            $args[] = "extension=" . $loaded[$name];
        }
    }
    return $args;
}

$dir = sys_get_temp_dir();
$mockFile = tempnam($dir, "clickhouse-mock-");
$clientFile = tempnam($dir, "clickhouse-client-");
$responseFile = tempnam($dir, "clickhouse-response-");
$warmupFile = tempnam($dir, "clickhouse-warmup-");
register_shutdown_function(function () use ($mockFile, $clientFile, $responseFile, $warmupFile) {
    @unlink($mockFile);
    @unlink($clientFile);
    @unlink($responseFile);
    @unlink($warmupFile);
});
file_put_contents($mockFile, "<?php\n" . $mockCode);
file_put_contents($clientFile, "<?php\n" . $clientCode);

/* Returns [result, VmPeak growth KiB, VmHWM growth KiB, exception details];
 * result is "exception" or the md5 of the serialized FETCH_COLUMN rows. A
 * $warmup response is served to a first, unmeasured query in the same client. */
function run($response, $warmup = null, $compression = "none") {
    global $mockFile, $clientFile, $responseFile, $warmupFile;
    file_put_contents($responseFile, $response);
    $responses = [$responseFile];
    if ($warmup !== null) {
        file_put_contents($warmupFile, $warmup);
        array_unshift($responses, $warmupFile);
    }
    $mock = proc_open(array_merge([PHP_BINARY, "-n", "-f", $mockFile], $responses),
        [0 => ["pipe", "r"], 1 => ["pipe", "w"], 2 => ["pipe", "w"]], $mockPipes);
    fclose($mockPipes[0]);
    $ports = explode(" ", trim((string)fgets($mockPipes[1])));
    $port = array_pop($ports);
    $warmPort = $ports ? $ports[0] : "-";
    $errorFile = tempnam(sys_get_temp_dir(), "clickhouse-client-err-");
    $client = proc_open(array_merge(clientArgs(), [$clientFile, $port, $compression, $warmPort]),
        [0 => ["pipe", "r"], 1 => ["pipe", "w"], 2 => ["file", $errorFile, "w"]], $clientPipes);
    fclose($clientPipes[0]);
    $line = trim((string)stream_get_contents($clientPipes[1]));
    fclose($clientPipes[1]);
    proc_close($client);
    $errors = trim((string)file_get_contents($errorFile));
    @unlink($errorFile);
    fclose($mockPipes[1]);
    fclose($mockPipes[2]);
    proc_close($mock);
    $fields = explode(" ", $line);
    if (count($fields) !== 4) return ["client failed: $line $errors", 0, 0, null];
    return [$fields[0], (int)$fields[1], (int)$fields[2],
        $fields[3] === "-" ? null : json_decode(base64_decode($fields[3]), true)];
}

function report($label, $result, $peak, $hwm, $checkPeak = true) {
    echo $label, ": ", $result === "exception" ? "exception" : $result,
        $checkPeak ? (", address space " . ($peak < BOUND_KB ? "bounded" : "grew {$peak} KiB")) : "",
        ", resident ", $hwm < BOUND_KB ? "bounded" : "grew {$hwm} KiB", "\n";
}

function varint($value) {
    $out = "";
    do {
        $byte = $value & 0x7f;
        $value >>= 7;
        $out .= chr($value ? $byte | 0x80 : $byte);
    } while ($value);
    return $out;
}

function str($value) {
    return varint(strlen($value)) . $value;
}

const BLOCK_INFO = "\x01\x00\x02\xff\xff\xff\xff\x00";

function block($rows, $type, $body, $name = "c0") {
    return "\x01" . str("") . BLOCK_INFO . varint(1) . varint($rows) . str($name) . str($type) . "\x00" . $body;
}

function response($rows, $type, $body, $complete = false) {
    return block(0, $type, "") . block($rows, $type, $body) . ($complete ? "\x05" : "");
}

$prefixes = [
    "exception-name" => "\x02" . pack("V", 1000),
    "column-name" => "\x01" . str("") . BLOCK_INFO . varint(1) . varint(0),
    "column-type" => "\x01" . str("") . BLOCK_INFO . varint(1) . varint(0) . str("c0"),
];
foreach ($prefixes as $label => $prefix) {
    list($result, $peak, $hwm) = run($prefix . varint(0xFFFFFF) . "abc", $prefix . varint(4) . "abc");
    report($label, $result, $peak, $hwm);
}

/* The binding truncates long messages, so check the code, name, and a prefix;
 * the exception only decodes if all 3 MiB of text were read. */
$text = str_repeat("server said no. ", 196609);
list($result, , , $detail) = run("\x02" . pack("V", 1000) . str("DB::Exception") . str($text) . str("") . "\x00");
echo "valid exception: ", $result === "exception" && $detail[0] === 1000 && $detail[1] === "DB::Exception"
    && strpos($detail[2], substr($text, 0, 512)) === 0 ? "ok" : json_encode($detail), "\n";

$name = str_repeat("n", 1048577);
list($result) = run(block(0, "UInt8", "") . block(1, "UInt8", "\x2a", $name) . "\x05");
echo "valid column name: ", $result === md5(serialize([42])) ? "ok" : $result, "\n";
?>
--EXPECT--
exception-name: exception, address space bounded, resident bounded
column-name: exception, address space bounded, resident bounded
column-type: exception, address space bounded, resident bounded
valid exception: ok
valid column name: ok
