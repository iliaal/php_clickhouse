--TEST--
ClickHouse does not allocate server-declared column sizes before the data arrives
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
if (!function_exists("proc_open")) print "skip proc_open unavailable";
elseif (!is_readable("/proc/self/status") || !is_readable("/proc/self/maps")) print "skip needs /proc";
?>
--FILE--
<?php
/* Each malformed block declares 256 MiB of column data but carries a few
 * bytes, then the mock closes the connection. Before the fix the client
 * reserved the declared size up front (VmPeak) and zero-filled it for
 * fixed-width columns (VmHWM); the bound is a quarter of that. */
const DECLARED = 268435456;
const BOUND_KB = 65536;

$mockCode = <<<'PHP'
$response = file_get_contents($argv[1]);
$server = stream_socket_server("tcp://127.0.0.1:0", $errno, $error);
if (!$server) {
    fwrite(STDERR, "$errno: $error\n");
    exit(1);
}
$address = stream_socket_get_name($server, false);
fwrite(STDOUT, substr(strrchr($address, ":"), 1) . "\n");
fflush(STDOUT);
$peer = stream_socket_accept($server, 5);
if ($peer === false) exit(2);
if (fread($peer, 1) !== "\x00") exit(3);
fwrite($peer, hex2bin("00044d6f636b0101bba90303555443044d6f636b00"));
for ($i = 0; $i < 256; $i++) {
    $byte = fread($peer, 1);
    if ($byte === false || $byte === "") exit(4);
    if ($byte === "\x01") break;
}
fwrite($peer, $response);
stream_socket_shutdown($peer, STREAM_SHUT_WR);
stream_set_timeout($peer, 2);
while (!feof($peer)) {
    $chunk = fread($peer, 65536);
    if ($chunk === false || $chunk === "") break;
}
fclose($peer);
fclose($server);
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
@file_put_contents("/proc/self/clear_refs", "5");
list($peak, $hwm) = memory();
try {
    $client = new ClickHouse([
        "host" => "127.0.0.1", "port" => (int)$argv[1], "compression" => $argv[2] === "none" ? false : $argv[2],
        "retry_count" => 0, "connect_timeout_ms" => 1000,
        "receive_timeout_ms" => 5000, "send_timeout_ms" => 1000,
    ]);
    $result = md5(serialize($client->select("SELECT c0", [], ClickHouse::FETCH_COLUMN)));
    $detail = "-";
} catch (ClickHouseException $e) {
    $result = "exception";
    $detail = base64_encode(json_encode([$e->getServerCode(), $e->getServerName(), substr($e->getMessage(), 0, 1024)]));
}
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
register_shutdown_function(function () use ($mockFile, $clientFile, $responseFile) {
    @unlink($mockFile);
    @unlink($clientFile);
    @unlink($responseFile);
});
file_put_contents($mockFile, "<?php\n" . $mockCode);
file_put_contents($clientFile, "<?php\n" . $clientCode);

/* Returns [result, VmPeak growth KiB, VmHWM growth KiB, exception details];
 * result is "exception" or the md5 of the serialized FETCH_COLUMN rows. */
function run($response, $compression = "none") {
    global $mockFile, $clientFile, $responseFile;
    file_put_contents($responseFile, $response);
    $mock = proc_open([PHP_BINARY, "-n", "-f", $mockFile, $responseFile],
        [0 => ["pipe", "r"], 1 => ["pipe", "w"], 2 => ["pipe", "w"]], $mockPipes);
    fclose($mockPipes[0]);
    $port = (int)trim((string)fgets($mockPipes[1]));
    $errorFile = tempnam(sys_get_temp_dir(), "clickhouse-client-err-");
    $client = proc_open(array_merge(clientArgs(), [$clientFile, (string)$port, $compression]),
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

$truncated = [
    "string-length" => response(1, "String", varint(DECLARED) . "abc"),
    "fixedstring-width" => response(1, "FixedString(" . DECLARED . ")", str_repeat("A", 16)),
    "uint64-rows" => response(DECLARED / 8, "UInt64", ""),
    "array-offset" => response(1, "Array(UInt64)", pack("P", DECLARED / 8)),
];
foreach ($truncated as $label => $response) {
    list($result, $peak, $hwm) = run($response);
    report($label, $result, $peak, $hwm);
}

$large = str_repeat("0123456789abcdef", 196609);
$wide = str_repeat("w", 1048577);
$other = str_repeat("v", 1048577);
$rows = 300000;
$valid = [
    "string" => [response(3, "String", str("a") . str($large) . str("z"), true), ["a", $large, "z"]],
    "fixedstring" => [response(2, "FixedString(1048577)", $wide . $other, true), [$wide, $other]],
    "uint64" => [response($rows, "UInt64", pack("P*", ...range(1, $rows)), true), range(1, $rows)],
    "array" => [response(2, "Array(UInt64)", pack("P*", 1, $rows + 1) . pack("P*", ...range(0, $rows)), true),
        [[0], range(1, $rows)]],
];
foreach ($valid as $label => list($response, $expected)) {
    list($result) = run($response);
    echo "valid $label: ", $result === md5(serialize($expected)) ? "ok" : $result, "\n";
}
?>
--EXPECT--
string-length: exception, address space bounded, resident bounded
fixedstring-width: exception, address space bounded, resident bounded
uint64-rows: exception, address space bounded, resident bounded
array-offset: exception, address space bounded, resident bounded
valid string: ok
valid fixedstring: ok
valid uint64: ok
valid array: ok
