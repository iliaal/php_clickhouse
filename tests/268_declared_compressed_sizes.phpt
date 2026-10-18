--TEST--
ClickHouse does not commit server-declared compressed frame sizes before the data arrives
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
if (!function_exists("proc_open")) print "skip proc_open unavailable";
elseif (!is_readable("/proc/self/status") || !is_readable("/proc/self/maps")) print "skip needs /proc";
?>
--FILE--
<?php
/* The frames carry a valid checksum and a one-row UInt8 block, but declare
 * 256 MiB: as the decompressed size (LZ4, ZSTD, and a ZSTD frame whose own
 * content size also claims 256 MiB), or as the compressed size with only 64
 * bytes behind it. Before the fix the client zero-filled the declared size
 * (VmHWM). The decompression buffer is still reserved, uninitialized, up to the
 * 1 GiB limit, so only the resident size is bounded for those; the
 * compressed-size case is also bounded in address space. */
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

$frames = [
    "lz4-original" => ["lz4", hex2bin("010050f219dc7a3399dfa0916fcc28ed2d6f822000000000000010f006010002ffffffff0001010263300555496e74380007"), false],
    "zstd-original" => ["zstd", hex2bin("01008967bac15e438c2f2dbeae7c6b34391d902e0000000000001028b52ffde01500000000000000a90000010002ffffffff0001010263300555496e74380007"), false],
    "zstd-content-size" => ["zstd", hex2bin("0100eb9a6f7b0aa27db8c6b38cabe0c6ecc4902e0000000000001028b52ffde00000001000000000a90000010002ffffffff0001010263300555496e74380007"), false],
    "lz4-compressed" => ["lz4", hex2bin("0100" . str_repeat("aa", 16) . "82") . pack("VV", 268435456, 16) . str_repeat("A", 64), true],
];
foreach ($frames as $label => list($compression, $response, $checkPeak)) {
    list($result, $peak, $hwm) = run($response, $compression);
    report($label, $result, $peak, $hwm, $checkPeak);
}

foreach ([
    "lz4" => hex2bin("01009f21aae673ee54d281428468839ece5d822000000015000000f006010002ffffffff0001010263300555496e7438000705"),
    "zstd" => hex2bin("0100329b1beb5e32459947432131ea6e63b4902e0000001500000028b52ffde01500000000000000a90000010002ffffffff0001010263300555496e7438000705"),
] as $compression => $response) {
    list($result) = run($response, $compression);
    echo "valid $compression: ", $result === md5(serialize([7])) ? "ok" : $result, "\n";
}
?>
--EXPECT--
lz4-original: exception, resident bounded
zstd-original: exception, resident bounded
zstd-content-size: exception, resident bounded
lz4-compressed: exception, address space bounded, resident bounded
valid lz4: ok
valid zstd: ok
