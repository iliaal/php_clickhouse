<?php
/* Exception names and column names are length-prefixed strings capped at
 * 16 MiB. Each response declares the cap but carries three bytes. Before the
 * fix the client zero-filled the declared length before reading it. */
const BOUND_KB = 8192;

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
function rss() {
    $status = file_get_contents("/proc/self/status");
    $o = [];
    foreach (["VmSize","VmRSS","RssAnon","RssFile","RssShmem"] as $k) { preg_match("/^$k:\\s+(\\d+)/m", $status, $m); $o[$k] = (int)$m[1]; }
    return $o;
}
function smaps() { $o=[]; $cur=null; foreach (file("/proc/self/smaps") as $l) { if (preg_match('/^[0-9a-f]+-[0-9a-f]+ \\S+ \\S+ \\S+ \\S+\\s*(.*)$/', $l, $m)) { $cur = $m[1] === "" ? "[anon]" : basename($m[1]); } elseif (preg_match('/^Rss:\\s+(\\d+)/', $l, $m)) { $o[$cur] = ($o[$cur] ?? 0) + (int)$m[1]; } } return $o; }
rss(); $s0 = smaps(); $r0 = rss();
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
$r1 = rss(); $d = []; foreach ($r1 as $k => $v) $d[] = "$k=" . ($v - $r0[$k]);
$s1 = smaps(); $sd=[]; foreach ($s1 as $k=>$v) { $dv = $v - ($s0[$k] ?? 0); if ($dv >= 64) $sd[] = "$k:$dv"; } fwrite(STDERR, "SMAPS " . implode(" ", $sd) . "\n");
fwrite(STDERR, "DELTA " . implode(" ", $d) . "\n");
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
    $errors = trim((string)file_get_contents($errorFile)); $GLOBALS["lastErrors"] = $errors;
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

$declared = varint(0xFFFFFF) . "abc";
$truncated = [
    "exception-name" => "\x02" . pack("V", 1000) . $declared,
    "column-name" => "\x01" . str("") . BLOCK_INFO . varint(1) . varint(0) . $declared,
    "column-type" => "\x01" . str("") . BLOCK_INFO . varint(1) . varint(0) . str("c0") . $declared,
];
for ($iter = 0; $iter < (int)($argv[1] ?? 1); $iter++) foreach ($truncated as $label => $response) {
    list($result, $peak, $hwm) = run($response);
    report($label, $result, $peak, $hwm);
    echo "  raw peak=$peak hwm=$hwm ", $GLOBALS["lastErrors"], "\n";
}

