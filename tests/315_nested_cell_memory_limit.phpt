--TEST--
A server-sized nested cell ends in the memory_limit error, not a process exit
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
if (!function_exists("proc_open")) print "skip proc_open unavailable";
elseif (!is_readable("/proc/self/maps")) print "skip needs /proc";
?>
--INI--
memory_limit=512M
--FILE--
<?php
require __DIR__ . "/_wire_mock.inc";
/* On PHP builds with a hard memory limit (twice memory_limit by default), a
 * Zend allocation past it exits the process with status 137. Before the fix
 * an Array cell sized one array from its element count, and an Array of
 * LowCardinality strings kept copying past the soft limit. Each case runs in
 * a child with memory_limit=128M and the Zend allocator, which enforces it. */
$clientCode = <<<'PHP'
$client = new ClickHouse([
    "host" => "127.0.0.1", "port" => (int)$argv[1], "compression" => 0, "retry_count" => 0,
    "connect_timeout_ms" => 2000, "receive_timeout_ms" => 10000, "send_timeout_ms" => 2000,
]);
$client->select("SELECT c0 FROM t");
echo "select returned\n";
PHP;
$clientFile = tempnam(sys_get_temp_dir(), "clickhouse-client-");
file_put_contents($clientFile, "<?php\n" . $clientCode);
register_shutdown_function(function () use ($clientFile) {
    @unlink($clientFile);
});

function one_row(string $type, string $body): string {
    return wire_data_block([["c0", $type, ""]], 0) . wire_data_block([["c0", $type, $body]], 1) . "\x05";
}

$elements = (1 << 24) + 1;
$text = str_repeat("x", 48 << 20);
$cases = [
    "Array(UInt8) of 2^24+1 elements" => one_row("Array(UInt8)", wire_u64($elements) . str_repeat("\0", $elements)),
    /* Key version, offsets, UInt8 keys with additional keys, one 48 MiB
     * dictionary string, and eight keys that all select it. */
    "Array(LowCardinality(String)) of eight 48 MiB strings" => one_row("Array(LowCardinality(String))",
        wire_u64(1, 8, 1 << 9, 1) . wire_str($text) . wire_u64(8) . str_repeat("\0", 8)),
];
unset($text);
foreach ($cases as $label => $response) {
    $mock = wire_mock_start(wire_default_hello_hex(), wire_response_file($response));
    unset($response);
    $child = proc_open(array_merge(wire_child_php(["memory_limit" => "128M", "display_errors" => "1"]),
        [$clientFile, (string)$mock[2]]),
        [0 => ["pipe", "r"], 1 => ["pipe", "w"], 2 => ["redirect", 1]], $pipes,
        null, ["USE_ZEND_ALLOC" => "1"] + getenv());
    fclose($pipes[0]);
    $output = stream_get_contents($pipes[1]);
    fclose($pipes[1]);
    $status = proc_close($child);
    $fatal = strpos($output, "Allowed memory size of 134217728 bytes exhausted") !== false;
    echo $label, ": ", $status === 255 && $fatal ? "memory_limit error" : "exit $status " . trim(substr($output, 0, 300)),
        wire_mock_stop($mock), "\n";
}
?>
--EXPECT--
Array(UInt8) of 2^24+1 elements: memory_limit error
Array(LowCardinality(String)) of eight 48 MiB strings: memory_limit error
