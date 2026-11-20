--TEST--
A memory_limit fatal at any allocation of select() conversion leaves the client usable
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
require __DIR__ . "/_clickhouse.inc";
clickhouse_skip_if_no_server();
clickhouse_skip_unless_shutdown_after_oom();
if (!function_exists("proc_open")) {
    print "skip proc_open unavailable";
} elseif (!is_readable("/proc/self/maps")) {
    print "skip /proc/self/maps is needed to locate the extension";
}
?>
--FILE--
<?php
/* Which allocation fails depends on the heap layout, so each child shifts its
 * ballast by 16 bytes, stepping the failure through more than one row's worth
 * of conversion allocations (976 bytes per row). The failure is exact under
 * run-tests.php --asan, whose tracked allocator checks the limit on every
 * allocation; there a fatal error left the client busy or destroyed a
 * half-inserted array. */
if (!preg_match('~\s(/\S*clickhouse\.so)$~m', file_get_contents("/proc/self/maps"), $m)) {
    exit("clickhouse.so is not in /proc/self/maps\n");
}
$php = getenv("TEST_PHP_EXECUTABLE") ?: PHP_BINARY;
$offsets = range(0, 1104, 16);
$failures = [];
foreach (array_chunk($offsets, 8) as $batch) {
    $running = [];
    foreach ($batch as $offset) {
        $proc = proc_open([$php, "-n", "-d", "extension=" . $m[1],
                           "-d", "memory_limit=8M", "-d", "display_errors=1",
                           __DIR__ . "/_memory_limit_unwind_child.php", (string)$offset],
                          [1 => ["pipe", "w"], 2 => ["redirect", 1]], $pipes);
        $running[$offset] = [$proc, $pipes[1]];
    }
    foreach ($running as $offset => [$proc, $out]) {
        $output = stream_get_contents($out);
        fclose($out);
        $status = proc_close($proc);
        $ok = $status === 255
            && substr_count($output, "Fatal error: Allowed memory size") === 1
            && substr($output, -10) === "[{\"x\":1}]\n"
            && strpos($output, "Sanitizer") === false
            && strpos($output, "runtime error:") === false;
        if (!$ok) {
            $failures[] = "offset $offset, exit $status:\n" . trim($output);
        }
    }
}
echo count($offsets), " offsets, ", count($failures), " failures\n";
echo implode("\n\n", $failures), "\n";
?>
--EXPECT--
70 offsets, 0 failures
