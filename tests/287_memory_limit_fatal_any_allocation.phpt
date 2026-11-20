--TEST--
A memory_limit fatal at any allocation of a ClickHouse call leaves the client usable
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
/* Which allocation fails depends on the heap layout, so each child shifts
 * where it reaches memory_limit, stepping the failure through more than one
 * row's worth of conversion allocations (under 1 KiB per row), or through
 * the allocations select() makes before converting rows. The children use
 * the tracked allocator, which checks the limit on every allocation, so the
 * failing allocation does not depend on allocator chunk sizes. */
if (!preg_match('~\s(/\S*clickhouse\.so)$~m', file_get_contents("/proc/self/maps"), $m)) {
    exit("clickhouse.so is not in /proc/self/maps\n");
}
$php = getenv("TEST_PHP_EXECUTABLE") ?: PHP_BINARY;
$env = getenv();
$env["USE_ZEND_ALLOC"] = "0";
$env["USE_TRACKED_ALLOC"] = "1";
$cases = [
    "select" => range(0, 1104, 16),
    "statement" => range(0, 1104, 16),
    "iterator" => range(0, 1104, 16),
    "prepare" => range(0, 1600, 8),
];
foreach ($cases as $mode => $shifts) {
    $failures = [];
    $fatals = 0;
    foreach (array_chunk($shifts, 8) as $batch) {
        $running = [];
        foreach ($batch as $shift) {
            $proc = proc_open([$php, "-n", "-d", "extension=" . $m[1],
                               "-d", "memory_limit=8M", "-d", "display_errors=1",
                               __DIR__ . "/_memory_limit_unwind_child.php", (string)$shift, $mode],
                              [1 => ["pipe", "w"], 2 => ["redirect", 1]], $pipes, null, $env);
            $running[$shift] = [$proc, $pipes[1]];
        }
        foreach ($running as $shift => [$proc, $out]) {
            $output = stream_get_contents($out);
            fclose($out);
            $status = proc_close($proc);
            $count = substr_count($output, "Fatal error: Allowed memory size");
            $fatals += $count;
            /* A prepare child cannot lower the limit below its usage, and
             * with enough headroom it completes its query. */
            $completed = $mode === "prepare" && $count === 0 && $status === 0
                && preg_match('~^(limit below usage|completed)\n~', $output);
            $ok = ($completed || ($status === 255 && $count === 1))
                && substr($output, -10) === "[{\"x\":1}]\n"
                && strpos($output, "unreachable") === false
                && strpos($output, "iteration finished") === false
                && strpos($output, "Sanitizer") === false
                && strpos($output, "runtime error:") === false;
            if (!$ok) {
                $failures[] = "$mode, shift $shift, exit $status:\n" . trim($output);
            }
        }
    }
    if ($fatals === 0) {
        $failures[] = "$mode: no child reached memory_limit";
    }
    echo $mode, ": ", count($shifts), " children, ", count($failures), " failures\n";
    if ($failures) {
        echo implode("\n\n", $failures), "\n";
    }
}
?>
--EXPECT--
select: 70 children, 0 failures
statement: 70 children, 0 failures
iterator: 70 children, 0 failures
prepare: 201 children, 0 failures
