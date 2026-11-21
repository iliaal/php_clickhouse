<?php

function clickhouse_test_config(): array
{
    $config = [
        "host"        => getenv("CLICKHOUSE_HOST") ?: "clickhouse",
        "port"        => (int)(getenv("CLICKHOUSE_PORT") ?: "9000"),
        "compression" => true,
    ];
    $user = getenv("CLICKHOUSE_USER");
    $passwd = getenv("CLICKHOUSE_PASSWD");
    if ($user !== false && $user !== "") {
        $config["user"] = $user;
    }
    if ($passwd !== false && $passwd !== "") {
        $config["passwd"] = $passwd;
    }
    return $config;
}

/**
 * For use inside --SKIPIF-- sections: prints "skip ..." and exits if
 * the clickhouse extension is missing or the server isn't reachable.
 */
function clickhouse_skip_if_no_server(): void
{
    if (!extension_loaded("clickhouse")) {
        print "skip clickhouse extension not loaded";
        exit;
    }
    /* Windows CI runners have no ClickHouse server reachable. Skip
     * up-front rather than burning a 1s connect timeout per test. */
    if (PHP_OS_FAMILY === "Windows") {
        print "skip ClickHouse server unavailable on Windows runner";
        exit;
    }
    $h = getenv("CLICKHOUSE_HOST") ?: "clickhouse";
    $p = (int)(getenv("CLICKHOUSE_PORT") ?: "9000");
    $sock = @stream_socket_client("tcp://$h:$p", $err, $errstr, 1);
    if (!$sock) {
        print "skip ClickHouse server not reachable";
        exit;
    }
    fclose($sock);
}

/**
 * For use inside --SKIPIF-- sections after clickhouse_skip_if_no_server():
 * skip when the server is too old to support the JSON column type (or has
 * it disabled), so the JSON suite degrades cleanly on older containers.
 */
function clickhouse_skip_if_no_json(): void
{
    try {
        $c = new ClickHouse(clickhouse_test_config());
        $c->setSettings(["allow_experimental_json_type" => 1]);
        $c->execute("CREATE DATABASE IF NOT EXISTS test");
        $c->execute("DROP TABLE IF EXISTS test._json_probe");
        $c->execute("CREATE TABLE test._json_probe (j JSON) ENGINE = Memory");
        $c->execute("DROP TABLE test._json_probe");
    } catch (ClickHouseException $e) {
        $message = strtolower($e->getMessage());
        $json_error = strpos($message, "json") !== false;
        $capability_error = strpos($message, "unknown data type") !== false
            || strpos($message, "not supported") !== false
            || strpos($message, "experimental") !== false
            || strpos($message, "allow_experimental_json_type") !== false;
        if ($json_error && $capability_error) {
            print "skip JSON type not supported by server: " . $e->getMessage();
            exit;
        }
        throw $e;
    }
}

function clickhouse_tls_test_config(): array
{
    $config = clickhouse_test_config();
    $config["port"] = (int)(getenv("CLICKHOUSE_TLS_PORT") ?: "9440");
    $config["ssl"]  = true;
    /* Tests must opt out of TLS verification explicitly. */
    if (getenv("CLICKHOUSE_TLS_SKIP_VERIFY") === "1") {
        $config["ssl_skip_verify"] = true;
    }
    if ($ca = getenv("CLICKHOUSE_TLS_CA_FILE")) {
        $config["ssl_ca_files"] = $ca;
    }
    return $config;
}

/**
 * Skip a TLS test when either the server isn't reachable on the TLS
 * port or the extension was built without --enable-clickhouse-openssl.
 * Only connection/handshake failures skip: auth and config errors are
 * rethrown so a bad fixture fails loudly instead of going green-by-skip.
 */
function clickhouse_skip_if_no_tls_server(): void
{
    if (!extension_loaded("clickhouse")) {
        print "skip clickhouse extension not loaded";
        exit;
    }
    if (PHP_OS_FAMILY === "Windows") {
        print "skip ClickHouse TLS server unavailable on Windows runner";
        exit;
    }
    $h = getenv("CLICKHOUSE_HOST") ?: "clickhouse";
    $p = (int)(getenv("CLICKHOUSE_TLS_PORT") ?: "9440");
    $sock = @stream_socket_client("tcp://$h:$p", $err, $errstr, 1);
    if (!$sock) {
        print "skip ClickHouse TLS server not reachable";
        exit;
    }
    fclose($sock);
    try {
        new ClickHouse(clickhouse_tls_test_config());
    } catch (ClickHouseException $e) {
        if (strpos($e->getMessage(), "without TLS") !== false) {
            print "skip extension built without TLS support";
            exit;
        }
        $message = strtolower($e->getMessage());
        $transport = false;
        foreach (array("connect", "refused", "timeout", "handshake", "certificate", "verify", "tls", "ssl", "socket", "network", "unreachable", "reset", "eof", "broken") as $needle) {
            if (strpos($message, $needle) !== false) {
                $transport = true;
                break;
            }
        }
        if (!$transport) {
            throw $e;
        }
        print "skip TLS handshake failed: " . $e->getMessage();
        exit;
    }
}

/* Back-compat aliases. Old function names kept for one release. */
function seasclick_test_config(): array { return clickhouse_test_config(); }
function seasclick_skip_if_no_server(): void { clickhouse_skip_if_no_server(); }

/**
 * For use inside --SKIPIF-- sections of tests that hit memory_limit and then
 * check the client from a shutdown function.
 */
function clickhouse_skip_unless_shutdown_after_oom(): void
{
    $php = getenv("TEST_PHP_EXECUTABLE") ?: PHP_BINARY;
    $out = shell_exec(escapeshellarg($php) . " -n -d display_errors=0 "
        . escapeshellarg(__DIR__ . "/_oom_shutdown_probe.php") . " 2>&1");
    if ($out !== "ok") {
        print "skip shutdown functions do not run after a memory_limit error on this PHP build";
        exit;
    }
}

/**
 * For use inside --SKIPIF-- sections of tests whose child processes run with
 * the tracked allocator, which PHP 8.0 introduced.
 */
function clickhouse_skip_unless_tracked_allocator(): void
{
    $php = getenv("TEST_PHP_EXECUTABLE") ?: PHP_BINARY;
    $out = shell_exec("USE_ZEND_ALLOC=0 USE_TRACKED_ALLOC=1 " . escapeshellarg($php)
        . " -n -d display_errors=0 " . escapeshellarg(__DIR__ . "/_oom_shutdown_probe.php") . " 2>&1");
    if ($out !== "ok") {
        print "skip the tracked allocator does not enforce memory_limit on this PHP build";
        exit;
    }
}

/**
 * Runs tests/_memory_limit_unwind_child.php once per shift for each mode, at
 * most eight at a time, and prints the failures per mode. Each child must hit
 * memory_limit exactly once and still query from its shutdown function;
 * headroom modes may instead complete. The children use the tracked
 * allocator, which checks the limit on every allocation, so the failing
 * allocation does not depend on allocator chunk sizes. Each child is its own
 * CLI process, so under AddressSanitizer it runs with leak detection on even
 * when the runner's does not; a leak report fails the child.
 */
function clickhouse_run_memory_limit_children(array $cases, array $headroom_modes = []): void
{
    if (!preg_match('~\s(/\S*clickhouse\.so)$~m', file_get_contents("/proc/self/maps"), $m)) {
        exit("clickhouse.so is not in /proc/self/maps\n");
    }
    $php = getenv("TEST_PHP_EXECUTABLE") ?: PHP_BINARY;
    $env = getenv();
    $env["USE_ZEND_ALLOC"] = "0";
    $env["USE_TRACKED_ALLOC"] = "1";
    $env["ASAN_OPTIONS"] = (isset($env["ASAN_OPTIONS"]) && $env["ASAN_OPTIONS"] !== ""
        ? $env["ASAN_OPTIONS"] . ":" : "") . "detect_leaks=1";
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
                /* A headroom child cannot lower the limit below its usage, and
                 * with enough headroom it completes its query. */
                $completed = in_array($mode, $headroom_modes, true) && $count === 0 && $status === 0
                    && preg_match('~^(limit below usage|completed)\n~', $output);
                /* After the fatal error, and the stack trace PHP 8.5 adds to it,
                 * only the shutdown function's query may print: the call must
                 * not continue past it. */
                $after_fatal = $count === 1
                    ? preg_replace('~^\nStack trace:\n(?:#\d+ [^\n]*\n)+~', "\n",
                                   strstr(strstr($output, "Fatal error: Allowed memory size"), "\n"))
                    : "";
                $ok = ($completed || ($status === 255 && $count === 1 && $after_fatal === "\n[{\"x\":1}]\n"))
                    && substr($output, -10) === "[{\"x\":1}]\n"
                    && strpos($output, "unreachable") === false
                    && strpos($output, "iteration finished") === false
                    && strpos($output, "Sanitizer") === false
                    && strpos($output, "runtime error:") === false;
                if (!$ok) {
                    /* Sanitizer reports from many children can exceed the
                     * runner's own memory_limit when it diffs the output. */
                    if (strlen($output) > 8192) {
                        $output = substr($output, 0, 4096) . "\n[...]\n" . substr($output, -4096);
                    }
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
}
