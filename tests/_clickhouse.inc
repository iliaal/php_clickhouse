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
