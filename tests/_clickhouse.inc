<?php
/**
 * Shared helpers for php_clickhouse phpt tests.
 *
 * Read connection settings from the environment so tests work both in
 * a docker-compose layout (host=clickhouse) and against a local
 * server started via the README's docker run snippet.
 */

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
    $h = getenv("CLICKHOUSE_HOST") ?: "clickhouse";
    $p = (int)(getenv("CLICKHOUSE_PORT") ?: "9000");
    $sock = @stream_socket_client("tcp://$h:$p", $err, $errstr, 1);
    if (!$sock) {
        print "skip ClickHouse not reachable at $h:$p";
        exit;
    }
    fclose($sock);
}

function clickhouse_tls_test_config(): array
{
    $config = clickhouse_test_config();
    $config["port"] = (int)(getenv("CLICKHOUSE_TLS_PORT") ?: "9440");
    $config["ssl"]  = true;
    if (getenv("CLICKHOUSE_TLS_SKIP_VERIFY") !== "0") {
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
 */
function clickhouse_skip_if_no_tls_server(): void
{
    if (!extension_loaded("clickhouse")) {
        print "skip clickhouse extension not loaded";
        exit;
    }
    $h = getenv("CLICKHOUSE_HOST") ?: "clickhouse";
    $p = (int)(getenv("CLICKHOUSE_TLS_PORT") ?: "9440");
    $sock = @stream_socket_client("tcp://$h:$p", $err, $errstr, 1);
    if (!$sock) {
        print "skip ClickHouse TLS not reachable at $h:$p";
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
        print "skip TLS handshake failed: " . $e->getMessage();
        exit;
    }
}

/* Back-compat aliases. Old function names kept for one release. */
function seasclick_test_config(): array { return clickhouse_test_config(); }
function seasclick_skip_if_no_server(): void { clickhouse_skip_if_no_server(); }
