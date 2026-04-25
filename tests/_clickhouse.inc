<?php
/**
 * Shared helpers for SeasClick phpt tests.
 *
 * Read connection settings from the environment so tests work both in
 * a docker-compose layout (host=clickhouse) and against a local
 * server started via the README's docker run snippet.
 */

function seasclick_test_config(): array
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
 * the SeasClick extension is missing or the server isn't reachable.
 */
function seasclick_skip_if_no_server(): void
{
    if (!extension_loaded("SeasClick")) {
        print "skip SeasClick extension not loaded";
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
