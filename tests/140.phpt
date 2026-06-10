--TEST--
endpoints config does not inject a phantom host:port endpoint when host is unset
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
require __DIR__ . "/_clickhouse.inc";
clickhouse_skip_if_no_server();
/* The phantom endpoint the bug injects uses the "host" property default
 * (127.0.0.1) plus the top-level port. The repro can only connect through
 * it when the real server is reachable on loopback. */
$h = getenv("CLICKHOUSE_HOST") ?: "clickhouse";
if (!in_array($h, ["127.0.0.1", "localhost"], true)) {
    print "skip phantom-endpoint repro needs server on loopback (host=$h)";
}
?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$base = clickhouse_test_config();

/* endpoints points at a closed port; the top-level port is the real one,
 * and host is left unset. Pre-fix, clickhouse-cpp prepended the property
 * default host (127.0.0.1) + top-level port as a phantom first endpoint,
 * so the client silently connected to the real server despite the
 * endpoints list naming only a dead port. With host cleared, the
 * endpoints list is authoritative and the connect must fail. */
$cfg = [
    "port"          => $base["port"],                       // real port
    "endpoints"     => [["host" => "127.0.0.1", "port" => 1]], // closed
    "connect_timeout" => 2,
    "retry_count"   => 1,
];
foreach (["user", "passwd"] as $k) {
    if (isset($base[$k])) $cfg[$k] = $base[$k];
}

try {
    $ch = new ClickHouse($cfg);
    echo "connected: ", ($ch->ping() ? "yes" : "no"), "\n";
} catch (ClickHouseException $e) {
    echo "rejected\n";
}
?>
--EXPECT--
rejected
