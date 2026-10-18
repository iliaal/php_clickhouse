--TEST--
TLS handshake against a silent peer is bounded by connect_timeout and fails over to the next endpoint
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
require __DIR__ . "/_clickhouse.inc";
clickhouse_skip_if_no_tls_server();
$ca = getenv("CLICKHOUSE_TLS_CA_FILE");
if (!$ca || !is_file($ca)) {
    print "skip TLS CA fixture not configured";
}
?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

/* Never accepted: the kernel completes the TCP handshake from the backlog,
 * so the ClientHello goes unanswered. */
$tarpit = stream_socket_server("tcp://127.0.0.1:0", $errno, $errstr);
if (!$tarpit) {
    die("tarpit listen failed: $errno $errstr\n");
}
$tarpit_name = stream_socket_get_name($tarpit, false);
$tarpit_port = (int)substr(strrchr($tarpit_name, ":"), 1);

$config = clickhouse_tls_test_config();
$healthy = ["host" => $config["host"], "port" => $config["port"]];
unset($config["host"], $config["port"]);
$config["endpoints"] = [
    ["host" => "127.0.0.1", "port" => $tarpit_port],
    $healthy,
];
$config["retry_count"] = 1;
$config["connect_timeout_ms"] = 500;
/* Hang guard only: an unbounded handshake would otherwise block forever. */
$config["receive_timeout_ms"] = 8000;

$start = microtime(true);
$c = new ClickHouse($config);
$elapsed = microtime(true) - $start;

echo ($elapsed >= 0.45 && $elapsed < 3.0) ? "bounded\n" : sprintf("elapsed %.3fs\n", $elapsed);
echo stream_socket_accept($tarpit, 0) !== false ? "tarpit dialed\n" : "tarpit not dialed\n";
$ep = $c->getCurrentEndpoint();
echo ($ep["port"] === $healthy["port"]) ? "healthy endpoint\n" : "endpoint " . json_encode($ep) . "\n";
var_dump($c->select("SELECT 1 AS one", [], ClickHouse::FETCH_ONE));
?>
--EXPECT--
bounded
tarpit dialed
healthy endpoint
int(1)
