--TEST--
TLS handshake against a single silent peer throws ClickHouseException within connect_timeout
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
require __DIR__ . "/_clickhouse.inc";
clickhouse_skip_if_no_tls_server();
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
$config["host"] = "127.0.0.1";
$config["port"] = $tarpit_port;
$config["retry_count"] = 1;
$config["connect_timeout_ms"] = 500;
/* Hang guard only: an unbounded handshake would otherwise block forever. */
$config["receive_timeout_ms"] = 8000;

$start = microtime(true);
try {
    new ClickHouse($config);
    echo "connected\n";
} catch (ClickHouseException $e) {
    echo get_class($e), "\n";
    echo strpos($e->getMessage(), "timed out") !== false ? "timed out\n" : "message: " . $e->getMessage() . "\n";
}
$elapsed = microtime(true) - $start;
echo ($elapsed >= 0.45 && $elapsed < 3.0) ? "bounded\n" : sprintf("elapsed %.3fs\n", $elapsed);
echo stream_socket_accept($tarpit, 0) !== false ? "tarpit dialed\n" : "tarpit not dialed\n";
?>
--EXPECT--
ClickHouseException
timed out
bounded
tarpit dialed
