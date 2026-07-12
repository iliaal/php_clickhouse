--TEST--
ClickHouse endpoint failover skips a peer with a malformed native handshake
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
require __DIR__ . "/_clickhouse.inc";
clickhouse_skip_if_no_server();
if (!function_exists("proc_open")) {
    print "skip proc_open unavailable";
}
?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$serverCode = <<<'PHP'
$server = stream_socket_server("tcp://127.0.0.1:0", $errno, $error);
if (!$server) {
    fwrite(STDERR, "$errno: $error\n");
    exit(1);
}
$address = stream_socket_get_name($server, false);
echo substr(strrchr($address, ":"), 1), "\n";
fflush(STDOUT);
$peer = stream_socket_accept($server, 10);
if ($peer) {
    fread($peer, 1);
    fwrite($peer, "\x03");
    fclose($peer);
}
fclose($server);
PHP;

$descriptors = [
    0 => ["pipe", "r"],
    1 => ["pipe", "w"],
    2 => ["pipe", "w"],
];
$command = escapeshellarg(PHP_BINARY) . " -n -r " . escapeshellarg($serverCode);
$process = proc_open($command, $descriptors, $pipes);
if (!is_resource($process)) {
    echo "malformed peer start failed\n";
    exit;
}
fclose($pipes[0]);
$badPort = (int)trim(fgets($pipes[1]));

$base = clickhouse_test_config();
$config = $base;
unset($config["host"], $config["port"]);
$config["endpoints"] = [
    ["host" => "127.0.0.1", "port" => $badPort],
    ["host" => $base["host"], "port" => $base["port"]],
];
$config["connect_timeout"] = 2;
$config["recv_timeout"] = 2;

try {
    $client = new ClickHouse($config);
    $endpoint = $client->getCurrentEndpoint();
    $ok = $client->ping()
        && $endpoint["host"] === $base["host"]
        && $endpoint["port"] === $base["port"];
    echo "constructor failover: ", $ok ? "ok" : "wrong endpoint", "\n";
} catch (ClickHouseException $e) {
    echo "constructor failover: failed\n";
}

$stderr = stream_get_contents($pipes[2]);
fclose($pipes[1]);
fclose($pipes[2]);
$status = proc_close($process);
if ($status !== 0 || $stderr !== "") {
    echo "malformed peer helper failed\n";
}
?>
--EXPECT--
constructor failover: ok
