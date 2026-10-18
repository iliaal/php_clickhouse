--TEST--
ClickHouse rejects NUL bytes in host and endpoint hosts before connecting
--EXTENSIONS--
clickhouse
--FILE--
<?php
$base = [
    "port" => 1,
    "retry_count" => 0,
    "retry_timeout" => 0,
    "connect_timeout_ms" => 500,
];

$cases = [
    "host" => ["host" => "127.0.0.1\0.example.com"],
    "endpoint" => ["host" => "127.0.0.1", "endpoints" => [["host" => "127.0.0.1\0.example.com", "port" => 1]]],
    "second-endpoint" => ["endpoints" => [["host" => "127.0.0.1", "port" => 1], ["host" => "\0", "port" => 1]]],
    "control-host" => ["host" => "127.0.0.1"],
    "control-endpoint" => ["endpoints" => [["host" => "127.0.0.1", "port" => 1]]],
];

foreach ($cases as $name => $config) {
    try {
        new ClickHouse($config + $base);
        echo "$name: connected\n";
    } catch (ClickHouseException $e) {
        $nul = $e->getMessage() === "host must not contain NUL bytes";
        echo "$name: ", $nul ? "rejected" : "other error", "\n";
    }
}
?>
--EXPECT--
host: rejected
endpoint: rejected
second-endpoint: rejected
control-host: other error
control-endpoint: other error
