--TEST--
ClickHouse constructor rejects values wider than native option sinks
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";
$base = clickhouse_test_config();
$probes = [
    "retry_count" => 4294967296,
    "connect_timeout_ms" => 2147483648,
    "receive_timeout_ms" => 4294967296,
    "send_timeout_ms" => 4294967296,
    "connect_timeout" => 2147484,
    "receive_timeout" => 4294968,
    "send_timeout" => 4294968,
    "tcp_keepalive_idle" => 2147483648,
    "tcp_keepalive_intvl" => 2147483648,
    "tcp_keepalive_cnt" => 2147483648,
    "max_compression_chunk_size" => 2147483648,
];

foreach ($probes as $key => $value) {
    try {
        new ClickHouse([$key => $value] + $base);
        echo $key, ": accepted\n";
    } catch (ClickHouseException $e) {
        echo $key, ": rejected\n";
    }
}

new ClickHouse([
    "retry_count" => 2,
    "connect_timeout_ms" => 1000,
    "receive_timeout_ms" => 1000,
    "send_timeout_ms" => 1000,
    "tcp_keepalive_idle" => 60,
    "tcp_keepalive_intvl" => 5,
    "tcp_keepalive_cnt" => 3,
    "max_compression_chunk_size" => 65535,
] + $base);
echo "ordinary values: accepted\n";
?>
--EXPECT--
retry_count: rejected
connect_timeout_ms: rejected
receive_timeout_ms: rejected
send_timeout_ms: rejected
connect_timeout: rejected
receive_timeout: rejected
send_timeout: rejected
tcp_keepalive_idle: rejected
tcp_keepalive_intvl: rejected
tcp_keepalive_cnt: rejected
max_compression_chunk_size: rejected
ordinary values: accepted
