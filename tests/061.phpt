--TEST--
ClickHouse __construct rejects negative seconds-based timeouts (regression for unsigned-wrap)
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// Regression for CR-207: retry_count used to silently wrap when
// passed a negative value (-1 → ~UINT_MAX retries). The ms-variant
// blocks already validated; the seconds variants didn't.

$base = clickhouse_test_config();

$probes = [
    "retry_count -1"     => ['retry_count'    => -1],
    "retry_timeout -1"   => ['retry_timeout'  => -1],
    "connect_timeout -1" => ['connect_timeout' => -1],
    "receive_timeout -1" => ['receive_timeout' => -1],
    "send_timeout -1"    => ['send_timeout'    => -1],
    "tcp_keepalive_idle -1"  => ['tcp_keepalive_idle'  => -1],
    "tcp_keepalive_intvl -1" => ['tcp_keepalive_intvl' => -1],
];
foreach ($probes as $label => $extra) {
    try {
        new ClickHouse($extra + $base);
        echo "$label: NO THROW\n";
    } catch (ClickHouseException $e) {
        echo "$label: REJECTED\n";
    }
}

// Sanity: 0 is allowed (means use library default).
try {
    $c = new ClickHouse(['retry_count' => 0] + $base);
    echo "retry_count 0: ok\n";
} catch (Throwable $e) {
    echo "retry_count 0: UNEXPECTED — ", $e->getMessage(), "\n";
}
?>
--EXPECT--
retry_count -1: REJECTED
retry_timeout -1: REJECTED
connect_timeout -1: REJECTED
receive_timeout -1: REJECTED
send_timeout -1: REJECTED
tcp_keepalive_idle -1: REJECTED
tcp_keepalive_intvl -1: REJECTED
retry_count 0: ok
