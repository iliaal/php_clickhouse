--TEST--
ClickHouse connect_timeout_ms triggers fast failure
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// 192.0.2.0/24 is reserved for documentation (RFC 5737); routing it
// produces a SYN-RETRANS that won't complete inside our budget.
$cfg = [
    "host"               => "192.0.2.1",
    "port"               => 9000,
    "connect_timeout_ms" => 200,
    "retry_count"        => 1,
];

$t0 = microtime(true);
try {
    $c = new ClickHouse($cfg);
    echo "no throw\n";
} catch (ClickHouseException $e) {
    $elapsed_ms = (microtime(true) - $t0) * 1000;
    echo "throw: yes\n";
    echo "fast: ", ($elapsed_ms < 2000 ? "yes" : "no:" . (int)$elapsed_ms), "\n";
}

// Negative ms rejected.
try {
    new ClickHouse(["host" => "127.0.0.1", "port" => 9000, "connect_timeout_ms" => -1]);
    echo "neg: no throw\n";
} catch (ClickHouseException $e) {
    echo "neg: throw\n";
}
?>
--EXPECT--
throw: yes
fast: yes
neg: throw
