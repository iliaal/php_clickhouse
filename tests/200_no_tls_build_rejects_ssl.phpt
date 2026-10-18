--TEST--
ClickHouse builds without OpenSSL reject ssl=true before connecting
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
$withoutTls = false;
try {
    new ClickHouse([
        "host" => "127.0.0.1",
        "port" => 1,
        "ssl" => true,
        "ssl_min_protocol_version" => "invalid-test-value",
    ]);
} catch (ClickHouseException $e) {
    $withoutTls = strpos($e->getMessage(), "built without TLS support") !== false;
}
if (!$withoutTls) {
    print "skip extension built with TLS support";
}
?>
--FILE--
<?php
try {
    new ClickHouse([
        "host" => "127.0.0.1",
        "port" => 1,
        "ssl" => true,
        "ssl_min_protocol_version" => "invalid-test-value",
    ]);
    echo "without_tls=accepted\n";
} catch (ClickHouseException $e) {
    echo "without_tls=", strpos($e->getMessage(), "built without TLS support") !== false
        ? "rejected\n"
        : "wrong_error\n";
}
?>
--EXPECT--
without_tls=rejected
