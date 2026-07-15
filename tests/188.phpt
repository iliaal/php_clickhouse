--TEST--
ClickHouse TLS verifies the configured CA and peer hostname
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
require __DIR__ . "/_clickhouse.inc";
clickhouse_skip_if_no_tls_server();
$ca = getenv("CLICKHOUSE_TLS_CA_FILE");
$wrong = getenv("CLICKHOUSE_TLS_WRONG_CA_FILE");
if (!$ca || !is_file($ca) || !$wrong || !is_file($wrong)) {
    print "skip TLS verification fixtures not configured";
}
?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$base = clickhouse_test_config();
$base["port"] = (int)(getenv("CLICKHOUSE_TLS_PORT") ?: "9440");
$base["ssl"] = true;
$base["ssl_skip_verify"] = false;

$valid = $base;
$valid["ssl_ca_files"] = getenv("CLICKHOUSE_TLS_CA_FILE");
$c = new ClickHouse($valid);
echo "valid=", $c->ping() ? "ok" : "fail", "\n";

$wrongCa = $base;
$wrongCa["ssl_ca_files"] = getenv("CLICKHOUSE_TLS_WRONG_CA_FILE");
try {
    new ClickHouse($wrongCa);
    echo "wrong_ca=accepted\n";
} catch (ClickHouseException $e) {
    echo "wrong_ca=rejected\n";
}

$wrongHost = $valid;
$wrongHost["host"] = "127.0.0.2";
try {
    new ClickHouse($wrongHost);
    echo "wrong_host=accepted\n";
} catch (ClickHouseException $e) {
    echo "wrong_host=", stripos($e->getMessage(), "mismatch") !== false
        ? "verified_mismatch\n"
        : "transport_error\n";
}
?>
--EXPECT--
valid=ok
wrong_ca=rejected
wrong_host=verified_mismatch
