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
    exit;
}
require __DIR__ . "/_clickhouse.inc";
clickhouse_skip_if_no_server();
?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";
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

/* Default-valued booleans and empty CA paths carry no TLS material. */
function attempt($label, array $extra) {
    $cfg = clickhouse_test_config() + ["retry_timeout" => 0];
    foreach ($extra as $k => $v) {
        $cfg[$k] = $v;
    }
    try {
        $c = new ClickHouse($cfg);
        echo $label, ": connected ", $c->select("SELECT 1 AS one", [], ClickHouse::FETCH_ONE), "\n";
    } catch (ClickHouseException $e) {
        echo $label, ": ", $e->getMessage(), "\n";
    }
}
attempt("ssl false", ["ssl" => false]);
attempt("ssl ''", ["ssl" => ""]);
attempt("use_default_ca true", ["ssl_use_default_ca" => true]);
attempt("use_default_ca '1'", ["ssl_use_default_ca" => "1"]);
attempt("skip_verify false", ["ssl_skip_verify" => false]);
attempt("skip_verify ''", ["ssl_skip_verify" => ""]);
attempt("empty CA paths", ["ssl_ca_files" => "", "ssl_ca_directory" => false]);
attempt("use_default_ca false", ["ssl_use_default_ca" => false]);
attempt("skip_verify true", ["ssl_skip_verify" => true]);
attempt("ca_files set", ["ssl_ca_files" => "/etc/ssl/certs/ca-certificates.crt"]);
attempt("skip_verify 'no'", ["ssl_skip_verify" => "no"]);
?>
--EXPECT--
without_tls=rejected
ssl false: connected 1
ssl '': connected 1
use_default_ca true: connected 1
use_default_ca '1': connected 1
skip_verify false: connected 1
skip_verify '': connected 1
empty CA paths: connected 1
use_default_ca false: ssl_* option 'ssl_use_default_ca' requires php_clickhouse built with TLS support (--enable-clickhouse-openssl)
skip_verify true: ssl_* option 'ssl_skip_verify' requires php_clickhouse built with TLS support (--enable-clickhouse-openssl)
ca_files set: ssl_* option 'ssl_ca_files' requires php_clickhouse built with TLS support (--enable-clickhouse-openssl)
skip_verify 'no': ssl_skip_verify must be a boolean (true, false, 0, 1, "true", "false", "0", "1", or "" for false)
