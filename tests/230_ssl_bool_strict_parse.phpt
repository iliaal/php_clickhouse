--TEST--
TLS boolean options accept only strict boolean spellings
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
require __DIR__ . "/_clickhouse.inc";
clickhouse_skip_if_no_tls_server();
$ca = getenv("CLICKHOUSE_TLS_CA_FILE");
if (!$ca || !is_file($ca)) {
    print "skip TLS CA fixture not configured";
    exit;
}
$probe = clickhouse_tls_test_config();
unset($probe["ssl_ca_files"], $probe["ssl_skip_verify"]);
try {
    new ClickHouse($probe);
    print "skip server certificate is trusted by the system CA store";
} catch (ClickHouseException $e) {
}
?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$base = clickhouse_tls_test_config();
unset($base["ssl_ca_files"], $base["ssl_skip_verify"]);
$base["retry_timeout"] = 0;

function attempt($label, array $cfg) {
    try {
        $c = new ClickHouse($cfg);
        echo $label, ": connected ", $c->select("SELECT 1 AS one", [], ClickHouse::FETCH_ONE), "\n";
    } catch (ClickHouseException $e) {
        $m = $e->getMessage();
        if (strpos($m, "must be a boolean") !== false) {
            echo $label, ": rejected\n";
        } elseif (strpos($m, "Failed to verify SSL connection") !== false) {
            echo $label, ": verification failed\n";
        } else {
            echo $label, ": other error: ", strtok($m, "\n"), "\n";
        }
    }
}

foreach (["ssl", "ssl_skip_verify", "ssl_use_default_ca"] as $key) {
    foreach (["no", "off", "yes", "on", " true", "2", 2, -1, 1.0, [], [0], new stdClass()] as $bad) {
        $cfg = $base;
        $cfg[$key] = $bad;
        try {
            new ClickHouse($cfg);
            echo "$key=", var_export($bad, true), ": NO EXCEPTION\n";
        } catch (ClickHouseException $e) {
            if (strpos($e->getMessage(), "$key must be a boolean") === false) {
                echo "$key=", json_encode($bad), ": unexpected: ", strtok($e->getMessage(), "\n"), "\n";
            }
        }
    }
}
echo "invalid spellings checked\n";

foreach (["false", "FALSE", "0", "", 0, false] as $v) {
    attempt("skip_verify=" . var_export($v, true), $base + ["ssl_use_default_ca" => false, "ssl_skip_verify" => $v]);
}
foreach (["true", "True", "1", 1, true] as $v) {
    attempt("skip_verify=" . var_export($v, true), $base + ["ssl_use_default_ca" => false, "ssl_skip_verify" => $v]);
}

/* Point OpenSSL's default store at the test CA so ssl_use_default_ca is observable. */
putenv("SSL_CERT_FILE=" . getenv("CLICKHOUSE_TLS_CA_FILE"));
attempt("default store", $base);
foreach (["false", "False", "0", 0, false] as $v) {
    attempt("use_default_ca=" . var_export($v, true), $base + ["ssl_use_default_ca" => $v]);
}
foreach (["true", "1", 1, true] as $v) {
    attempt("use_default_ca=" . var_export($v, true), $base + ["ssl_use_default_ca" => $v]);
}

$plain = clickhouse_test_config();
$plain["retry_timeout"] = 0;
foreach (["false", "0", 0] as $v) {
    attempt("plain port ssl=" . var_export($v, true), $plain + ["ssl" => $v]);
}
?>
--EXPECT--
invalid spellings checked
skip_verify='false': verification failed
skip_verify='FALSE': verification failed
skip_verify='0': verification failed
skip_verify='': verification failed
skip_verify=0: verification failed
skip_verify=false: verification failed
skip_verify='true': connected 1
skip_verify='True': connected 1
skip_verify='1': connected 1
skip_verify=1: connected 1
skip_verify=true: connected 1
default store: connected 1
use_default_ca='false': verification failed
use_default_ca='False': verification failed
use_default_ca='0': verification failed
use_default_ca=0: verification failed
use_default_ca=false: verification failed
use_default_ca='true': connected 1
use_default_ca='1': connected 1
use_default_ca=1: connected 1
use_default_ca=true: connected 1
plain port ssl='false': connected 1
plain port ssl='0': connected 1
plain port ssl=0: connected 1
