--TEST--
TLS options without ssl => true are rejected instead of silently connecting in plaintext
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
require __DIR__ . "/_clickhouse.inc";
clickhouse_skip_if_no_server();
$probe = clickhouse_test_config();
$probe["ssl"] = true;
$probe["ssl_min_protocol_version"] = "invalid-test-value";
try {
    new ClickHouse($probe);
} catch (ClickHouseException $e) {
    if (strpos($e->getMessage(), "without TLS") !== false) {
        print "skip extension built without TLS support";
    }
}
?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$base = clickhouse_test_config();
$base["retry_timeout"] = 0;

function attempt($label, array $cfg) {
    try {
        $c = new ClickHouse($cfg);
        echo $label, ": connected ", $c->select("SELECT 1 AS one", [], ClickHouse::FETCH_ONE), "\n";
    } catch (ClickHouseException $e) {
        echo $label, ": ", strtok($e->getMessage(), "\n"), "\n";
    }
}

$material = [
    "ssl_ca_files" => "/etc/ssl/certs/ca-certificates.crt",
    "ssl_ca_directory" => "/etc/ssl/certs",
    "ssl_min_protocol_version" => "tls1.3",
    "ssl_skip_verify" => true,
    "ssl_use_default_ca" => true,
];
foreach ([["absent", null], ["false", false], ["'0'", "0"]] as $mode) {
    foreach ($material as $key => $val) {
        $cfg = $base;
        if ($mode[1] !== null) {
            $cfg["ssl"] = $mode[1];
        }
        $cfg[$key] = $val;
        attempt("ssl " . $mode[0] . ", $key", $cfg);
    }
}

attempt("ssl false, explicit false booleans and nulls", $base + [
    "ssl" => false,
    "ssl_skip_verify" => false,
    "ssl_use_default_ca" => "0",
    "ssl_ca_files" => null,
    "ssl_ca_directory" => null,
    "ssl_min_protocol_version" => null,
]);
attempt("ssl absent, invalid boolean", $base + ["ssl_skip_verify" => "no"]);
attempt("ssl absent, unknown key", $base + ["ssl_verify_peer" => true]);
?>
--EXPECT--
ssl absent, ssl_ca_files: ssl_* option 'ssl_ca_files' requires 'ssl' => true
ssl absent, ssl_ca_directory: ssl_* option 'ssl_ca_directory' requires 'ssl' => true
ssl absent, ssl_min_protocol_version: ssl_* option 'ssl_min_protocol_version' requires 'ssl' => true
ssl absent, ssl_skip_verify: ssl_* option 'ssl_skip_verify' requires 'ssl' => true
ssl absent, ssl_use_default_ca: ssl_* option 'ssl_use_default_ca' requires 'ssl' => true
ssl false, ssl_ca_files: ssl_* option 'ssl_ca_files' requires 'ssl' => true
ssl false, ssl_ca_directory: ssl_* option 'ssl_ca_directory' requires 'ssl' => true
ssl false, ssl_min_protocol_version: ssl_* option 'ssl_min_protocol_version' requires 'ssl' => true
ssl false, ssl_skip_verify: ssl_* option 'ssl_skip_verify' requires 'ssl' => true
ssl false, ssl_use_default_ca: ssl_* option 'ssl_use_default_ca' requires 'ssl' => true
ssl '0', ssl_ca_files: ssl_* option 'ssl_ca_files' requires 'ssl' => true
ssl '0', ssl_ca_directory: ssl_* option 'ssl_ca_directory' requires 'ssl' => true
ssl '0', ssl_min_protocol_version: ssl_* option 'ssl_min_protocol_version' requires 'ssl' => true
ssl '0', ssl_skip_verify: ssl_* option 'ssl_skip_verify' requires 'ssl' => true
ssl '0', ssl_use_default_ca: ssl_* option 'ssl_use_default_ca' requires 'ssl' => true
ssl false, explicit false booleans and nulls: connected 1
ssl absent, invalid boolean: ssl_skip_verify must be a boolean (true, false, 0, 1, "true", "false", "0", or "1")
ssl absent, unknown key: unknown ssl_* option 'ssl_verify_peer'
