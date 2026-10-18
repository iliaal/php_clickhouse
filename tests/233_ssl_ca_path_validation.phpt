--TEST--
TLS CA paths reject NUL bytes and report the failing CA file
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
require __DIR__ . "/_clickhouse.inc";
clickhouse_skip_if_no_tls_server();
$ca = getenv("CLICKHOUSE_TLS_CA_FILE");
if (!$ca || !is_file($ca)) {
    print "skip TLS CA fixture not configured";
}
?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$ca = getenv("CLICKHOUSE_TLS_CA_FILE");
$base = clickhouse_tls_test_config();
unset($base["ssl_skip_verify"]);
$base["ssl_ca_files"] = $ca;
$base["retry_timeout"] = 0;

function attempt($label, array $cfg) {
    try {
        $c = new ClickHouse($cfg);
        echo $label, ": connected ", $c->select("SELECT 1 AS one", [], ClickHouse::FETCH_ONE), "\n";
    } catch (ClickHouseException $e) {
        echo $label, ": ", strtok($e->getMessage(), "\n"), "\n";
    }
}

attempt("valid CA", $base);
attempt("ca_files string NUL", array_merge($base, ["ssl_ca_files" => $ca . "\0/nonexistent"]));
attempt("ca_files element NUL", array_merge($base, ["ssl_ca_files" => [$ca . "\0/nonexistent"]]));
attempt("ca_files second element NUL", array_merge($base, ["ssl_ca_files" => [$ca, $ca . "\0x"]]));
attempt("ca_directory NUL", $base + ["ssl_ca_directory" => "/nonexistent-dir\0/etc/ssl/certs"]);
attempt("ca_directory false", $base + ["ssl_ca_directory" => false]);
attempt("ca_directory empty", $base + ["ssl_ca_directory" => ""]);
attempt("ca_directory array", $base + ["ssl_ca_directory" => ["/etc/ssl/certs"]]);
attempt("min_protocol NUL", $base + ["ssl_min_protocol_version" => "tls1.2\0junk"]);
attempt("min_protocol prefix", $base + ["ssl_min_protocol_version" => "tls1.2x"]);
attempt("min_protocol upper", $base + ["ssl_min_protocol_version" => "TLS1.2"]);

$missing = "/nonexistent/php-clickhouse-233/ca.pem";
try {
    new ClickHouse(array_merge($base, ["ssl_ca_files" => [$ca, $missing]]));
    echo "missing CA: connected\n";
} catch (ClickHouseException $e) {
    $m = $e->getMessage();
    echo "missing CA names file: ", var_export(strpos($m, "'" . $missing . "'") !== false, true), "\n";
    echo "missing CA names reason: ", var_export(stripos($m, "No such file or directory") !== false, true), "\n";
    echo "missing CA unknown error: ", var_export(strpos($m, "Unknown SSL error") !== false, true), "\n";
}
?>
--EXPECT--
valid CA: connected 1
ca_files string NUL: ssl_ca_files must not contain NUL bytes
ca_files element NUL: ssl_ca_files must not contain NUL bytes
ca_files second element NUL: ssl_ca_files must not contain NUL bytes
ca_directory NUL: ssl_ca_directory must not contain NUL bytes
ca_directory false: connected 1
ca_directory empty: connected 1
ca_directory array: ssl_ca_directory must be a string
min_protocol NUL: ssl_min_protocol_version must be one of tls1.0, tls1.1, tls1.2, tls1.3
min_protocol prefix: ssl_min_protocol_version must be one of tls1.0, tls1.1, tls1.2, tls1.3
min_protocol upper: connected 1
missing CA names file: true
missing CA names reason: true
missing CA unknown error: false
