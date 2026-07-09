--TEST--
DR-007: a typed-parameter parse error redacts the bound value instead of leaking it
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// ClickHouse echoes the bound value in a type-parse error ("Value <X> cannot
// be parsed as <Type> ..."), ahead of the SQL markers the sanitizer cuts on,
// so a secret-looking parameter leaked into the exception message. The value
// fragment is now redacted.

$c = new ClickHouse(clickhouse_test_config());

try {
    $c->select("SELECT {x:UInt32} AS n", array("x" => "s3cr3t_value"));
    echo "no throw\n";
} catch (ClickHouseException $e) {
    $msg = $e->getMessage();
    echo "leaks value: ", (strpos($msg, "s3cr3t_value") !== false ? "YES" : "no"), "\n";
    echo "redacted: ", (strpos($msg, "<redacted>") !== false ? "YES" : "no"), "\n";
}
?>
--EXPECT--
leaks value: no
redacted: YES
