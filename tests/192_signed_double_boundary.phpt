--TEST--
ClickHouse Int64 rejects a double at positive 2^63 without corrupting the row
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.signed_double_boundary");
$c->execute("CREATE TABLE test.signed_double_boundary (v Int64) ENGINE=Memory");

try {
    $c->insert("test.signed_double_boundary", ["v"], [[9223372036854775808.0]]);
    echo "upper accepted\n";
} catch (ClickHouseException $e) {
    echo "upper rejected\n";
}

$c->insert("test.signed_double_boundary", ["v"], [[-9223372036854775808.0]]);
$row = $c->select("SELECT v FROM test.signed_double_boundary")[0];
echo "stored=", $row["v"], "\n";
$c->execute("DROP TABLE test.signed_double_boundary");
?>
--EXPECT--
upper rejected
stored=-9223372036854775808
