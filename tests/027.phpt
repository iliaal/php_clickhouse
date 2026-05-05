--TEST--
ClickHouse TLS round-trip
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_tls_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";
$c = new ClickHouse(clickhouse_tls_test_config());
var_dump($c->ping());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.tls_t");
$c->execute("CREATE TABLE test.tls_t (id UInt32) ENGINE = Memory");
$c->insert("test.tls_t", ["id"], [[1], [2], [3]]);
$rows = $c->select("SELECT id FROM test.tls_t ORDER BY id");
var_dump($rows);
$c->execute("DROP TABLE test.tls_t");
?>
--EXPECT--
bool(true)
array(3) {
  [0]=>
  array(1) {
    ["id"]=>
    int(1)
  }
  [1]=>
  array(1) {
    ["id"]=>
    int(2)
  }
  [2]=>
  array(1) {
    ["id"]=>
    int(3)
  }
}
