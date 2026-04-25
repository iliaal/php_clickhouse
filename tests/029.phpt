--TEST--
ClickHouse default-database config knob
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$bootstrap = new ClickHouse(clickhouse_test_config());
$bootstrap->execute("CREATE DATABASE IF NOT EXISTS test");
unset($bootstrap);

$config = clickhouse_test_config();
$config["database"] = "test";
$c = new ClickHouse($config);

$c->execute("DROP TABLE IF EXISTS default_db_t");
$c->execute("CREATE TABLE default_db_t (id UInt32) ENGINE = Memory");
$c->insert("default_db_t", ["id"], [[1], [2], [3]]);
$rows = $c->select("SELECT id FROM default_db_t ORDER BY id");
var_dump($rows);
$c->execute("DROP TABLE default_db_t");
?>
--EXPECT--
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
