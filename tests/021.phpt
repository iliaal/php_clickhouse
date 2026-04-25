--TEST--
ClickHouse ZSTD compression round-trip
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$config = clickhouse_test_config();
$config["compression"] = "zstd";

$c = new ClickHouse($config);
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.zstd_t");
$c->execute("CREATE TABLE test.zstd_t (id UInt32, payload String) ENGINE = Memory");

$payload = str_repeat("ab", 10000);
$c->insert("test.zstd_t", ["id", "payload"], [[1, $payload], [2, $payload]]);

$rows = $c->select("SELECT id, length(payload) AS l FROM test.zstd_t ORDER BY id");
foreach ($rows as $r) echo $r["id"], " len=", $r["l"], "\n";

$c->execute("DROP TABLE test.zstd_t");
?>
--EXPECT--
1 len=20000
2 len=20000
