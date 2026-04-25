--TEST--
ClickHouse query_id propagates through writeStart
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$qid = "clickhouse-test-q-ws-" . bin2hex(random_bytes(4));

$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.qid_ws_t");
$c->execute("CREATE TABLE test.qid_ws_t (id UInt32) ENGINE = Memory");

$c->writeStart("test.qid_ws_t", ["id"], $qid);
$c->write([[1], [2], [3]]);
$c->writeEnd();

$c->execute("SYSTEM FLUSH LOGS");
$found = $c->select(
    "SELECT count(*) FROM system.query_log WHERE query_id = '" . $qid . "'",
    [], ClickHouse::FETCH_ONE
);
echo "found: ", ((int)$found >= 1) ? "yes" : "no ($found)", "\n";

$c->execute("DROP TABLE test.qid_ws_t");
?>
--EXPECT--
found: yes
