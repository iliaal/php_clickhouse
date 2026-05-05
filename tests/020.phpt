--TEST--
ClickHouse query_id propagates to system.query_log
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());

$qid_select = "clickhouse-test-q-sel-" . bin2hex(random_bytes(4));
$qid_insert = "clickhouse-test-q-ins-" . bin2hex(random_bytes(4));
$qid_drop   = "clickhouse-test-q-drop-" . bin2hex(random_bytes(4));

$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.qid_t");
$c->execute("CREATE TABLE test.qid_t (id UInt32) ENGINE = Memory");
$c->insert("test.qid_t", ["id"], [[1], [2]], $qid_insert);
$c->select("SELECT * FROM test.qid_t", [], 0, $qid_select);
$c->execute("DROP TABLE test.qid_t", [], $qid_drop);

$c->execute("SYSTEM FLUSH LOGS");

$found = $c->select(
    "SELECT count(*) FROM system.query_log WHERE query_id IN ('" .
    $qid_select . "', '" . $qid_insert . "', '" . $qid_drop . "')",
    [], ClickHouse::FETCH_ONE
);

echo "found: ", ((int)$found >= 3) ? "yes" : "no ($found)", "\n";
?>
--EXPECT--
found: yes
