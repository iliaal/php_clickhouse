--TEST--
ClickHouse Phase D: getStatistics() echoes the query_id supplied to select/execute/insert
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());

// 1. select() with explicit query_id round-trips through getStatistics()
$qid = "qid-select-" . bin2hex(random_bytes(4));
$c->select("SELECT 1", [], 0, $qid);
$st = $c->getStatistics();
echo "select_qid_match=", ($st["query_id"] === $qid) ? 1 : 0, "\n";

// 2. execute() with query_id
$qid2 = "qid-exec-" . bin2hex(random_bytes(4));
$c->execute("SELECT 1", [], $qid2);
echo "execute_qid_match=", ($c->getStatistics()["query_id"] === $qid2) ? 1 : 0, "\n";

// 3. select() without query_id => empty string
$c->select("SELECT 1");
echo "no_qid_is_empty=", ($c->getStatistics()["query_id"] === "") ? 1 : 0, "\n";

// 4. insert() with query_id
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.qid_t");
$c->execute("CREATE TABLE test.qid_t (k UInt32) ENGINE=Memory");
$qid3 = "qid-ins-" . bin2hex(random_bytes(4));
$c->insert("test.qid_t", ["k"], [[1]], $qid3);
echo "insert_qid_match=", ($c->getStatistics()["query_id"] === $qid3) ? 1 : 0, "\n";
$c->execute("DROP TABLE test.qid_t");

?>
--EXPECT--
select_qid_match=1
execute_qid_match=1
no_qid_is_empty=1
insert_qid_match=1
