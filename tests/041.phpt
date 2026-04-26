--TEST--
ClickHouse enableLogQueries / getLogQueries accumulator
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";
$c = new ClickHouse(clickhouse_test_config());

// Disabled by default: no entries collected.
$c->select("SELECT 1");
echo "off count: ", count($c->getLogQueries()), "\n";

// Enable and run a mix of statements.
$c->enableLogQueries(true);
$c->select("SELECT 1 AS x");
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.log_round_trip");
$c->execute("CREATE TABLE test.log_round_trip (id UInt32) ENGINE = Memory");
$c->insert("test.log_round_trip", ["id"], [[1], [2], [3]]);

// One server error to verify error_code routing.
try {
    $c->select("SELECT * FROM test.no_such_xyz", [], 0, "qid-error");
} catch (ClickHouseException $e) {
}

$log = $c->getLogQueries();
echo "log entries: ", count($log), "\n";

// Inspect first entry shape.
$first = $log[0];
echo "first sql: ",        $first["sql"], "\n";
echo "first query_id: ",   var_export($first["query_id"], true), "\n";
echo "first elapsed>=0: ", ($first["elapsed_ms"] >= 0 ? "yes" : "no"), "\n";
echo "first error_code: ", $first["error_code"], "\n";

// The INSERT should have rows_read==0 (server doesn't return rows on insert)
// but its SQL should be the INSERT INTO statement.
$insert_log = null;
foreach ($log as $entry) {
    if (strpos($entry["sql"], "INSERT INTO test.log_round_trip") === 0) {
        $insert_log = $entry;
        break;
    }
}
echo "insert sql found: ", ($insert_log ? "yes" : "no"), "\n";
echo "insert error_code: ", ($insert_log["error_code"] ?? -99), "\n";

// The error entry should carry the server code and the failing SQL.
$error_log = null;
foreach ($log as $entry) {
    if ($entry["error_code"] !== 0) {
        $error_log = $entry;
        break;
    }
}
echo "error captured: ",   ($error_log ? "yes" : "no"), "\n";
echo "error qid: ",        $error_log["query_id"], "\n";
echo "error code>0: ",     ($error_log["error_code"] > 0 ? "yes" : "no"), "\n";
echo "error msg set: ",    ($error_log["error_message"] !== "" ? "yes" : "no"), "\n";

// getLogQueries clears the buffer.
echo "second get: ", count($c->getLogQueries()), "\n";

// Disable: subsequent queries don't add.
$c->enableLogQueries(false);
$c->select("SELECT 2");
echo "after off: ", count($c->getLogQueries()), "\n";

$c->execute("DROP TABLE test.log_round_trip");
?>
--EXPECT--
off count: 0
log entries: 6
first sql: SELECT 1 AS x
first query_id: ''
first elapsed>=0: yes
first error_code: 0
insert sql found: yes
insert error_code: 0
error captured: yes
error qid: qid-error
error code>0: yes
error msg set: yes
second get: 0
after off: 0
