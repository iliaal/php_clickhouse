--TEST--
ClickHouse query log caps SQL, resets error stats, and logs streaming inserts at writeEnd
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->enableLogQueries(true);

$c->select("SELECT number FROM numbers(3)");
try {
    $c->execute("THIS IS NOT VALID SQL");
    echo "bad execute: NO THROW\n";
} catch (ClickHouseException $e) {
    echo "bad execute: REJECTED\n";
}

$longSql = "SELECT 1 AS x /*" . str_repeat("a", 20000) . "*/";
$c->select($longSql, [], ClickHouse::FETCH_ONE);
$longQid = str_repeat("q", 20000);
$c->select("SELECT 2 AS x", [], ClickHouse::FETCH_ONE, $longQid);
$logs = $c->getLogQueries();
echo "log count=", count($logs), "\n";
echo "error rows_read=", $logs[1]["rows_read"], "\n";
echo "error bytes_read=", $logs[1]["bytes_read"], "\n";
echo "long sql capped=", strlen($logs[2]["sql"]) <= 8192 ? "yes" : "no", "\n";
echo "long qid capped=", strlen($logs[3]["query_id"]) <= 8192 ? "yes" : "no", "\n";

$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.write_log");
$c->execute("CREATE TABLE test.write_log (id UInt32) ENGINE=Memory");
$c->getLogQueries();

$c->select("SELECT number FROM numbers(3)");
$c->getLogQueries();
$c->writeStart("test.write_log", ["id"]);
try {
    $c->select("SELECT 1");
    echo "active insert select: NO THROW\n";
} catch (ClickHouseException $e) {
    echo "active insert select: REJECTED\n";
}
$logs = $c->getLogQueries();
echo "active error rows_read=", $logs[0]["rows_read"], "\n";
echo "active error bytes_read=", $logs[0]["bytes_read"], "\n";
$c->resetConnection();
$c->getLogQueries();

$c->writeStart("test.write_log", ["id"]);
echo "after writeStart logs=", count($c->getLogQueries()), "\n";
$c->write([[1], [2]]);
$c->writeEnd();
$logs = $c->getLogQueries();
echo "after writeEnd logs=", count($logs), "\n";
echo "write log sql=", $logs[0]["sql"], "\n";
echo "write log error=", $logs[0]["error_code"], "\n";

$c->execute("DROP TABLE test.write_log");
?>
--EXPECT--
bad execute: REJECTED
log count=4
error rows_read=0
error bytes_read=0
long sql capped=yes
long qid capped=yes
active insert select: REJECTED
active error rows_read=0
active error bytes_read=0
after writeStart logs=0
after writeEnd logs=1
write log sql=INSERT INTO test.write_log ( id ) VALUES
write log error=0
