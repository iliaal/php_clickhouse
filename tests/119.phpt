--TEST--
ClickHouse helper regressions: quoted database showTables, resetConnection database, tableSize miss, uptime arity
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");

$c->execute("DROP DATABASE IF EXISTS `test-hyphen`");
$c->execute("CREATE DATABASE `test-hyphen`");
$c->execute("CREATE TABLE `test-hyphen`.`t-hyphen` (id UInt8) ENGINE=MergeTree ORDER BY id");
$c->execute("INSERT INTO `test-hyphen`.`t-hyphen` SELECT 1");
$tables = $c->showTables("test-hyphen");
echo "hyphen tables=", implode(",", $tables), "\n";

$hyphen = new ClickHouse(array_merge(clickhouse_test_config(), ["database" => "test-hyphen"]));
$hyphen->resetConnection();
echo "hyphen current after reset=", $hyphen->select("SELECT currentDatabase()", [], ClickHouse::FETCH_ONE), "\n";
$c->setDatabase("test-hyphen");
echo "hyphen current after setDatabase=", $c->select("SELECT currentDatabase()", [], ClickHouse::FETCH_ONE), "\n";
echo "hyphen databaseSize rows>=1=", ($c->databaseSize()["rows"] >= 1 ? "yes" : "no"), "\n";
$hyphenTablesSize = $c->tablesSize();
echo "hyphen tablesSize has_t=", (in_array("t-hyphen", array_column($hyphenTablesSize, "table")) ? "yes" : "no"), "\n";
echo "hyphen tableSize rows=", $c->tableSize("t-hyphen")["rows"], "\n";
echo "hyphen partitions rows=", count($c->partitions("t-hyphen")), "\n";
$c->setDatabase("test");

$c->execute("CREATE DATABASE IF NOT EXISTS test_reset_db");
$c->execute("DROP TABLE IF EXISTS test_reset_db.t");
$c->execute("CREATE TABLE test_reset_db.t (id UInt8) ENGINE=Memory");
$c->setDatabase("test_reset_db");
$c->resetConnection();
echo "current after reset=", $c->select("SELECT currentDatabase()", [], ClickHouse::FETCH_ONE), "\n";
try {
    $c->insert("missing_after_reset", ["id"], [[1]]);
    echo "failed insert: NO THROW\n";
} catch (ClickHouseException $e) {
    echo "failed insert: REJECTED\n";
}
echo "current after failed insert=", $c->select("SELECT currentDatabase()", [], ClickHouse::FETCH_ONE), "\n";
$c->setDatabase("test");
$c->execute("DROP DATABASE test_reset_db");

$missing = $c->tableSize("test.no_such_table_119");
echo "missing tableSize count=", count($missing), "\n";

$c->execute("DROP DATABASE `test-hyphen`");

try {
    $c->getServerUptime("extra");
    echo "uptime extra: NO THROW\n";
} catch (Throwable $e) {
    echo "uptime extra: REJECTED\n";
}
?>
--EXPECT--
hyphen tables=t-hyphen
hyphen current after reset=test-hyphen
hyphen current after setDatabase=test-hyphen
hyphen databaseSize rows>=1=yes
hyphen tablesSize has_t=yes
hyphen tableSize rows=1
hyphen partitions rows=1
current after reset=test_reset_db
failed insert: REJECTED
current after failed insert=test_reset_db
missing tableSize count=0
uptime extra: REJECTED
