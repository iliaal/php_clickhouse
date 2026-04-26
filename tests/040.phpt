--TEST--
ClickHouse SQL helper methods (databaseSize, tablesSize, partitions, showTables, showCreateTable, getServerUptime)
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";
$c = new ClickHouse(clickhouse_test_config() + ["database" => "test"]);

$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.helpers_a");
$c->execute("DROP TABLE IF EXISTS test.helpers_b");
$c->execute("CREATE TABLE test.helpers_a (id UInt32) ENGINE = MergeTree ORDER BY id");
$c->execute("CREATE TABLE test.helpers_b (n UInt64) ENGINE = MergeTree ORDER BY n");
$c->insert("test.helpers_a", ["id"], [[1], [2], [3]]);
$c->insert("test.helpers_b", ["n"],  [[10], [20]]);

// showTables filters by database; default-database aware.
$tables = $c->showTables("test", "helpers_%");
echo "showTables: ",  implode(",", $tables), "\n";

// showCreateTable returns the DDL string; just check the leading prefix.
$ddl = $c->showCreateTable("test.helpers_a");
echo "showCreateTable starts: ", (strpos($ddl, "CREATE TABLE test.helpers_a") === 0 ? "yes" : "no"), "\n";

// databaseSize: rows summed across active parts.
$sz = $c->databaseSize("test");
echo "databaseSize rows>=5: ", ($sz["rows"] >= 5 ? "yes" : "no"), "\n";
echo "databaseSize bytes>0: ", ($sz["bytes_on_disk"] > 0 ? "yes" : "no"), "\n";

// tablesSize lists the helper tables.
$ts = $c->tablesSize("test");
$names = array_column($ts, "table");
echo "tablesSize has helpers_a: ", (in_array("helpers_a", $names) ? "yes" : "no"), "\n";
echo "tablesSize has helpers_b: ", (in_array("helpers_b", $names) ? "yes" : "no"), "\n";

// partitions: at least one row for the populated table.
$parts = $c->partitions("test.helpers_a");
echo "partitions rows: ", count($parts), "\n";

// getServerUptime returns a non-negative integer.
$up = $c->getServerUptime();
echo "uptime>=0: ", ($up >= 0 ? "yes" : "no"), "\n";

// Identifier validation: malicious-looking arg rejected.
try {
    $c->showCreateTable("foo' OR 1=1 --");
    echo "bad ident: no throw\n";
} catch (ClickHouseException $e) {
    echo "bad ident: throw\n";
}

$c->execute("DROP TABLE test.helpers_a");
$c->execute("DROP TABLE test.helpers_b");
?>
--EXPECT--
showTables: helpers_a,helpers_b
showCreateTable starts: yes
databaseSize rows>=5: yes
databaseSize bytes>0: yes
tablesSize has helpers_a: yes
tablesSize has helpers_b: yes
partitions rows: 1
uptime>=0: yes
bad ident: throw
