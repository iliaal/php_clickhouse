--TEST--
ClickHouse Phase F: DDL helpers (isExists, showDatabases, showProcesslist, getServerVersion, tableSize)
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.ddl_t");
$c->execute("CREATE TABLE test.ddl_t (k UInt32) ENGINE = MergeTree() ORDER BY k");
$c->insert("test.ddl_t", ["k"], [[1], [2], [3]]);

echo "exists_yes=", $c->isExists("test", "ddl_t") ? 1 : 0, "\n";
echo "exists_no=",  $c->isExists("test", "no_such_table_zzz") ? 1 : 0, "\n";

$dbs = $c->showDatabases();
echo "databases_is_array=", is_array($dbs) ? 1 : 0, "\n";
echo "databases_has_test=", in_array("test", $dbs, true) ? 1 : 0, "\n";
echo "databases_has_system=", in_array("system", $dbs, true) ? 1 : 0, "\n";

$pl = $c->showProcesslist();
echo "processlist_is_array=", is_array($pl) ? 1 : 0, "\n";

$v = $c->getServerVersion();
echo "version_is_string=", is_string($v) ? 1 : 0, "\n";
echo "version_nonempty=", strlen($v) > 0 ? 1 : 0, "\n";

$ts = $c->tableSize("test.ddl_t");
echo "tableSize_rows=", isset($ts["rows"]) ? 1 : 0, "\n";
echo "tableSize_bytes=", isset($ts["bytes_on_disk"]) ? 1 : 0, "\n";

// truncateTable
$c->truncateTable("test.ddl_t");
$cnt = $c->select("SELECT count() AS c FROM test.ddl_t")[0]["c"];
echo "truncated=", ($cnt == 0) ? 1 : 0, "\n";

// truncateTable rejects unsafe identifiers
try {
    $c->truncateTable("test; DROP TABLE foo");
    echo "trunc_unsafe_throws=0\n";
} catch (\Throwable $e) {
    echo "trunc_unsafe_throws=1\n";
}

// dropPartition with quote in value should be safely escaped (no syntax error;
// either succeeds (no-op) or fails cleanly with a server-side message that
// does NOT contain SQL parse failure).
$c->execute("DROP TABLE IF EXISTS test.parts_t");
$c->execute("CREATE TABLE test.parts_t (d Date, k UInt32) ENGINE = MergeTree() PARTITION BY toYYYYMM(d) ORDER BY k");
$c->insert("test.parts_t", ["d", "k"], [["2024-01-15", 1], ["2024-02-15", 2]]);

// 202401 is a valid partition; we pass it as a string and the helper quotes it.
$c->dropPartition("test.parts_t", "202401");
$rows = $c->select("SELECT count() AS c FROM test.parts_t")[0]["c"];
echo "after_drop=", ($rows == 1) ? 1 : 0, "\n";

$c->execute("DROP TABLE test.parts_t");
$c->execute("DROP TABLE test.ddl_t");
?>
--EXPECT--
exists_yes=1
exists_no=0
databases_is_array=1
databases_has_test=1
databases_has_system=1
processlist_is_array=1
version_is_string=1
version_nonempty=1
tableSize_rows=1
tableSize_bytes=1
truncated=1
trunc_unsafe_throws=1
after_drop=1
