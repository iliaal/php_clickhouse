--TEST--
ClickHouse BeginInsert() failure leaves the client usable for both insert() and writeStart()
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// Regression for round-16-followup CR-001: the vendored client sets
// its inserting_ flag before sending the BeginInsert query and before
// receiving the server's schema block, so a server-side error during
// that phase (missing table, bad column name, permissions) left the
// flag stuck. Both insert() and writeStart() called BeginInsert
// without recovery, so the next select/execute on the same handle
// threw "cannot execute query while inserting" until userland called
// resetConnection() by hand. Both call sites now ResetConnection() on
// BeginInsert failure.

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.begin_recover");
$c->execute("CREATE TABLE test.begin_recover (id UInt32) ENGINE=Memory");

// Path 1: direct insert() against a missing table.
try {
    $c->insert("test.no_such_table", ["id"], [[1]]);
    echo "direct insert missing table: NO THROW\n";
} catch (ClickHouseException $e) {
    echo "direct insert missing table: REJECTED\n";
}
$x = $c->select("SELECT 43 AS x", [], ClickHouse::FETCH_ONE);
echo "select after direct insert begin error: $x\n";

// Path 2: writeStart() against a missing table.
try {
    $c->writeStart("test.no_such_table", ["id"]);
    echo "writeStart missing table: NO THROW\n";
} catch (ClickHouseException $e) {
    echo "writeStart missing table: REJECTED\n";
}
$x = $c->select("SELECT 44 AS x", [], ClickHouse::FETCH_ONE);
echo "select after writeStart begin error: $x\n";

// Path 3: bad column on a real table.
try {
    $c->writeStart("test.begin_recover", ["nonexistent_col"]);
    echo "writeStart bad column: NO THROW\n";
} catch (ClickHouseException $e) {
    echo "writeStart bad column: REJECTED\n";
}
$x = $c->select("SELECT 45 AS x", [], ClickHouse::FETCH_ONE);
echo "select after writeStart bad column: $x\n";

// Sanity: a fresh streaming cycle on the same handle still works.
$c->writeStart("test.begin_recover", ["id"]);
$c->write([[10], [20]]);
$c->writeEnd();
$cnt = $c->select("SELECT count() FROM test.begin_recover", [], ClickHouse::FETCH_ONE);
echo "rowcount after fresh cycle: $cnt\n";

$c->execute("DROP TABLE test.begin_recover");
?>
--EXPECT--
direct insert missing table: REJECTED
select after direct insert begin error: 43
writeStart missing table: REJECTED
select after writeStart begin error: 44
writeStart bad column: REJECTED
select after writeStart bad column: 45
rowcount after fresh cycle: 2
