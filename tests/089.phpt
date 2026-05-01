--TEST--
ClickHouse write() failure after prior blocks rolls back instead of partial-committing
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// Regression for round-12-followup CR-002: round 12 added an
// EndInsert() best-effort to write()'s catch path so the next
// select/execute on the same client wouldn't wedge with "cannot
// execute query while inserting". That fix was correct for a write()
// that fails before any block has been sent, but EndInsert() on a
// session that already shipped one or more blocks finalizes the
// native insert — turning a thrown write() into a silent partial
// commit. The recovery now branches: ResetConnection() once any
// block has been sent, EndInsert() only on a still-clean session.

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.write_rollback");
$c->execute("CREATE TABLE test.write_rollback (id UInt32) ENGINE=Memory");

// Phase 1: send a successful block, then a write() that throws.
// Without the rollback the first block would survive in the table.
$c->writeStart("test.write_rollback", ["id"]);
$c->write([[1], [2]]);
try {
    $c->write([[-1]]);
    echo "second write: NO THROW\n";
} catch (ClickHouseException $e) {
    echo "second write: REJECTED\n";
}

$cnt = $c->select("SELECT count() FROM test.write_rollback", [], ClickHouse::FETCH_ONE);
echo "rowcount after rollback: $cnt\n";

// Phase 2: connection should be reusable (ResetConnection reconnects).
$c->writeStart("test.write_rollback", ["id"]);
$c->write([[10], [20], [30]]);
$c->writeEnd();
$cnt = $c->select("SELECT count() FROM test.write_rollback", [], ClickHouse::FETCH_ONE);
echo "rowcount after fresh cycle: $cnt\n";

// Phase 3: clean-session failure (no prior block sent) still works
// — EndInsert path, no reset needed.
$c->writeStart("test.write_rollback", ["id"]);
try {
    $c->write([[-1]]);
    echo "clean-session write: NO THROW\n";
} catch (ClickHouseException $e) {
    echo "clean-session write: REJECTED\n";
}
$x = $c->select("SELECT 42 AS x", [], ClickHouse::FETCH_ONE);
echo "select after clean-session failure: $x\n";

$c->execute("DROP TABLE test.write_rollback");
?>
--EXPECT--
second write: REJECTED
rowcount after rollback: 0
rowcount after fresh cycle: 3
clean-session write: REJECTED
select after clean-session failure: 42
