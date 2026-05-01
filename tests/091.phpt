--TEST--
ClickHouse destructor on an in-flight streaming insert rolls back, does not commit
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// Regression for round-13-followup CR-001: clickhouse_free_obj() called
// EndInsert() unconditionally when has_insert_block was true. EndInsert()
// commits any blocks the session already sent — so writeStart() + write()
// followed by unset()/teardown without writeEnd() implicitly committed
// partial data. write()'s own catch path was already routed through
// ResetConnection() for dirty sessions in Round 13; the destructor now
// follows the same policy.

$cfg = clickhouse_test_config();
$c = new ClickHouse($cfg);
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.destruct_rollback");
$c->execute("CREATE TABLE test.destruct_rollback (id UInt32) ENGINE=Memory");

// Dirty session: rows sent, no writeEnd, then drop the handle.
$c2 = new ClickHouse($cfg);
$c2->writeStart("test.destruct_rollback", ["id"]);
$c2->write([[1], [2]]);
unset($c2);

$cnt = $c->select("SELECT count() FROM test.destruct_rollback", [], ClickHouse::FETCH_ONE);
echo "after dirty unset: $cnt\n";

// Clean session: writeStart but never wrote — destructor should still
// close the empty insert without throwing or wedging the server.
$c3 = new ClickHouse($cfg);
$c3->writeStart("test.destruct_rollback", ["id"]);
unset($c3);

$cnt = $c->select("SELECT count() FROM test.destruct_rollback", [], ClickHouse::FETCH_ONE);
echo "after clean unset: $cnt\n";

// Sanity: a complete cycle on a fresh handle still lands as expected.
$c4 = new ClickHouse($cfg);
$c4->writeStart("test.destruct_rollback", ["id"]);
$c4->write([[10], [20], [30]]);
$c4->writeEnd();
unset($c4);

$cnt = $c->select("SELECT count() FROM test.destruct_rollback", [], ClickHouse::FETCH_ONE);
echo "after full cycle: $cnt\n";

$c->execute("DROP TABLE test.destruct_rollback");
?>
--EXPECT--
after dirty unset: 0
after clean unset: 0
after full cycle: 3
