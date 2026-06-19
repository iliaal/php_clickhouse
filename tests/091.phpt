--TEST--
ClickHouse destructor on an in-flight streaming insert finalizes it (no rollback)
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// ClickHouse inserts are not transactional. A streaming insert that is
// abandoned without writeEnd() (script bailout, exception unwind, unset())
// is wound down by ~Client, which finalizes whatever the server accepted on
// the existing wire. The destructor does NOT reconnect-to-discard: that only
// ever dropped inserts small enough to still sit in the server squash buffer,
// so it was size-dependent and silently partial. Callers needing exactly-once
// must use explicit insert deduplication, not handle teardown.

$cfg = clickhouse_test_config();
$c = new ClickHouse($cfg);
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.destruct_finalize");
$c->execute("CREATE TABLE test.destruct_finalize (id UInt32) ENGINE=Memory");

// Dirty session: rows sent, no writeEnd, then drop the handle. The
// destructor finalizes the in-flight insert; the rows land.
$c2 = new ClickHouse($cfg);
$c2->writeStart("test.destruct_finalize", ["id"]);
$c2->write([[1], [2]]);
unset($c2);

$cnt = $c->select("SELECT count() FROM test.destruct_finalize", [], ClickHouse::FETCH_ONE);
echo "after dirty unset: $cnt\n";

// Clean session: writeStart but never wrote — destructor closes the empty
// insert without throwing or wedging the server. No rows added.
$c3 = new ClickHouse($cfg);
$c3->writeStart("test.destruct_finalize", ["id"]);
unset($c3);

$cnt = $c->select("SELECT count() FROM test.destruct_finalize", [], ClickHouse::FETCH_ONE);
echo "after clean unset: $cnt\n";

// Sanity: a complete cycle on a fresh handle still lands as expected.
$c4 = new ClickHouse($cfg);
$c4->writeStart("test.destruct_finalize", ["id"]);
$c4->write([[10], [20], [30]]);
$c4->writeEnd();
unset($c4);

$cnt = $c->select("SELECT count() FROM test.destruct_finalize", [], ClickHouse::FETCH_ONE);
echo "after full cycle: $cnt\n";

$c->execute("DROP TABLE test.destruct_finalize");
?>
--EXPECT--
after dirty unset: 2
after clean unset: 2
after full cycle: 5
