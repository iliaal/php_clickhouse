--TEST--
ClickHouse write() failure on a healthy wire finalizes prior blocks (no rollback)
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// ClickHouse inserts are not transactional. When a later write() throws
// during row conversion, the wire is still healthy and earlier blocks have
// already been streamed, so the catch path finalizes the insert (EndInsert)
// rather than reconnecting to fake a rollback. The earlier blocks commit;
// only the rejected row is dropped. The handle stays usable either way. This
// matches the destructor's finalize-on-teardown policy. A genuinely dirty
// wire (SendInsertBlock itself throwing mid-frame) still ResetConnection()s,
// but that is handle recovery, not rollback.

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.write_finalize");
$c->execute("CREATE TABLE test.write_finalize (id UInt32) ENGINE=Memory");

// Phase 1: send a successful block, then a write() that throws in
// conversion. The earlier block is finalized, not discarded.
$c->writeStart("test.write_finalize", ["id"]);
$c->write([[1], [2]]);
try {
    $c->write([[-1]]);
    echo "second write: NO THROW\n";
} catch (ClickHouseException $e) {
    echo "second write: REJECTED\n";
}

$cnt = $c->select("SELECT count() FROM test.write_finalize", [], ClickHouse::FETCH_ONE);
echo "rowcount after mid-stream failure: $cnt\n";

// Phase 2: handle is usable for a fresh cycle.
$c->writeStart("test.write_finalize", ["id"]);
$c->write([[10], [20], [30]]);
$c->writeEnd();
$cnt = $c->select("SELECT count() FROM test.write_finalize", [], ClickHouse::FETCH_ONE);
echo "rowcount after fresh cycle: $cnt\n";

// Phase 3: clean-session failure (no prior block sent) closes the empty
// insert and leaves the handle usable.
$c->writeStart("test.write_finalize", ["id"]);
try {
    $c->write([[-1]]);
    echo "clean-session write: NO THROW\n";
} catch (ClickHouseException $e) {
    echo "clean-session write: REJECTED\n";
}
$x = $c->select("SELECT 42 AS x", [], ClickHouse::FETCH_ONE);
echo "select after clean-session failure: $x\n";

$c->execute("DROP TABLE test.write_finalize");
?>
--EXPECT--
second write: REJECTED
rowcount after mid-stream failure: 2
rowcount after fresh cycle: 5
clean-session write: REJECTED
select after clean-session failure: 42
