--TEST--
ClickHouse ping() during an open streaming insert is rejected, insert survives
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// Regression for CR-001: writeStart() opens an insert that holds the wire
// in insert mode for the whole writeStart..writeEnd span, but query_active
// is only set for the duration of each individual call. ping() therefore
// used to reach the vendored client mid-insert and surface its cryptic
// "cannot execute query" error. Reject it up front with the same message
// every other query path uses, and prove the insert is unaffected.

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.cr155");
$c->execute("CREATE TABLE test.cr155 (a UInt32) ENGINE = Memory");

$c->writeStart("test.cr155", ["a"]);

try {
    $c->ping();
    echo "ping: no throw\n";
} catch (ClickHouseException $e) {
    echo "ping: ", $e->getMessage(), "\n";
}

// The streaming insert must still be intact and completable.
$c->write([[1], [2], [3]]);
$c->writeEnd();

$n = $c->select("SELECT count() FROM test.cr155", [], ClickHouse::FETCH_ONE);
echo "rows: $n\n";

// ping() works normally once the insert is closed.
var_dump($c->ping());

$c->execute("DROP TABLE test.cr155");
?>
--EXPECT--
ping: The insert operation is now in progress
rows: 3
bool(true)
