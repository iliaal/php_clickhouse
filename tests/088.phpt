--TEST--
ClickHouse write() rejects rows narrower than the writeStart() column count
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// Regression for round-12-followup CR-001: write() used to derive
// columns_count from the first row, so a row narrower than the
// writeStart() declaration silently sent a partial block. The server
// then materialized the missing columns from their defaults and the
// caller saw a successful write of truncated data. write() now uses
// the BeginInsert block's column count as the authoritative width
// and rejects narrow rows up front.

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.write_short");
$c->execute("CREATE TABLE test.write_short (a UInt8, b UInt8) ENGINE=Memory");

$probes = [
    "short first row"        => [[[1]]],
    "short later row"        => [[[1, 2], [3]]],
    "short first row 2cols"  => [[["a"=>1]]],
];
foreach ($probes as $label => [$rows]) {
    $c->writeStart("test.write_short", ["a", "b"]);
    try {
        $c->write($rows);
        echo "$label: NO THROW\n";
    } catch (ClickHouseException $e) {
        echo "$label: REJECTED\n";
    }
    // Recovery: client should be reusable for a fresh writeStart cycle.
    try { $c->execute("SELECT 1"); }
    catch (ClickHouseException $e) { echo "$label: client wedged: ", $e->getMessage(), "\n"; }
}

// Sanity: well-formed write still lands cleanly on the same handle.
$c->writeStart("test.write_short", ["a", "b"]);
$c->write([[10, 20], [11, 21]]);
$c->writeEnd();
$cnt = $c->select("SELECT count() FROM test.write_short", [], ClickHouse::FETCH_ONE);
echo "rowcount: $cnt\n";

$c->execute("DROP TABLE test.write_short");
?>
--EXPECT--
short first row: REJECTED
short later row: REJECTED
short first row 2cols: REJECTED
rowcount: 2
