--TEST--
Int128/UInt128 string insert rejects out-of-range values instead of wrapping
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$ch = new ClickHouse(clickhouse_test_config());
$ch->execute("CREATE DATABASE IF NOT EXISTS test");
$ch->execute("DROP TABLE IF EXISTS test.b128");
$ch->execute("CREATE TABLE test.b128 (u UInt128, i Int128) ENGINE=Memory");

/* 4e38 is a 39-digit value above 2^128; the old post-multiply overflow
 * check missed it and stored a wrapped number. It must throw. */
try {
    $ch->insert("test.b128", ["u"], [["400000000000000000000000000000000000000"]]);
    echo "uint128 overflow: stored ", $ch->select("SELECT toString(u) AS u FROM test.b128")[0]["u"], "\n";
} catch (ClickHouseException $e) {
    echo "uint128 overflow: ", (strpos($e->getMessage(), "overflows the 128-bit range") !== false ? "rejected" : "other"), "\n";
}

/* A value in (2^127, 2^128) is a valid UInt128 but out of Int128 range. */
try {
    $ch->insert("test.b128", ["i"], [["200000000000000000000000000000000000000"]]);
    echo "int128 over: no throw\n";
} catch (ClickHouseException $e) {
    echo "int128 over: ", (strpos($e->getMessage(), "exceeds 2^127-1") !== false ? "rejected" : "other"), "\n";
}

/* The legitimate maxima still round-trip. */
$ch->execute("TRUNCATE TABLE test.b128");
$umax = "340282366920938463463374607431768211455"; // 2^128 - 1
$imax = "170141183460469231731687303715884105727"; // 2^127 - 1
$ch->insert("test.b128", ["u", "i"], [[$umax, $imax]]);
$row = $ch->select("SELECT toString(u) AS u, toString(i) AS i FROM test.b128")[0];
var_dump($row["u"] === $umax, $row["i"] === $imax);

$ch->execute("DROP TABLE test.b128");
?>
--EXPECT--
uint128 overflow: rejected
int128 over: rejected
bool(true)
bool(true)
