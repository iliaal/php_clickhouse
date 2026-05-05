--TEST--
ClickHouse UInt64 inserts accept decimal and hex strings above ZEND_LONG_MAX, scalar and Map
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// Regression for round-12-followup CR-003: scalar UInt64 inserts
// accepted hex strings ("0xFFFF...") for upper-half values but not
// decimal strings, and Map(*, UInt64) values rejected both forms —
// strict_zval_long capped at ZEND_LONG_MAX. The new strict_zval_u64
// parser handles long, decimal string, and hex string with the same
// full-consumption discipline strict_zval_long uses, and is shared
// across the scalar and Map UInt64 insert paths.

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
foreach (["u64s", "u64m"] as $t) $c->execute("DROP TABLE IF EXISTS test.$t");
$c->execute("CREATE TABLE test.u64s (u UInt64) ENGINE=Memory");
$c->execute("CREATE TABLE test.u64m (m Map(String, UInt64)) ENGINE=Memory");

// Scalar UInt64: long, decimal string, hex string, all upper-half.
$c->insert("test.u64s", ["u"], [
    [9223372036854775807],            // ZEND_LONG_MAX as long
    ["18446744073709551615"],         // 2^64-1 as decimal string
    ["0xFFFFFFFFFFFFFFFE"],           // 2^64-2 as hex
    [0],
]);
$rows = $c->select("SELECT u FROM test.u64s ORDER BY u DESC");
foreach ($rows as $r) echo "scalar: ", var_export($r['u'], true), "\n";

// Bad scalar inputs still reject.
$bad = [
    "negative long"     => [-1],
    "negative decimal"  => ["-1"],
    "decimal w/ junk"   => ["12345abc"],
    "hex w/ junk"       => ["0xFFFFGG"],
    "empty string"      => [""],
    "fractional double" => [1.5],
    "out-of-range dec"  => ["18446744073709551616"], // 2^64
];
foreach ($bad as $label => $vals) {
    try { $c->insert("test.u64s", ["u"], [$vals]); echo "scalar bad $label: NO THROW\n"; }
    catch (ClickHouseException $e) { echo "scalar bad $label: REJECTED\n"; }
}

// Map(String, UInt64): values as long, decimal string, hex string.
$c->insert("test.u64m", ["m"], [[[
    "a" => 9223372036854775807,
    "b" => "18446744073709551615",
    "c" => "0xFFFFFFFFFFFFFFFE",
    "d" => 0,
]]]);
$row = $c->select("SELECT m FROM test.u64m")[0];
ksort($row['m']);
foreach ($row['m'] as $k => $v) echo "map[$k]: ", var_export($v, true), "\n";

// Map bad inputs still reject.
try { $c->insert("test.u64m", ["m"], [[["x" => -1]]]); echo "map negative: NO THROW\n"; }
catch (ClickHouseException $e) { echo "map negative: REJECTED\n"; }
try { $c->insert("test.u64m", ["m"], [[["x" => "not-a-number"]]]); echo "map junk: NO THROW\n"; }
catch (ClickHouseException $e) { echo "map junk: REJECTED\n"; }

foreach (["u64s", "u64m"] as $t) $c->execute("DROP TABLE test.$t");
?>
--EXPECT--
scalar: '18446744073709551615'
scalar: '18446744073709551614'
scalar: 9223372036854775807
scalar: 0
scalar bad negative long: REJECTED
scalar bad negative decimal: REJECTED
scalar bad decimal w/ junk: REJECTED
scalar bad hex w/ junk: REJECTED
scalar bad empty string: REJECTED
scalar bad fractional double: REJECTED
scalar bad out-of-range dec: REJECTED
map[a]: 9223372036854775807
map[b]: '18446744073709551615'
map[c]: '18446744073709551614'
map[d]: 0
map negative: REJECTED
map junk: REJECTED
