--TEST--
ClickHouse numeric inserts reject non-numeric strings, fractions, NaN/Inf, and out-of-range hex
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// Regression for CR-003: appendIntColumn / appendUIntColumnWithHex /
// appendFloatColumn used zval_get_long / zval_get_double, which silently
// coerce "abc" → 0, [] → 1, and accept fractional / NaN / Inf doubles
// for integer columns. Hex literals "0x100000000" silently truncated to
// UInt32 0. The strict_zval_long / strict_zval_double helpers reject
// non-numeric strings, fractional doubles for ints, and non-finite
// doubles; the hex path width-checks against MaxV.

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.coerce");
$c->execute("CREATE TABLE test.coerce (
    i32 Int32, u32 UInt32, u64 UInt64, f32 Float32, f64 Float64
) ENGINE=Memory");

$probes = [
    "Int32 from 'abc'"          => [['i32'], [['abc']]],
    "Int32 from 'abc123'"       => [['i32'], [['abc123']]],
    "Int32 from '123abc'"       => [['i32'], [['123abc']]],
    "Int32 from empty string"   => [['i32'], [['']]],
    "Int32 from 1.5 (frac)"     => [['i32'], [[1.5]]],
    "Int32 from NaN"            => [['i32'], [[NAN]]],
    "Int32 from Inf"            => [['i32'], [[INF]]],
    "Int32 from array"          => [['i32'], [[[1, 2]]]],
    "UInt32 hex out of range"   => [['u32'], [['0x100000000']]],
    "UInt32 decimal too big"    => [['u32'], [['5000000000']]],
    "UInt64 from 'xyz'"         => [['u64'], [['xyz']]],
    "Float32 from 'abc'"        => [['f32'], [['abc']]],
    "Float32 from NaN"          => [['f32'], [[NAN]]],
    "Float64 from Inf"          => [['f64'], [[INF]]],
    "Float64 from '1.5x'"       => [['f64'], [['1.5x']]],
];
foreach ($probes as $label => [$cols, $vals]) {
    try { $c->insert("test.coerce", $cols, $vals); echo "$label: NO THROW\n"; }
    catch (ClickHouseException $e) { echo "$label: REJECTED\n"; }
}

// Sanity: well-formed values still land.
$c->insert("test.coerce", ['i32', 'u32', 'u64', 'f32', 'f64'],
    [[42, 4000000000, '0xDEADBEEFCAFEBABE', 1.5, 2.5]]);
$rows = $c->select("SELECT count() FROM test.coerce", [], ClickHouse::FETCH_ONE);
echo "rowcount: $rows\n";

$c->execute("DROP TABLE test.coerce");
?>
--EXPECT--
Int32 from 'abc': REJECTED
Int32 from 'abc123': REJECTED
Int32 from '123abc': REJECTED
Int32 from empty string: REJECTED
Int32 from 1.5 (frac): REJECTED
Int32 from NaN: REJECTED
Int32 from Inf: REJECTED
Int32 from array: REJECTED
UInt32 hex out of range: REJECTED
UInt32 decimal too big: REJECTED
UInt64 from 'xyz': REJECTED
Float32 from 'abc': REJECTED
Float32 from NaN: REJECTED
Float64 from Inf: REJECTED
Float64 from '1.5x': REJECTED
rowcount: 1
