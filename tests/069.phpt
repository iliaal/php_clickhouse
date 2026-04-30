--TEST--
ClickHouse Int128 string insert rejects magnitudes outside [-2^127, 2^127-1]
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// Regression for CR-504: parse_uint128_dec accepts up to 2^128-1; the
// Int128 insert path then static_cast<Int128>(uint128) which silently
// wraps magnitudes in (2^127, 2^128-1] to negative. The reciprocal
// UInt128 path is correct because uint128 is its native range. Bound
// the magnitude per-sign before the cast; INT128_MIN gets a special
// case because -INT128_MIN is undefined behavior.

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.int128_t");
$c->execute("CREATE TABLE test.int128_t (v Int128) ENGINE = Memory");

$INT128_MAX = "170141183460469231731687303715884105727";   // 2^127 - 1
$INT128_MIN = "-170141183460469231731687303715884105728";  // -2^127
$OVER_MAX   = "170141183460469231731687303715884105728";   // 2^127
$UNDER_MIN  = "-170141183460469231731687303715884105729";  // -(2^127 + 1)
$UINT128_MAX_AS_INT_INPUT = "340282366920938463463374607431768211455"; // 2^128 - 1

$probes_ok = [
    "INT128_MAX"  => $INT128_MAX,
    "INT128_MIN"  => $INT128_MIN,
    "zero"        => "0",
    "small pos"   => "12345",
    "small neg"   => "-12345",
];
$probes_reject = [
    "over INT128_MAX"      => $OVER_MAX,
    "under INT128_MIN"     => $UNDER_MIN,
    "uint128 max as Int128" => $UINT128_MAX_AS_INT_INPUT,
];

foreach ($probes_ok as $label => $val) {
    try { $c->insert("test.int128_t", ["v"], [[$val]]); echo "$label: ACCEPTED\n"; }
    catch (ClickHouseException $e) { echo "$label: UNEXPECTED REJECT\n"; }
}
foreach ($probes_reject as $label => $val) {
    try { $c->insert("test.int128_t", ["v"], [[$val]]); echo "$label: NO THROW\n"; }
    catch (ClickHouseException $e) { echo "$label: REJECTED\n"; }
}

// Round-trip the boundary values.
$rows = $c->select("SELECT toString(v) AS s FROM test.int128_t ORDER BY v");
echo "rowcount: ", count($rows), "\n";
echo "min on wire: ", $rows[0]["s"], "\n";
echo "max on wire: ", $rows[count($rows)-1]["s"], "\n";

$c->execute("DROP TABLE test.int128_t");
?>
--EXPECT--
INT128_MAX: ACCEPTED
INT128_MIN: ACCEPTED
zero: ACCEPTED
small pos: ACCEPTED
small neg: ACCEPTED
over INT128_MAX: REJECTED
under INT128_MIN: REJECTED
uint128 max as Int128: REJECTED
rowcount: 5
min on wire: -170141183460469231731687303715884105728
max on wire: 170141183460469231731687303715884105727
