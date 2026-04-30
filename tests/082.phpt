--TEST--
ClickHouse non-Nullable numeric / temporal / map columns reject NULL inserts
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// Regression for round-9-followup CR-002: strict_zval_long /
// strict_zval_double historically returned 0 / 0.0 for IS_NULL, so a
// PHP `null` silently landed as 0 (Int32), 0.0 (Float64), epoch (Date),
// or midnight (Time) in NON-NULLABLE columns. The strict helpers now
// reject NULL by default; the Nullable insert path bumps a thread-local
// guard so its recursive child build accepts NULL → typed-zero
// placeholder while the null mask captures the actual NULL.

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.nn_strict");
$c->execute("CREATE TABLE test.nn_strict (
    i Int32, f Float64, d Date, dt DateTime, t Time, m Map(String,UInt8)
) ENGINE=Memory");

$probes = [
    "Int32 null"   => [['i'],  [[null]]],
    "Float64 null" => [['f'],  [[null]]],
    "Date null"    => [['d'],  [[null]]],
    "DateTime null"=> [['dt'], [[null]]],
    "Time null"    => [['t'],  [[null]]],
    "Map null val" => [['m'],  [[['k' => null]]]],
];
foreach ($probes as $label => [$cols, $vals]) {
    try { $c->insert("test.nn_strict", $cols, $vals); echo "$label: NO THROW\n"; }
    catch (ClickHouseException $e) { echo "$label: REJECTED\n"; }
}

// Sanity: Nullable variants still accept NULL via the AllowNullGuard path.
$c->execute("DROP TABLE IF EXISTS test.nn_ok");
$c->execute("CREATE TABLE test.nn_ok (
    i Nullable(Int32), f Nullable(Float64), d Nullable(Date)
) ENGINE=Memory");
$c->insert("test.nn_ok", ['i','f','d'], [[null, null, null], [42, 1.5, '2024-01-15']]);
$rows = $c->select("SELECT i, f, toString(d) AS d FROM test.nn_ok ORDER BY i NULLS FIRST");
echo "nullable rowcount: ", count($rows), "\n";
echo "nullable null row: ", json_encode($rows[0]), "\n";
echo "nullable real row: ", json_encode($rows[1]), "\n";

$c->execute("DROP TABLE test.nn_strict");
$c->execute("DROP TABLE test.nn_ok");
?>
--EXPECT--
Int32 null: REJECTED
Float64 null: REJECTED
Date null: REJECTED
DateTime null: REJECTED
Time null: REJECTED
Map null val: REJECTED
nullable rowcount: 2
nullable null row: {"i":null,"f":null,"d":null}
nullable real row: {"i":42,"f":1.5,"d":"2024-01-15"}
