--TEST--
ClickHouse integer insert rejects out-of-range values for narrow column types
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// Regression for CR-208: appendIntColumn used to silently truncate
// inserts for Int8/Int16/UInt8/UInt16 etc. Now they throw.

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.range_t");
$c->execute("CREATE TABLE test.range_t (i8 Int8, u8 UInt8, i16 Int16, u16 UInt16) ENGINE = Memory");

$probes = [
    "Int8 too high"    => [['i8'], [[200]]],
    "Int8 too low"     => [['i8'], [[-200]]],
    "UInt8 too high"   => [['u8'], [[300]]],
    "UInt8 negative"   => [['u8'], [[-1]]],
    "Int16 too high"   => [['i16'], [[40000]]],
    "UInt16 too high"  => [['u16'], [[70000]]],
];
foreach ($probes as $label => [$cols, $vals]) {
    try { $c->insert("test.range_t", $cols, $vals); echo "$label: NO THROW\n"; }
    catch (ClickHouseException $e) { echo "$label: REJECTED\n"; }
}

// Sanity: in-range values land successfully.
$c->insert("test.range_t", ['i8', 'u8', 'i16', 'u16'], [[-128, 255, -32768, 65535]]);
$rows = $c->select("SELECT i8, u8, i16, u16 FROM test.range_t");
echo "ok rowcount: ", count($rows), "\n";
echo "i8: ",  $rows[0]["i8"],  "\n";
echo "u8: ",  $rows[0]["u8"],  "\n";
echo "i16: ", $rows[0]["i16"], "\n";
echo "u16: ", $rows[0]["u16"], "\n";
$c->execute("DROP TABLE test.range_t");
?>
--EXPECT--
Int8 too high: REJECTED
Int8 too low: REJECTED
UInt8 too high: REJECTED
UInt8 negative: REJECTED
Int16 too high: REJECTED
UInt16 too high: REJECTED
ok rowcount: 1
i8: -128
u8: 255
i16: -32768
u16: 65535
