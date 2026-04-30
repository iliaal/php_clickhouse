--TEST--
ClickHouse Map insert rejects out-of-range values for narrow Int / UInt key and value columns
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// Regression for CR-503: Map insert dispatch used a single i64Val /
// u64Val extractor for every narrow column width, so Map(K, Int8) with
// PHP value 1000 silently truncated to int8_t -24 inside ColumnInt8::
// Append. The non-Map insert path has had per-width range checks since
// pass 1; the Map path was missed. Same gap on the key side: numeric
// keys parsed via strtoll were appended to ColumnInt8 keys without a
// width check.

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.map_range_t");
$c->execute("CREATE TABLE test.map_range_t (
    m_i8  Map(String, Int8),
    m_u8  Map(String, UInt8),
    m_i16 Map(String, Int16),
    m_u16 Map(String, UInt16),
    k_i8  Map(Int8, String),
    k_u16 Map(UInt16, String)
) ENGINE = Memory");

$probes = [
    "Map value Int8 too high"   => [['m_i8'],  [[['k' => 200]]]],
    "Map value Int8 too low"    => [['m_i8'],  [[['k' => -200]]]],
    "Map value UInt8 too high"  => [['m_u8'],  [[['k' => 300]]]],
    "Map value UInt8 negative"  => [['m_u8'],  [[['k' => -1]]]],
    "Map value Int16 too high"  => [['m_i16'], [[['k' => 40000]]]],
    "Map value UInt16 too high" => [['m_u16'], [[['k' => 70000]]]],
    "Map key Int8 too high"     => [['k_i8'],  [[[200 => 'x']]]],
    "Map key UInt16 too high"   => [['k_u16'], [[[70000 => 'x']]]],
];
foreach ($probes as $label => [$cols, $vals]) {
    try { $c->insert("test.map_range_t", $cols, $vals); echo "$label: NO THROW\n"; }
    catch (ClickHouseException $e) { echo "$label: REJECTED\n"; }
}

// Sanity: in-range maps still land.
$c->insert("test.map_range_t",
    ['m_i8', 'm_u8', 'm_i16', 'm_u16', 'k_i8', 'k_u16'],
    [[
        ['a' => -128, 'b' => 127],
        ['a' => 0, 'b' => 255],
        ['a' => -32768, 'b' => 32767],
        ['a' => 0, 'b' => 65535],
        [-128 => 'a', 127 => 'b'],
        [0 => 'a', 65535 => 'b'],
    ]]);
$rows = $c->select("SELECT count() AS c FROM test.map_range_t", [], ClickHouse::FETCH_ONE);
echo "ok rowcount: ", $rows, "\n";
$c->execute("DROP TABLE test.map_range_t");
?>
--EXPECT--
Map value Int8 too high: REJECTED
Map value Int8 too low: REJECTED
Map value UInt8 too high: REJECTED
Map value UInt8 negative: REJECTED
Map value Int16 too high: REJECTED
Map value UInt16 too high: REJECTED
Map key Int8 too high: REJECTED
Map key UInt16 too high: REJECTED
ok rowcount: 1
