--TEST--
ClickHouse Map numeric keys reject non-numeric strings instead of silently coercing
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// Regression for CR-209: Map(Int*, *) / Map(UInt*, *) / Map(Float*, *)
// keys were parsed via strtoll/strtoull/strtod with no end-pointer
// check, so "abc" silently became 0 and "12x" silently became 12.

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.map_keys");
$c->execute("CREATE TABLE test.map_keys (id UInt8, m_i Map(Int64, String), m_u Map(UInt64, String), m_f Map(Float64, String)) ENGINE = Memory");

$probes = [
    "Int64 garbage key"  => [['m_i'],          [[['abc' => 'x']]]],
    "Int64 partial key"  => [['m_i'],          [[['12x' => 'x']]]],
    "UInt64 garbage key" => [['m_u'],          [[['abc' => 'x']]]],
    "Float64 garbage"    => [['m_f'],          [[['abc' => 'x']]]],
];
foreach ($probes as $label => [$cols, $vals]) {
    try { $c->insert("test.map_keys", ['id', ...$cols], [[1, ...$vals[0]]]); echo "$label: NO THROW\n"; }
    catch (ClickHouseException $e) { echo "$label: REJECTED\n"; }
}

// Sanity: valid numeric-string keys still work.
$c->insert("test.map_keys", ['id', 'm_i', 'm_u', 'm_f'],
    [[1, [1 => 'a', 2 => 'b'], [10 => 'x'], ['1.5' => 'y']]]);
$rows = $c->select("SELECT id FROM test.map_keys");
echo "ok rowcount: ", count($rows), "\n";
$c->execute("DROP TABLE test.map_keys");
?>
--EXPECT--
Int64 garbage key: REJECTED
Int64 partial key: REJECTED
UInt64 garbage key: REJECTED
Float64 garbage: REJECTED
ok rowcount: 1
