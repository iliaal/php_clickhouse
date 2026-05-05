--TEST--
ClickHouse UInt64 values above ZEND_LONG_MAX read back as decimal strings, not negative ints
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// Regression for round-11-followup CR-002: UInt64 values above
// ZEND_LONG_MAX (2^63-1) used to surface as negative PHP integers
// because the read path cast through (zend_long), losing the
// unsigned semantics. For Map(UInt64, *) keys this also collapsed
// distinct unsigned values into the same PHP-signed key. Now the
// scalar / Map-key / Map-value read paths emit a decimal string
// for any UInt64 > ZEND_LONG_MAX. Smaller values continue to
// surface as PHP int.

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
foreach (["u64_scalar","u64_map"] as $t) $c->execute("DROP TABLE IF EXISTS test.$t");
$c->execute("CREATE TABLE test.u64_scalar (u UInt64) ENGINE=Memory");
$c->execute("CREATE TABLE test.u64_map (m Map(UInt64, UInt64)) ENGINE=Memory");

// Insert via SQL so we don't need the Map insert path to round-trip
// huge unsigned values through PHP.
$c->execute("INSERT INTO test.u64_scalar VALUES (toUInt64(18446744073709551615)), (toUInt64(9223372036854775807)), (toUInt64(0))");
$c->execute("INSERT INTO test.u64_map VALUES (map(toUInt64(18446744073709551615), toUInt64(18446744073709551614)))");

$rows = $c->select("SELECT u FROM test.u64_scalar ORDER BY u DESC");
echo "max:      ", var_export($rows[0]['u'], true), "\n";
echo "midpoint: ", var_export($rows[1]['u'], true), "\n";
echo "zero:     ", var_export($rows[2]['u'], true), "\n";

$rows = $c->select("SELECT m FROM test.u64_map");
$key = array_keys($rows[0]['m'])[0];
$val = array_values($rows[0]['m'])[0];
echo "map key:   ", var_export($key, true), "\n";
echo "map value: ", var_export($val, true), "\n";

foreach (["u64_scalar","u64_map"] as $t) $c->execute("DROP TABLE test.$t");
?>
--EXPECT--
max:      '18446744073709551615'
midpoint: 9223372036854775807
zero:     0
map key:   '18446744073709551615'
map value: '18446744073709551614'
