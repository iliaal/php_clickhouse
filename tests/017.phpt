--TEST--
ClickHouse Int128 / UInt128 / Decimal128 string round-trip
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.bignum_t");
$c->execute("CREATE TABLE test.bignum_t (
    id UInt32,
    i128 Int128,
    u128 UInt128,
    dec128 Decimal128(10)
) ENGINE = Memory");

$c->insert("test.bignum_t", ["id", "i128", "u128", "dec128"], [
    [1, "170141183460469231731687303715884105727", "340282366920938463463374607431768211455", "12345.6789012345"],
    [2, "-170141183460469231731687303715884105727", "0", "-99999.9999999999"],
    [3, 42, 42, "0"],
]);

$rows = $c->select("SELECT id, i128, u128, dec128 FROM test.bignum_t ORDER BY id");
foreach ($rows as $r) {
    echo $r["id"], "|", $r["i128"], "|", $r["u128"], "|", $r["dec128"], "\n";
}

$c->execute("DROP TABLE test.bignum_t");
?>
--EXPECT--
1|170141183460469231731687303715884105727|340282366920938463463374607431768211455|12345.6789012345
2|-170141183460469231731687303715884105727|0|-99999.9999999999
3|42|42|0.0000000000
