--TEST--
DR-002: non-Nullable Decimal rejects PHP null (no silent 0 coercion); Nullable(Decimal) accepts it
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// A bare PHP null on a non-Nullable Decimal used to flow through ZStrGuard
// as "" and land as a silent 0 (ColumnDecimal parses "" to int_value 0),
// unlike the strict_zval_* scalar helpers which reject null by default.

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.dr002");
$c->execute("CREATE TABLE test.dr002 (d Decimal(10,2)) ENGINE = Memory");

try { $c->insert("test.dr002", ['d'], [[null]]); echo "non-nullable null: NO THROW\n"; }
catch (ClickHouseException $e) { echo "non-nullable null: REJECTED\n"; }

// A real (string) value still inserts and reads back.
$c->insert("test.dr002", ['d'], [["12.34"]]);
$rows = $c->select("SELECT toString(d) v FROM test.dr002");
echo "value: ", $rows[0]['v'], "\n";

// Nullable(Decimal) still accepts null via the AllowNullGuard path.
$c->execute("DROP TABLE IF EXISTS test.dr002n");
$c->execute("CREATE TABLE test.dr002n (d Nullable(Decimal(10,2))) ENGINE = Memory");
$c->insert("test.dr002n", ['d'], [[null], ["9.99"]]);
$rows = $c->select("SELECT isNull(d) AS n FROM test.dr002n ORDER BY n");
echo "nullable rows: ", json_encode(array_column($rows, 'n')), "\n";

$c->execute("DROP TABLE test.dr002");
$c->execute("DROP TABLE test.dr002n");
?>
--EXPECT--
non-nullable null: REJECTED
value: 12.34
nullable rows: [0,1]
