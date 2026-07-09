--TEST--
DR-001: non-Nullable JSON rejects PHP null (no silent {} coercion); Nullable(JSON) accepts it
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); clickhouse_skip_if_no_json(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// A bare PHP null on a non-Nullable JSON column used to be stored silently
// as the empty object {} (the Nullable placeholder), unlike Bool/IPv4/String
// which reject null unless the AllowNullGuard is active.

$c = new ClickHouse(clickhouse_test_config());
$c->setSettings([
    "allow_experimental_json_type"              => 1,
    "output_format_native_write_json_as_string" => 1,
]);
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.dr001");
$c->execute("CREATE TABLE test.dr001 (j JSON) ENGINE = Memory");

try { $c->insert("test.dr001", ['j'], [[null]]); echo "non-nullable null: NO THROW\n"; }
catch (ClickHouseException $e) { echo "non-nullable null: REJECTED\n"; }

// A real JSON value still inserts and reads back.
$c->insert("test.dr001", ['j'], [['{"ok":1}']]);
$rows = $c->select("SELECT toString(j) v FROM test.dr001");
echo "rowcount: ", count($rows), " value: ", $rows[0]['v'], "\n";

// Nullable(JSON) still accepts null via the AllowNullGuard path.
$c->execute("DROP TABLE IF EXISTS test.dr001n");
$c->execute("CREATE TABLE test.dr001n (j Nullable(JSON)) ENGINE = Memory");
$c->insert("test.dr001n", ['j'], [[null], ['{"x":2}']]);
$rows = $c->select("SELECT isNull(j) AS n FROM test.dr001n ORDER BY n");
echo "nullable rows: ", json_encode(array_column($rows, 'n')), "\n";

$c->execute("DROP TABLE test.dr001");
$c->execute("DROP TABLE test.dr001n");
?>
--EXPECT--
non-nullable null: REJECTED
rowcount: 1 value: {"ok":1}
nullable rows: [0,1]
