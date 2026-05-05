--TEST--
ClickHouse Enum8 / Enum16 inserts reject undeclared integers, NULL on non-Nullable, and unknown names
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// Regression for round-10-followup CR-001: appendEnumColumn used the
// unchecked numeric overload of ColumnEnum*::Append, so integer cells
// like 0, 3, or 127 silently landed inside an
// Enum8('One'=1,'Two'=2) column. Reads through the extension then
// threw `map::at` when the read path looked up the name for the
// stored numeric. NULL took the same path as integer 0. Now the
// numeric input is validated against the type's declared value set
// via EnumType::HasEnumValue, NULL is rejected unless the parent
// Nullable wrapper has bumped AllowNullGuard, and unknown string
// names continue to throw via Append(name).

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.enum_strict");
$c->execute("DROP TABLE IF EXISTS test.enum_nullable");
$c->execute("CREATE TABLE test.enum_strict (e Enum8('One'=1,'Two'=2)) ENGINE=Memory");
$c->execute("CREATE TABLE test.enum_nullable (e Nullable(Enum8('One'=1,'Two'=2))) ENGINE=Memory");

$probes_reject = [
    "Enum int 0 (not declared)"   => [['e'], [[0]]],
    "Enum int 3 (not declared)"   => [['e'], [[3]]],
    "Enum int 127 (not declared)" => [['e'], [[127]]],
    "Enum int -1"                 => [['e'], [[-1]]],
    "Enum int 999"                => [['e'], [[999]]],
    "Enum NULL on non-Nullable"   => [['e'], [[null]]],
    "Enum unknown name"           => [['e'], [['Three']]],
    "Enum empty name"             => [['e'], [['']]],
];
foreach ($probes_reject as $label => [$cols, $vals]) {
    try { $c->insert("test.enum_strict", $cols, $vals); echo "$label: NO THROW\n"; }
    catch (ClickHouseException $e) { echo "$label: REJECTED\n"; }
}

// Sanity: declared values land cleanly via either int or name, and
// reads round-trip without map::at.
$c->insert("test.enum_strict", ['e'], [[1], [2], ['One'], ['Two']]);
$rows = $c->select("SELECT e FROM test.enum_strict ORDER BY e");
echo "strict rowcount: ", count($rows), "\n";
echo "strict values: ", implode(",", array_column($rows, 'e')), "\n";

// Nullable enum: NULL flows through the AllowNullGuard path and the
// null mask captures it; declared values still validate.
$c->insert("test.enum_nullable", ['e'], [[null], [1], ['Two']]);
$rows = $c->select("SELECT e FROM test.enum_nullable ORDER BY e NULLS FIRST");
echo "nullable rowcount: ", count($rows), "\n";
echo "nullable values: ", json_encode(array_column($rows, 'e')), "\n";

// Nullable enum with bad integer still rejects (the AllowNullGuard
// only silences IS_NULL, not undeclared numerics).
try {
    $c->insert("test.enum_nullable", ['e'], [[127]]);
    echo "Nullable bad int: NO THROW\n";
} catch (ClickHouseException $e) { echo "Nullable bad int: REJECTED\n"; }

$c->execute("DROP TABLE test.enum_strict");
$c->execute("DROP TABLE test.enum_nullable");
?>
--EXPECT--
Enum int 0 (not declared): REJECTED
Enum int 3 (not declared): REJECTED
Enum int 127 (not declared): REJECTED
Enum int -1: REJECTED
Enum int 999: REJECTED
Enum NULL on non-Nullable: REJECTED
Enum unknown name: REJECTED
Enum empty name: REJECTED
strict rowcount: 4
strict values: One,One,Two,Two
nullable rowcount: 3
nullable values: [null,"One","Two"]
Nullable bad int: REJECTED
