--TEST--
ClickHouse insert/writeStart reject unsafe table and column identifiers
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";
$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.id_validate");
$c->execute("CREATE TABLE test.id_validate (id UInt32) ENGINE = Memory");

function expect_throw(string $label, callable $fn): void {
    try { $fn(); echo $label, ": no throw\n"; }
    catch (ClickHouseException $e) { echo $label, ": throw\n"; }
}

expect_throw("table semicolon", function () use ($c) {
    $c->insert("test.id_validate; DROP TABLE test.id_validate", ["id"], [[1]]);
});
expect_throw("column quote", function () use ($c) {
    $c->insert("test.id_validate", ["id', x"], [[1, 2]]);
});
expect_throw("table empty", function () use ($c) {
    $c->insert("", ["id"], [[1]]);
});
expect_throw("column starts with digit", function () use ($c) {
    $c->insert("test.id_validate", ["1id"], [[1]]);
});
expect_throw("writeStart bad table", function () use ($c) {
    $c->writeStart("test.id`drop", ["id"]);
});

// Valid forms still work.
$c->insert("test.id_validate", ["id"], [[1], [2]]);
$rows = $c->select("SELECT id FROM test.id_validate ORDER BY id");
echo "rows: ", count($rows), "\n";

$c->execute("DROP TABLE test.id_validate");
?>
--EXPECT--
table semicolon: throw
column quote: throw
table empty: throw
column starts with digit: throw
writeStart bad table: throw
rows: 2
