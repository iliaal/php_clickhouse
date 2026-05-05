--TEST--
ClickHouse error paths raise ClickHouseException
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";
$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");

function expect_throw(string $label, callable $fn): void {
    try {
        $fn();
        echo $label, ": no throw\n";
    } catch (ClickHouseException $e) {
        echo $label, ": throw\n";
    }
}

expect_throw("select bad sql", function () use ($c) {
    $c->select("SELECT * FROM test.this_table_does_not_exist_xyz");
});

expect_throw("insert nonexistent table", function () use ($c) {
    $c->insert("test.no_such_table_xyz", ["id"], [[1]]);
});

expect_throw("writeStart nonexistent table", function () use ($c) {
    $c->writeStart("test.no_such_table_xyz", ["id"]);
});

expect_throw("execute bad sql", function () use ($c) {
    $c->execute("THIS IS NOT SQL");
});
?>
--EXPECT--
select bad sql: throw
insert nonexistent table: throw
writeStart nonexistent table: throw
execute bad sql: throw
