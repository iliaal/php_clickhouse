--TEST--
Conversion NULL/depth state does not leak into selectWithExternalData or row iterators
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

function probe(string $label): void
{
    $c = new ClickHouse(clickhouse_test_config());
    try {
        $c->selectWithExternalData("SELECT v FROM ext", [
            ["name" => "ext", "columns" => ["v" => "Int32"], "rows" => [[null]]],
        ]);
        echo "$label: NULL accepted for Int32\n";
    } catch (ClickHouseException $e) {
        echo "$label: ", $e->getMessage(), "\n";
    }
    try {
        $r = $c->selectWithExternalData("SELECT v FROM ext", [
            ["name" => "ext", "columns" => ["v" => "Array(Int32)"], "rows" => [[[7]]]],
        ]);
        echo "$label: external Array(Int32) = ", json_encode($r), "\n";
    } catch (ClickHouseException $e) {
        echo "$label: ", $e->getMessage(), "\n";
    }
    try {
        foreach ($c->selectStream("SELECT [1, 2] AS a") as $row) {
            echo "$label: iterator row = ", json_encode($row), "\n";
        }
    } catch (ClickHouseException $e) {
        echo "$label: ", $e->getMessage(), "\n";
    }
}

class Reenter
{
    public function __toString(): string
    {
        probe("reentrant");
        return "x";
    }
}

class Fatal
{
    public function __toString(): string
    {
        eval('function clickhouse_276_dup() {} function clickhouse_276_dup() {}');
        return "x";
    }
}

/* Thirty Tuple levels put the leaf String at the conversion depth limit. */
$depth = 30;
$type = str_repeat("Tuple(", $depth) . "Nullable(String)" . str_repeat(")", $depth);
$wrap = function ($leaf) use ($depth) {
    for ($i = 0; $i < $depth; $i++) {
        $leaf = [$leaf];
    }
    return $leaf;
};

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.conv_state_276");
$c->execute("CREATE TABLE test.conv_state_276 (s $type) ENGINE=Memory");

$c->insert("test.conv_state_276", ["s"], [[$wrap(new Reenter)]]);

/* A fatal error longjmps past the conversion guards' destructors. */
register_shutdown_function(function () {
    probe("after fatal");
});
$c->insert("test.conv_state_276", ["s"], [[$wrap(new Fatal)]]);
echo "unreachable\n";
?>
--CLEAN--
<?php
require __DIR__ . "/_clickhouse.inc";
$c = new ClickHouse(clickhouse_test_config());
$c->execute("DROP TABLE IF EXISTS test.conv_state_276");
?>
--EXPECTF--
reentrant: null cannot be assigned to non-Nullable column Int32
reentrant: external Array(Int32) = [{"v":[7]}]
reentrant: iterator row = {"a":[1,2]}

Fatal error: Cannot redeclare %A
after fatal: null cannot be assigned to non-Nullable column Int32
after fatal: external Array(Int32) = [{"v":[7]}]
after fatal: iterator row = {"a":[1,2]}
