--TEST--
ClickHouse {placeholder} substitution rejects unsafe values
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
    try { $fn(); echo $label, ": no throw\n"; }
    catch (ClickHouseException $e) { echo $label, ": throw\n"; }
}

expect_throw("semicolon",  function () use ($c) {
    $c->select("SELECT {x} FROM system.one", ['x' => "1; DROP TABLE foo"]);
});
expect_throw("single quote", function () use ($c) {
    $c->select("SELECT {x} FROM system.one", ['x' => "1' OR '1'='1"]);
});
expect_throw("backslash",  function () use ($c) {
    $c->select("SELECT {x} FROM system.one", ['x' => "1\\0"]);
});
expect_throw("missing key", function () use ($c) {
    $c->select("SELECT 1 FROM system.one", ['x' => "1"]);
});

// Safe values still work.
$rows = $c->select("SELECT {col} FROM system.one", ['col' => 'dummy']);
echo "safe identifier: ", isset($rows[0]['dummy']) ? "ok" : "fail", "\n";

$one = $c->select("SELECT {n} AS x FROM system.one", ['n' => '42'], ClickHouse::FETCH_ONE);
echo "safe numeric: $one\n";

// Multiple occurrences of the same placeholder all replaced.
$rows2 = $c->select("SELECT {col} AS a, {col} AS b FROM system.one", ['col' => '7']);
echo "duplicate placeholder: ", $rows2[0]['a'], "/", $rows2[0]['b'], "\n";
?>
--EXPECT--
semicolon: throw
single quote: throw
backslash: throw
missing key: throw
safe identifier: ok
safe numeric: 42
duplicate placeholder: 7/7
