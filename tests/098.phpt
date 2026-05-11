--TEST--
ClickHouse selectToStream basic TSV happy path
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.stream_basic");
$c->execute("CREATE TABLE test.stream_basic (id UInt32, name String, score Float64) ENGINE=Memory");
$c->insert("test.stream_basic", ["id", "name", "score"], [
    [1, "alice",   1.5],
    [2, "bob",     2.25],
    [3, "carol",   3.75],
]);

$mem = fopen("php://memory", "w+b");
$n = $c->selectToStream(
    "SELECT id, name, score FROM test.stream_basic ORDER BY id",
    [],
    $mem
);
echo "rows: $n\n";

rewind($mem);
echo stream_get_contents($mem);
fclose($mem);

// TSV alias.
$mem = fopen("php://memory", "w+b");
$n = $c->selectToStream(
    "SELECT id FROM test.stream_basic ORDER BY id LIMIT 2",
    [], $mem, "TSV"
);
rewind($mem);
echo "TSV alias rows=$n:\n" . stream_get_contents($mem);
fclose($mem);

// Escaping: tab and newline embedded in a string cell.
$c->execute("DROP TABLE IF EXISTS test.stream_esc");
$c->execute("CREATE TABLE test.stream_esc (s String) ENGINE=Memory");
$c->insert("test.stream_esc", ["s"], [
    ["a\tb"],
    ["x\ny"],
    ["back\\slash"],
]);
$mem = fopen("php://memory", "w+b");
$c->selectToStream("SELECT s FROM test.stream_esc ORDER BY s", [], $mem);
rewind($mem);
echo "--- escapes ---\n" . stream_get_contents($mem);
fclose($mem);

$c->execute("DROP TABLE test.stream_basic");
$c->execute("DROP TABLE test.stream_esc");
?>
--EXPECT--
rows: 3
1	alice	1.5
2	bob	2.25
3	carol	3.75
TSV alias rows=2:
1
2
--- escapes ---
a\tb
back\\slash
x\ny
