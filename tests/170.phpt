--TEST--
DR-006: insertFromStream() rejects an unbounded batch_rows
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// batch_rows is the flush threshold: rows buffer in per-column PHP zval
// accumulators until it is reached. A huge value (PHP_INT_MAX) defeats
// streaming and buffers the whole input in memory. Only batch_rows < 1 was
// rejected; there is now an upper cap as well. A normal value still works.

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.dr006");
$c->execute("CREATE TABLE test.dr006 (n UInt32) ENGINE = Memory");

$stream = fopen("php://memory", "r+");
fwrite($stream, "1\n2\n3\n");
rewind($stream);

// unbounded batch_rows -> rejected
try {
    $c->insertFromStream("test.dr006", ["n"], $stream, "TabSeparated", PHP_INT_MAX);
    echo "huge batch_rows: NO THROW\n";
} catch (ClickHouseException $e) {
    echo "huge batch_rows: REJECTED\n";
}
fclose($stream);

// below the lower bound -> still rejected
$stream = fopen("php://memory", "r+");
fwrite($stream, "1\n");
rewind($stream);
try {
    $c->insertFromStream("test.dr006", ["n"], $stream, "TabSeparated", 0);
    echo "zero batch_rows: NO THROW\n";
} catch (ClickHouseException $e) {
    echo "zero batch_rows: REJECTED\n";
}
fclose($stream);

// a normal batch_rows still inserts
$stream = fopen("php://memory", "r+");
fwrite($stream, "10\n20\n30\n");
rewind($stream);
$c->insertFromStream("test.dr006", ["n"], $stream, "TabSeparated", 2);
fclose($stream);
$r = $c->select("SELECT count() c, sum(n) s FROM test.dr006");
echo "normal insert: count=", $r[0]['c'], " sum=", $r[0]['s'], "\n";

$c->execute("DROP TABLE test.dr006");
?>
--EXPECT--
huge batch_rows: REJECTED
zero batch_rows: REJECTED
normal insert: count=3 sum=60
