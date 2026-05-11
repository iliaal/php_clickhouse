--TEST--
ClickHouse insertFromStream rejects bad format, non-stream argument, malformed rows
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.bad_stream");
$c->execute("CREATE TABLE test.bad_stream (id UInt32, name String) ENGINE=Memory");

function probe(string $label, callable $fn): void {
    try {
        $fn();
        echo "$label: NO THROW\n";
    } catch (ClickHouseException $e) {
        echo "$label: REJECTED\n";
    }
}

// 1. Bad format.
$mem = fopen("php://memory", "w+b");
fwrite($mem, "1\talice\n");
rewind($mem);
probe("bad-format", fn() =>
    $c->insertFromStream("test.bad_stream", ["id", "name"], $mem, "Parquet"));
fclose($mem);

// 2. Not a stream. `@`-suppress: PHP 7.4 emits an E_WARNING before
// php_stream_from_zval_no_verify returns NULL on a non-resource argument;
// PHP 8.x is silent. The throw is identical on both.
probe("not-a-stream", fn() =>
    @$c->insertFromStream("test.bad_stream", ["id", "name"], "not a stream"));

// 3. batch_rows <= 0.
$mem = fopen("php://memory", "w+b");
probe("zero-batch", fn() =>
    $c->insertFromStream("test.bad_stream", ["id", "name"], $mem, "TSV", 0));
fclose($mem);

// 4. Empty columns list.
$mem = fopen("php://memory", "w+b");
probe("no-columns", fn() =>
    $c->insertFromStream("test.bad_stream", [], $mem));
fclose($mem);

// 5. Wrong cell count in a row.
$mem = fopen("php://memory", "w+b");
fwrite($mem, "1\talice\n2\textra\tcolumn\textra\n");
rewind($mem);
probe("row-too-wide", fn() =>
    $c->insertFromStream("test.bad_stream", ["id", "name"], $mem));
fclose($mem);

// 6. Unterminated quoted cell in CSV.
$mem = fopen("php://memory", "w+b");
fwrite($mem, "1,\"never-closed\n");
rewind($mem);
probe("unterminated-csv", fn() =>
    $c->insertFromStream("test.bad_stream", ["id", "name"], $mem, "CSV"));
fclose($mem);

// 7. Sanity: a clean import after all the failures still works and the
// table has only the rows we now insert. Row-too-wide may have left
// a partial row 1, so truncate first to assert the clean count.
$c->execute("TRUNCATE TABLE test.bad_stream");
$mem = fopen("php://memory", "w+b");
fwrite($mem, "1\tfoo\n2\tbar\n");
rewind($mem);
$n = $c->insertFromStream("test.bad_stream", ["id", "name"], $mem);
echo "clean rows: $n / total: " .
    $c->select("SELECT count() FROM test.bad_stream", [], ClickHouse::FETCH_ONE) . "\n";
fclose($mem);

$c->execute("DROP TABLE test.bad_stream");
?>
--EXPECT--
bad-format: REJECTED
not-a-stream: REJECTED
zero-batch: REJECTED
no-columns: REJECTED
row-too-wide: REJECTED
unterminated-csv: REJECTED
clean rows: 2 / total: 2
