--TEST--
ClickHouse selectToStream rejects bad format, non-stream argument, unsupported types
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.stream_bad");
$c->execute("CREATE TABLE test.stream_bad (id UInt32, tags Array(UInt32)) ENGINE=Memory");
$c->insert("test.stream_bad", ["id", "tags"], [[1, [10, 20]], [2, [30]]]);

function probe(string $label, callable $fn): void {
    try {
        $fn();
        echo "$label: NO THROW\n";
    } catch (ClickHouseException $e) {
        echo "$label: REJECTED\n";
    } catch (TypeError $e) {
        echo "$label: TYPE_ERROR\n";
    }
}

$mem = fopen("php://memory", "w+b");

probe("bad-format", fn() =>
    $c->selectToStream("SELECT id FROM test.stream_bad", [], $mem, "Parquet"));

probe("not-a-stream", fn() =>
    $c->selectToStream("SELECT id FROM test.stream_bad", [], "not a stream"));

probe("array-column", fn() =>
    $c->selectToStream("SELECT id, tags FROM test.stream_bad", [], $mem));

probe("tuple-column", fn() =>
    $c->selectToStream("SELECT (1, 'a') AS t", [], $mem));

probe("map-column", fn() =>
    $c->selectToStream("SELECT map('k', 1) AS m", [], $mem));

// Sanity: scalar-only query still works after all those rejections.
rewind($mem);
ftruncate($mem, 0);
$n = $c->selectToStream("SELECT id FROM test.stream_bad ORDER BY id", [], $mem);
rewind($mem);
echo "ok rows=$n body=" . trim(stream_get_contents($mem)) . "\n";

fclose($mem);
$c->execute("DROP TABLE test.stream_bad");
?>
--EXPECT--
bad-format: REJECTED
not-a-stream: REJECTED
array-column: REJECTED
tuple-column: REJECTED
map-column: REJECTED
ok rows=2 body=1
2
