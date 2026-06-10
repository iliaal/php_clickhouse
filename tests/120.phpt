--TEST--
clone of ClickHouse / ClickHouseRowIterator is rejected instead of corrupting the heap
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());

$it = $c->selectStream("SELECT number FROM system.numbers LIMIT 3");
try {
    $copy = clone $it;
    echo "iterator clone: NO THROW\n";
    var_dump($copy->valid());
} catch (Throwable $e) {
    echo "iterator clone rejected: ", get_class($e), "\n";
}

try {
    $copy = clone $c;
    echo "client clone: NO THROW\n";
} catch (Throwable $e) {
    echo "client clone rejected: ", get_class($e), "\n";
}

/* The originals stay fully usable after the rejected clones. */
$n = 0;
foreach ($it as $row) {
    $n++;
}
echo "iterator rows=", $n, "\n";
echo "ping=", $c->ping() ? "ok" : "fail", "\n";
?>
--EXPECT--
iterator clone rejected: Error
client clone rejected: Error
iterator rows=3
ping=ok
