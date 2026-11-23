--TEST--
selectStream() retains String buffers at their loaded size
--EXTENSIONS--
clickhouse
--INI--
memory_limit=256M
--SKIPIF--
<?php
if (getenv("USE_ZEND_ALLOC") === "0") die("skip Zend allocator disabled");
require __DIR__ . "/_clickhouse.inc";
clickhouse_skip_if_no_server();
?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";
$c = new ClickHouse(clickhouse_test_config());
$c->select("SELECT 1");
$width = 70000;
$rows = 64;
$payload = 3 * $width * $rows;
$before = memory_get_usage();
$iterator = $c->selectStream("SELECT repeat('a', $width) AS s, [repeat('b', $width)] AS a, toNullable(repeat('c', $width)) AS n FROM numbers($rows)");
$retained = memory_get_usage() - $before;
echo "counted=", $retained >= $payload ? "yes" : "no", "\n";
echo "trimmed=", $retained < $payload * 1.25 ? "yes" : "no ($retained for $payload)", "\n";
$iterator->rewind();
$row = $iterator->current();
echo "data=", count($iterator) === $rows && $row["s"] === str_repeat("a", $width)
    && $row["a"] === array(str_repeat("b", $width)) && $row["n"] === str_repeat("c", $width) ? "yes" : "no", "\n";
?>
--EXPECT--
counted=yes
trimmed=yes
data=yes
