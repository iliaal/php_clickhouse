--TEST--
Zend-managed native blocks are not charged twice by the iterator budget
--EXTENSIONS--
clickhouse
--INI--
memory_limit=64M
--SKIPIF--
<?php
if (getenv("USE_ZEND_ALLOC") === "0") die("skip Zend allocator disabled");
require __DIR__ . "/_clickhouse.inc";
clickhouse_skip_if_no_server();
?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";
$config = clickhouse_test_config();
$config["compression"] = false;
$c = new ClickHouse($config);
$before = memory_get_usage();
$iterator = $c->selectStream(
    "SELECT CAST(repeat('x', 256), 'FixedString(256)') AS payload FROM numbers(196608)",
    array(), "", array("max_block_size" => 32768)
);
echo "rows=", count($iterator), "\n";
echo "accounted=", memory_get_usage() - $before >= 50331648 ? "yes" : "no", "\n";
unset($iterator);
echo "ping=", $c->ping() ? "yes" : "no", "\n";
?>
--EXPECT--
rows=196608
accounted=yes
ping=yes
