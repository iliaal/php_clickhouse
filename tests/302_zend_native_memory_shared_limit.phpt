--TEST--
Retained iterators share the Zend request memory limit and recover after rejection
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
$iterators = array();
$rejected = false;
try {
    for ($i = 0; $i < 12; ++$i) {
        $iterators[] = $c->selectStream("SELECT CAST(repeat('x', 256), 'FixedString(256)') AS payload FROM numbers(32768)");
    }
} catch (ClickHouseException $e) {
    $rejected = strpos($e->getMessage(), "memory_limit") !== false;
    if (!$rejected) throw $e;
}
echo "shared limit=", $rejected && count($iterators) >= 2 && count($iterators) < 12 ? "yes" : "no", "\n";
$iterators[0]->rewind();
echo "retained data=", strlen($iterators[0]->current()["payload"]) === 256 ? "yes" : "no", "\n";
unset($iterators);
gc_collect_cycles();
echo "released=", memory_get_usage() < $before + 1048576 ? "yes" : "no", "\n";
echo "recovered=", $c->ping() && $c->select("SELECT 42", array(), ClickHouse::FETCH_ONE) === 42 ? "yes" : "no", "\n";
?>
--EXPECT--
shared limit=yes
retained data=yes
released=yes
recovered=yes
