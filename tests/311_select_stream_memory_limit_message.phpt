--TEST--
selectStream() reports a result that does not fit memory_limit with the message that points to selectStreamCallback()
--EXTENSIONS--
clickhouse
--INI--
memory_limit=16M
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";
$c = new ClickHouse(clickhouse_test_config());
try {
    $c->selectStream("SELECT repeat('x', 999999) AS payload FROM numbers(32)");
    echo "accepted\n";
} catch (ClickHouseException $e) {
    echo $e->getMessage(), "\n";
}
echo json_encode($c->select("SELECT 1 AS x")), "\n";
?>
--EXPECT--
selectStream: buffered result exceeds PHP memory_limit; use selectStreamCallback() for unbounded streaming or raise memory_limit
[{"x":1}]
