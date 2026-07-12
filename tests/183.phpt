--TEST--
ClickHouse Map negative integer keys preserve signed values or reject unsigned destinations
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.map_negative_keys");
$c->execute("CREATE TABLE test.map_negative_keys (
    id UInt8,
    m_i Map(Int64, String),
    m_u Map(UInt64, String),
    m_f Map(Float64, String)
) ENGINE=Memory");

$c->insert("test.map_negative_keys", ["id", "m_i", "m_f"], [
    [1, [-1 => "signed"], [-1 => "float"]],
]);
$row = $c->select("SELECT m_i, m_f FROM test.map_negative_keys WHERE id = 1")[0];
echo array_key_first($row["m_i"]), "\n";
echo array_key_first($row["m_f"]), "\n";

try {
    $c->insert("test.map_negative_keys", ["id", "m_u"], [
        [2, [-1 => "unsigned"]],
    ]);
    echo "unsigned accepted\n";
} catch (ClickHouseException $e) {
    echo "unsigned rejected\n";
}

$c->execute("DROP TABLE test.map_negative_keys");
?>
--EXPECT--
-1
-1
unsigned rejected
