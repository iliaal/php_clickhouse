--TEST--
ClickHouse Float32 inserts reject finite values outside the destination range
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.float32_range");
$c->execute("CREATE TABLE test.float32_range (
    f Float32,
    m Map(String, Float32),
    mk Map(Float32, String)
) ENGINE=Memory");

foreach ([
    "direct positive" => ["f", 1e100],
    "direct negative" => ["f", -1e100],
    "map positive" => ["m", ["x" => 1e100]],
    "map negative" => ["m", ["x" => -1e100]],
    "map key positive" => ["mk", ["1e100" => "x"]],
    "map key negative" => ["mk", ["-1e100" => "x"]],
] as $label => $probe) {
    try {
        $c->insert("test.float32_range", [$probe[0]], [[$probe[1]]]);
        echo $label, " accepted\n";
    } catch (ClickHouseException $e) {
        echo $label, " rejected\n";
    }
}

$c->insert("test.float32_range", ["f", "m"], [[1.5, ["x" => -2.5]]]);
$row = $c->select("SELECT f, m FROM test.float32_range WHERE f = 1.5")[0];
echo $row["f"], "\n";
echo $row["m"]["x"], "\n";

$c->execute("DROP TABLE test.float32_range");
?>
--EXPECT--
direct positive rejected
direct negative rejected
map positive rejected
map negative rejected
map key positive rejected
map key negative rejected
1.5
-2.5
