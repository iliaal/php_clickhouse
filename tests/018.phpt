--TEST--
ClickHouse LowCardinality(String) and LowCardinality(FixedString)
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.lc_t");
$c->execute("SET allow_suspicious_low_cardinality_types = 1");
$c->execute("CREATE TABLE test.lc_t (
    id UInt32,
    env LowCardinality(String),
    code LowCardinality(FixedString(4))
) ENGINE = Memory");

$c->insert("test.lc_t", ["id", "env", "code"], [
    [1, "prod", "EU01"],
    [2, "dev",  "US01"],
    [3, "prod", "EU01"],
    [4, "prod", "EU01"],
]);

$rows = $c->select("SELECT id, env, code FROM test.lc_t ORDER BY id");
foreach ($rows as $r) {
    echo $r["id"], "|", $r["env"], "|", trim($r["code"]), "\n";
}

echo "distinct env: ", $c->select("SELECT count(DISTINCT env) FROM test.lc_t", [], ClickHouse::FETCH_ONE), "\n";
echo "distinct code: ", $c->select("SELECT count(DISTINCT code) FROM test.lc_t", [], ClickHouse::FETCH_ONE), "\n";

$c->execute("DROP TABLE test.lc_t");
?>
--EXPECT--
1|prod|EU01
2|dev|US01
3|prod|EU01
4|prod|EU01
distinct env: 2
distinct code: 2
