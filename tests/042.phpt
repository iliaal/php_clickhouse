--TEST--
ClickHouse LowCardinality(Nullable(String)) and LowCardinality(Nullable(FixedString)) round-trip
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.lc_n_t");
$c->execute("SET allow_suspicious_low_cardinality_types = 1");
$c->execute("CREATE TABLE test.lc_n_t (
    id UInt32,
    tag LowCardinality(Nullable(String)),
    code LowCardinality(Nullable(FixedString(4)))
) ENGINE = Memory");

$c->insert("test.lc_n_t", ["id", "tag", "code"], [
    [1, "prod", "EU01"],
    [2, null,   "US01"],
    [3, "prod", null],
    [4, null,   null],
    [5, "dev",  "EU01"],
    [6, "prod", "EU01"],
]);

$rows = $c->select("SELECT id, tag, code FROM test.lc_n_t ORDER BY id");
foreach ($rows as $r) {
    $tag  = $r["tag"]  === null ? "NULL" : $r["tag"];
    $code = $r["code"] === null ? "NULL" : trim($r["code"]);
    echo $r["id"], "|", $tag, "|", $code, "\n";
}

echo "non-null tag: ",  $c->select("SELECT count() FROM test.lc_n_t WHERE tag IS NOT NULL",  [], ClickHouse::FETCH_ONE), "\n";
echo "null code: ",     $c->select("SELECT count() FROM test.lc_n_t WHERE code IS NULL",     [], ClickHouse::FETCH_ONE), "\n";
echo "distinct tag: ",  $c->select("SELECT count(DISTINCT tag) FROM test.lc_n_t",            [], ClickHouse::FETCH_ONE), "\n";

$c->execute("DROP TABLE test.lc_n_t");
?>
--EXPECT--
1|prod|EU01
2|NULL|US01
3|prod|NULL
4|NULL|NULL
5|dev|EU01
6|prod|EU01
non-null tag: 4
null code: 2
distinct tag: 2
