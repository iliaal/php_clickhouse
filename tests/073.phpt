--TEST--
ClickHouse {name} placeholder rejects SQL line-comment markers
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// Regression for CR-002: the prior whitelist allowed `-`, so the value
// "test.review_inject --" landed verbatim and the trailing predicate
// got commented out. Baseline returned 1 (tenant=1), placeholder returned
// 2 (entire table). Same shape as the SQL injection patterns the
// {name:Type} typed-parameter form exists to prevent.

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.cmt_inject");
$c->execute("CREATE TABLE test.cmt_inject (tenant Int32) ENGINE=Memory");
$c->insert("test.cmt_inject", ["tenant"], [[1], [2]]);

$baseline = $c->select(
    "SELECT count() FROM test.cmt_inject WHERE tenant = 1",
    [],
    ClickHouse::FETCH_ONE
);
echo "baseline: $baseline\n";

try {
    $c->select(
        "SELECT count() FROM {tbl} WHERE tenant = 1",
        ["tbl" => "test.cmt_inject --"],
        ClickHouse::FETCH_ONE
    );
    echo "comment injection: NO THROW\n";
} catch (ClickHouseException $e) {
    echo "comment injection: REJECTED\n";
}

// Single-dash also rejected (was ALLOWED in earlier rounds).
try {
    $c->select("SELECT {x} AS r", ["x" => "neg-name"]);
    echo "single dash: NO THROW\n";
} catch (ClickHouseException $e) {
    echo "single dash: REJECTED\n";
}

$c->execute("DROP TABLE test.cmt_inject");
?>
--EXPECT--
baseline: 1
comment injection: REJECTED
single dash: REJECTED
