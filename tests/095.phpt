--TEST--
ClickHouse selectWithExternalData basic IN-clause against an external table
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.ext_users");
$c->execute("CREATE TABLE test.ext_users (id UInt64, name String) ENGINE=Memory");
$c->insert("test.ext_users", ["id", "name"], [
    [1,  "alice"],
    [2,  "bob"],
    [3,  "carol"],
    [10, "ten"],
    [42, "answer"],
    [99, "nope"],
]);

// 1. Filter against a 3-row external table.
$rows = $c->selectWithExternalData(
    "SELECT id, name FROM test.ext_users WHERE id IN ext_ids ORDER BY id",
    [
        [
            "name"    => "ext_ids",
            "columns" => ["id" => "UInt64"],
            "rows"    => [[1], [10], [42]],
        ],
    ]
);
foreach ($rows as $r) echo "row: {$r['id']} {$r['name']}\n";

// 2. Same shape, FETCH_COLUMN flatten.
$ids = $c->selectWithExternalData(
    "SELECT id FROM test.ext_users WHERE id IN ext_ids ORDER BY id",
    [
        [
            "name"    => "ext_ids",
            "columns" => ["id" => "UInt64"],
            "rows"    => [[2], [3]],
        ],
    ],
    [], ClickHouse::FETCH_COLUMN
);
echo "ids: " . implode(",", $ids) . "\n";

// 3. Query_id propagation surfaces in getStatistics.
$c->selectWithExternalData(
    "SELECT count() FROM test.ext_users WHERE id IN ext_ids",
    [
        [
            "name"    => "ext_ids",
            "columns" => ["id" => "UInt64"],
            "rows"    => [[1]],
        ],
    ],
    [], 0, "ext-qid-001"
);
$st = $c->getStatistics();
echo "qid: {$st['query_id']}\n";

// 4. Sanity: same handle still does plain select().
$cnt = $c->select("SELECT count() FROM test.ext_users", [], ClickHouse::FETCH_ONE);
echo "rowcount: $cnt\n";

$c->execute("DROP TABLE test.ext_users");
?>
--EXPECT--
row: 1 alice
row: 10 ten
row: 42 answer
ids: 2,3
qid: ext-qid-001
rowcount: 6
