--TEST--
ClickHouse selectWithExternalData multiple externals, varied column types
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.ext_events");
$c->execute("CREATE TABLE test.ext_events (
    id UInt64,
    tag String,
    ts Date,
    note Nullable(String)
) ENGINE=Memory");
$c->insert("test.ext_events", ["id", "tag", "ts", "note"], [
    [1, "alpha", "2026-01-01", "one"],
    [2, "beta",  "2026-01-02", null],
    [3, "alpha", "2026-02-01", "three"],
    [4, "gamma", "2026-03-01", "four"],
    [5, "beta",  "2026-03-01", null],
]);

// Two external tables joined into one query. The query body filters by
// both an ID set and a tag set; the server resolves each against its
// own external table.
$rows = $c->selectWithExternalData(
    "SELECT id, tag, ts, note FROM test.ext_events
     WHERE id IN ext_ids AND tag IN ext_tags
     ORDER BY id",
    [
        [
            "name"    => "ext_ids",
            "columns" => ["id" => "UInt64"],
            "rows"    => [[1], [2], [3], [5]],
        ],
        [
            "name"    => "ext_tags",
            "columns" => ["tag" => "String"],
            "rows"    => [["alpha"], ["beta"]],
        ],
    ],
    [], ClickHouse::DATE_AS_STRINGS
);
foreach ($rows as $r) {
    $note = $r["note"] === null ? "NULL" : $r["note"];
    echo "row: {$r['id']} {$r['tag']} {$r['ts']} $note\n";
}

// Multi-column external table — pair lookup of (tag, ts).
$rows = $c->selectWithExternalData(
    "SELECT id, tag, ts FROM test.ext_events
     WHERE (tag, ts) IN (SELECT tag, ts FROM ext_pairs)
     ORDER BY id",
    [
        [
            "name"    => "ext_pairs",
            "columns" => ["tag" => "String", "ts" => "Date"],
            "rows"    => [
                ["alpha", "2026-01-01"],
                ["beta",  "2026-03-01"],
            ],
        ],
    ],
    [], ClickHouse::DATE_AS_STRINGS
);
foreach ($rows as $r) echo "pair: {$r['id']} {$r['tag']} {$r['ts']}\n";

$c->execute("DROP TABLE test.ext_events");
?>
--EXPECT--
row: 1 alpha 2026-01-01 one
row: 2 beta 2026-01-02 NULL
row: 3 alpha 2026-02-01 three
row: 5 beta 2026-03-01 NULL
pair: 1 alpha 2026-01-01
pair: 5 beta 2026-03-01
