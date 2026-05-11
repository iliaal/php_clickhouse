--TEST--
ClickHouse selectWithExternalData rejects malformed external-table entries
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.ext_target");
$c->execute("CREATE TABLE test.ext_target (id UInt64) ENGINE=Memory");
$c->insert("test.ext_target", ["id"], [[1], [2], [3]]);

function probe(ClickHouse $c, string $label, array $externals): void {
    try {
        $c->selectWithExternalData(
            "SELECT id FROM test.ext_target WHERE id IN ext_x",
            $externals
        );
        echo "$label: NO THROW\n";
    } catch (ClickHouseException $e) {
        echo "$label: REJECTED\n";
    }
}

// 1. Empty externals — must be rejected up-front.
probe($c, "empty",          []);

// 2. Missing required keys.
probe($c, "no-name",        [["columns" => ["id" => "UInt64"], "rows" => [[1]]]]);
probe($c, "no-columns",     [["name" => "ext_x", "rows" => [[1]]]]);
probe($c, "no-rows",        [["name" => "ext_x", "columns" => ["id" => "UInt64"]]]);

// 3. Wrong scalar shape.
probe($c, "name-not-str",   [["name" => 42, "columns" => ["id" => "UInt64"], "rows" => [[1]]]]);

// 4. Invalid identifier.
probe($c, "bad-ident",      [["name" => "1bad", "columns" => ["id" => "UInt64"], "rows" => [[1]]]]);

// 5. Columns map not associative.
probe($c, "pos-columns",    [["name" => "ext_x", "columns" => ["UInt64"], "rows" => [[1]]]]);

// 6. Unsupported type.
probe($c, "bad-type",       [["name" => "ext_x", "columns" => ["id" => "NotAType"], "rows" => [[1]]]]);

// 7. Row width mismatch (more cells than columns).
probe($c, "wide-row",       [["name" => "ext_x", "columns" => ["id" => "UInt64"], "rows" => [[1, 2]]]]);

// 8. Sanity: the handle still works after all those failures.
$cnt = $c->select("SELECT count() FROM test.ext_target", [], ClickHouse::FETCH_ONE);
echo "rowcount: $cnt\n";

// 9. And a successful call right after a failure.
$rows = $c->selectWithExternalData(
    "SELECT id FROM test.ext_target WHERE id IN ext_x ORDER BY id",
    [["name" => "ext_x", "columns" => ["id" => "UInt64"], "rows" => [[1], [3]]]],
    [], ClickHouse::FETCH_COLUMN
);
echo "ok: " . implode(",", $rows) . "\n";

$c->execute("DROP TABLE test.ext_target");
?>
--EXPECT--
empty: REJECTED
no-name: REJECTED
no-columns: REJECTED
no-rows: REJECTED
name-not-str: REJECTED
bad-ident: REJECTED
pos-columns: REJECTED
bad-type: REJECTED
wide-row: REJECTED
rowcount: 3
ok: 1,3
