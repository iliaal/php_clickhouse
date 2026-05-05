--TEST--
ClickHouse insertAssoc() rejects integer keys and key-set drift in later rows
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// Regression for round-15-followup CR-001: Round 15 dropped the
// positional copy in insertAssoc() but kept only a key-count check
// for later rows. The shared column gatherer tries integer-index
// lookup before name lookup, so a later row like `[0 => 99, "b" => 4]`
// silently landed `99` into column `a`. insertAssoc()'s contract is
// associative-only with a key set fixed by the first row; every row
// must have the same string-key set as the first row.

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.assoc_shape");
$c->execute("CREATE TABLE test.assoc_shape (a UInt8, b UInt8) ENGINE=Memory");

$probes = [
    "later row has integer key" => [
        ["a"=>1, "b"=>2],
        [0=>99, "b"=>4],
    ],
    "first row mixed positional" => [
        [0=>1, "b"=>2],
    ],
    "later row drops 'a' for 'c'" => [
        ["a"=>1, "b"=>2],
        ["c"=>3, "b"=>4],
    ],
    "later row missing 'a'" => [
        ["a"=>1, "b"=>2],
        ["b"=>4],
    ],
    "later row extra key" => [
        ["a"=>1, "b"=>2],
        ["a"=>3, "b"=>4, "c"=>5],
    ],
];
foreach ($probes as $label => $rows) {
    try { $c->insertAssoc("test.assoc_shape", $rows); echo "$label: NO THROW\n"; }
    catch (ClickHouseException $e) { echo "$label: REJECTED\n"; }
}

// Sanity: well-formed assoc rows still land — including with keys in
// a different order than the first row (assoc means by name, not
// position, so different ordering must continue to work).
$c->insertAssoc("test.assoc_shape", [
    ["a" => 10, "b" => 20],
    ["b" => 21, "a" => 11],
]);
$rows = $c->select("SELECT a, b FROM test.assoc_shape ORDER BY a");
foreach ($rows as $r) echo "row: a={$r['a']} b={$r['b']}\n";

$c->execute("DROP TABLE test.assoc_shape");
?>
--EXPECT--
later row has integer key: REJECTED
first row mixed positional: REJECTED
later row drops 'a' for 'c': REJECTED
later row missing 'a': REJECTED
later row extra key: REJECTED
row: a=10 b=20
row: a=11 b=21
