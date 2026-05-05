--TEST--
ClickHouse insert rejects sparse / associative / non-string columns array (regression for NULL-deref)
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// Regression for CR-201: insert() used to do
//   zval *key = zend_hash_index_find(columns_ht, i)
//   ... Z_TYPE_P(key) ...
// which segfaulted on a sparse or associative $columns array because
// the indexed lookup returned NULL. Now the function iterates the
// HashTable in order and validates every entry is a string. Sparse /
// assoc literal forms are accepted (the keys are ignored, only the
// string values matter); non-string members are rejected cleanly.

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.cols_shape");
$c->execute("CREATE TABLE test.cols_shape (a UInt8, b UInt8) ENGINE = Memory");

// Non-string values must be rejected up front, no segfault.
$reject_probes = [
    "int member"  => fn() => $c->insert("test.cols_shape",
        ['a', 42], [[1, 2]]),
    "null member" => fn() => $c->insert("test.cols_shape",
        ['a', null], [[1, 2]]),
    "array member" => fn() => $c->insert("test.cols_shape",
        ['a', ['b']], [[1, 2]]),
];
foreach ($reject_probes as $label => $fn) {
    try { $fn(); echo "$label: NO THROW\n"; }
    catch (ClickHouseException $e) { echo "$label: REJECTED\n"; }
}

// Sparse / assoc literal forms used to crash; now they simply
// iterate the HashTable in insertion order and behave like a normal list.
$c->insert("test.cols_shape", [1 => 'a', 5 => 'b'], [[1, 2]]);
$c->insert("test.cols_shape", ['x' => 'a', 'y' => 'b'], [[3, 4]]);
$c->insert("test.cols_shape", ['a', 'b'], [[7, 8]]);
$rows = $c->select("SELECT a, b FROM test.cols_shape ORDER BY a");
echo "rowcount: ", count($rows), "\n";
foreach ($rows as $r) {
    echo "row: a=", $r["a"], " b=", $r["b"], "\n";
}
$c->execute("DROP TABLE test.cols_shape");
?>
--EXPECT--
int member: REJECTED
null member: REJECTED
array member: REJECTED
rowcount: 3
row: a=1 b=2
row: a=3 b=4
row: a=7 b=8
