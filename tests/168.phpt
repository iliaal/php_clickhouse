--TEST--
DR-nested-zval-leak: a throw mid-build of a nested Array/Tuple read frees the partial array
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// The nested Array/Tuple read builders allocate a heap HashTable
// (return_tmp) that is only attached to the parent array after the element
// loop completes. A throw mid-loop -- e.g. the nested-type depth cap firing
// on a deeply nested Tuple -- used to orphan that HashTable (one leak per
// recursion level). The build loop now frees it on unwind.

$c = new ClickHouse(clickhouse_test_config());

// A Tuple nested deeper than the depth cap (32) makes convertToZval throw
// partway through the recursive read.
$expr = "1";
for ($i = 0; $i < 40; $i++) { $expr = "tuple($expr)"; }

$threw = false;
try { $c->select("SELECT $expr AS t"); }
catch (ClickHouseException $e) { $threw = true; }
echo "deep tuple threw: ", $threw ? "yes" : "no", "\n";

// Repeat the throwing read many times; a per-call leak would grow the Zend
// MM footprint monotonically. Post-fix the partial array is freed each time.
for ($w = 0; $w < 50; $w++) { try { $c->select("SELECT $expr AS t"); } catch (ClickHouseException $e) {} }
$base = memory_get_usage();
for ($i = 0; $i < 500; $i++) { try { $c->select("SELECT $expr AS t"); } catch (ClickHouseException $e) {} }
$growth = memory_get_usage() - $base;

// Pre-fix this grew by ~32 * 56 bytes per call (~900 KB over 500 iters);
// post-fix it is flat. Allow a small slack for allocator bookkeeping.
echo "leak-free: ", ($growth < 8192) ? "yes" : "no ($growth bytes)", "\n";
?>
--EXPECT--
deep tuple threw: yes
leak-free: yes
