--TEST--
DR-014: a Map read that throws on an unsupported value type frees the partial map
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// The Map read allocated the result array (map_zv) before the key/value
// column casts and the unsupported-inner-type throw, with no cleanup guard,
// so a Map with an unsupported value type (e.g. Map(String, Date)) leaked the
// partial array on every row. It is now freed on unwind.

$c = new ClickHouse(clickhouse_test_config());

$sql = "SELECT map('a', toDate('2024-01-01')) AS m";

$threw = false;
try { $c->select($sql); }
catch (ClickHouseException $e) { $threw = true; }
echo "unsupported map value threw: ", ($threw ? "yes" : "no"), "\n";

// Repeat the throwing read many times; a per-call leak would grow the Zend MM
// footprint monotonically. Post-fix it is flat.
for ($w = 0; $w < 50; $w++) { try { $c->select($sql); } catch (ClickHouseException $e) {} }
$base = memory_get_usage();
for ($i = 0; $i < 500; $i++) { try { $c->select($sql); } catch (ClickHouseException $e) {} }
$growth = memory_get_usage() - $base;

echo "leak-free: ", ($growth < 8192) ? "yes" : "no ($growth bytes)", "\n";
?>
--EXPECT--
unsupported map value threw: yes
leak-free: yes
