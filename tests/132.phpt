--TEST--
selectWithExternalData accepts external tables left as references by foreach-by-ref
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$ch = new ClickHouse(clickhouse_test_config());

/* foreach ($externals as &$e) leaves IS_REFERENCE buckets; the entry and
 * its name/columns/rows members must be dereffed before the type checks,
 * matching the insert() paths. */
$ext = [["name" => "ext_ids", "columns" => ["id" => "UInt64"], "rows" => [[1], [2], [3]]]];
foreach ($ext as &$e) {}
unset($e);
foreach ($ext[0] as &$m) {}
unset($m);

$rows = $ch->selectWithExternalData("SELECT id FROM ext_ids ORDER BY id", $ext);
echo "rows=", count($rows), "\n";
echo "first=", $rows[0]["id"], "\n";
?>
--EXPECT--
rows=3
first=1
