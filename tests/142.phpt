--TEST--
write([]) is a no-op and does not tear down an in-flight streaming insert
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.write_empty");
$c->execute("CREATE TABLE test.write_empty (id UInt32) ENGINE=Memory");

/* An empty rows array means "append nothing" -- it must be a no-op. The
 * old code threw on an empty batch, and the catch path tore down the
 * in-flight insert over a benign empty write. */
$c->writeStart("test.write_empty", ["id"]);
$c->write([[1], [2]]);
$c->write([]);            // no-op; must not discard the rows above
$c->write([[3]]);
$c->writeEnd();

$rows = [];
foreach ($c->select("SELECT id FROM test.write_empty ORDER BY id") as $r) {
    $rows[] = $r["id"];
}
echo "rows=", implode(",", $rows), "\n";

$c->execute("DROP TABLE test.write_empty");
?>
--EXPECT--
rows=1,2,3
