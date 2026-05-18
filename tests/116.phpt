--TEST--
ClickHouse insertFromStream preserves middle blank rows and ignores trailing blank lines
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.blank_rows");
$c->execute("CREATE TABLE test.blank_rows (s String) ENGINE=Memory");

$mem = fopen("php://memory", "w+b");
fwrite($mem, "alpha\n\nbeta\n\n");
rewind($mem);
$n = $c->insertFromStream("test.blank_rows", ["s"], $mem);
fclose($mem);
echo "rows=$n\n";

foreach ($c->select("SELECT s, length(s) AS l FROM test.blank_rows") as $row) {
    echo "row '{$row['s']}' len={$row['l']}\n";
}

$c->execute("DROP TABLE IF EXISTS test.blank_rows_2");
$c->execute("CREATE TABLE test.blank_rows_2 (a String, b String) ENGINE=Memory");
$mem = fopen("php://memory", "w+b");
fwrite($mem, "a\tb\n\nc\td\n");
rewind($mem);
try {
    $c->insertFromStream("test.blank_rows_2", ["a", "b"], $mem);
    echo "blank two-col: NO THROW\n";
} catch (ClickHouseException $e) {
    echo "blank two-col: REJECTED\n";
}
fclose($mem);

$c->execute("DROP TABLE test.blank_rows");
$c->execute("DROP TABLE test.blank_rows_2");
?>
--EXPECT--
rows=3
row 'alpha' len=5
row '' len=0
row 'beta' len=4
blank two-col: REJECTED
