--TEST--
insertFromStream tolerates a trailing blank line terminated by CRLF
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.crlf_tail");
$c->execute("CREATE TABLE test.crlf_tail (id UInt32, name String) ENGINE=Memory");

/* CRLF line endings with a trailing blank line at EOF. The blank line's
 * deferred materialization used to fire on the CRLF tail '\n' at the top
 * of the parse loop, turning the trailing blank into a real 0-column row
 * and failing the row-width check. The trailing blank must be dropped. */
$mem = fopen("php://memory", "w+b");
fwrite($mem, "1\talice\r\n2\tbob\r\n\r\n");
rewind($mem);
$n = $c->insertFromStream("test.crlf_tail", ["id", "name"], $mem);
fclose($mem);
echo "rows inserted: $n\n";

foreach ($c->select("SELECT id, name FROM test.crlf_tail ORDER BY id") as $r) {
    echo "row: {$r['id']} {$r['name']}\n";
}

$c->execute("DROP TABLE test.crlf_tail");
?>
--EXPECT--
rows inserted: 2
row: 1 alice
row: 2 bob
