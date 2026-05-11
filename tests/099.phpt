--TEST--
ClickHouse selectToStream CSV quoting and escaping
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.stream_csv");
$c->execute("CREATE TABLE test.stream_csv (id UInt32, s String) ENGINE=Memory");
$c->insert("test.stream_csv", ["id", "s"], [
    [1, "plain"],
    [2, "with,comma"],
    [3, 'has "quote"'],
    [4, "embedded\nnewline"],
    [5, "carriage\rreturn"],
    [6, ""],
]);

$mem = fopen("php://memory", "w+b");
$n = $c->selectToStream(
    "SELECT id, s FROM test.stream_csv ORDER BY id",
    [], $mem, "CSV"
);
echo "rows: $n\n";
rewind($mem);
$raw = stream_get_contents($mem);
// Visualize \r and \n explicitly so the EXPECT block stays clean.
echo str_replace(["\r", "\n"], ['<CR>', "<LF>\n"], $raw);
fclose($mem);

$c->execute("DROP TABLE test.stream_csv");
?>
--EXPECT--
rows: 6
1,plain<CR><LF>
2,"with,comma"<CR><LF>
3,"has ""quote"""<CR><LF>
4,"embedded<LF>
newline"<CR><LF>
5,"carriage<CR>return"<CR><LF>
6,<CR><LF>
