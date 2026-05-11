--TEST--
ClickHouse selectToStream Nullable: \N in TSV, empty in CSV
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.stream_nul");
$c->execute("CREATE TABLE test.stream_nul (id UInt32, note Nullable(String), v Nullable(UInt64)) ENGINE=Memory");
$c->insert("test.stream_nul", ["id", "note", "v"], [
    [1, "alpha", 100],
    [2, null,    null],
    [3, "tab\there", 7],
    [4, null,    42],
]);

// TSV: NULL → \N (raw two bytes, backslash N).
$mem = fopen("php://memory", "w+b");
$n = $c->selectToStream(
    "SELECT id, note, v FROM test.stream_nul ORDER BY id",
    [], $mem, "TSV"
);
echo "tsv rows=$n\n";
rewind($mem);
echo stream_get_contents($mem);
fclose($mem);

// CSV: NULL → empty cell.
$mem = fopen("php://memory", "w+b");
$n = $c->selectToStream(
    "SELECT id, note, v FROM test.stream_nul ORDER BY id",
    [], $mem, "CSV"
);
echo "csv rows=$n\n";
rewind($mem);
$raw = stream_get_contents($mem);
echo str_replace(["\r", "\n"], ['<CR>', "<LF>\n"], $raw);
fclose($mem);

$c->execute("DROP TABLE test.stream_nul");
?>
--EXPECT--
tsv rows=4
1	alpha	100
2	\N	\N
3	tab\there	7
4	\N	42
csv rows=4
1,alpha,100<CR><LF>
2,,<CR><LF>
3,tab	here,7<CR><LF>
4,,42<CR><LF>
