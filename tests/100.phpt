--TEST--
ClickHouse selectToStream WithNames variants emit a column-name header
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.stream_hdr");
$c->execute("CREATE TABLE test.stream_hdr (id UInt32, label String) ENGINE=Memory");
$c->insert("test.stream_hdr", ["id", "label"], [[1, "one"], [2, "two"]]);

// TSVWithNames header.
$mem = fopen("php://memory", "w+b");
$n = $c->selectToStream(
    "SELECT id, label FROM test.stream_hdr ORDER BY id",
    [], $mem, "TabSeparatedWithNames"
);
echo "tsv rows=$n\n";
rewind($mem);
echo stream_get_contents($mem);
fclose($mem);

// CSVWithNames header — names with characters needing quoting are
// escaped consistently with cell values.
$mem = fopen("php://memory", "w+b");
$n = $c->selectToStream(
    "SELECT id AS \"id,quoted\", label FROM test.stream_hdr ORDER BY id",
    [], $mem, "CSVWithNames"
);
echo "csv rows=$n\n";
rewind($mem);
$raw = stream_get_contents($mem);
echo str_replace(["\r", "\n"], ['<CR>', "<LF>\n"], $raw);
fclose($mem);

// Header still emits when the result is empty.
$mem = fopen("php://memory", "w+b");
$n = $c->selectToStream(
    "SELECT id, label FROM test.stream_hdr WHERE id > 99",
    [], $mem, "TSVWithNames"
);
echo "empty rows=$n\n";
rewind($mem);
$out = stream_get_contents($mem);
echo "empty body bytes=" . strlen($out) . "\n";
echo $out;
fclose($mem);

$c->execute("DROP TABLE test.stream_hdr");
?>
--EXPECT--
tsv rows=2
id	label
1	one
2	two
csv rows=2
"id,quoted",label<CR><LF>
1,one<CR><LF>
2,two<CR><LF>
empty rows=0
empty body bytes=9
id	label
