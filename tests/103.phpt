--TEST--
ClickHouse insertFromStream TSV basic happy path
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.from_stream");
$c->execute("CREATE TABLE test.from_stream (id UInt32, name String, score Float64) ENGINE=Memory");

// 1. Bare TabSeparated.
$mem = fopen("php://memory", "w+b");
fwrite($mem, "1\talice\t1.5\n2\tbob\t2.25\n3\tcarol\t3.75\n");
rewind($mem);
$n = $c->insertFromStream("test.from_stream", ["id", "name", "score"], $mem);
echo "rows inserted: $n\n";
fclose($mem);

foreach ($c->select("SELECT id, name, score FROM test.from_stream ORDER BY id") as $r) {
    echo "row: {$r['id']} {$r['name']} {$r['score']}\n";
}

// 2. TSVWithNames — header row gets skipped.
$c->execute("TRUNCATE TABLE test.from_stream");
$mem = fopen("php://memory", "w+b");
fwrite($mem, "id\tname\tscore\n10\tdave\t9.0\n20\teve\t8.5\n");
rewind($mem);
$n = $c->insertFromStream("test.from_stream", ["id", "name", "score"], $mem,
                          "TabSeparatedWithNames");
echo "with-names rows: $n\n";
fclose($mem);
foreach ($c->select("SELECT id FROM test.from_stream ORDER BY id") as $r) {
    echo "wn: {$r['id']}\n";
}

// 3. TSV escapes round-trip through the parser.
$c->execute("TRUNCATE TABLE test.from_stream");
$mem = fopen("php://memory", "w+b");
fwrite($mem, "1\ta\\tb\t1.0\n2\tback\\\\slash\t2.0\n3\tline\\nbreak\t3.0\n");
rewind($mem);
$n = $c->insertFromStream("test.from_stream", ["id", "name", "score"], $mem);
echo "esc rows: $n\n";
fclose($mem);
foreach ($c->select("SELECT id, name FROM test.from_stream ORDER BY id") as $r) {
    echo "esc: {$r['id']} " . strtr($r['name'], ["\t" => "<TAB>", "\n" => "<LF>"]) . "\n";
}

$c->execute("DROP TABLE test.from_stream");
?>
--EXPECT--
rows inserted: 3
row: 1 alice 1.5
row: 2 bob 2.25
row: 3 carol 3.75
with-names rows: 2
wn: 10
wn: 20
esc rows: 3
esc: 1 a<TAB>b
esc: 2 back\slash
esc: 3 line<LF>break
