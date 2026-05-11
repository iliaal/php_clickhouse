--TEST--
ClickHouse insertFromStream CSV with quoting, embedded delimiters, CRLF
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.from_csv");
$c->execute("CREATE TABLE test.from_csv (id UInt32, s String) ENGINE=Memory");

// CSV with: plain cell, quoted cell with comma, quoted cell with embedded
// double-quote (""), quoted cell containing a CRLF newline, mixed line
// endings (LF and CRLF rows in one file), and an empty quoted cell.
$payload =
    "1,plain\r\n" .
    "2,\"with,comma\"\r\n" .
    "3,\"has \"\"quote\"\"\"\r\n" .
    "4,\"embedded\nnewline\"\r\n" .
    "5,\"\"\r\n" .
    "6,trailing-LF-only\n";

$mem = fopen("php://memory", "w+b");
fwrite($mem, $payload);
rewind($mem);
$n = $c->insertFromStream("test.from_csv", ["id", "s"], $mem, "CSV");
echo "rows: $n\n";
fclose($mem);

foreach ($c->select("SELECT id, s FROM test.from_csv ORDER BY id") as $r) {
    $vis = strtr($r['s'], ["\n" => "<LF>", "\r" => "<CR>"]);
    echo "row {$r['id']}: '$vis'\n";
}

// CSVWithNames — first row discarded.
$c->execute("TRUNCATE TABLE test.from_csv");
$mem = fopen("php://memory", "w+b");
fwrite($mem, "id,s\r\n100,first\r\n200,second\r\n");
rewind($mem);
$n = $c->insertFromStream("test.from_csv", ["id", "s"], $mem, "CSVWithNames");
echo "wn rows: $n\n";
fclose($mem);
foreach ($c->select("SELECT id, s FROM test.from_csv ORDER BY id") as $r) {
    echo "wn row {$r['id']}: {$r['s']}\n";
}

$c->execute("DROP TABLE test.from_csv");
?>
--EXPECT--
rows: 6
row 1: 'plain'
row 2: 'with,comma'
row 3: 'has "quote"'
row 4: 'embedded<LF>newline'
row 5: ''
row 6: 'trailing-LF-only'
wn rows: 2
wn row 100: first
wn row 200: second
