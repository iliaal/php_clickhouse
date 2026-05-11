--TEST--
ClickHouse insertFromStream NULL handling and batching across chunk boundaries
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.from_null");
$c->execute("CREATE TABLE test.from_null (id UInt32, note Nullable(String), v Nullable(UInt32)) ENGINE=Memory");

// NULLs via literal `\N` in TSV.
$mem = fopen("php://memory", "w+b");
fwrite($mem, "1\talpha\t100\n2\t\\N\t\\N\n3\tgamma\t300\n");
rewind($mem);
$n = $c->insertFromStream("test.from_null", ["id", "note", "v"], $mem);
echo "tsv rows: $n\n";
fclose($mem);

foreach ($c->select("SELECT id, note, v FROM test.from_null ORDER BY id") as $r) {
    $note = $r["note"] === null ? "NULL" : $r["note"];
    $v    = $r["v"]    === null ? "NULL" : $r["v"];
    echo "row {$r['id']}: $note / $v\n";
}

// Same NULL marker in CSV (literal \N, unquoted).
$c->execute("TRUNCATE TABLE test.from_null");
$mem = fopen("php://memory", "w+b");
fwrite($mem, "10,delta,400\r\n20,\\N,\\N\r\n30,epsilon,600\r\n");
rewind($mem);
$n = $c->insertFromStream("test.from_null", ["id", "note", "v"], $mem, "CSV");
echo "csv rows: $n\n";
fclose($mem);
foreach ($c->select("SELECT id, note, v FROM test.from_null ORDER BY id") as $r) {
    $note = $r["note"] === null ? "NULL" : $r["note"];
    $v    = $r["v"]    === null ? "NULL" : $r["v"];
    echo "row {$r['id']}: $note / $v\n";
}

// Batch boundary: insert 25 rows with batch_rows=10 to force 3 flushes
// (10 + 10 + 5). Each batch is sent as its own block.
$c->execute("TRUNCATE TABLE test.from_null");
$tsv = "";
for ($i = 1; $i <= 25; ++$i) $tsv .= "$i\tn$i\t" . ($i * 10) . "\n";
$mem = fopen("php://memory", "w+b");
fwrite($mem, $tsv);
rewind($mem);
$n = $c->insertFromStream("test.from_null", ["id", "note", "v"], $mem,
                          "TabSeparated", 10);
echo "batched rows: $n\n";
fclose($mem);
$total = $c->select("SELECT count() FROM test.from_null", [], ClickHouse::FETCH_ONE);
$first = $c->select("SELECT note FROM test.from_null WHERE id=1",  [], ClickHouse::FETCH_ONE);
$mid   = $c->select("SELECT note FROM test.from_null WHERE id=15", [], ClickHouse::FETCH_ONE);
$last  = $c->select("SELECT note FROM test.from_null WHERE id=25", [], ClickHouse::FETCH_ONE);
echo "total=$total first=$first mid=$mid last=$last\n";

$c->execute("DROP TABLE test.from_null");
?>
--EXPECT--
tsv rows: 3
row 1: alpha / 100
row 2: NULL / NULL
row 3: gamma / 300
csv rows: 3
row 10: delta / 400
row 20: NULL / NULL
row 30: epsilon / 600
batched rows: 25
total=25 first=n1 mid=n15 last=n25
