--TEST--
ClickHouse selectToStream CSV distinguishes NULL marker from literal \N
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.csv_null_literal");
$c->execute("CREATE TABLE test.csv_null_literal (id UInt32, note Nullable(String)) ENGINE=Memory");
$c->insert("test.csv_null_literal", ["id", "note"], [
    [1, null],
    [2, "\\N"],
    [3, ""],
]);

$mem = fopen("php://memory", "w+b");
$n = $c->selectToStream(
    "SELECT id, note FROM test.csv_null_literal ORDER BY id",
    [], $mem, "CSV"
);
echo "rows=$n\n";
rewind($mem);
$raw = stream_get_contents($mem);
echo str_replace(["\r", "\n"], ["<CR>", "<LF>\n"], $raw);
fclose($mem);

$c->execute("DROP TABLE IF EXISTS test.csv_null_roundtrip");
$c->execute("CREATE TABLE test.csv_null_roundtrip (id UInt32, note Nullable(String)) ENGINE=Memory");
$mem = fopen("php://memory", "w+b");
fwrite($mem, $raw);
rewind($mem);
$n = $c->insertFromStream("test.csv_null_roundtrip", ["id", "note"], $mem, "CSV");
fclose($mem);
echo "roundtrip rows=$n\n";

foreach ($c->select("SELECT id, note, isNull(note) AS is_null FROM test.csv_null_roundtrip ORDER BY id") as $row) {
    $note = $row["note"] === null ? "NULL" : $row["note"];
    echo "row {$row['id']}: {$note} null={$row['is_null']}\n";
}

$c->execute("DROP TABLE test.csv_null_literal");
$c->execute("DROP TABLE test.csv_null_roundtrip");
?>
--EXPECT--
rows=3
1,\N<CR><LF>
2,"\N"<CR><LF>
3,<CR><LF>
roundtrip rows=3
row 1: NULL null=1
row 2: \N null=0
row 3:  null=0
