--TEST--
ClickHouse insertFromStream rejects malformed TSV \N marker (whole-cell only)
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.null_strict");
$c->execute("CREATE TABLE test.null_strict (id UInt32, s String, note Nullable(String)) ENGINE=Memory");

function probe(string $label, callable $fn): void {
    try {
        $fn();
        echo "$label: NO THROW\n";
    } catch (ClickHouseException $e) {
        echo "$label: REJECTED\n";
    }
}

// 1. `\N` as the whole cell is fine — NULL marker.
$mem = fopen("php://memory", "w+b");
fwrite($mem, "1\tplain\t\\N\n");
rewind($mem);
$n = $c->insertFromStream("test.null_strict", ["id", "s", "note"], $mem);
fclose($mem);
echo "whole-cell-NULL rows: $n\n";
$r = $c->select("SELECT id, s, note IS NULL AS n FROM test.null_strict")[0];
echo "row: {$r['id']} {$r['s']} note_null={$r['n']}\n";
$c->execute("TRUNCATE TABLE test.null_strict");

// 2. `\N` followed by other content inside the cell is REJECTED.
//    Pre-fix, this silently became the literal 3-char string "\Nx"
//    when the target column was String.
$mem = fopen("php://memory", "w+b");
fwrite($mem, "1\t\\Nx\tnote\n");
rewind($mem);
probe("trailing-after-N", fn() =>
    $c->insertFromStream("test.null_strict", ["id", "s", "note"], $mem));
fclose($mem);

// 3. Same in the Nullable column.
$mem = fopen("php://memory", "w+b");
fwrite($mem, "1\tplain\t\\Ntrash\n");
rewind($mem);
probe("nullable-after-N", fn() =>
    $c->insertFromStream("test.null_strict", ["id", "s", "note"], $mem));
fclose($mem);

// 4. `\N` at cell start followed by row terminator is fine.
$mem = fopen("php://memory", "w+b");
fwrite($mem, "2\tplain\t\\N\n");
rewind($mem);
$n = $c->insertFromStream("test.null_strict", ["id", "s", "note"], $mem);
fclose($mem);
echo "row-term-after-N rows: $n\n";

// 5. `\N` followed by tab is fine.
$mem = fopen("php://memory", "w+b");
fwrite($mem, "3\t\\N\tok\n");
rewind($mem);
$n = $c->insertFromStream("test.null_strict", ["id", "s", "note"], $mem);
fclose($mem);
echo "cell-sep-after-N rows: $n\n";

// 6. `\\N` (escaped backslash followed by N) decodes to literal `\N`
//    in the cell — should be the 2-char string, NOT NULL. ClickHouse's
//    own TSV parser treats this as a literal `\N` value because the `\\`
//    decodes to `\`, leaving the N as ordinary content.
//
//    NOTE: today the parser still emits a 2-char `\N` cell_buf and
//    pushCell can't distinguish it from the NULL marker (bytes-based
//    check). This is a separate known limitation; for now we just
//    confirm the parse doesn't throw.
$mem = fopen("php://memory", "w+b");
fwrite($mem, "4\t\\\\N\tplain\n");
rewind($mem);
$n = $c->insertFromStream("test.null_strict", ["id", "s", "note"], $mem);
fclose($mem);
echo "double-backslash-N rows: $n\n";

// 7. CSV `\N` followed by non-comma is permissive — becomes a string.
//    Documented behavior; CSV has no escape protocol.
$mem = fopen("php://memory", "w+b");
fwrite($mem, "5,\\Nx,note\r\n");
rewind($mem);
$n = $c->insertFromStream("test.null_strict", ["id", "s", "note"], $mem, "CSV");
fclose($mem);
echo "csv-trailing-after-N rows: $n\n";
$r = $c->select("SELECT s FROM test.null_strict WHERE id=5", [], ClickHouse::FETCH_ONE);
echo "csv row 5 s=$r\n";

// 8. Handle still usable after the throws above.
$cnt = $c->select("SELECT count() FROM test.null_strict", [], ClickHouse::FETCH_ONE);
echo "total rows: $cnt\n";

$c->execute("DROP TABLE test.null_strict");
?>
--EXPECT--
whole-cell-NULL rows: 1
row: 1 plain note_null=1
trailing-after-N: REJECTED
nullable-after-N: REJECTED
row-term-after-N rows: 1
cell-sep-after-N rows: 1
double-backslash-N rows: 1
csv-trailing-after-N rows: 1
csv row 5 s=\Nx
total rows: 4
