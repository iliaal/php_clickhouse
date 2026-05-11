--TEST--
ClickHouse insertFromStream TSV escape across 64 KiB chunk boundary; do_select_into recovery on mid-callback throw
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.boundary");
$c->execute("CREATE TABLE test.boundary (id UInt32, s String) ENGINE=Memory");

// --- SS-001: TSV escape straddling the 64 KiB read boundary -------------
//
// Pre-fix, the parser pushed the `\` literally when its lookahead fell
// off the end of a feed() chunk, then the next chunk's first byte was
// treated as ordinary content. A `\t` escape split across the 65 536-
// byte boundary silently parsed as the two-character literal "\t"
// instead of TAB.
//
// Layout chosen so the escape's `\` lands at offset 65 535:
//   offset 0       : '1'
//   offset 1       : TAB (cell separator)
//   offset 2..65534: 65 533 'a' bytes  (padding inside the String cell)
//   offset 65535   : '\' (escape opener)
//   offset 65536   : 't' (escape body — first byte of the next chunk)
//   offset 65537+  : 'after\n'
//
// Decoded cell value: 65 533 'a' bytes + TAB + "after".
$pad = str_repeat('a', 65533);
$payload = "1\t" . $pad . "\\tafter\n";
assert(strlen($payload) >= 65538);

$mem = fopen("php://memory", "w+b");
fwrite($mem, $payload);
rewind($mem);
$n = $c->insertFromStream("test.boundary", ["id", "s"], $mem);
fclose($mem);
echo "boundary rows: $n\n";

$full = $c->select("SELECT length(s) AS len, position(s, char(9)) AS tab_pos, "
                 . "substring(s, 65534, 6) AS around FROM test.boundary");
$r = $full[0];
echo "len={$r['len']} tab_pos={$r['tab_pos']} around=" .
     strtr($r['around'], ["\t" => "<TAB>"]) . "\n";

// --- SS-002: do_select_into resets the connection on mid-callback throw --
//
// FETCH_KEY_PAIR throws "Key pair mode requires at least 2 columns" from
// inside the OnData lambda when the result has one column. Pre-fix, the
// throw left clickhouse-cpp with residual blocks on the wire and the
// next query on the same handle saw corrupted data. The fix wraps the
// dispatch with ResetConnection().
$c->execute("INSERT INTO test.boundary VALUES (10, 'after-throw')");
try {
    $c->select("SELECT id FROM test.boundary", [], ClickHouse::FETCH_KEY_PAIR);
    echo "key-pair-1col: NO THROW\n";
} catch (ClickHouseException $e) {
    echo "key-pair-1col: REJECTED\n";
}
// Same handle, plain query — must succeed cleanly.
$cnt = $c->select("SELECT count() FROM test.boundary", [], ClickHouse::FETCH_ONE);
echo "rowcount after throw: $cnt\n";

// And via selectWithExternalData (which routes through the same
// do_select_into path with externals attached).
try {
    $c->selectWithExternalData(
        "SELECT id FROM test.boundary WHERE id IN ext_ids",
        [["name" => "ext_ids", "columns" => ["id" => "UInt32"], "rows" => [[1]]]],
        [], ClickHouse::FETCH_KEY_PAIR
    );
    echo "ext key-pair-1col: NO THROW\n";
} catch (ClickHouseException $e) {
    echo "ext key-pair-1col: REJECTED\n";
}
$cnt = $c->select("SELECT count() FROM test.boundary WHERE id = 1",
                  [], ClickHouse::FETCH_ONE);
echo "rowcount after ext throw: $cnt\n";

$c->execute("DROP TABLE test.boundary");
?>
--EXPECT--
boundary rows: 1
len=65539 tab_pos=65534 around=<TAB>after
key-pair-1col: REJECTED
rowcount after throw: 2
ext key-pair-1col: REJECTED
rowcount after ext throw: 1
