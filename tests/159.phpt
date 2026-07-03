--TEST--
ClickHouse fused numeric insert path: boundaries, hex/large forms, named + by-ref rows, range rejection, external tables, streaming write
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

function probe(string $label, callable $fn): void {
    try { $fn(); echo "$label: NO THROW\n"; }
    catch (Throwable $e) { echo "$label: REJECTED\n"; }
}

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.fused_ins");
$c->execute("CREATE TABLE test.fused_ins (
    i8 Int8, u8 UInt8, i16 Int16, u16 UInt16,
    i32 Int32, u32 UInt32, i64 Int64, u64 UInt64,
    f32 Float32, f64 Float64
) ENGINE=Memory");

$cols = ["i8","u8","i16","u16","i32","u32","i64","u64","f32","f64"];

/* Row A: positional, lower/edge boundaries. UInt64 max exceeds PHP_INT_MAX
 * so it travels as a decimal string; that is the fused UInt64 form. */
$rowA = [-128, 255, -32768, 65535, -2147483648, 4294967295,
         PHP_INT_MIN, "18446744073709551615", 0.5, 1.25];

/* Row B: associative keys (name-fallback path) + UInt32 hex-string form. */
$rowB = ["i8"=>127, "u8"=>0, "i16"=>32767, "u16"=>0, "i32"=>2147483647,
         "u32"=>"0xFFFFFFFF", "i64"=>PHP_INT_MAX, "u64"=>"0", "f32"=>-0.5, "f64"=>-2.5];

$c->insert("test.fused_ins", $cols, [$rowA]);

/* By-ref rows and by-ref cells must be dereferenced, not rejected. */
$byref = [$rowB];
foreach ($byref as &$r) { foreach ($r as &$cell) { $cell = $cell; } unset($cell); } unset($r);
$c->insert("test.fused_ins", $cols, $byref);

foreach ($c->select("SELECT * FROM test.fused_ins ORDER BY i8") as $row) {
    echo json_encode($row), "\n";
}

/* Range / type rejections still fire through the fused path. */
probe("i8 over",   fn() => $c->insert("test.fused_ins", ["i8"], [[128]]));
probe("i8 under",  fn() => $c->insert("test.fused_ins", ["i8"], [[-129]]));
probe("u8 over",   fn() => $c->insert("test.fused_ins", ["u8"], [[256]]));
probe("u32 neg",   fn() => $c->insert("test.fused_ins", ["u32"], [[-1]]));
probe("f64 array", fn() => $c->insert("test.fused_ins", ["f64"], [[[1,2]]]));
probe("i64 float", fn() => $c->insert("test.fused_ins", ["i64"], [[1.5]]));
probe("short row", fn() => $c->insert("test.fused_ins", ["i8","u8"], [[1]]));

$c->execute("DROP TABLE test.fused_ins");

/* External-table numeric column also routes through the fused builder. */
$ext = $c->selectWithExternalData(
    "SELECT sum(x) AS s FROM ext",
    [["name"=>"ext", "columns"=>["x"=>"Int64"], "rows"=>[[10],[20],[30]]]]
);
echo "external sum=", $ext[0]["s"], "\n";

/* Streaming write() (positional, no name fallback) also fused. */
$c->execute("DROP TABLE IF EXISTS test.fused_stream");
$c->execute("CREATE TABLE test.fused_stream (a Int64, b UInt32, c Float64) ENGINE=Memory");
$c->writeStart("test.fused_stream", ["a","b","c"]);
$c->write([[1, 100, 1.5], [2, 200, 2.5]]);
$c->writeEnd();
echo "stream sum=", $c->select("SELECT sum(a) AS a, sum(b) AS b FROM test.fused_stream")[0]["a"], "\n";
$c->execute("DROP TABLE test.fused_stream");
?>
--EXPECT--
{"i8":-128,"u8":255,"i16":-32768,"u16":65535,"i32":-2147483648,"u32":4294967295,"i64":-9223372036854775808,"u64":"18446744073709551615","f32":0.5,"f64":1.25}
{"i8":127,"u8":0,"i16":32767,"u16":0,"i32":2147483647,"u32":4294967295,"i64":9223372036854775807,"u64":0,"f32":-0.5,"f64":-2.5}
i8 over: REJECTED
i8 under: REJECTED
u8 over: REJECTED
u32 neg: REJECTED
f64 array: REJECTED
i64 float: REJECTED
short row: REJECTED
external sum=60
stream sum=3
