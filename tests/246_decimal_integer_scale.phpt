--TEST--
Decimal inserts scale integer-form values instead of storing raw units
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.t_85s_decint");
$c->execute("CREATE TABLE test.t_85s_decint (n UInt8, d Decimal(9,2), nd Nullable(Decimal(18,4)), ad Array(Decimal(9,2)), wd Decimal128(10)) ENGINE=Memory");

$values = ["1", "-1", "0", "00042", "9999999", 7, -3, true, 0];
$rows = [];
foreach ($values as $i => $v) $rows[] = [$i, $v, $v, [$v, "2.5"], $v];
$c->insert("test.t_85s_decint", ["n", "d", "nd", "ad", "wd"], $rows);
foreach ($c->select("SELECT n, d, nd, ad, wd FROM test.t_85s_decint ORDER BY n") as $row) {
    echo json_encode($values[$row["n"]]), " => ", $row["d"], " ", $row["nd"], " ", json_encode($row["ad"]), " ", $row["wd"], "\n";
}

try {
    $c->insert("test.t_85s_decint", ["n", "d", "nd", "ad", "wd"], [[50, "10000000", null, [], 0]]);
    echo "over-range: accepted\n";
} catch (ClickHouseException $e) {
    echo "over-range: ", strpos($e->getMessage(), "exceeds the range") !== false ? "rejected" : $e->getMessage(), "\n";
}

$r = $c->selectWithExternalData("SELECT toInt64(v * 100) AS v FROM e ORDER BY v", [
    ["name" => "e", "columns" => ["v" => "Decimal(9,2)"], "rows" => [[5], ["12"]]],
]);
echo "external: ", json_encode(array_column($r, "v")), "\n";
$c->execute("DROP TABLE test.t_85s_decint");
?>
--EXPECT--
"1" => 1.00 1.0000 ["1.00","2.50"] 1.0000000000
"-1" => -1.00 -1.0000 ["-1.00","2.50"] -1.0000000000
"0" => 0.00 0.0000 ["0.00","2.50"] 0.0000000000
"00042" => 42.00 42.0000 ["42.00","2.50"] 42.0000000000
"9999999" => 9999999.00 9999999.0000 ["9999999.00","2.50"] 9999999.0000000000
7 => 7.00 7.0000 ["7.00","2.50"] 7.0000000000
-3 => -3.00 -3.0000 ["-3.00","2.50"] -3.0000000000
true => 1.00 1.0000 ["1.00","2.50"] 1.0000000000
0 => 0.00 0.0000 ["0.00","2.50"] 0.0000000000
over-range: rejected
external: [500,1200]
