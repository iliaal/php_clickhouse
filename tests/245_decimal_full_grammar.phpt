--TEST--
Decimal inserts validate the whole string instead of truncating malformed text
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.t_85s_dec");
$c->execute("CREATE TABLE test.t_85s_dec (n UInt8, d Decimal(9,2), nd Nullable(Decimal(18,4))) ENGINE=Memory");

/* Each of these used to be stored as a truncated or zero value. */
$bad = ["1.2345x", "-", ".", "-.", "", "1.23 ", "99999999999.00x", "7.50\0"];
foreach ($bad as $i => $value) {
    try {
        $c->insert("test.t_85s_dec", ["n", "d", "nd"], [[100 + $i, $value, null]]);
        echo json_encode($value), ": accepted\n";
    } catch (ClickHouseException $e) {
        echo json_encode($value), ": ", strpos($e->getMessage(), "not a valid decimal") !== false ? "rejected" : $e->getMessage(), "\n";
    }
}
try {
    $c->insert("test.t_85s_dec", ["n", "d", "nd"], [[120, "1", "2.50000x"]]);
    echo "nullable: accepted\n";
} catch (ClickHouseException $e) {
    echo "nullable: ", strpos($e->getMessage(), "not a valid decimal") !== false ? "rejected" : $e->getMessage(), "\n";
}

$valid = ["1.5", "-1.5", ".5", "-.5", "5.", "0", "-0", "00012.30", "9999999.99", "-9999999.99", 1.25, -0.5, false];
$rows = [];
foreach ($valid as $i => $value) $rows[] = [$i, $value, $i % 2 ? null : $value];
$c->insert("test.t_85s_dec", ["n", "d", "nd"], $rows);
foreach ($c->select("SELECT n, d, nd FROM test.t_85s_dec ORDER BY n") as $row) {
    echo $row["n"], " ", json_encode($valid[$row["n"]] ?? null), " => ", $row["d"], " ", json_encode($row["nd"]), "\n";
}
$c->execute("DROP TABLE test.t_85s_dec");
?>
--EXPECT--
"1.2345x": rejected
"-": rejected
".": rejected
"-.": rejected
"": rejected
"1.23 ": rejected
"99999999999.00x": rejected
"7.50\u0000": rejected
nullable: rejected
0 "1.5" => 1.50 "1.5000"
1 "-1.5" => -1.50 null
2 ".5" => 0.50 "0.5000"
3 "-.5" => -0.50 null
4 "5." => 5.00 "5.0000"
5 "0" => 0.00 null
6 "-0" => 0.00 "0.0000"
7 "00012.30" => 12.30 null
8 "9999999.99" => 9999999.99 "9999999.9900"
9 "-9999999.99" => -9999999.99 null
10 1.25 => 1.25 "1.2500"
11 -0.5 => -0.50 null
12 false => 0.00 "0.0000"
