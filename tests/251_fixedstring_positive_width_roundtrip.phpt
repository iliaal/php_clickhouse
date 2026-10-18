--TEST--
ClickHouse FixedString columns with positive widths round-trip after width validation
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$client = new ClickHouse(clickhouse_test_config());
$client->execute("DROP TABLE IF EXISTS t_eyn1_fs");
$client->execute("CREATE TABLE t_eyn1_fs (id UInt8, a FixedString(16), n Nullable(FixedString(4)),"
    . " l LowCardinality(FixedString(1))) ENGINE = Memory");
$client->insert("t_eyn1_fs", ["id", "a", "n", "l"], [
    [1, "0123456789abcdef", "abcd", "x"],
    [2, "short", null, "y"],
]);

foreach ([0, ClickHouse::FIXEDSTRING_BINARY] as $flags) {
    foreach ($client->select("SELECT id, a, n, l FROM t_eyn1_fs ORDER BY id", [], $flags) as $row) {
        echo $row["id"], " ", bin2hex($row["a"]), " ", $row["n"] === null ? "NULL" : $row["n"], " ", $row["l"], "\n";
    }
}

try {
    $client->insert("t_eyn1_fs", ["id", "a", "n", "l"], [[3, str_repeat("z", 17), null, "x"]]);
    echo "oversized accepted\n";
} catch (ClickHouseException $e) {
    echo "oversized rejected\n";
}

$client->execute("DROP TABLE t_eyn1_fs");
?>
--EXPECT--
1 30313233343536373839616263646566 abcd x
2 73686f7274 NULL y
1 30313233343536373839616263646566 abcd x
2 73686f72740000000000000000000000 NULL y
oversized rejected
