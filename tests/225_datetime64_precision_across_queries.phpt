--TEST--
ClickHouse DateTime64 and Time64 precision is read per column across consecutive queries
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
require __DIR__ . "/_clickhouse.inc";
clickhouse_skip_if_no_server();
?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$mode = ClickHouse::DATE_AS_STRINGS;

$has_time64 = true;
try {
    $c->select("SELECT CAST(0 AS Time64(0)) AS x");
} catch (ClickHouseException $e) {
    $has_time64 = false;
}

$cases = [
    3 => ["2024-01-02 03:04:05.123", "12.34", "01:02:03.123"],
    6 => ["2024-01-02 03:04:05.123456", "12.340000", "01:02:03.123456"],
];

$bad = 0;
$first = null;
for ($i = 0; $i < 400; $i++) {
    $p = ($i % 2) ? 3 : 6;
    $sql = "SELECT toDateTime64('2024-01-02 03:04:05.123456', $p, 'UTC') AS t, "
         . "toDecimal64('12.34', $p == 3 ? 2 : 6) AS d"
         . ($has_time64 ? ", CAST('01:02:03.123456' AS Time64($p)) AS tm" : "");
    $r = $c->select($sql, [], $mode)[0];
    $ok = $r["t"] === $cases[$p][0] && $r["d"] === $cases[$p][1]
        && (!$has_time64 || $r["tm"] === $cases[$p][2]);
    if (!$ok) {
        if ($first === null) {
            $first = "iter $i p=$p got " . json_encode($r);
        }
        ++$bad;
    }
}
echo "mismatches: $bad / 400\n";
if ($first !== null) {
    echo $first, "\n";
}
?>
--EXPECT--
mismatches: 0 / 400
