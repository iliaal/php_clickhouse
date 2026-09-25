--TEST--
ClickHouse UInt64 strings use strict grammar and IPv4 doubles retain the full uint32 range
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("DROP TABLE IF EXISTS test.protocol_conversions");
$c->execute("CREATE TABLE test.protocol_conversions (u UInt64, ip IPv4) ENGINE=Memory");

$c->insert("test.protocol_conversions", ["u", "ip"], [
    [0, 4294967295.0],
    ["1", 2147483648.0],
    ["0xFFFFFFFFFFFFFFFE", 2147483647.0],
    ["18446744073709551615", 0.0],
]);
$rows = $c->select("SELECT u, ip FROM test.protocol_conversions ORDER BY u");
echo "u0=", $rows[0]['u'], "\n";
echo "u1=", $rows[1]['u'], "\n";
echo "u2=", $rows[2]['u'], "\n";
echo "u3=", $rows[3]['u'], "\n";
echo "ip-u0=", $rows[0]['ip'], "\n";
echo "ip-u1=", $rows[1]['ip'], "\n";
echo "ip-maxminus=", $rows[2]['ip'], "\n";
echo "ip-max=", $rows[3]['ip'], "\n";

$c->execute("DROP TABLE IF EXISTS test.protocol_map");
$c->execute("CREATE TABLE test.protocol_map (m Map(UInt64, String)) ENGINE=Memory");
$c->insert("test.protocol_map", ["m"], [[[
    "1" => "one",
    "18446744073709551615" => "max",
]]]);
$map = $c->select("SELECT m FROM test.protocol_map")[0]['m'];
echo "map-one=", $map[1], "\n";
echo "map-max=", $map["18446744073709551615"], "\n";
foreach ([" -1", "+1", "1 "] as $value) {
    try {
        $c->insert("test.protocol_map", ["m"], [[[$value => "bad"]]]);
        echo "map bad: NO THROW\n";
    } catch (ClickHouseException $e) {
        echo "map bad: REJECTED\n";
    }
}

$badU64 = [
    "leading space" => " 1",
    "trailing space" => "1 ",
    "hex space" => "0x 1",
    "leading plus" => "+1",
    "trailing newline" => "1\n",
];
foreach ($badU64 as $label => $value) {
    try {
        $c->insert("test.protocol_conversions", ["u", "ip"], [[$value, 0.0]]);
        echo "u64 $label: NO THROW\n";
    } catch (ClickHouseException $e) {
        echo "u64 $label: REJECTED\n";
    }
}

$badIPv4 = [
    "negative" => -1.0,
    "fractional" => 1.5,
    "above uint32" => 4294967296.0,
];
foreach ($badIPv4 as $label => $value) {
    try {
        $c->insert("test.protocol_conversions", ["u", "ip"], [["0", $value]]);
        echo "ipv4 $label: NO THROW\n";
    } catch (ClickHouseException $e) {
        echo "ipv4 $label: REJECTED\n";
    }
}

$c->execute("DROP TABLE test.protocol_conversions");
$c->execute("DROP TABLE test.protocol_map");
?>
--EXPECT--
u0=0
u1=1
u2=18446744073709551614
u3=18446744073709551615
ip-u0=255.255.255.255
ip-u1=128.0.0.0
ip-maxminus=127.255.255.255
ip-max=0.0.0.0
map-one=one
map-max=max
map bad: REJECTED
map bad: REJECTED
map bad: REJECTED
u64 leading space: REJECTED
u64 trailing space: REJECTED
u64 hex space: REJECTED
u64 leading plus: REJECTED
u64 trailing newline: REJECTED
ipv4 negative: REJECTED
ipv4 fractional: REJECTED
ipv4 above uint32: REJECTED
