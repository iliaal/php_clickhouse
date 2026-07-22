--TEST--
ClickHouse Bool, IPv4 (string + integer), and IPv6 read/write round-trip
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.bip_t");
$c->execute("CREATE TABLE test.bip_t (id UInt32, b Bool, ip4 IPv4, ip6 IPv6) ENGINE = Memory");

$c->insert("test.bip_t", ["id", "b", "ip4", "ip6"], [
    [1, true,  "1.2.3.4",   "::1"],
    [2, false, 16909060,    "2001:db8::1"],   // ip4 given as an integer
    [3, 1,     "10.0.0.1",  "fe80::abcd"],    // bool given as an int
]);

echo "-- round-trip --\n";
foreach ($c->select("SELECT id, b, ip4, ip6 FROM test.bip_t ORDER BY id") as $r) {
    printf("%d b=%s(%s) ip4=%s ip6=%s\n",
        $r["id"], $r["b"] ? "true" : "false", gettype($r["b"]), $r["ip4"], $r["ip6"]);
}

echo "-- integer ip4 matches toIPv4() --\n";
$ours   = $c->select("SELECT toString(ip4) FROM test.bip_t WHERE id = 2", [], ClickHouse::FETCH_ONE);
$server = $c->select("SELECT toString(toIPv4(16909060))", [], ClickHouse::FETCH_ONE);
echo "ours=$ours server=$server match=", $ours === $server ? "1" : "0", "\n";

echo "-- Nullable --\n";
$c->execute("DROP TABLE IF EXISTS test.bipn_t");
$c->execute("CREATE TABLE test.bipn_t (id UInt32, b Nullable(Bool), ip4 Nullable(IPv4), ip6 Nullable(IPv6)) ENGINE = Memory");
$c->insert("test.bipn_t", ["id", "b", "ip4", "ip6"], [
    [1, true, "9.9.9.9", "::2"],
    [2, null, null, null],
]);
foreach ($c->select("SELECT id, b, ip4, ip6 FROM test.bipn_t ORDER BY id") as $r) {
    printf("%d b=%s ip4=%s ip6=%s\n", $r["id"],
        $r["b"] === null ? "NULL" : ($r["b"] ? "1" : "0"),
        $r["ip4"] === null ? "NULL" : $r["ip4"],
        $r["ip6"] === null ? "NULL" : $r["ip6"]);
}

echo "-- bool strings --\n";
$c->execute("DROP TABLE IF EXISTS test.bool_s");
$c->execute("CREATE TABLE test.bool_s (id UInt32, b Bool) ENGINE = Memory");
$c->insert("test.bool_s", ["id", "b"], [
    [1, "true"],
    [2, "false"],
    [3, "0"],
    [4, "1"],
]);
foreach ($c->select("SELECT id, b FROM test.bool_s ORDER BY id") as $r) {
    printf("%d b=%s\n", $r["id"], $r["b"] ? "true" : "false");
}

echo "-- validation --\n";
function expect_throw(string $label, callable $fn): void {
    try { $fn(); echo $label, ": no throw\n"; }
    catch (ClickHouseException $e) { echo $label, ": throw\n"; }
}
expect_throw("ip4 bad string", fn() => $c->insert("test.bip_t", ["id", "ip4"], [[9, "not.an.ip"]]));
expect_throw("ip4 int out of range", fn() => $c->insert("test.bip_t", ["id", "ip4"], [[9, 4294967296]]));
expect_throw("ip6 bad string", fn() => $c->insert("test.bip_t", ["id", "ip6"], [[9, "zzz"]]));
expect_throw("bool string false-word was truthy", fn() =>
    $c->insert("test.bool_s", ["id", "b"], [[9, "no"]]));
expect_throw("bool string garbage", fn() =>
    $c->insert("test.bool_s", ["id", "b"], [[9, "maybe"]]));

$c->execute("DROP TABLE test.bip_t");
$c->execute("DROP TABLE test.bipn_t");
$c->execute("DROP TABLE test.bool_s");
?>
--EXPECT--
-- round-trip --
1 b=true(boolean) ip4=1.2.3.4 ip6=::1
2 b=false(boolean) ip4=1.2.3.4 ip6=2001:db8::1
3 b=true(boolean) ip4=10.0.0.1 ip6=fe80::abcd
-- integer ip4 matches toIPv4() --
ours=1.2.3.4 server=1.2.3.4 match=1
-- Nullable --
1 b=1 ip4=9.9.9.9 ip6=::2
2 b=NULL ip4=NULL ip6=NULL
-- bool strings --
1 b=true
2 b=false
3 b=false
4 b=true
-- validation --
ip4 bad string: throw
ip4 int out of range: throw
ip6 bad string: throw
bool string false-word was truthy: throw
bool string garbage: throw
