--TEST--
ClickHouse review hardening: Decimal scale bound, throwing __toString, nested refs, DateTime64 float, IPv4 float
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");

function expect_throw(string $label, callable $fn): void {
    try { $fn(); echo $label, ": no throw\n"; }
    catch (ClickHouseException $e) { echo $label, ": throw\n"; }
    catch (Throwable $e) { echo $label, ": ", get_class($e), "(", $e->getMessage(), ")\n"; }
}

// CR-001: reading a Decimal with scale > 38 (Decimal256 range) must throw a
// bounded error, not smash the fixed stack buffer. Populate server-side
// because the Int128-backed insert path can't encode Decimal256.
$c->execute("DROP TABLE IF EXISTS test.h_dec");
$c->execute("CREATE TABLE test.h_dec (d Decimal(50,40)) ENGINE = Memory");
$c->execute("INSERT INTO test.h_dec VALUES (1.5)");
expect_throw("decimal scale>38 read", fn() => $c->select("SELECT d FROM test.h_dec"));
echo "alive after guard: ", $c->select("SELECT 7 x", [], ClickHouse::FETCH_ONE) === 7 ? "yes" : "no", "\n";
// A normal Decimal128 still round-trips.
$c->execute("DROP TABLE IF EXISTS test.h_dec2");
$c->execute("CREATE TABLE test.h_dec2 (d Decimal(10,2)) ENGINE = Memory");
$c->insert("test.h_dec2", ["d"], [["12.34"]]);
echo "decimal128 ok: ", $c->select("SELECT d FROM test.h_dec2", [], ClickHouse::FETCH_ONE), "\n";

// CR-002: a throwing __toString() on an insert cell must surface the original
// exception (not commit a corrupted "" row) and leave the client usable.
$c->execute("DROP TABLE IF EXISTS test.h_s");
$c->execute("CREATE TABLE test.h_s (v String) ENGINE = Memory");
$boom = new class {
    public function __toString(): string { throw new RuntimeException("boom"); }
};
try {
    $c->insert("test.h_s", ["v"], [[$boom]]);
    echo "tostring: no throw\n";
} catch (RuntimeException $e) {
    echo "tostring surfaced: ", $e->getMessage(), "\n";
}
echo "client usable after: ", $c->select("SELECT 1 x", [], ClickHouse::FETCH_ONE) === 1 ? "yes" : "no", "\n";
echo "no row committed: ", (int) $c->select("SELECT count() c FROM test.h_s", [], ClickHouse::FETCH_ONE), "\n";

// CR-005: a reference inside a nested array element derefs instead of throwing
// the misleading "array/object/resource" error.
$c->execute("DROP TABLE IF EXISTS test.h_arr");
$c->execute("CREATE TABLE test.h_arr (a Array(Int32)) ENGINE = Memory");
$x = 5;
$inner = [1, &$x, 3];
$c->insert("test.h_arr", ["a"], [[$inner]]);
echo "nested ref: ", implode(",", $c->select("SELECT a FROM test.h_arr", [], ClickHouse::FETCH_ONE)), "\n";

// CR-006: float into DateTime64(9) is rejected (lossy); int ticks work.
$c->execute("DROP TABLE IF EXISTS test.h_dt");
$c->execute("CREATE TABLE test.h_dt (t DateTime64(9)) ENGINE = Memory");
expect_throw("dt64(9) float", fn() => $c->insert("test.h_dt", ["t"], [[1700000000.5]]));
$c->insert("test.h_dt", ["t"], [[1700000000000000000]]);
echo "dt64(9) int ok: ", (int) $c->select("SELECT count() c FROM test.h_dt", [], ClickHouse::FETCH_ONE), "\n";

// CR-007: IPv4 accepts an integral float (consistent with integer columns);
// a fractional float is rejected.
$c->execute("DROP TABLE IF EXISTS test.h_ip");
$c->execute("CREATE TABLE test.h_ip (a IPv4) ENGINE = Memory");
$c->insert("test.h_ip", ["a"], [[16909060.0]]);
echo "ipv4 float: ", $c->select("SELECT a FROM test.h_ip", [], ClickHouse::FETCH_ONE), "\n";
expect_throw("ipv4 fractional", fn() => $c->insert("test.h_ip", ["a"], [[1.5]]));

foreach (["h_dec","h_dec2","h_s","h_arr","h_dt","h_ip"] as $t) $c->execute("DROP TABLE test.$t");
?>
--EXPECT--
decimal scale>38 read: throw
alive after guard: yes
decimal128 ok: 12.34
tostring surfaced: boom
client usable after: yes
no row committed: 0
nested ref: 1,5,3
dt64(9) float: throw
dt64(9) int ok: 1
ipv4 float: 1.2.3.4
ipv4 fractional: throw
