--TEST--
ClickHouse object lifecycle: failed construct, half-finished insert, missing writeStart
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

function expect_throw(string $label, callable $fn): void {
    try { $fn(); echo $label, ": no throw\n"; }
    catch (ClickHouseException $e) { echo $label, ": throw\n"; }
}

// __construct against an unreachable host throws and the next method
// call must throw cleanly (not std::out_of_range escaping the boundary).
expect_throw("bad host construct", function () {
    new ClickHouse([
        "host" => "127.0.0.1",
        "port" => 1,        // nothing listening
        "connect_timeout" => 1,
    ]);
});

// writeEnd without writeStart throws.
$c = new ClickHouse(clickhouse_test_config());
expect_throw("writeEnd no start", function () use ($c) {
    $c->writeEnd();
});

// write without writeStart throws.
expect_throw("write no start", function () use ($c) {
    $c->write([[1]]);
});

// writeStart followed by orphan __destruct (no writeEnd) must not crash.
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.lifecycle_t");
$c->execute("CREATE TABLE test.lifecycle_t (id UInt32) ENGINE = Memory");
$c2 = new ClickHouse(clickhouse_test_config());
$c2->writeStart("test.lifecycle_t", ["id"]);
$c2->write([[1], [2]]);
unset($c2);  // __destruct runs with the insert still pending
echo "destruct after orphan writeStart: ok\n";

// Re-construct on the same handle is rejected (would leak the old Client).
$c3 = new ClickHouse(clickhouse_test_config());
expect_throw("double construct", function () use ($c3) {
    $c3->__construct(clickhouse_test_config());
});

$c->execute("DROP TABLE test.lifecycle_t");
?>
--EXPECT--
bad host construct: throw
writeEnd no start: throw
write no start: throw
destruct after orphan writeStart: ok
double construct: throw
