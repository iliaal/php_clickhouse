--TEST--
ClickHouse same-client reentry inside row callback throws cleanly instead of crashing
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// Regression for CR-001: a row / progress / profile / verbose callback
// that calls back into the SAME ClickHouse instance pushes a new query
// onto a wire still owned by the outer Client::Impl::ExecuteQuery loop.
// The next ReceivePacket walks invalidated state and SEGVs the worker.
// Per-object QueryActiveGuard rejects the reentry with a ClickHouse
// exception. A separate ClickHouse instance is still allowed.

$c = new ClickHouse(clickhouse_test_config());

// Same-client reentry: should throw, not crash.
try {
    $c->selectStreamCallback(
        "SELECT number FROM system.numbers LIMIT 3",
        function ($row) use ($c) {
            $c->select("SELECT 1", [], ClickHouse::FETCH_ONE);
        }
    );
    echo "same-client: no throw\n";
} catch (ClickHouseException $e) {
    if (strpos($e->getMessage(), "Reentrant") !== false) {
        echo "same-client: REJECTED\n";
    } else {
        echo "same-client: other: ", $e->getMessage(), "\n";
    }
}

// After the failed reentry, the outer client must still be usable.
try {
    $c->resetConnection();
    $r = $c->select("SELECT 42 AS x", [], ClickHouse::FETCH_ONE);
    echo "post-reentry recovery: $r\n";
} catch (Throwable $e) {
    echo "post-reentry recovery: failed: ", $e->getMessage(), "\n";
}

// Separate-client reentry remains supported.
$other = new ClickHouse(clickhouse_test_config());
$seen = 0;
try {
    $c->selectStreamCallback(
        "SELECT number FROM system.numbers LIMIT 3",
        function ($row) use ($other, &$seen) {
            $seen++;
            $other->select("SELECT 1", [], ClickHouse::FETCH_ONE);
        }
    );
    echo "separate-client rows: $seen\n";
} catch (Throwable $e) {
    echo "separate-client: failed: ", $e->getMessage(), "\n";
}
?>
--EXPECT--
same-client: REJECTED
post-reentry recovery: 42
separate-client rows: 3
