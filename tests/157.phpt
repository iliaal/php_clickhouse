--TEST--
ClickHouse same-client reentry from progress / profile / verbose callbacks throws cleanly
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// Regression for CR-016: 072 only exercises the reentry guard from a row
// callback. The same per-object QueryActiveGuard must also reject a query
// fired from a progress, profile, or verbose sink on the same instance.
// The outer query scans enough rows to guarantee progress/profile packets.

$OUTER = "SELECT count() FROM (SELECT number FROM system.numbers LIMIT 8000000)";

function reentry_from(callable $install): string
{
    global $OUTER;
    $c = new ClickHouse(clickhouse_test_config());
    $c->setSettings(["max_block_size" => 65536]);
    $verdict = "callback did not fire";
    $reenter = function () use ($c, &$verdict) {
        try {
            $c->select("SELECT 1", [], ClickHouse::FETCH_ONE);
            $verdict = "reentry allowed";
        } catch (ClickHouseException $e) {
            $verdict = strpos($e->getMessage(), "Reentrant") !== false
                ? "REJECTED" : "other: " . $e->getMessage();
        }
    };
    $install($c, $reenter);
    try { $c->select($OUTER, [], ClickHouse::FETCH_ONE); } catch (Throwable $e) {}
    // The outer client must remain usable after the rejected reentry.
    $c->resetConnection();
    $ok = $c->select("SELECT 7", [], ClickHouse::FETCH_ONE);
    return "$verdict / recovery=$ok";
}

echo "verbose:  ", reentry_from(function ($c, $reenter) {
    $c->setVerbose(function ($event, $ctx) use ($reenter) { $reenter(); });
}), "\n";

echo "progress: ", reentry_from(function ($c, $reenter) {
    $c->setProgressCallback(function ($p) use ($reenter) { $reenter(); });
}), "\n";

echo "profile:  ", reentry_from(function ($c, $reenter) {
    $c->setProfileCallback(function ($p) use ($reenter) { $reenter(); });
}), "\n";
?>
--EXPECT--
verbose:  REJECTED / recovery=7
progress: REJECTED / recovery=7
profile:  REJECTED / recovery=7
