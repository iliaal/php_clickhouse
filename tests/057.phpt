--TEST--
ClickHouse progress/profile callback exceptions propagate to caller (regression for silent-drop)
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// Regression: a user-supplied progress/profile callback that throws used to
// have its exception silently overwritten by the surrounding query's own
// throwClickHouseError. Now the user's exception is preserved end-to-end.

class CallbackBoom extends \RuntimeException {}

// 1) Progress callback that throws.
$c = new ClickHouse(clickhouse_test_config());
$c->setProgressCallback(function () {
    throw new CallbackBoom("progress aborted");
});

$got = null;
try {
    $c->select("SELECT count() FROM numbers(100000)");
} catch (\Throwable $e) {
    $got = $e;
}
echo "progress throws: class=", $got ? get_class($got) : "(none)", "\n";
echo "progress throws: message=", $got ? $got->getMessage() : "(none)", "\n";

// 2) Profile callback that throws.
$c2 = new ClickHouse(clickhouse_test_config());
$c2->setProfileCallback(function () {
    throw new CallbackBoom("profile aborted");
});

$got2 = null;
try {
    $c2->select("SELECT count() FROM numbers(100000)");
} catch (\Throwable $e) {
    $got2 = $e;
}
echo "profile throws: class=", $got2 ? get_class($got2) : "(none)", "\n";
echo "profile throws: message=", $got2 ? $got2->getMessage() : "(none)", "\n";

// 3) Recovery: a callback abort leaves the wire mid-stream (clickhouse-cpp's
//    packet loop unwound on the C++ throw without draining the rest of the
//    server response), so callers need resetConnection() before reusing the
//    same client. This is documented contract, not silent breakage.
$c->setProgressCallback(null);
$c->resetConnection();
$res = $c->select("SELECT 1 AS one", [], ClickHouse::FETCH_ONE);
echo "recovered: $res\n";
?>
--EXPECT--
progress throws: class=CallbackBoom
progress throws: message=progress aborted
profile throws: class=CallbackBoom
profile throws: message=profile aborted
recovered: 1
