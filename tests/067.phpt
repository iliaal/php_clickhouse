--TEST--
ClickHouse selectStreamCallback row callback exception aborts the stream and surfaces to caller
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// Regression for CR-502: selectStreamCallback's OnData closure was the
// only call_user_function site that didn't re-raise on EG(exception).
// The other three (progress / profile / verbose) all check and throw
// to abort the packet loop. Without this check, a row callback that
// threw left the exception buffered while the stream kept consuming
// rows and recordQuerySuccess was still invoked. The fix mirrors the
// progress/profile/verbose pattern.
//
// Verify: a callback that throws on row 3 of 1000 stops at 3, the user
// exception surfaces at the call site, and getStatistics()/last error
// reflect the failure (not a "successful" row count).

$c = new ClickHouse(clickhouse_test_config());

$seen = 0;
try {
    $c->selectStreamCallback(
        "SELECT number FROM numbers(1000)",
        function ($row) use (&$seen) {
            $seen++;
            if ($seen >= 3) {
                throw new RuntimeException("stop at row " . $seen);
            }
        }
    );
    echo "no throw\n";
} catch (RuntimeException $e) {
    echo "user exception: ", $e->getMessage(), "\n";
}

echo "rows seen: ", $seen, "\n";
echo "stream stopped early: ", ($seen < 1000 ? "yes" : "no"), "\n";
?>
--EXPECT--
user exception: stop at row 3
rows seen: 3
stream stopped early: yes
