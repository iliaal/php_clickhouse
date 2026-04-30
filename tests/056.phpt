--TEST--
ClickHouse setVerbose: lifecycle event tracing via callable sink and stderr
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());

// Capturing sink: tally events by type. Block count varies by server
// version (system.numbers can come in 1+ blocks plus end-of-stream
// markers), so we assert "at least one of each".
$tally = [];
$ret = $c->setVerbose(function(string $event, array $ctx) use (&$tally) {
    $tally[$event] = ($tally[$event] ?? 0) + 1;
});
var_dump($ret === $c);

$rows = $c->select("SELECT number AS n FROM system.numbers LIMIT 3");
echo "rows_count=", count($rows), "\n";
echo "select_start=", ($tally["select_start"] ?? 0) >= 1 ? "yes" : "no", "\n";
echo "data_block=", ($tally["data_block"] ?? 0) >= 1 ? "yes" : "no", "\n";
echo "select_finish=", ($tally["select_finish"] ?? 0) >= 1 ? "yes" : "no", "\n";

// Execute path: emits execute_start + execute_finish, no data_block.
$tally = [];
$c->execute("CREATE DATABASE IF NOT EXISTS test");
echo "execute_start=", ($tally["execute_start"] ?? 0), "\n";
echo "execute_finish=", ($tally["execute_finish"] ?? 0), "\n";
echo "execute_data_block=", ($tally["data_block"] ?? 0), "\n";

// Server exception fires server_exception.
$tally = [];
try { $c->execute("THIS IS NOT VALID SQL"); } catch (ClickHouseException $e) {}
echo "server_exception=", ($tally["server_exception"] ?? 0) >= 1 ? "yes" : "no", "\n";

// Disable: no further events.
$tally = [];
$c->setVerbose(false);
$c->select("SELECT 1");
echo "after_disable_total=", count($tally), "\n";

// Stderr mode: chainable, runs without crashing. We can't easily
// capture this process's stderr, so assert chainable + no crash and
// match the stderr noise via %A.
$ret = $c->setVerbose(true);
var_dump($ret === $c);
$c->select("SELECT 1");
$c->setVerbose(false);
echo "stderr_mode_ok=yes\n";

// Reject non-bool, non-null, non-callable.
try {
    $c->setVerbose(42);
    echo "int_arg: NO EXCEPTION (BUG)\n";
} catch (ClickHouseException $e) {
    echo "int_arg rejected: ", $e->getMessage(), "\n";
}

// null is accepted as a synonym for false, matching the ?callable
// signature of setProgressCallback / setProfileCallback. The previous
// version threw on null, which surprised callers using the obvious
// "remove the sink" idiom.
$tally = [];
$c->setVerbose(function(string $event, array $ctx) use (&$tally) {
    $tally[$event] = 1;
});
$c->setVerbose(null);
$c->select("SELECT 1");
echo "after_null_disable_total=", count($tally), "\n";
?>
--EXPECTF--
bool(true)
rows_count=3
select_start=yes
data_block=yes
select_finish=yes
execute_start=1
execute_finish=1
execute_data_block=0
server_exception=yes
after_disable_total=0
bool(true)
%Astderr_mode_ok=yes
int_arg rejected: setVerbose expects bool, null, or callable
after_null_disable_total=0
