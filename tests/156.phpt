--TEST--
ClickHouse verbose server_exception message is sanitized like the thrown one
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// Regression for CR-004: the verbose "server_exception" event emitted the
// raw server display_text, which embeds the executing SQL (and any bound
// literals in it). throwClickHouseError() strips that tail at the
// "while executing" marker; the verbose path must do the same so a trace
// sink does not leak what the exception itself hides.

$c = new ClickHouse(clickhouse_test_config());

$verbose_msg = null;
$c->setVerbose(function ($event, $ctx) use (&$verbose_msg) {
    if ($event === "server_exception") {
        $verbose_msg = $ctx["message"];
    }
});

$thrown_msg = null;
try {
    // throwIf raises mid-execution; display_text carries a long
    // "while executing 'FUNCTION throwIf(...)'" tail.
    $c->select("SELECT throwIf(number = 2, 'BOOM') FROM system.numbers LIMIT 5", []);
} catch (ClickHouseException $e) {
    $thrown_msg = $e->getMessage();
}

var_dump($verbose_msg === $thrown_msg);
var_dump(strpos($verbose_msg, "while executing") === false);
echo $verbose_msg, "\n";
?>
--EXPECT--
bool(true)
bool(true)
DB::Exception: BOOM
