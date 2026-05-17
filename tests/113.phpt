--TEST--
ClickHouse invalid callback setters preserve the previous callback
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

function rejected(string $label, callable $fn): void {
    try {
        $fn();
        echo "$label: NO THROW\n";
    } catch (Throwable $e) {
        echo "$label: REJECTED\n";
    }
}

$cp = new ClickHouse(clickhouse_test_config());
$progress_count = 0;
$cp->setProgressCallback(function () use (&$progress_count) {
    $progress_count++;
});
rejected("progress bad setter", fn() =>
    $cp->setProgressCallback("not_a_real_progress_callback_review"));
$cp->select("SELECT count() FROM numbers(100000)");
echo "progress preserved: ", ($progress_count > 0 ? "yes" : "no"), "\n";

$cprof = new ClickHouse(clickhouse_test_config());
$profile_count = 0;
$cprof->setProfileCallback(function () use (&$profile_count) {
    $profile_count++;
});
rejected("profile bad setter", fn() =>
    $cprof->setProfileCallback("not_a_real_profile_callback_review"));
$cprof->select("SELECT count() FROM numbers(100000)");
echo "profile preserved: ", ($profile_count > 0 ? "yes" : "no"), "\n";

$cv = new ClickHouse(clickhouse_test_config());
$events = [];
$cv->setVerbose(function (string $event, array $ctx) use (&$events) {
    $events[] = $event;
});
rejected("verbose bad setter", fn() =>
    $cv->setVerbose("not_a_real_verbose_callback_review"));
$cv->execute("SELECT 1");
echo "verbose preserved: ",
    (in_array("execute_start", $events, true) &&
     in_array("execute_finish", $events, true) ? "yes" : "no"), "\n";
?>
--EXPECT--
progress bad setter: REJECTED
progress preserved: yes
profile bad setter: REJECTED
profile preserved: yes
verbose bad setter: REJECTED
verbose preserved: yes
