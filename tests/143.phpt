--TEST--
selectStreamCallback emits verbose lifecycle events (select_start, data_block, select_finish)
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());

/* selectStreamCallback attached the verbose handler to the query but
 * never emitted the start/data/finish events do_select_into emits, so a
 * verbose sink saw nothing for streamed reads. */
$tally = [];
$c->setVerbose(function (string $event, array $ctx) use (&$tally) {
    $tally[$event] = ($tally[$event] ?? 0) + 1;
});

$seen = 0;
$c->selectStreamCallback("SELECT number AS n FROM system.numbers LIMIT 3",
    function ($row) use (&$seen) { $seen += 1; });

echo "rows=", $seen, "\n";
echo "select_start=", (($tally["select_start"] ?? 0) >= 1 ? "yes" : "no"), "\n";
echo "data_block=", (($tally["data_block"] ?? 0) >= 1 ? "yes" : "no"), "\n";
echo "select_finish=", (($tally["select_finish"] ?? 0) >= 1 ? "yes" : "no"), "\n";

$c->setVerbose(false);
?>
--EXPECT--
rows=3
select_start=yes
data_block=yes
select_finish=yes
