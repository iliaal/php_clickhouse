--TEST--
sanitizeError strips the lowercase "while executing" SQL fragment (case-insensitive)
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$ch = new ClickHouse(clickhouse_test_config());
$ch->enableLogQueries(true);

/* ClickHouse 26.x appends a lowercase "while executing 'FUNCTION ...'"
 * fragment that echoes the call's literal arguments (here the message
 * literal, in real cases a bound value). A case-sensitive marker scan
 * missed the lowercase form and left the fragment — and any literal in
 * it — in the message and the query log. The literal must appear exactly
 * once (the genuine error text), not a second time echoed in the fragment. */
try {
    $ch->execute("SELECT throwIf(1, 'SENTINEL_LITERAL')");
    echo "no exception\n";
} catch (ClickHouseException $e) {
    $m = $e->getMessage();
    echo "fragment stripped: ", (stripos($m, "while executing") === false ? "yes" : "no"), "\n";
    echo "literal occurrences: ", substr_count($m, "SENTINEL_LITERAL"), "\n";
}

$logEchoed = false;
foreach ($ch->getLogQueries() as $entry) {
    if (substr_count($entry["error_message"], "SENTINEL_LITERAL") > 1) {
        $logEchoed = true;
    }
}
echo "log fragment echo: ", ($logEchoed ? "yes" : "no"), "\n";
?>
--EXPECT--
fragment stripped: yes
literal occurrences: 1
log fragment echo: no
