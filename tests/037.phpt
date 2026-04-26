--TEST--
ClickHouse setProgressCallback fires + getStatistics returns rows/bytes
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";
$c = new ClickHouse(clickhouse_test_config());

// Progress callback collects rows seen across all packets.
$seen_rows = 0;
$call_count = 0;
$c->setProgressCallback(function (array $p) use (&$seen_rows, &$call_count) {
    $seen_rows += $p["rows"];
    $call_count++;
});

$res = $c->select("SELECT count() FROM numbers(100000)");
echo "select count: ", $res[0]["count()"], "\n";
echo "callback fired: ", ($call_count > 0 ? "yes" : "no"), "\n";
echo "rows>=100000: ", ($seen_rows >= 100000 ? "yes" : "no"), "\n";

$stats = $c->getStatistics();
echo "stat rows>=100000: ", ($stats["rows_read"] >= 100000 ? "yes" : "no"), "\n";
echo "stat bytes>0: ",      ($stats["bytes_read"] > 0 ? "yes" : "no"), "\n";
echo "stat blocks>=1: ",    ($stats["blocks"] >= 1 ? "yes" : "no"), "\n";
echo "stat elapsed>0: ",    ($stats["elapsed_ms"] > 0 ? "yes" : "no"), "\n";

// Stats reset on each call.
$c->select("SELECT 1");
$stats = $c->getStatistics();
echo "after small: rows<10: ", ($stats["rows_read"] < 10 ? "yes" : "no"), "\n";

// Removing the callback works.
$c->setProgressCallback(null);
$call_count = 0;
$c->select("SELECT count() FROM numbers(100000)");
echo "no callback: ", ($call_count === 0 ? "0" : (string)$call_count), "\n";

// Bad callable rejected.
try {
    $c->setProgressCallback("not_a_real_function_xyz");
    echo "bad: no throw\n";
} catch (ClickHouseException $e) {
    echo "bad: throw\n";
}
?>
--EXPECT--
select count: 100000
callback fired: yes
rows>=100000: yes
stat rows>=100000: yes
stat bytes>0: yes
stat blocks>=1: yes
stat elapsed>0: yes
after small: rows<10: yes
no callback: 0
bad: throw
