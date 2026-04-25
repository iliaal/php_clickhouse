--TEST--
SeasClick DateTime64 sub-second round-trip
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; seasclick_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new SeasClick(seasclick_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.dt64_t");
$c->execute("CREATE TABLE test.dt64_t (id UInt32, ts DateTime64(3, 'UTC'), ts6 DateTime64(6, 'UTC')) ENGINE = Memory");

// Pass plain epoch seconds (int); the lib scales by 10^precision internally.
// Only ints here; "Y-m-d H:i:s" string input goes through mktime which
// is host-TZ-dependent and would make the test flaky.
$c->insert("test.dt64_t", ["id", "ts", "ts6"], [
    [1, 1714000000, 1714000000],
    [2, 1745515200, 1745515200],
]);

$rows = $c->select("SELECT id, ts, ts6 FROM test.dt64_t ORDER BY id", [], SeasClick::DATE_AS_STRINGS);
foreach ($rows as $r) {
    echo $r["id"], "|", $r["ts"], "|", $r["ts6"], "\n";
}

$c->execute("DROP TABLE test.dt64_t");
?>
--EXPECT--
1|2024-04-24 23:06:40.000|2024-04-24 23:06:40.000000
2|2025-04-24 17:20:00.000|2025-04-24 17:20:00.000000
