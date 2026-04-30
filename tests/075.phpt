--TEST--
ClickHouse Date / DateTime / DateTime64 inserts reject trailing garbage and normalized dates
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// Regression for CR-004: std::get_time stops at the first non-matching
// character without raising failbit, so "2024-01-01abc" parsed as
// 2024-01-01. timegm normalizes invalid dates silently, so "2024-02-30"
// became 2024-03-01. The DateTime64 fractional path stopped reading
// fractional digits without rejecting trailing junk, so
// "2024-01-01 00:00:00.123abc" silently dropped the abc.
// Now: peek() must hit EOF after the format, gmtime round-trip must
// match, and the fractional path requires no characters after the
// digits.

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.dt_strict");
$c->execute("CREATE TABLE test.dt_strict (
    d Date, dt DateTime, dt64 DateTime64(3)
) ENGINE=Memory");

$probes = [
    "Date trailing garbage"        => [['d'], [['2024-01-01abc']]],
    "DateTime trailing garbage"    => [['dt'], [['2024-01-01 00:00:00abc']]],
    "DateTime64 trailing garbage"  => [['dt64'], [['2024-01-01 00:00:00.123abc']]],
    "Date Feb 30 (normalized)"     => [['d'], [['2024-02-30']]],
    "DateTime Feb 30"              => [['dt'], [['2024-02-30 00:00:00']]],
    "DateTime64 Feb 30"            => [['dt64'], [['2024-02-30 00:00:00.000']]],
    "Date Apr 31 (normalized)"     => [['d'], [['2024-04-31']]],
    "Date month 13"                => [['d'], [['2024-13-01']]],
];
foreach ($probes as $label => [$cols, $vals]) {
    try { $c->insert("test.dt_strict", $cols, $vals); echo "$label: NO THROW\n"; }
    catch (ClickHouseException $e) { echo "$label: REJECTED\n"; }
}

// Sanity: well-formed values still round-trip.
$c->insert("test.dt_strict", ['d', 'dt', 'dt64'],
    [['2024-01-15', '2024-01-15 12:34:56', '2024-01-15 12:34:56.789']]);
$rows = $c->select("SELECT toString(d) AS d, toString(dt) AS dt, toString(dt64) AS dt64 FROM test.dt_strict");
echo "rowcount: ", count($rows), "\n";
echo "d: ", $rows[0]['d'], "\n";
echo "dt: ", $rows[0]['dt'], "\n";
echo "dt64: ", $rows[0]['dt64'], "\n";

$c->execute("DROP TABLE test.dt_strict");
?>
--EXPECT--
Date trailing garbage: REJECTED
DateTime trailing garbage: REJECTED
DateTime64 trailing garbage: REJECTED
Date Feb 30 (normalized): REJECTED
DateTime Feb 30: REJECTED
DateTime64 Feb 30: REJECTED
Date Apr 31 (normalized): REJECTED
Date month 13: REJECTED
rowcount: 1
d: 2024-01-15
dt: 2024-01-15 12:34:56
dt64: 2024-01-15 12:34:56.789
