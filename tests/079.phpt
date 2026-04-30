--TEST--
ClickHouse Date / DateTime / DateTime64 / Time / Time64 reject non-numeric strings instead of coercing to 0
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// Regression for FR-003: CR-004 made formatted "YYYY-MM-DD..." strings
// validate strictly, but the dash-only gate routed dashless strings
// through zval_get_long, which silently coerced "abc" to 0 and landed
// the cell as the epoch / midnight. Time and Time64 inserts had no
// string handling at all; "abc" likewise coerced to 0. Now: any string
// hits to_time_t / to_time_t_with_frac (which fully validate and fail
// on bad input), and Time/Time64 reject string inputs explicitly until
// a proper "HH:MM:SS" parser is added.

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
foreach (["fr3"] as $t) $c->execute("DROP TABLE IF EXISTS test.$t");
$c->execute("CREATE TABLE test.fr3 (
    d Date, dt DateTime, dt64 DateTime64(3), t Time, t64 Time64(3)
) ENGINE=Memory");

$probes = [
    "Date abc"       => [['d'],    [['abc']]],
    "DateTime abc"   => [['dt'],   [['abc']]],
    "DateTime64 abc" => [['dt64'], [['abc']]],
    "Time abc"       => [['t'],    [['abc']]],
    "Time64 abc"     => [['t64'],  [['abc']]],
    "Date empty"     => [['d'],    [['']]],
    "DateTime numeric-string-only" => [['dt'], [['1234567']]],  // no dash → was coerced to long
];
foreach ($probes as $label => [$cols, $vals]) {
    try { $c->insert("test.fr3", $cols, $vals); echo "$label: NO THROW\n"; }
    catch (ClickHouseException $e) { echo "$label: REJECTED\n"; }
}

// Sanity: numeric (not string) inputs still land — Date / DateTime /
// DateTime64 accept epoch seconds as integers; Time accepts seconds-
// since-midnight as integer / float.
$c->insert("test.fr3", ['d','dt','dt64','t','t64'],
    [[19737, 1705319696, 1705319696.789, 12345, 12345.5]]);
$rows = $c->select("SELECT count() FROM test.fr3", [], ClickHouse::FETCH_ONE);
echo "rowcount: $rows\n";

foreach (["fr3"] as $t) $c->execute("DROP TABLE test.$t");
?>
--EXPECT--
Date abc: REJECTED
DateTime abc: REJECTED
DateTime64 abc: REJECTED
Time abc: REJECTED
Time64 abc: REJECTED
Date empty: REJECTED
DateTime numeric-string-only: REJECTED
rowcount: 1
