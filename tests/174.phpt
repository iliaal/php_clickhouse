--TEST--
DR-003: Date / Date32 / DateTime reject out-of-range values instead of silently wrapping
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// clickhouse-cpp narrows the epoch into the column storage (uint16 days for
// Date, uint32 seconds for DateTime) with a bare static_cast, so an
// out-of-range value used to wrap silently (Date "3000-01-01" -> 2102-11-05,
// DateTime "1960-01-01" -> 2096). The boundary now range-checks.

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");

function try_insert($c, $label, $type, $val) {
    $c->execute("DROP TABLE IF EXISTS test.dr003");
    $c->execute("CREATE TABLE test.dr003 (v $type) ENGINE = Memory");
    try {
        $c->insert("test.dr003", array('v'), array(array($val)));
        $r = $c->select("SELECT toString(v) s FROM test.dr003");
        echo "$label: stored ", $r[0]['s'], "\n";
    } catch (ClickHouseException $e) {
        echo "$label: REJECTED\n";
    }
}

// out of range -> rejected
try_insert($c, "Date 3000-01-01",     "Date",   "3000-01-01");
try_insert($c, "DateTime 1960",       "DateTime", "1960-01-01 00:00:00");
try_insert($c, "Date32 3000-01-01",   "Date32", "3000-01-01");
// boundaries and normal values still work
try_insert($c, "Date 2149-06-06",     "Date",   "2149-06-06");
try_insert($c, "Date32 1900-01-01",   "Date32", "1900-01-01");
try_insert($c, "Date32 2299-12-31",   "Date32", "2299-12-31");
try_insert($c, "DateTime 2024",       "DateTime", "2024-01-15 10:30:00");

$c->execute("DROP TABLE IF EXISTS test.dr003");
?>
--EXPECT--
Date 3000-01-01: REJECTED
DateTime 1960: REJECTED
Date32 3000-01-01: REJECTED
Date 2149-06-06: stored 2149-06-06
Date32 1900-01-01: stored 1900-01-01
Date32 2299-12-31: stored 2299-12-31
DateTime 2024: stored 2024-01-15 10:30:00
