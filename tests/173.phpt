--TEST--
DR-002: non-Nullable Decimal rejects over-precision / over-scale instead of silently storing a wrong value
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// ColumnDecimal::Append(string) scales the text into the backing int with no
// precision/scale check, so a native block insert used to silently store an
// out-of-range value (Decimal(5,2) accepting 1000.00, truncating 12.999 to
// 12.99). The boundary now validates the plain-decimal form.

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");

function try_insert($c, $label, $type, $val) {
    $c->execute("DROP TABLE IF EXISTS test.dr002");
    $c->execute("CREATE TABLE test.dr002 (v $type) ENGINE = Memory");
    try {
        $c->insert("test.dr002", array('v'), array(array($val)));
        $r = $c->select("SELECT toString(v) s FROM test.dr002");
        echo "$label: stored ", $r[0]['s'], "\n";
    } catch (ClickHouseException $e) {
        echo "$label: REJECTED\n";
    }
}

// over precision (integer part too big) and over scale (extra fractional digits)
try_insert($c, "Dec(5,2) 1000.00", "Decimal(5,2)", "1000.00");
try_insert($c, "Dec(5,2) 12.999",  "Decimal(5,2)", "12.999");
// in-range values still work at the boundary
try_insert($c, "Dec(5,2) 999.99",  "Decimal(5,2)", "999.99");
try_insert($c, "Dec(5,2) -999.99", "Decimal(5,2)", "-999.99");
try_insert($c, "Dec(5,2) 0.5",     "Decimal(5,2)", "0.5");

$c->execute("DROP TABLE IF EXISTS test.dr002");
?>
--EXPECT--
Dec(5,2) 1000.00: REJECTED
Dec(5,2) 12.999: REJECTED
Dec(5,2) 999.99: stored 999.99
Dec(5,2) -999.99: stored -999.99
Dec(5,2) 0.5: stored 0.5
