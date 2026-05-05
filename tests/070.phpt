--TEST--
ClickHouse Map(Float, *) read keys are locale-independent
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
require __DIR__ . "/_clickhouse.inc";
clickhouse_skip_if_no_server();
$prev = setlocale(LC_NUMERIC, 0);
$got = setlocale(LC_NUMERIC, 'de_DE.UTF-8', 'de_DE.utf8', 'de_DE');
setlocale(LC_NUMERIC, $prev);
if ($got === false) {
    echo "skip de_DE locale not installed";
}
?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// Regression for CR-507: Map read decoder formatted Float keys via
// snprintf("%.17g", k) which honors LC_NUMERIC. Under de_DE the same
// Float64 map key materializes under "1,5" instead of "1.5", so the
// PHP-side array key changes between locales for identical server
// data. The fix uses php_gcvt with explicit '.' separator, the same
// path applied at the SQL parameter boundary in CR-303.

$prev = setlocale(LC_NUMERIC, 0);
$applied = setlocale(LC_NUMERIC, 'de_DE.UTF-8', 'de_DE.utf8', 'de_DE');
echo "locale: ", ($applied === false ? "(not set)" : $applied), "\n";

// Probe: PHP itself sees the comma decimal under de_DE.
echo "php sprintf 1.5: ", sprintf('%g', 1.5), "\n";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.fmap_t");
$c->execute("CREATE TABLE test.fmap_t (m Map(Float64, String)) ENGINE = Memory");
$c->insert("test.fmap_t", ["m"], [[[1.5 => "a", 0.1 => "b"]]]);

$rows = $c->select("SELECT m FROM test.fmap_t");
$keys = array_keys($rows[0]["m"]);
sort($keys);
echo "key 0: ", $keys[0], "\n";
echo "key 1: ", $keys[1], "\n";

setlocale(LC_NUMERIC, $prev);
$c->execute("DROP TABLE test.fmap_t");
?>
--EXPECTF--
locale: %s
php sprintf 1.5: %s
key 0: 0.1
key 1: 1.5
