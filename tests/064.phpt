--TEST--
ClickHouse server-side typed Float params are locale-independent
--SKIPIF--
<?php
require __DIR__ . "/_clickhouse.inc";
clickhouse_skip_if_no_server();
// de_DE.UTF-8 has to actually exist for the locale switch to take. Skip
// when the runner image doesn't ship it (Alpine, minimal Docker, etc.).
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

// Regression for CR-303: formatScalarParam IS_DOUBLE used to call
// snprintf("%.17g") which honors LC_NUMERIC. Under de_DE the decimal
// point became a comma, the wire payload looked like "1,5" and the
// server rejected it. php_gcvt with explicit '.' is locale-independent.

$prev = setlocale(LC_NUMERIC, 0);
$applied = setlocale(LC_NUMERIC, 'de_DE.UTF-8', 'de_DE.utf8', 'de_DE');
echo "locale: ", ($applied === false ? "(not set)" : $applied), "\n";

$c = new ClickHouse(clickhouse_test_config());

// Sanity: PHP itself sees the comma decimal under de_DE.
$probe = sprintf('%g', 1.5);
echo "php sprintf 1.5 under locale: ", $probe, "\n";

// Server-side typed Float64 round-trip. With the prior bug this throws
// because "1,5" is not a valid Float64 literal.
$res = $c->select("SELECT {x:Float64} AS x", ["x" => 1.5], ClickHouse::FETCH_ONE);
echo "float64 1.5 round-trip: ", $res, "\n";

$res = $c->select("SELECT {x:Float64} AS x", ["x" => 0.1], ClickHouse::FETCH_ONE);
echo "float64 0.1 round-trip: ", $res, "\n";

// Restore locale before exit so other tests aren't affected.
setlocale(LC_NUMERIC, $prev);
?>
--EXPECTF--
locale: %s
php sprintf 1.5 under locale: %s
float64 1.5 round-trip: 1.5
float64 0.1 round-trip: 0.1
