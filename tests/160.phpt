--TEST--
ClickHouse by-reference select() params: applyPlaceholders derefs IS_REFERENCE buckets (typed NULL stays NULL, arrays are not stringified)
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

/* A plain `foreach ($params as $k => &$v)` leaves every bucket as
 * IS_REFERENCE. applyPlaceholders() must ZVAL_DEREF before its IS_NULL /
 * IS_ARRAY shape checks, or a by-ref NULL typed param is coerced to '' and
 * a by-ref array param stringifies to the literal "Array". 150.phpt covers
 * by-ref values inside insert columns and settings; this covers the
 * server-side / client-side placeholder path in select(). */
$c = new ClickHouse(clickhouse_test_config());

/* 1. by-ref NULL bound to a server-side Nullable typed placeholder must
 *    reach the server as NULL, not the empty string. */
$p1 = ["p" => null];
foreach ($p1 as $k => &$v) { $v = $v; }
unset($v);
$isNull = $c->select("SELECT {p:Nullable(String)} IS NULL AS n", $p1, ClickHouse::FETCH_ONE);
echo "by-ref NULL typed param IS NULL: ", ($isNull ? "yes" : "no"), "\n";

/* 2. by-ref array bound to a server-side Array typed placeholder must be
 *    sent as an array (length 3), not stringified. */
$p2 = ["ids" => [10, 20, 30]];
foreach ($p2 as $k => &$v) { $v = $v; }
unset($v);
$len = $c->select("SELECT length({ids:Array(UInt64)}) AS l", $p2, ClickHouse::FETCH_ONE);
echo "by-ref Array typed param length: ", $len, "\n";

/* 3. by-ref array bound to a client-side {tbl} identifier list must expand
 *    to the identifiers, not the literal "Array". */
$p3 = ["cols" => ["number"]];
foreach ($p3 as $k => &$v) { $v = $v; }
unset($v);
$rows = $c->select("SELECT {cols} FROM system.numbers LIMIT 1", $p3);
echo "by-ref identifier list key: ", implode(",", array_keys($rows[0])), "\n";

/* 4. Control: a plain (non-ref) NULL must behave identically. */
$isNull2 = $c->select("SELECT {p:Nullable(String)} IS NULL AS n", ["p" => null], ClickHouse::FETCH_ONE);
echo "plain NULL typed param IS NULL: ", ($isNull2 ? "yes" : "no"), "\n";
?>
--EXPECT--
by-ref NULL typed param IS NULL: yes
by-ref Array typed param length: 3
by-ref identifier list key: number
plain NULL typed param IS NULL: yes
