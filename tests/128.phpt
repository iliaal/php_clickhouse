--TEST--
fetchOne returns the full row for a multi-column result with duplicate column names
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$ch = new ClickHouse(clickhouse_test_config());

/* Duplicate column names collapse in the assoc row, so counting assoc
 * elements misclassified a genuine 2-column result as single-column and
 * wrongly unwrapped it to a scalar. fetchOne must consult the positional
 * row count and return the full row. */
$dup = $ch->selectStatement("SELECT number, number FROM system.numbers LIMIT 1 OFFSET 41");
var_dump(is_array($dup->fetchOne()));

/* A genuine single-column result still unwraps to the scalar. */
$one = $ch->selectStatement("SELECT 7 AS x");
var_dump($one->fetchOne());

/* A distinct multi-column result returns the row array. */
$multi = $ch->selectStatement("SELECT 1 AS a, 2 AS b");
var_dump($multi->fetchOne());
?>
--EXPECT--
bool(true)
int(7)
array(2) {
  ["a"]=>
  int(1)
  ["b"]=>
  int(2)
}
