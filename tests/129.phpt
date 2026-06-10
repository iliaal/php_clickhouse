--TEST--
fetchKeyPair / FETCH_KEY_PAIR reject a composite (non-scalar) key column
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$ch = new ClickHouse(clickhouse_test_config());
$sql = "SELECT [number, number+1] AS k, toString(number) AS v FROM system.numbers LIMIT 3";

/* An Array key column used to coerce to the literal string "Array" with
 * an E_WARNING, collapsing all rows onto a single key. Both the
 * statement path and the select FETCH_KEY_PAIR path must reject it. */
try {
    $ch->selectStatement($sql)->fetchKeyPair();
    echo "statement: no throw\n";
} catch (ClickHouseException $e) {
    echo "statement: ", (strpos($e->getMessage(), "scalar key") !== false ? "rejected" : "other"), "\n";
}

try {
    $ch->select($sql, [], ClickHouse::FETCH_KEY_PAIR);
    echo "select: no throw\n";
} catch (ClickHouseException $e) {
    echo "select: ", (strpos($e->getMessage(), "scalar key") !== false ? "rejected" : "other"), "\n";
}

/* A scalar key column still works. */
$ok = $ch->selectStatement("SELECT number AS k, toString(number*10) AS v FROM system.numbers LIMIT 2");
var_dump($ok->fetchKeyPair());
?>
--EXPECT--
statement: rejected
select: rejected
array(2) {
  [0]=>
  string(1) "0"
  [1]=>
  string(2) "10"
}
