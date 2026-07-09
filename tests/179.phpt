--TEST--
DR-008b: ClickHouseRowIterator methods reject surplus arguments
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// The iterator methods have zero-arg arginfo but did not call
// zend_parse_parameters_none(), so a surplus argument was silently ignored
// (and fatally "Arginfo / zpp mismatch" on a debug build). They now reject
// extra arguments with an ArgumentCountError.

$c = new ClickHouse(clickhouse_test_config());
$it = $c->selectStream("SELECT number AS n FROM numbers(3)");

// A surplus arg is a thrown ArgumentCountError on PHP 8.x and a Warning on
// 7.4; both mean "rejected". Normalize so the assertion is version-agnostic.
$methods = array('rewind', 'valid', 'current', 'key', 'next', 'count');
foreach ($methods as $m) {
    $rejected = false;
    set_error_handler(function () use (&$rejected) { $rejected = true; return true; });
    try {
        $it->$m("extra");
    } catch (ArgumentCountError $e) {
        $rejected = true;
    }
    restore_error_handler();
    echo "$m: ", ($rejected ? "rejected surplus arg" : "accepted surplus arg"), "\n";
}

// Normal zero-arg iteration still works.
$sum = 0;
foreach ($it as $row) { $sum += $row['n']; }
echo "sum: $sum\n";
?>
--EXPECT--
rewind: rejected surplus arg
valid: rejected surplus arg
current: rejected surplus arg
key: rejected surplus arg
next: rejected surplus arg
count: rejected surplus arg
sum: 3
