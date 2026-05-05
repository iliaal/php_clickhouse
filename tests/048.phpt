--TEST--
ClickHouse Phase E: ClickHouseRowIterator (selectStream) basic foreach + count + key
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());

$iter = $c->selectStream("SELECT number AS n FROM system.numbers LIMIT 5");
echo "is_iter=", ($iter instanceof ClickHouseRowIterator) ? 1 : 0, "\n";
echo "is_iterator_iface=", ($iter instanceof Iterator) ? 1 : 0, "\n";
echo "is_countable_iface=", ($iter instanceof Countable) ? 1 : 0, "\n";
echo "count=", count($iter), "\n";

$keys = [];
$vals = [];
foreach ($iter as $k => $row) {
    $keys[] = $k;
    $vals[] = $row["n"];
}
echo "keys=", implode(",", $keys), "\n";
echo "vals=", implode(",", $vals), "\n";

// Iterator survives unset of the source client (blocks are owned by iter via shared_ptr).
$iter2 = $c->selectStream("SELECT number AS n FROM system.numbers LIMIT 3");
unset($c);
$out = [];
foreach ($iter2 as $row) {
    $out[] = $row["n"];
}
echo "after_unset=", implode(",", $out), "\n";

// selectStreamCallback fires per-row.
$c2 = new ClickHouse(clickhouse_test_config());
$got = [];
$c2->selectStreamCallback("SELECT number AS n FROM system.numbers LIMIT 4", function(array $row) use (&$got) {
    $got[] = $row["n"];
});
echo "cb_rows=", implode(",", $got), "\n";

?>
--EXPECT--
is_iter=1
is_iterator_iface=1
is_countable_iface=1
count=5
keys=0,1,2,3,4
vals=0,1,2,3,4
after_unset=0,1,2
cb_rows=0,1,2,3
