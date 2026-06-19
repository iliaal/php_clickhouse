--TEST--
ClickHouse review round 2: re-construct guard, nested-array placeholder, stream shape-flag masking
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");

// fnd_e76c3771: a second __construct() must reject before mutating any
// property, leaving the live object's config untouched.
$rp = (new ReflectionObject($c))->getProperty("host");
if (PHP_VERSION_ID < 80100) { $rp->setAccessible(true); }
$before = $rp->getValue($c);
try {
    $c->__construct(["host" => "10.0.0.99", "port" => 1234]);
    echo "re-construct: no throw\n";
} catch (ClickHouseException $e) {
    echo "re-construct: throw\n";
}
echo "host unchanged: ", ($rp->getValue($c) === $before ? "yes" : "no"), "\n";

// fnd_020ad08b: a nested array element in a {placeholder} list must be
// rejected, not stringified to the bogus identifier "Array".
try {
    $c->select("SELECT {cols} FROM system.one", ["cols" => [["nested"], "x"]]);
    echo "nested array placeholder: no throw\n";
} catch (ClickHouseException $e) {
    echo "nested array placeholder: throw\n";
}
// A Stringable object element still works (its __toString is honored).
$obj = new class {
    public function __toString(): string { return "col_a"; }
};
$c->execute("DROP TABLE IF EXISTS test.ph");
$c->execute("CREATE TABLE test.ph (col_a Int32) ENGINE = Memory");
$c->insert("test.ph", ["col_a"], [[7]]);
$r = $c->select("SELECT {cols} FROM test.ph", ["cols" => [$obj]]);
echo "stringable element: ", $r[0]["col_a"], "\n";

// fnd_eb7a982d: result-shape fetch flags (FETCH_ONE/KEY_PAIR/COLUMN) are
// meaningless per-cell in the row iterator / callback and must be ignored,
// not applied (FETCH_ONE used to make current() emit a scalar and fatal on
// the ?array return type).
$c->execute("DROP TABLE IF EXISTS test.s");
$c->execute("CREATE TABLE test.s (a Int32, b Int32) ENGINE = Memory");
$c->insert("test.s", ["a", "b"], [[1, 2], [3, 4]]);
$rows = [];
foreach ($c->selectStream("SELECT a, b FROM test.s ORDER BY a", [], "", [], ClickHouse::FETCH_ONE) as $row) {
    $rows[] = $row;
}
echo "selectStream FETCH_ONE ignored: ", json_encode($rows), "\n";
$cb = [];
$c->selectStreamCallback("SELECT a, b FROM test.s ORDER BY a", function ($row) use (&$cb) {
    $cb[] = $row;
}, [], "", [], ClickHouse::FETCH_COLUMN);
echo "selectStreamCallback FETCH_COLUMN ignored: ", json_encode($cb), "\n";

// DATE_AS_STRINGS (a value-shaping flag) still applies on the stream.
$c->execute("DROP TABLE IF EXISTS test.d");
$c->execute("CREATE TABLE test.d (t DateTime('UTC')) ENGINE = Memory");
$c->insert("test.d", ["t"], [[1700000000]]);
foreach ($c->selectStream("SELECT t FROM test.d", [], "", [], ClickHouse::DATE_AS_STRINGS) as $row) {
    echo "stream DATE_AS_STRINGS: ", (is_string($row["t"]) ? "string" : gettype($row["t"])), "\n";
}

foreach (["ph", "s", "d"] as $t) $c->execute("DROP TABLE test.$t");
?>
--EXPECT--
re-construct: throw
host unchanged: yes
nested array placeholder: throw
stringable element: 7
selectStream FETCH_ONE ignored: [{"a":1,"b":2},{"a":3,"b":4}]
selectStreamCallback FETCH_COLUMN ignored: [{"a":1,"b":2},{"a":3,"b":4}]
stream DATE_AS_STRINGS: string
