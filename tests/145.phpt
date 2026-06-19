--TEST--
ClickHouse JSON type: insert (array/object/raw string) and read (raw, JSON_AS_ARRAY, JSON_AS_OBJECT)
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
require __DIR__ . "/_clickhouse.inc";
clickhouse_skip_if_no_server();
clickhouse_skip_if_no_json();
?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

/* Canonicalize a JSON value (array, stdClass, or JSON string) into a
 * key-sorted JSON string so the expected output stays independent of the
 * server's JSON path ordering across ClickHouse versions. */
function cksort(&$a) { if (is_array($a)) { ksort($a); foreach ($a as &$x) cksort($x); } }
function cj($v) {
    $v = is_string($v) ? json_decode($v, true) : json_decode(json_encode($v), true);
    cksort($v);
    return json_encode($v);
}

$c = new ClickHouse(clickhouse_test_config());
$c->setSettings([
    "allow_experimental_json_type"              => 1,
    "output_format_native_write_json_as_string" => 1,
]);
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.json_t");
$c->execute("CREATE TABLE test.json_t (id UInt32, j JSON) ENGINE = Memory");

$obj = new stdClass();
$obj->k = "v";
$obj->n = 3;

$c->insert("test.json_t", ["id", "j"], [
    [1, ["a" => 1, "b" => ["c" => 2]]],   // PHP array  -> json_encode
    [2, $obj],                            // PHP object -> json_encode
    [3, '{"raw":true}'],                  // raw string -> stored verbatim
]);

echo "-- default (raw string) --\n";
$rows = $c->select("SELECT id, j FROM test.json_t ORDER BY id");
foreach ($rows as $r) {
    echo $r["id"], " isstr=", is_string($r["j"]) ? 1 : 0, " ", cj($r["j"]), "\n";
}

echo "-- JSON_AS_ARRAY --\n";
$rows = $c->select("SELECT id, j FROM test.json_t ORDER BY id", [], ClickHouse::JSON_AS_ARRAY);
foreach ($rows as $r) {
    echo $r["id"], " isarr=", is_array($r["j"]) ? 1 : 0, " ", cj($r["j"]), "\n";
}
echo "field a=", $rows[0]["j"]["a"], " c=", $rows[0]["j"]["b"]["c"], "\n";

echo "-- JSON_AS_OBJECT (FETCH_ONE) --\n";
$one = $c->select("SELECT j FROM test.json_t WHERE id = 1", [], ClickHouse::JSON_AS_OBJECT | ClickHouse::FETCH_ONE);
echo "isobj=", is_object($one) ? 1 : 0, " a=", $one->a, " c=", $one->b->c, "\n";

$c->execute("DROP TABLE test.json_t");
?>
--EXPECT--
-- default (raw string) --
1 isstr=1 {"a":1,"b":{"c":2}}
2 isstr=1 {"k":"v","n":3}
3 isstr=1 {"raw":true}
-- JSON_AS_ARRAY --
1 isarr=1 {"a":1,"b":{"c":2}}
2 isarr=1 {"k":"v","n":3}
3 isarr=1 {"raw":true}
field a=1 c=2
-- JSON_AS_OBJECT (FETCH_ONE) --
isobj=1 a=1 c=2
