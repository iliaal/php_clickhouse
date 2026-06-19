--TEST--
ClickHouse JSON type: streaming decode, Nullable(JSON), and insert validation
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

$c = new ClickHouse(clickhouse_test_config());
$c->setSettings([
    "allow_experimental_json_type"              => 1,
    "output_format_native_write_json_as_string" => 1,
]);
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.json_s");
$c->execute("CREATE TABLE test.json_s (id UInt32, j JSON) ENGINE = Memory");

$c->insert("test.json_s", ["id", "j"], [
    [1, ["a" => 1]],
    [2, ["a" => 2]],
]);

echo "-- selectStream JSON_AS_ARRAY --\n";
$it = $c->selectStream("SELECT id, j FROM test.json_s ORDER BY id", [], "", [], ClickHouse::JSON_AS_ARRAY);
foreach ($it as $row) {
    echo $row["id"], " isarr=", is_array($row["j"]) ? 1 : 0, " a=", $row["j"]["a"], "\n";
}

echo "-- selectStreamCallback JSON_AS_OBJECT --\n";
$c->selectStreamCallback(
    "SELECT id, j FROM test.json_s ORDER BY id",
    function ($r) {
        echo $r["id"], " isobj=", is_object($r["j"]) ? 1 : 0, " a=", $r["j"]->a, "\n";
    },
    [], "", [], ClickHouse::JSON_AS_OBJECT
);

echo "-- selectStream default stays raw string --\n";
$it = $c->selectStream("SELECT j FROM test.json_s WHERE id = 1");
foreach ($it as $row) {
    echo "isstr=", is_string($row["j"]) ? 1 : 0, "\n";
}

echo "-- Nullable(JSON) --\n";
$c->execute("DROP TABLE IF EXISTS test.json_n");
$c->execute("CREATE TABLE test.json_n (id UInt32, j Nullable(JSON)) ENGINE = Memory");
$c->insert("test.json_n", ["id", "j"], [
    [1, ["x" => 1]],
    [2, null],
]);
$rows = $c->select("SELECT id, j FROM test.json_n ORDER BY id", [], ClickHouse::JSON_AS_ARRAY);
echo "id1 x=", $rows[0]["j"]["x"], "\n";
echo "id2 isnull=", $rows[1]["j"] === null ? 1 : 0, "\n";

echo "-- insert validation --\n";
function expect_throw(string $label, callable $fn): void {
    try { $fn(); echo $label, ": no throw\n"; }
    catch (ClickHouseException $e) { echo $label, ": throw\n"; }
}
expect_throw("invalid json string", function () use ($c) {
    $c->insert("test.json_s", ["id", "j"], [[9, "not json"]]);
});
expect_throw("unsupported scalar", function () use ($c) {
    $c->insert("test.json_s", ["id", "j"], [[9, 12345]]);
});

$c->execute("DROP TABLE test.json_s");
$c->execute("DROP TABLE test.json_n");
?>
--EXPECT--
-- selectStream JSON_AS_ARRAY --
1 isarr=1 a=1
2 isarr=1 a=2
-- selectStreamCallback JSON_AS_OBJECT --
1 isobj=1 a=1
2 isobj=1 a=2
-- selectStream default stays raw string --
isstr=1
-- Nullable(JSON) --
id1 x=1
id2 isnull=1
-- insert validation --
invalid json string: throw
unsupported scalar: throw
