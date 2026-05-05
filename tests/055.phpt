--TEST--
ClickHouseStatement: smi2-style result wrapper. Iterator + Countable + ArrayAccess + JsonSerializable + fetch helpers
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.stmt_t");
$c->execute("CREATE TABLE test.stmt_t (id UInt32, name String) ENGINE = Memory");
$c->insert("test.stmt_t", ["id", "name"], [
    [1, "alice"],
    [2, "bob"],
    [3, "carol"],
]);

$stmt = $c->selectStatement("SELECT id, name FROM test.stmt_t ORDER BY id");

// Type check
echo "type=", get_class($stmt), "\n";
echo "is_iterator=",     ($stmt instanceof Iterator)         ? "yes" : "no", "\n";
echo "is_countable=",    ($stmt instanceof Countable)        ? "yes" : "no", "\n";
echo "is_arrayaccess=",  ($stmt instanceof ArrayAccess)      ? "yes" : "no", "\n";
echo "is_jsonser=",      ($stmt instanceof JsonSerializable) ? "yes" : "no", "\n";

// Countable
echo "count=", count($stmt), "\n";

// Iterator (foreach)
foreach ($stmt as $k => $row) {
    echo "row[", $k, "]=", $row["id"], ":", $row["name"], "\n";
}

// ArrayAccess
echo "stmt[0]=", json_encode($stmt[0]), "\n";
echo "stmt[2]=", json_encode($stmt[2]), "\n";
echo "isset[1]=", isset($stmt[1]) ? "yes" : "no", "\n";
echo "isset[99]=", isset($stmt[99]) ? "yes" : "no", "\n";

// JsonSerializable
echo "json=", json_encode($stmt), "\n";

// Read-only enforcement
try {
    $stmt[0] = ["x" => 1];
    echo "offsetSet: NO EXCEPTION (BUG)\n";
} catch (ClickHouseException $e) {
    echo "offsetSet rejected: ", $e->getMessage(), "\n";
}
try {
    unset($stmt[0]);
    echo "offsetUnset: NO EXCEPTION (BUG)\n";
} catch (ClickHouseException $e) {
    echo "offsetUnset rejected: ", $e->getMessage(), "\n";
}

// toArray() returns plain array suitable for native array_* funcs
$arr = $stmt->toArray();
echo "toArray_is_array=", is_array($arr) ? "yes" : "no", "\n";
echo "names=", json_encode(array_column($arr, "name")), "\n";

// statistics() returns the per-call snapshot. Should reflect the SELECT
// not any later query on the same client.
$stats = $stmt->statistics();
echo "stats_has_elapsed=", isset($stats["elapsed_ms"]) ? "yes" : "no", "\n";
echo "stats_query_id=", $stats["query_id"], "\n";

// Run another query on the client; statement's stats must be unchanged.
$c->select("SELECT 1");
$stats2 = $stmt->statistics();
echo "stats_immutable=", ($stats === $stats2) ? "yes" : "no", "\n";

// fetchOne: single row, single col yields scalar; multi-col yields full row.
$one = $c->selectStatement("SELECT name FROM test.stmt_t WHERE id = 2")->fetchOne();
echo "fetchOne_scalar=", $one, "\n";
$row = $c->selectStatement("SELECT id, name FROM test.stmt_t WHERE id = 2")->fetchOne();
echo "fetchOne_row=", json_encode($row), "\n";
$none = $c->selectStatement("SELECT id FROM test.stmt_t WHERE id = 999")->fetchOne();
var_dump($none);

// fetchKeyPair: 2-col rows -> assoc map
$map = $c->selectStatement("SELECT id, name FROM test.stmt_t ORDER BY id")->fetchKeyPair();
echo "fetchKeyPair=", json_encode($map), "\n";

// fetchKeyPair with <2 cols throws
try {
    $c->selectStatement("SELECT id FROM test.stmt_t LIMIT 1")->fetchKeyPair();
    echo "fetchKeyPair: NO EXCEPTION (BUG)\n";
} catch (ClickHouseException $e) {
    echo "fetchKeyPair rejected: ", $e->getMessage(), "\n";
}

// fetchColumn: flat list of first-column values
$col = $c->selectStatement("SELECT id FROM test.stmt_t ORDER BY id")->fetchColumn();
echo "fetchColumn=", json_encode($col), "\n";

// Constructor is private; new ClickHouseStatement() must throw.
try {
    new ClickHouseStatement();
    echo "ctor: NO EXCEPTION (BUG)\n";
} catch (Throwable $e) {
    echo "ctor rejected: ", get_class($e), "\n";
}

// Statement survives unset($c) since it owns its rows + stats.
$held = $c->selectStatement("SELECT id FROM test.stmt_t ORDER BY id");
unset($c);
echo "after_unset_count=", count($held), "\n";
foreach ($held as $row) {
    echo "still_iter=", $row["id"], "\n";
}

$c2 = new ClickHouse(clickhouse_test_config());
$c2->execute("DROP TABLE test.stmt_t");
?>
--EXPECT--
type=ClickHouseStatement
is_iterator=yes
is_countable=yes
is_arrayaccess=yes
is_jsonser=yes
count=3
row[0]=1:alice
row[1]=2:bob
row[2]=3:carol
stmt[0]={"id":1,"name":"alice"}
stmt[2]={"id":3,"name":"carol"}
isset[1]=yes
isset[99]=no
json=[{"id":1,"name":"alice"},{"id":2,"name":"bob"},{"id":3,"name":"carol"}]
offsetSet rejected: ClickHouseStatement is read-only; offsetSet is not supported
offsetUnset rejected: ClickHouseStatement is read-only; offsetUnset is not supported
toArray_is_array=yes
names=["alice","bob","carol"]
stats_has_elapsed=yes
stats_query_id=
stats_immutable=yes
fetchOne_scalar=bob
fetchOne_row={"id":2,"name":"bob"}
NULL
fetchKeyPair={"1":"alice","2":"bob","3":"carol"}
fetchKeyPair rejected: fetchKeyPair requires each row to have at least 2 columns
fetchColumn=[1,2,3]
ctor rejected: Error
after_unset_count=3
still_iter=1
still_iter=2
still_iter=3
