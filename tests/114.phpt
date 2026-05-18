--TEST--
ClickHouseStatement positional rows, numeric-string offsets, cloning, and empty iterator current()
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.stmt_regress");
$c->execute("CREATE TABLE test.stmt_regress (id UInt32, name String) ENGINE=Memory");
$c->insert("test.stmt_regress", ["id", "name"], [
    [1, "alice"],
    [2, "bob"],
]);

$stmt = $c->selectStatement("SELECT id, name FROM test.stmt_regress ORDER BY id");
echo "isset string zero=", isset($stmt["0"]) ? "yes" : "no", "\n";
echo "offset string zero=", json_encode($stmt["0"]), "\n";

try {
    $copy = clone $stmt;
    echo "clone: NO THROW\n";
    echo "clone count=", count($copy), "\n";
} catch (Throwable $e) {
    echo "clone rejected: ", get_class($e), "\n";
}

try {
    serialize($stmt);
    echo "serialize: NO THROW\n";
} catch (Throwable $e) {
    echo "serialize rejected: yes\n";
}

set_error_handler(function($severity, $message) {
    throw new ErrorException($message, 0, $severity);
});
try {
    unserialize('O:19:"ClickHouseStatement":0:{}');
    echo "unserialize: NO THROW\n";
} catch (Throwable $e) {
    echo "unserialize rejected: yes\n";
} finally {
    restore_error_handler();
}

$dup = $c->selectStatement(
    "SELECT name, name FROM test.stmt_regress ORDER BY id"
);
echo "duplicate visible rows=", json_encode($dup->toArray()), "\n";
echo "duplicate key pair=", json_encode($dup->fetchKeyPair()), "\n";
echo "duplicate column=", json_encode($dup->fetchColumn()), "\n";

$it = $c->selectStream("SELECT id FROM test.stmt_regress WHERE id = 0");
var_dump($it->current());

$c->execute("DROP TABLE test.stmt_regress");
?>
--EXPECT--
isset string zero=yes
offset string zero={"id":1,"name":"alice"}
clone rejected: Error
serialize rejected: yes
unserialize rejected: yes
duplicate visible rows=[{"name":"alice"},{"name":"bob"}]
duplicate key pair={"alice":"alice","bob":"bob"}
duplicate column=["alice","bob"]
NULL
