--TEST--
ClickHouse DDL helpers reject malformed identifiers; isExists neutralizes them as string literals
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());

// Each row: [method label, callable]. The callable is expected to throw
// ClickHouseException with a message starting "<what> ...". We print the
// label + the exception message so a regression on the validator surface
// is a one-line diff.
$probes = [
    "tableSize(empty)"                  => fn() => $c->tableSize(""),
    "tableSize(trailing dot)"           => fn() => $c->tableSize("a."),
    "tableSize(leading dot)"            => fn() => $c->tableSize(".a"),
    "truncateTable(injection)"          => fn() => $c->truncateTable("t; DROP DATABASE test"),
    "truncateTable(empty)"              => fn() => $c->truncateTable(""),
    "truncateTable(quote)"              => fn() => $c->truncateTable("a'b"),
    "dropPartition(injection in table)" => fn() => $c->dropPartition("t; SELECT 1", "1"),
    "dropPartition(empty table)"        => fn() => $c->dropPartition("", "1"),
    "showCreateTable(injection)"        => fn() => $c->showCreateTable("t UNION SELECT 1"),
    "showCreateTable(hyphen)"           => fn() => $c->showCreateTable("a-b"),
];

foreach ($probes as $label => $fn) {
    try {
        $fn();
        echo $label, ": NO EXCEPTION (BUG)\n";
    } catch (ClickHouseException $e) {
        echo $label, ": ", $e->getMessage(), "\n";
    } catch (Throwable $e) {
        echo $label, ": WRONG-EXCEPTION-CLASS ", get_class($e), ": ", $e->getMessage(), "\n";
    }
}

$literal = $c->tableSize("test; SELECT 1");
echo "tableSize literal special count=", count($literal), "\n";

// isExists compares its arguments as string LITERALS against system.tables
// (not as interpolated identifiers), so injection/special-char input is
// neutralized by escaping rather than rejected: each returns a bool with no
// SQL executed. This is the same string-literal treatment showTables() uses.
$isexists_probes = [
    "isExists(SQL injection in db)"    => fn() => $c->isExists("test; DROP TABLE x", "t"),
    "isExists(SQL injection in table)" => fn() => $c->isExists("test", "t; DROP TABLE x"),
    "isExists(empty db)"               => fn() => $c->isExists("", "t"),
    "isExists(backtick in db)"         => fn() => $c->isExists("d`b", "t"),
    "isExists(special char in table)"  => fn() => $c->isExists("test", "my-table"),
    "isExists(space in db)"            => fn() => $c->isExists("ba d", "t"),
];
foreach ($isexists_probes as $label => $fn) {
    try {
        $r = $fn();
        echo $label, ": ", (is_bool($r) ? ($r ? "true" : "false") : "NON-BOOL"), "\n";
    } catch (Throwable $e) {
        echo $label, ": UNEXPECTED ", get_class($e), "\n";
    }
}

// Sanity: a valid identifier must NOT throw the validator.
try {
    $c->execute("CREATE DATABASE IF NOT EXISTS test");
    $c->execute("DROP TABLE IF EXISTS test.id_ok");
    $c->execute("CREATE TABLE test.id_ok (a UInt8) ENGINE = Memory");
    $exists = $c->isExists("test", "id_ok");
    echo "valid path isExists=", ($exists ? "true" : "false"), "\n";
    $c->execute("DROP TABLE test.id_ok");
} catch (Throwable $e) {
    echo "valid path UNEXPECTED: ", $e->getMessage(), "\n";
}
?>
--EXPECT--
tableSize(empty): table name must not be empty
tableSize(trailing dot): table name must not be empty
tableSize(leading dot): database name must not be empty
truncateTable(injection): table name contains an invalid character
truncateTable(empty): table name must not be empty
truncateTable(quote): table name contains an invalid character
dropPartition(injection in table): table name contains an invalid character
dropPartition(empty table): table name must not be empty
showCreateTable(injection): table name contains an invalid character
showCreateTable(hyphen): table name contains an invalid character
tableSize literal special count=0
isExists(SQL injection in db): false
isExists(SQL injection in table): false
isExists(empty db): false
isExists(backtick in db): false
isExists(special char in table): false
isExists(space in db): false
valid path isExists=true
