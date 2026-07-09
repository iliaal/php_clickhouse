--TEST--
DR-008: a reentrant insert on a second client does not inherit the allow-null guard
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// g_allow_null_in_strict is thread-local and shared across ClickHouse
// clients. It is bumped only during a Nullable child build, but that build
// runs userland (__toString / jsonSerialize). If that userland reenters a
// SECOND client's insert into a NON-Nullable column with a bare null, the
// second insert used to observe the first's relaxed state and silently
// store 0/"". Each insert entrypoint now resets the guard, so the second
// client rejects the null.

$cfg = clickhouse_test_config();
$a = new ClickHouse($cfg);
$b = new ClickHouse($cfg);
$a->execute("CREATE DATABASE IF NOT EXISTS test");
$a->execute("DROP TABLE IF EXISTS test.dr008_nn");
$a->execute("CREATE TABLE test.dr008_nn (i Int32) ENGINE = Memory");
$a->execute("DROP TABLE IF EXISTS test.dr008_null");
$a->execute("CREATE TABLE test.dr008_null (s Nullable(String)) ENGINE = Memory");

class Reenter {
    public $b;
    public function __construct($b) { $this->b = $b; }
    public function __toString() {
        try { $this->b->insert("test.dr008_nn", ['i'], [[null]]); echo "reentrant null: NO THROW\n"; }
        catch (ClickHouseException $e) { echo "reentrant null: REJECTED\n"; }
        return "a-value";
    }
}

// Client A builds a Nullable(String) column; during the child build its
// __toString reenters client B's non-Nullable insert.
$a->insert("test.dr008_null", ['s'], [[new Reenter($b)]]);

$r = $b->select("SELECT count() c FROM test.dr008_nn");
echo "second-client rows: ", $r[0]['c'], "\n";

$a->execute("DROP TABLE test.dr008_nn");
$a->execute("DROP TABLE test.dr008_null");
?>
--EXPECT--
reentrant null: REJECTED
second-client rows: 0
