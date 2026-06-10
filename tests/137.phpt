--TEST--
insert() rejects a by-ref row reassigned to a non-array mid-conversion instead of crashing
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.byref_row");
$c->execute("CREATE TABLE test.byref_row (s String, n Int32) ENGINE=Memory");

/* The insert builds the block one column at a time. Column 0 (String)
 * converts the evil cell, whose __toString writes 42 through a reference
 * aliased to row 0 -- so by the time column 1 is built, row 0 is an int,
 * not an array. The per-row deref must recheck IS_ARRAY before indexing;
 * without it Z_ARRVAL_P walked a long and crashed. */
class Evil {
    public $ref;
    public function __toString(): string {
        $this->ref = 42;   // row 0 stops being an array
        return "x";
    }
}
$e = new Evil();
$rows = [[$e, 1], ["y", 2]];
$e->ref = &$rows[0];       // row 0 is now a by-ref bucket Evil writes through

try {
    $c->insert("test.byref_row", ["s", "n"], $rows);
    echo "no throw\n";
} catch (ClickHouseException $ex) {
    echo "rejected\n";
}

/* The handle must remain usable -- a failed insert should not leave the
 * native client wedged in inserting state. */
echo "ping=", ($c->ping() ? "ok" : "fail"), "\n";

$c->execute("DROP TABLE test.byref_row");
?>
--EXPECT--
rejected
ping=ok
