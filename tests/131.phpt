--TEST--
insertFromStream: interior blank line errors; ClickHouse TSV escapes decode correctly
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$ch = new ClickHouse(clickhouse_test_config());
$ch->execute("CREATE DATABASE IF NOT EXISTS test");

/* An interior blank line in a 2-column TSV must error rather than be
 * silently merged with the following row's leading separator. */
$ch->execute("DROP TABLE IF EXISTS test.sfs131a");
$ch->execute("CREATE TABLE test.sfs131a (a String, b String) ENGINE=Memory");
$fp = fopen("php://temp", "r+");
fwrite($fp, "a\tb\n\n\t\n");
rewind($fp);
try {
    $ch->insertFromStream("test.sfs131a", ["a", "b"], $fp, "TabSeparated");
    echo "blank line: inserted ", $ch->select("SELECT count() c FROM test.sfs131a")[0]["c"], " rows\n";
} catch (ClickHouseException $e) {
    echo "blank line: ", (strpos($e->getMessage(), "row has 1 cells") !== false ? "rejected" : "other"), "\n";
}
fclose($fp);
$ch->execute("DROP TABLE test.sfs131a");

/* ClickHouse TabSeparated escapes an apostrophe as \' on output. The
 * reader must fold it back to ' (the backslash was being kept). */
$ch->execute("DROP TABLE IF EXISTS test.sfs131b");
$ch->execute("CREATE TABLE test.sfs131b (s String) ENGINE=Memory");
$fp = fopen("php://temp", "r+");
fwrite($fp, "O\\'Brien\n");   // bytes: O \ ' B r i e n \n
rewind($fp);
$ch->insertFromStream("test.sfs131b", ["s"], $fp, "TabSeparated");
var_dump($ch->select("SELECT s FROM test.sfs131b")[0]["s"]);
fclose($fp);
$ch->execute("DROP TABLE test.sfs131b");

/* A blank line cannot stand in for the header of a *WithNames stream. */
$ch->execute("DROP TABLE IF EXISTS test.sfs131c");
$ch->execute("CREATE TABLE test.sfs131c (a String, b String) ENGINE=Memory");
$fp = fopen("php://temp", "r+");
fwrite($fp, "\na\tb\nv1\tv2\n");
rewind($fp);
try {
    $ch->insertFromStream("test.sfs131c", ["a", "b"], $fp, "TabSeparatedWithNames");
    echo "leading blank header: no throw\n";
} catch (ClickHouseException $e) {
    echo "leading blank header: ", (strpos($e->getMessage(), "blank line before header") !== false ? "rejected" : "other"), "\n";
}
fclose($fp);
$ch->execute("DROP TABLE test.sfs131c");
?>
--EXPECT--
blank line: rejected
string(7) "O'Brien"
leading blank header: rejected
