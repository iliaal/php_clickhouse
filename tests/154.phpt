--TEST--
ClickHouse FIXEDSTRING_BINARY flag preserves trailing NUL bytes on read
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.fsbin");
$c->execute("CREATE TABLE test.fsbin (
    fs FixedString(4),
    lc LowCardinality(FixedString(4))
) ENGINE=Memory");

/* fs carries an embedded and a trailing NUL; lc is a short value the server
 * pads with NULs up to the declared width. */
$c->insert("test.fsbin", ["fs", "lc"], [["A\x00B\x00", "Z"]]);

$sql = "SELECT fs, lc FROM test.fsbin";
$default = $c->select($sql)[0];
$binary  = $c->select($sql, [], ClickHouse::FIXEDSTRING_BINARY)[0];

echo "default fs: ", bin2hex($default["fs"]), "\n";
echo "default lc: ", bin2hex($default["lc"]), "\n";
echo "binary  fs: ", bin2hex($binary["fs"]), "\n";
echo "binary  lc: ", bin2hex($binary["lc"]), "\n";

$c->execute("DROP TABLE test.fsbin");
?>
--EXPECT--
default fs: 410042
default lc: 5a
binary  fs: 41004200
binary  lc: 5a000000
