--TEST--
DR-010b: UUID inserts accept only the two canonical forms and reject misplaced dashes
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// The UUID parser skipped a '-' at any position, silently canonicalizing
// malformed text instead of rejecting it. It now accepts only 32 hex digits
// (dashless) or the 8-4-4-4-12 dashed form.

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.dr010b");
$c->execute("CREATE TABLE test.dr010b (v UUID) ENGINE = Memory");

function try_insert($c, $label, $u) {
    try {
        $c->insert("test.dr010b", array('v'), array(array($u)));
        echo "$label: accepted\n";
    } catch (ClickHouseException $e) {
        echo "$label: REJECTED\n";
    }
}

// canonical forms accepted
try_insert($c, "dashed",   "61f0c404-5cb3-11e7-907b-a6006ad3dba0");
try_insert($c, "dashless", "61f0c4045cb311e7907ba6006ad3dba0");
// malformed rejected
try_insert($c, "misplaced-dash", "61f0-c404-5cb3-11e7-907b-a6006ad3dba0");
try_insert($c, "too-short",      "61f0c404");
try_insert($c, "non-hex",        "zzzzzzzz-5cb3-11e7-907b-a6006ad3dba0");

$c->execute("DROP TABLE IF EXISTS test.dr010b");
?>
--EXPECT--
dashed: accepted
dashless: accepted
misplaced-dash: REJECTED
too-short: REJECTED
non-hex: REJECTED
