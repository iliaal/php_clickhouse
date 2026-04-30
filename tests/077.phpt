--TEST--
ClickHouse {name} placeholder rejects internal whitespace and SQL keyword smuggling
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// Regression for FR-001: closing CR-002 by dropping `-` from the
// whitelist still left whitespace + SQL keywords accepted, so a value
// like "test.a ANY INNER JOIN test.secret USING tenant" landed
// verbatim inside `FROM {tbl}` and changed query semantics. The new
// validator parses each token structurally (numeric or identifier,
// optionally db-qualified) and rejects internal whitespace within a
// token. Whitespace remains valid only around commas in a list.

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
foreach (["fr1_a","fr1_secret"] as $t) $c->execute("DROP TABLE IF EXISTS test.$t");
$c->execute("CREATE TABLE test.fr1_a (tenant Int32) ENGINE=Memory");
$c->execute("CREATE TABLE test.fr1_secret (tenant Int32) ENGINE=Memory");

$probes_reject = [
    "join smuggle"      => "test.fr1_a ANY INNER JOIN test.fr1_secret USING tenant",
    "comma list"        => "test.fr1_a, test.fr1_secret",
    "comma plain"       => "a, b, c",
    "internal space"    => "tbl name",
    "internal tab"      => "tbl\tname",
    "leading space"     => " tbl",
    "trailing space"    => "tbl ",
    "trailing comma"    => "a,",
    "leading comma"     => ",a",
    "double dot"        => "a..b",
    "trailing dot"      => "a.",
    "sign without digit" => "+abc",
    "exponent without digit" => "1e",
];
foreach ($probes_reject as $label => $val) {
    try { $c->select("SELECT count() FROM {tbl}", ["tbl" => $val], ClickHouse::FETCH_ONE); echo "$label: NO THROW\n"; }
    catch (ClickHouseException $e) { echo "$label: REJECTED\n"; }
}

// Legitimate identifiers, numerics, and lists still pass validation
// (and reach the server, where they may or may not be valid SQL).
$probes_ok = [
    "identifier"        => "tbl_name",
    "db.tbl"            => "test.fr1_a",
    "numeric int"       => "42",
    "numeric float"     => "1.5",
    "numeric exp"       => "1.5e3",
    "numeric neg"       => "-42",
];
foreach ($probes_ok as $label => $val) {
    try { $c->select("SELECT 1 FROM {tbl}", ["tbl" => $val], ClickHouse::FETCH_ONE); echo "$label: ALLOWED\n"; }
    catch (ClickHouseException $e) {
        // Server-side parse error is the success signal; placeholder
        // rejection is the failure we're guarding against.
        if (strpos($e->getMessage(), "Placeholder value for") === 0) echo "$label: REJECTED-by-validator\n";
        else echo "$label: ALLOWED\n";
    }
}

foreach (["fr1_a","fr1_secret"] as $t) $c->execute("DROP TABLE test.$t");
?>
--EXPECT--
join smuggle: REJECTED
comma list: REJECTED
comma plain: REJECTED
internal space: REJECTED
internal tab: REJECTED
leading space: REJECTED
trailing space: REJECTED
trailing comma: REJECTED
leading comma: REJECTED
double dot: REJECTED
trailing dot: REJECTED
sign without digit: REJECTED
exponent without digit: REJECTED
identifier: ALLOWED
db.tbl: ALLOWED
numeric int: ALLOWED
numeric float: ALLOWED
numeric exp: ALLOWED
numeric neg: ALLOWED
