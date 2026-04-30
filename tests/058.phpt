--TEST--
ClickHouse client-side {name} placeholder rejects unsafe characters (parens, star, plus)
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// Regression for CR-016: the client-side {name} placeholder validator
// previously accepted `*`, `(`, `)`, `+` which let function-call and
// subquery-shaped fragments smuggle into identifier positions. The
// whitelist is now letters/digits/_/./,/whitespace/-.

$c = new ClickHouse(clickhouse_test_config());

$probes = [
    "star"           => "*",
    "open paren"     => "id, (",
    "close paren"    => "id, )",
    "plus"           => "id+1",
    "function call"  => "count(*)",
    "subquery"       => "(SELECT 1)",
    "semicolon"      => "id; DROP TABLE x",
    "single quote"   => "id', 'x",
    "backslash"      => "id\\x",
    "tab is ok"      => "id,\tname",
    "comma is ok"    => "id, name",
    "dot is ok"      => "db.tbl",
    "minus is ok"    => "neg-name",
];

foreach ($probes as $label => $val) {
    try {
        $c->select("SELECT {x} AS r", ["x" => $val]);
        echo "$label: NO THROW\n";
    } catch (ClickHouseException $e) {
        $msg = $e->getMessage();
        // Distinguish placeholder-validator rejections from server-side
        // SQL parse errors (the "ok" cases hit the server with an invalid
        // SQL fragment and come back with a parse error).
        if (strpos($msg, "unsafe character") !== false) {
            echo "$label: REJECTED\n";
        } else {
            echo "$label: ALLOWED (server-rejected)\n";
        }
    }
}
?>
--EXPECT--
star: REJECTED
open paren: REJECTED
close paren: REJECTED
plus: REJECTED
function call: REJECTED
subquery: REJECTED
semicolon: REJECTED
single quote: REJECTED
backslash: REJECTED
tab is ok: ALLOWED (server-rejected)
comma is ok: ALLOWED (server-rejected)
dot is ok: ALLOWED (server-rejected)
minus is ok: ALLOWED (server-rejected)
