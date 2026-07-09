--TEST--
DR-C6: Array(String) typed parameters round-trip quotes and backslashes correctly
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// The array-literal is sent as a bound parameter, whose Array-from-parameter
// string reader treats a backslash literally and closes an element on the
// first single quote. The old backslash-escape scheme rejected "it's" and
// doubled "c\d"; the escaper now doubles quotes (SQL style) and leaves
// backslashes literal.

$c = new ClickHouse(clickhouse_test_config());

$cases = array("it's", "c\\d", "plain", 'a"b', "both'\\x", "''");
foreach ($cases as $s) {
    $r = $c->select("SELECT arrayJoin({p:Array(String)}) AS v", array("p" => array($s)));
    $got = $r[0]['v'];
    echo bin2hex($s), " -> ", bin2hex($got), " ", ($got === $s ? "MATCH" : "DIFF"), "\n";
}

// multi-element with an embedded quote
$r = $c->select("SELECT arrayStringConcat({p:Array(String)}, '|') AS v",
                array("p" => array("a'b", "c")));
echo "multi: ", $r[0]['v'], "\n";
?>
--EXPECT--
69742773 -> 69742773 MATCH
635c64 -> 635c64 MATCH
706c61696e -> 706c61696e MATCH
612262 -> 612262 MATCH
626f7468275c78 -> 626f7468275c78 MATCH
2727 -> 2727 MATCH
multi: a'b|c
