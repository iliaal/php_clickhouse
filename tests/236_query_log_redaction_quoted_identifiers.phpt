--TEST--
Query-log and verbose redaction keep quoted identifiers and comments from shifting literal boundaries
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";
$c = new ClickHouse(clickhouse_test_config());
$c->enableLogQueries(true);
$verbose = null;
$c->setVerbose(function ($event, $ctx) use (&$verbose) {
    if ($event === "select_start") $verbose = $ctx["sql"];
});

$cases = [
    "double-quoted" => "SELECT 1 AS \"it's\", 'SECRET_DQ' AS pw",
    "backtick" => "SELECT 1 AS `it's`, 'SECRET_BT' AS pw",
    "escaped identifier" => "SELECT 1 AS \"a\\\"b'c\", 'SECRET_ESC' AS pw",
    "unicode quotes" => "SELECT 1 AS \u{201C}it's\u{201D}, \u{2018}SECRET_UQ\u{2019} AS pw, 'SECRET_UQ2' AS pw2",
    "dash comment" => "SELECT 1 -- don't\n, 'SECRET_DASH' AS pw",
    "hash comment" => "SELECT 1 # don't\n, 'SECRET_HASH' AS pw",
    "slash comment" => "SELECT 1 // don't\n, 'SECRET_SLASH' AS pw",
    "nested block comment" => "SELECT 1 /* a /* it's */ b */, 'SECRET_BLOCK' AS pw",
    "comment literal" => "SELECT 1 /* pw = 'SECRET_IN_COMMENT' */ AS x",
    "exponent" => "SELECT 1e-5 AS e, 0x1p-2 AS h, 'SECRET_EXP' AS pw",
    "tuple element after dot" => "SELECT CAST(tuple(7), 'Tuple(`1e` UInt8)').1e-- tuple's first field\n AS a, 'SECRET_VALID' AS pw",
    "hex and bin" => "SELECT x'4142' AS h, b'01000001' AS b, 'SECRET_HEX' AS pw",
];
foreach ($cases as $name => $sql) {
    $verbose = null;
    $c->select($sql);
    $logs = $c->getLogQueries();
    $logged = count($logs) === 1 ? $logs[0]["sql"] : "(missing)";
    echo $name, ": ", $logged === $verbose ? "" : "verbose differs: ", $logged, "\n";
    echo "  leaked: ", preg_match('/SECRET/', $logged . $verbose) ? "yes" : "no", "\n";
}

// Rejected statements are logged too; the number and '.' lexing must still match the server.
$errors = [
    "underscore exponent" => "SELECT 1_e-- it's\n, 'SECRET_US' AS pw",
    "double dot" => "SELECT 1.2.3e-- it's\n, 'SECRET_DD' AS pw",
    "spaced dot" => "SELECT 1 .5e-- it's\n, 'SECRET_SD' AS pw",
    "binary exponent" => "SELECT 0b1e--1, \$\$SECRET_BIN\$\$ AS pw",
    "vertical delimiter" => "SELECT 1 \\G.5e-- 'a\nSECRET_VD' AS pw",
];
foreach ($errors as $name => $sql) {
    $verbose = null;
    try {
        $c->select($sql);
        echo $name, ": accepted\n";
    } catch (ClickHouseException $e) {
    }
    $logs = $c->getLogQueries();
    $logged = count($logs) === 1 ? $logs[0]["sql"] : "(missing)";
    echo $name, ": ", $logged === $verbose ? "" : "verbose differs, ",
        preg_match('/SECRET/', $logged . $verbose) ? "leaked" : "redacted", "\n";
}
?>
--EXPECT--
double-quoted: SELECT 1 AS "it's", '?' AS pw
  leaked: no
backtick: SELECT 1 AS `it's`, '?' AS pw
  leaked: no
escaped identifier: SELECT 1 AS "a\"b'c", '?' AS pw
  leaked: no
unicode quotes: SELECT 1 AS “it's”, ‘?’ AS pw, '?' AS pw2
  leaked: no
dash comment: SELECT 1 -- don'?'
, '?' AS pw
  leaked: no
hash comment: SELECT 1 # don'?'
, '?' AS pw
  leaked: no
slash comment: SELECT 1 // don'?'
, '?' AS pw
  leaked: no
nested block comment: SELECT 1 /* a /* it'?'*/, '?' AS pw
  leaked: no
comment literal: SELECT 1 /* pw = '?' */ AS x
  leaked: no
exponent: SELECT 1e-5 AS e, 0x1p-2 AS h, '?' AS pw
  leaked: no
tuple element after dot: SELECT CAST(tuple(7), '?').1e-- tuple'?'
 AS a, '?' AS pw
  leaked: no
hex and bin: SELECT x'?' AS h, b'?' AS b, '?' AS pw
  leaked: no
underscore exponent: redacted
double dot: redacted
spaced dot: redacted
binary exponent: redacted
vertical delimiter: redacted
