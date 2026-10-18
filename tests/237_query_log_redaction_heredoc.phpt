--TEST--
Query-log and verbose redaction recognize $$ and $tag$ heredoc literals the way the server lexes them
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
    "plain heredoc" => 'SELECT $$SECRET_HD$$ AS pw',
    "tagged heredoc" => 'SELECT $tag$SECRET_TAG $$ $x$$tag$ AS pw, \'SECRET_AFTER\' AS y',
    "heredoc with apostrophe" => 'SELECT $$it\'s$$ AS a, \'SECRET_APOS\' AS b',
    "dollar inside identifier" => 'SELECT 1 AS a$x$, \'$x$SECRET_IDENT\' AS b',
    "unclosed tag" => 'SELECT 1 AS $a$b, \'SECRET_UNCLOSED\' AS c',
    "heredoc in array" => 'SELECT 1 AS n, [$$SECRET_ARR$$] AS a',
];
foreach ($cases as $name => $sql) {
    $verbose = null;
    $c->select($sql);
    $logs = $c->getLogQueries();
    $logged = count($logs) === 1 ? $logs[0]["sql"] : "(missing)";
    echo $name, ": ", $logged === $verbose ? "" : "verbose differs: ", $logged, "\n";
    echo "  leaked: ", preg_match('/SECRET/', $logged . $verbose) ? "yes" : "no", "\n";
}

// Past the heredoc tag index cap every heredoc is redacted through end of input.
$tags = "";
for ($i = 0; $i < 1100; $i++) $tags .= "\$$i\$";
$sql = "SELECT 1 /* $tags */, \$\$SECRET_OVER\$\$ AS pw, 'x' AS y";
$verbose = null;
$c->select($sql);
$logs = $c->getLogQueries();
$expected = "SELECT 1 /* $tags */, \$\$?";
echo "tag cap: ", $logs[0]["sql"] === $expected && $verbose === $expected ? "redacted to end" : "FAIL " . substr($verbose, -40), "\n";
?>
--EXPECT--
plain heredoc: SELECT $$?$$ AS pw
  leaked: no
tagged heredoc: SELECT $tag$?$tag$ AS pw, '?' AS y
  leaked: no
heredoc with apostrophe: SELECT $$?$$ AS a, '?' AS b
  leaked: no
dollar inside identifier: SELECT 1 AS a$x$, '?' AS b
  leaked: no
unclosed tag: SELECT 1 AS $a$b, '?' AS c
  leaked: no
heredoc in array: SELECT 1 AS n, [$$?$$] AS a
  leaked: no
tag cap: redacted to end
