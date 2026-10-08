--TEST--
Query log truncation preserves redacted SQL at the byte limit and leaves verbose SQL intact
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";
$c = new ClickHouse(clickhouse_test_config());
$c->enableLogQueries(true);

function cappedSql($redacted) {
  return strlen($redacted) <= 8192 ? $redacted
    : substr($redacted, 0, 8192 - strlen("... (truncated)")) . "... (truncated)";
}

$cases = [];
foreach ([8191, 8192, 8193, 65536] as $length) {
  $sql = "SELECT 1" . str_repeat(" ", $length - 8);
  $cases["plain-$length"] = [$sql, $sql];
}
// Redaction can expand an empty literal across the cap, or shrink a large one.
$prefix = "SELECT " . str_repeat(" ", 8183);
$cases["expanded-literal"] = [$prefix . "''", $prefix . "'?'"];
$cases["large-literal"] = ["SELECT '" . str_repeat("secret", 10000) . "'", "SELECT '?'"];
$cases["escaped-literals"] = ["SELECT 'one\\'two', 'three''four'", "SELECT '?', '?'"];
// A literal crossing the retained-prefix boundary must never expose its bytes.
$prefix = "SELECT " . str_repeat(" ", 8170);
$cases["crossing-literal"] = [$prefix . "'secret'" . str_repeat(" ", 100),
  $prefix . "'?'" . str_repeat(" ", 100)];

// Quotes, doubled quotes and escaped backslashes at both truncation boundaries.
foreach ([8176, 8190, 8191, 8192, 8193] as $position) {
  $prefix = "SELECT " . str_repeat(" ", $position - 7);
  $cases["escaped-boundary-$position"] = [$prefix . "'one\\\\two\\'three''four'",
    $prefix . "'?'"];
}
// Preserve the existing redactor's treatment of quotes inside SQL comments.
$prefix = "SELECT 1 /* " . str_repeat(" ", 8164);
$cases["comment-boundary"] = [$prefix . "'comment-secret'" . str_repeat(" ", 100) . "*/",
  $prefix . "'?'" . str_repeat(" ", 100) . "*/"];

$verboseSql = null;
$c->setVerbose(function ($event, $context) use (&$verboseSql) {
  if ($event === "select_start") $verboseSql = $context["sql"];
});
foreach ($cases as $name => $case) {
  $c->select($case[0]);
  $logs = $c->getLogQueries();
  echo $name, ": ", count($logs) === 1 && $logs[0]["sql"] === cappedSql($case[1])
    && $verboseSql === $case[1] ? "ok" : "FAIL", "\n";
}

// Error logs use the same bounded path, including unterminated literals.
try {
  $c->select("SELECT '" . str_repeat("private", 10000));
} catch (ClickHouseException $e) {
}
$logs = $c->getLogQueries();
echo "unterminated: ", count($logs) === 1 && $logs[0]["sql"] === "SELECT '?'"
  && $logs[0]["error_code"] !== 0 ? "ok" : "FAIL", "\n";
?>
--EXPECT--
plain-8191: ok
plain-8192: ok
plain-8193: ok
plain-65536: ok
expanded-literal: ok
large-literal: ok
escaped-literals: ok
crossing-literal: ok
escaped-boundary-8176: ok
escaped-boundary-8190: ok
escaped-boundary-8191: ok
escaped-boundary-8192: ok
escaped-boundary-8193: ok
comment-boundary: ok
unterminated: ok
