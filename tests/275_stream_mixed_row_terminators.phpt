--TEST--
insertFromStream preserves empty cells after CR-terminated rows
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";
$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.stream_mixed_terminators");
$c->execute("CREATE TABLE test.stream_mixed_terminators (a String, b String) ENGINE=Memory");
try {
    foreach (["CSV", "CSVWithNames", "TSV", "TSVWithNames"] as $format) {
        $separator = strpos($format, "CSV") === 0 ? "," : "\t";
        $header = strpos($format, "WithNames") !== false ? "a{$separator}b\r\n" : "";
        foreach (["\r", "\r\n", "\n"] as $terminator) {
          foreach (["first", str_repeat("x", 65536 - strlen($header) - 5)] as $first) {
            $c->execute("TRUNCATE TABLE test.stream_mixed_terminators");
            $stream = fopen("php://memory", "w+b");
            fwrite($stream, $header . "{$first}{$separator}row{$terminator}"
                . "{$separator}\nlast{$separator}row\n");
            rewind($stream);
            try {
                $count = $c->insertFromStream("test.stream_mixed_terminators", ["a", "b"], $stream, $format);
            } finally {
                fclose($stream);
            }
            $rows = $c->select("SELECT a, b FROM test.stream_mixed_terminators ORDER BY a = '', a = 'last' DESC");
            var_dump($count === 3 && $rows === [
                ["a" => "last", "b" => "row"],
                ["a" => $first, "b" => "row"],
                ["a" => "", "b" => ""],
            ]);
          }
        }
    }
} finally {
    $c->execute("DROP TABLE IF EXISTS test.stream_mixed_terminators");
}
?>
--EXPECT--
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
