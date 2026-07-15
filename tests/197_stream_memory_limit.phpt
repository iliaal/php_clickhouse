--TEST--
insertFromStream accounts delimiter-free native cell growth against memory_limit
--EXTENSIONS--
clickhouse
--INI--
memory_limit=16M
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

class LargeCellStream {
    public $context;
    private $remaining = 33554432;

    public function stream_open($path, $mode, $options, &$openedPath) {
        return true;
    }

    public function stream_read($count) {
        $length = min($count, $this->remaining);
        $this->remaining -= $length;
        return str_repeat("x", $length);
    }

    public function stream_eof() {
        return $this->remaining === 0;
    }

    public function stream_stat() {
        return [];
    }
}

stream_wrapper_register("large-cell", "LargeCellStream");
$stream = fopen("large-cell://input", "r");

$c = new ClickHouse(clickhouse_test_config());
$c->execute("DROP TABLE IF EXISTS test.stream_memory_limit");
$c->execute("CREATE TABLE test.stream_memory_limit (v String) ENGINE=Memory");

try {
    $c->insertFromStream("test.stream_memory_limit", ["v"], $stream, "TSV");
    echo "accepted\n";
} catch (ClickHouseException $e) {
    echo $e->getMessage(), "\n";
}

echo "ping=", $c->ping() ? "ok" : "fail", "\n";
$c->execute("DROP TABLE test.stream_memory_limit");
?>
--EXPECT--
insertFromStream: cell exceeds the available PHP memory_limit
ping=ok
