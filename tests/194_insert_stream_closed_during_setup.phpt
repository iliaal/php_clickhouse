--TEST--
insertFromStream rejects a stream closed by callback-capable query setup
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

class CloseStreamSetting {
    public function __toString() {
        fclose($GLOBALS["stream"]);
        return "1";
    }
}

$c = new ClickHouse(clickhouse_test_config());
$c->execute("DROP TABLE IF EXISTS test.stream_closed_setup");
$c->execute("CREATE TABLE test.stream_closed_setup (v UInt32) ENGINE=Memory");

$GLOBALS["stream"] = fopen("php://temp", "w+");
fwrite($GLOBALS["stream"], "1\n");
rewind($GLOBALS["stream"]);

try {
    $c->insertFromStream(
        "test.stream_closed_setup",
        ["v"],
        $GLOBALS["stream"],
        "TSV",
        1000,
        "",
        ["max_threads" => new CloseStreamSetting()]
    );
    echo "accepted\n";
} catch (ClickHouseException $e) {
    echo $e->getMessage(), "\n";
}

echo "ping=", $c->ping() ? "ok" : "fail", "\n";
$c->execute("DROP TABLE test.stream_closed_setup");
?>
--EXPECT--
insertFromStream: argument 3 must remain an open stream resource
ping=ok
