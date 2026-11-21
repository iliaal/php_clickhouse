--TEST--
A fatal error in a userland stream wrapper read by insertFromStream() leaves the client usable
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";
class FatalStream282
{
    public $context;
    private $sent = false;

    public function stream_open($path, $mode, $options, &$opened_path)
    {
        return true;
    }

    public function stream_read($count)
    {
        if (!$this->sent) {
            $this->sent = true;
            return "1\n2\n";
        }
        eval('function clickhouse_282_dup() {} function clickhouse_282_dup() {}');
        return "";
    }

    public function stream_eof()
    {
        return false;
    }
}
stream_wrapper_register("fatal282", "FatalStream282");

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.fatal_stream_282");
$c->execute("CREATE TABLE test.fatal_stream_282 (v UInt32) ENGINE=Memory");
register_shutdown_function(function () use ($c) {
    try {
        $c->insert("test.fatal_stream_282", ["v"], [[3]]);
        echo "rows after reuse: ", $c->select("SELECT count() FROM test.fatal_stream_282 WHERE v = 3", [], ClickHouse::FETCH_ONE), "\n";
    } catch (ClickHouseException $e) {
        echo get_class($e), ": ", $e->getMessage(), "\n";
    }
});
$c->insertFromStream("test.fatal_stream_282", ["v"], fopen("fatal282://x", "r"), "TSV");
echo "unreachable\n";
?>
--CLEAN--
<?php
require __DIR__ . "/_clickhouse.inc";
$c = new ClickHouse(clickhouse_test_config());
$c->execute("DROP TABLE IF EXISTS test.fatal_stream_282");
?>
--EXPECTF--
Fatal error: Cannot redeclare %A
rows after reuse: 1
