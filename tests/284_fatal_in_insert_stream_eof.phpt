--TEST--
A fatal error in a userland stream_eof() during insertFromStream() leaves the client usable
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";
class EofFatal284
{
    public $context;
    private $sent = false;
    private $justRead = false;

    public function stream_open($path, $mode, $options, &$opened_path)
    {
        return true;
    }

    public function stream_read($count)
    {
        $this->justRead = true;
        if (!$this->sent) {
            $this->sent = true;
            return "1\n2\n";
        }
        return "";
    }

    public function stream_eof()
    {
        /* PHP's own read path asks for EOF first; fail only on the
         * extension's check after a zero-byte read. */
        if ($this->justRead) {
            $this->justRead = false;
            return false;
        }
        eval('function clickhouse_284_dup() {} function clickhouse_284_dup() {}');
        return true;
    }
}
stream_wrapper_register("eoffatal284", "EofFatal284");

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.fatal_eof_284");
$c->execute("CREATE TABLE test.fatal_eof_284 (v UInt32) ENGINE=Memory");
register_shutdown_function(function () use ($c) {
    try {
        $c->insert("test.fatal_eof_284", ["v"], [[3]]);
        echo "rows after reuse: ", $c->select("SELECT count() FROM test.fatal_eof_284 WHERE v = 3", [], ClickHouse::FETCH_ONE), "\n";
    } catch (ClickHouseException $e) {
        echo get_class($e), ": ", $e->getMessage(), "\n";
    }
});
$c->insertFromStream("test.fatal_eof_284", ["v"], fopen("eoffatal284://x", "r"), "TSV");
echo "unreachable\n";
?>
--CLEAN--
<?php
require __DIR__ . "/_clickhouse.inc";
$c = new ClickHouse(clickhouse_test_config());
$c->execute("DROP TABLE IF EXISTS test.fatal_eof_284");
?>
--EXPECTF--
Fatal error: Cannot redeclare %A
rows after reuse: 1
