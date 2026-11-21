--TEST--
A fatal error while insertFromStream() reads its stream commits none of the stream's rows
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
require __DIR__ . "/_clickhouse.inc";
clickhouse_skip_if_no_server();
clickhouse_skip_unless_shutdown_after_oom();
if (!function_exists("proc_open")) {
    print "skip proc_open unavailable";
} elseif (!is_readable("/proc/self/maps")) {
    print "skip /proc/self/maps is needed to locate the extension";
}
?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";
/* The stream yields five CSV rows, so batch_rows = 2 sends two blocks before
 * the next read ends the script. Like a read error, the fatal error must
 * discard the sent blocks instead of committing them, whether or not a
 * shutdown function uses the client and when only the object's destruction
 * ends the insert. */
$child = <<<'PHP'
require $argv[1];
$table = $argv[2];
$kind = $argv[3];
class FatalStream
{
    public $context;
    private $reads = 0;
    public function stream_open($path, $mode, $options, &$opened_path)
    {
        return true;
    }
    public function stream_read($count)
    {
        if ($this->reads++ === 0) {
            return "1\n2\n3\n4\n5\n";
        }
        if ($GLOBALS["kind"] === "error") {
            trigger_error("stream failed", E_USER_ERROR);
        } elseif ($GLOBALS["kind"] === "memory") {
            ini_set("memory_limit", (string)(memory_get_usage(true) + (1 << 20)));
            $GLOBALS["hog"] = str_repeat("x", 8 << 20);
        } else {
            throw new RuntimeException("stream failed");
        }
        return "";
    }
    public function stream_eof()
    {
        return false;
    }
}
stream_wrapper_register("fatal308", "FatalStream");
$c = new ClickHouse(clickhouse_test_config());
if ($argv[4] === "1") {
    register_shutdown_function(function () use ($c, $table) {
        ini_set("memory_limit", "-1");
        try {
            echo "shutdown rows=", $c->select("SELECT count() FROM $table", [], ClickHouse::FETCH_ONE), "\n";
        } catch (ClickHouseException $e) {
            echo "shutdown ", get_class($e), ": ", $e->getMessage(), "\n";
        }
    });
}
$c->insertFromStream($table, ["v"], fopen("fatal308://x", "r"), "CSV", 2);
echo "unreachable\n";
PHP;
$file = tempnam(sys_get_temp_dir(), "clickhouse-308-");
file_put_contents($file, "<?php\n" . $child);
preg_match('~\s(/\S*clickhouse\.so)$~m', file_get_contents("/proc/self/maps"), $m);
$php = getenv("TEST_PHP_EXECUTABLE") ?: PHP_BINARY;
$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
foreach (["error", "memory", "exception"] as $kind) {
    foreach (["0", "1"] as $shutdown) {
        $table = "test.stream_fatal_308_{$kind}_$shutdown";
        $c->execute("DROP TABLE IF EXISTS $table");
        $c->execute("CREATE TABLE $table (v UInt32) ENGINE = MergeTree ORDER BY tuple()");
        $proc = proc_open([$php, "-n", "-d", "extension=" . $m[1], "-d", "display_errors=1",
                           $file, __DIR__ . "/_clickhouse.inc", $table, $kind, $shutdown],
                          [1 => ["pipe", "w"], 2 => ["redirect", 1]], $pipes);
        $output = stream_get_contents($pipes[1]);
        fclose($pipes[1]);
        proc_close($proc);
        preg_match('~^shutdown .*$~m', $output, $line);
        echo "$kind, shutdown function $shutdown: ",
            preg_match('~^(PHP )?Fatal error: (Allowed memory size|stream failed|Uncaught RuntimeException: stream failed)~m', $output) ? "fatal" : "no fatal",
            ", ", $line ? $line[0] . ", " : "",
            "rows=", $c->select("SELECT count() FROM $table", [], ClickHouse::FETCH_ONE), "\n";
        $c->execute("DROP TABLE $table");
    }
}
unlink($file);
?>
--EXPECT--
error, shutdown function 0: fatal, rows=0
error, shutdown function 1: fatal, shutdown rows=0, rows=0
memory, shutdown function 0: fatal, rows=0
memory, shutdown function 1: fatal, shutdown rows=0, rows=0
exception, shutdown function 0: fatal, rows=0
exception, shutdown function 1: fatal, shutdown rows=0, rows=0
