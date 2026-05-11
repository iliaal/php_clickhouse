--TEST--
ClickHouse insertFromStream rejects stream read errors; selectWithExternalData rejects empty external tables
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// --- CR-001: stream-read failure must not commit partial data -----------
//
// Userland wrapper whose second read returns false (the PHP convention
// for "could not read"). Pre-fix, insertFromStream treated the false
// return as EOF and EndInsert() committed whatever had been parsed up
// to the failure — partial rows from a transient read error landed in
// the table silently. Post-fix, the read error throws and the existing
// catch path resets the connection before any data crosses the wire.

class FailAfterFirstReadStream {
    public $context;
    private string $payload = "1\trow-one\n2\trow-two\n3\trow-three\n";
    private int $offset = 0;
    private int $reads = 0;

    public function stream_open(string $path, string $mode, int $options, ?string &$opened_path) {
        return true;
    }
    public function stream_read(int $count) {
        $this->reads++;
        if ($this->reads === 1) {
            // Return a small chunk so the second read fires before EOF.
            $chunk = substr($this->payload, $this->offset, 5);
            $this->offset += strlen($chunk);
            return $chunk;
        }
        trigger_error("simulated stream read failure", E_USER_WARNING);
        return false;
    }
    public function stream_eof(): bool { return false; }
    public function stream_close(): void {}
    public function stream_stat() { return []; }
}
stream_wrapper_register("failread", FailAfterFirstReadStream::class);

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.read_err");
$c->execute("CREATE TABLE test.read_err (id UInt32, s String) ENGINE=Memory");

$fh = fopen("failread://x", "rb");
try {
    @$c->insertFromStream("test.read_err", ["id", "s"], $fh);
    echo "cr001-read-fail: NO THROW\n";
} catch (ClickHouseException $e) {
    echo "cr001-read-fail: REJECTED\n";
}
fclose($fh);

// Crucial assertion: nothing committed. Pre-fix this printed 1.
$cnt = $c->select("SELECT count() FROM test.read_err", [], ClickHouse::FETCH_ONE);
echo "cr001 rowcount after failed read: $cnt\n";

// Same wrapper variant that returns 0 without setting EOF — caught by
// the second branch of the new read-loop check.
class ZeroWithoutEofStream {
    public $context;
    private int $reads = 0;
    public function stream_open(string $path, string $mode, int $options, ?string &$opened_path) { return true; }
    public function stream_read(int $count) {
        $this->reads++;
        return $this->reads === 1 ? "1\tfirst-row\n" : "";
    }
    public function stream_eof(): bool { return false; }  // never EOF
    public function stream_close(): void {}
    public function stream_stat() { return []; }
}
stream_wrapper_register("zeronoeof", ZeroWithoutEofStream::class);

$c->execute("TRUNCATE TABLE test.read_err");
$fh = fopen("zeronoeof://x", "rb");
try {
    @$c->insertFromStream("test.read_err", ["id", "s"], $fh);
    echo "cr001-zero-noeof: NO THROW\n";
} catch (ClickHouseException $e) {
    echo "cr001-zero-noeof: REJECTED\n";
}
fclose($fh);
$cnt = $c->select("SELECT count() FROM test.read_err", [], ClickHouse::FETCH_ONE);
echo "cr001 rowcount after zero-noeof: $cnt\n";

// Sanity: a healthy stream after the failures still works.
$mem = fopen("php://memory", "w+b");
fwrite($mem, "1\tclean\n2\trow\n");
rewind($mem);
$n = $c->insertFromStream("test.read_err", ["id", "s"], $mem);
fclose($mem);
echo "cr001 clean rows: $n\n";

// --- CR-002: empty external-table rows must be rejected ----------------
//
// clickhouse-cpp deliberately skips zero-row external blocks because
// the native protocol uses an empty block as the end-of-stream marker.
// Pre-fix, an empty `rows => []` external slipped past validation and
// the query failed server-side with the misleading "Unknown expression
// or table expression identifier ext_..." error. Reject upfront with a
// message that points to the userland workaround.

$c->execute("DROP TABLE IF EXISTS test.ext_empty");
$c->execute("CREATE TABLE test.ext_empty (id UInt32) ENGINE=Memory");
$c->insert("test.ext_empty", ["id"], [[1], [2], [3]]);

try {
    $c->selectWithExternalData(
        "SELECT id FROM test.ext_empty WHERE id IN ext_ids",
        [["name" => "ext_ids", "columns" => ["id" => "UInt32"], "rows" => []]]
    );
    echo "cr002-empty: NO THROW\n";
} catch (ClickHouseException $e) {
    // Distinguish the new client-side rejection from the pre-fix
    // server-side "Unknown expression or table expression identifier"
    // path, which also threw but only because the server gave up after
    // the lib silently skipped the empty named block.
    $msg = $e->getMessage();
    if (strpos($msg, "has no rows") !== false) {
        echo "cr002-empty: REJECTED-CLIENT\n";
    } elseif (strpos($msg, "Unknown expression") !== false ||
              strpos($msg, "Unknown identifier") !== false) {
        echo "cr002-empty: REJECTED-SERVER (bug)\n";
    } else {
        echo "cr002-empty: REJECTED-OTHER\n";
    }
}

// Sanity: non-empty external still works on the same handle.
$rows = $c->selectWithExternalData(
    "SELECT id FROM test.ext_empty WHERE id IN ext_ids ORDER BY id",
    [["name" => "ext_ids", "columns" => ["id" => "UInt32"], "rows" => [[1], [3]]]],
    [], ClickHouse::FETCH_COLUMN
);
echo "cr002-ok ids: " . implode(",", $rows) . "\n";

$c->execute("DROP TABLE test.read_err");
$c->execute("DROP TABLE test.ext_empty");
?>
--EXPECT--
cr001-read-fail: REJECTED
cr001 rowcount after failed read: 0
cr001-zero-noeof: REJECTED
cr001 rowcount after zero-noeof: 0
cr001 clean rows: 2
cr002-empty: REJECTED-CLIENT
cr002-ok ids: 1,3
