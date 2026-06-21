--TEST--
ClickHouse send/receive_timeout_ms config and insertAssoc() with per-call settings
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

function probe(string $label, callable $fn): void {
    try {
        $fn();
        echo "$label: NO THROW\n";
    } catch (Throwable $e) {
        echo "$label: REJECTED\n";
    }
}

/* send_timeout_ms / receive_timeout_ms are accepted at construction and the
 * client stays usable. */
$cfg = clickhouse_test_config();
$cfg["send_timeout_ms"] = 5000;
$cfg["receive_timeout_ms"] = 5000;
$c = new ClickHouse($cfg);
echo "ping: ", $c->ping() ? "ok" : "fail", "\n";
echo "query: ", $c->select("SELECT 1", [], ClickHouse::FETCH_ONE), "\n";

$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.assoc_settings");
$c->execute("CREATE TABLE test.assoc_settings (id UInt32, name String) ENGINE=Memory");

/* insertAssoc() forwards its $settings argument to the server: a valid
 * setting lets the insert through, a real setting with a malformed value
 * is rejected server-side (proving the settings actually transmit). */
probe("insertAssoc valid setting", fn() =>
    $c->insertAssoc(
        "test.assoc_settings",
        [["id" => 1, "name" => "alice"]],
        "",
        ["max_insert_block_size" => 1024]
    ));
probe("insertAssoc malformed setting value", fn() =>
    $c->insertAssoc(
        "test.assoc_settings",
        [["id" => 2, "name" => "bob"]],
        "",
        ["max_insert_block_size" => "abc"]
    ));

echo "rows: ", $c->select("SELECT count() FROM test.assoc_settings", [], ClickHouse::FETCH_ONE), "\n";

$c->execute("DROP TABLE test.assoc_settings");
?>
--EXPECT--
ping: ok
query: 1
insertAssoc valid setting: NO THROW
insertAssoc malformed setting value: REJECTED
rows: 1
