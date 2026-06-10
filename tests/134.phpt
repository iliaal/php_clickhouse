--TEST--
selectToStream surfaces a clean error when a callback closes the stream mid-query
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

/* The Select packet loop runs userland progress callbacks. One that
 * fclose()s the destination stream used to leave the cached php_stream*
 * dangling and the next block flush a use-after-free. The stream is now
 * re-resolved on every flush, so a closed stream becomes a clean throw. */
$ch = new ClickHouse(clickhouse_test_config());
$ch->setSettings(["interactive_delay" => "10000"]);
$fp = fopen("php://temp", "r+");
$ch->setProgressCallback(function ($p) use ($fp) {
    if (is_resource($fp)) {
        fclose($fp);
    }
});

try {
    $ch->selectToStream("SELECT number FROM system.numbers LIMIT 5000000", [], $fp, "TabSeparated");
    echo "no throw\n";
} catch (ClickHouseException $e) {
    echo (strpos($e->getMessage(), "closed during the query") !== false ? "closed cleanly" : "other"), "\n";
}

/* The client is still usable after the recovered failure. */
echo "ping: ", ($ch->ping() ? "ok" : "fail"), "\n";
?>
--EXPECT--
closed cleanly
ping: ok
