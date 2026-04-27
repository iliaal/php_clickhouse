--TEST--
ClickHouse smi2-style ergonomics: chainable settings, setSetting, setDatabase, exception getters
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());

// setSettings now returns $this; chaining works.
$ret = $c->setSettings(["max_threads" => "1"]);
var_dump($ret === $c);

// setSetting (singular) is chainable too. Combine multiple calls.
$ret = $c->setSetting("max_block_size", 4096)->setSetting("max_threads", 2);
var_dump($ret === $c);

// Settings actually got applied: SELECT one of them through a per-query
// settings probe to confirm the per-client override is in effect.
$mt = $c->select("SELECT getSetting('max_threads')", [], ClickHouse::FETCH_ONE);
echo "max_threads=", $mt, "\n";

// setDatabase issues USE and updates the cached default. Use that for a
// helper that consults the cache (databaseSize defaults to current DB).
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("CREATE DATABASE IF NOT EXISTS test_054_alt");
$c->execute("DROP TABLE IF EXISTS test_054_alt.t");
$c->execute("CREATE TABLE test_054_alt.t (a UInt8) ENGINE = Memory");

$ret = $c->setDatabase("test_054_alt");
var_dump($ret === $c);
$cur = $c->select("SELECT currentDatabase()", [], ClickHouse::FETCH_ONE);
echo "currentDatabase=", $cur, "\n";
// showTables() with no arg uses the cached default; should now list the alt DB.
$tables = $c->showTables();
echo "showTables_count=", count($tables), " has_t=", (in_array("t", $tables) ? "yes" : "no"), "\n";

// setDatabase rejects malformed identifiers.
try {
    $c->setDatabase("bad; DROP DATABASE test");
    echo "setDatabase: NO EXCEPTION (BUG)\n";
} catch (ClickHouseException $e) {
    echo "setDatabase rejected: ", $e->getMessage(), "\n";
}

// Switch back, drop the temp DB.
$c->setDatabase("test");
$c->execute("DROP DATABASE test_054_alt");

// Exception getters mirror the public properties.
try {
    $c->execute("THIS IS NOT VALID SQL");
} catch (ClickHouseException $e) {
    echo "code_match=",   var_export($e->getServerCode() === $e->server_code, true), "\n";
    echo "name_match=",   var_export($e->getServerName() === $e->server_name, true), "\n";
    echo "queryid_match=", var_export($e->getQueryId() === $e->query_id, true), "\n";
    echo "code_is_int=",  is_int($e->getServerCode()) ? "yes" : "no", "\n";
}
?>
--EXPECT--
bool(true)
bool(true)
max_threads=2
bool(true)
currentDatabase=test_054_alt
showTables_count=1 has_t=yes
setDatabase rejected: database name contains an invalid character
code_match=true
name_match=true
queryid_match=true
code_is_int=yes
