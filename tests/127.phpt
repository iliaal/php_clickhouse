--TEST--
setDatabase survives an explicit reconnect (no revert to constructor database)
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$boot = new ClickHouse(clickhouse_test_config());
$boot->execute("CREATE DATABASE IF NOT EXISTS test");
$boot->execute("CREATE DATABASE IF NOT EXISTS sc_db127");

/* Construct against a non-default database, switch to "default", then
 * reconnect. The session must remain in "default": the reconnect used
 * to fall back to the constructor-time database (sc_db127) while the
 * cached "database" property still read "default", silently routing
 * every subsequent query to the wrong database. */
$ch = new ClickHouse(clickhouse_test_config() + ["database" => "sc_db127"]);
var_dump($ch->select("SELECT currentDatabase() AS d")[0]["d"]);
$ch->setDatabase("default");
var_dump($ch->select("SELECT currentDatabase() AS d")[0]["d"]);
$ch->resetConnection();
var_dump($ch->select("SELECT currentDatabase() AS d")[0]["d"]);

$boot->execute("DROP DATABASE sc_db127");
?>
--EXPECT--
string(8) "sc_db127"
string(7) "default"
string(7) "default"
