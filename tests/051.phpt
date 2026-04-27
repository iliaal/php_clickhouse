--TEST--
ClickHouse offline surface: classes, constants, methods, exception path (no server)
--SKIPIF--
<?php if (!extension_loaded("clickhouse")) { echo "skip clickhouse extension not loaded"; } ?>
--FILE--
<?php
/* Server-free smoke. Validates that the .dll/.so loads, every public
 * class is registered, every constant has the documented value, every
 * method is present with the right argument count, and the exception
 * boundary fires cleanly when the constructor cannot reach a server.
 * Also doubles as a Windows CI smoke since the rest of the suite skips
 * up-front when no server is present. */

echo "ext=", extension_loaded("clickhouse") ? 1 : 0, "\n";

foreach (["ClickHouse", "ClickHouseException", "ClickHouseRowIterator",
          "ClickHouseStatement",
          "SeasClick", "SeasClickException"] as $cls) {
    echo "class.$cls=", class_exists($cls) ? 1 : 0, "\n";
}

echo "FETCH_ONE=",        ClickHouse::FETCH_ONE, "\n";
echo "FETCH_KEY_PAIR=",   ClickHouse::FETCH_KEY_PAIR, "\n";
echo "DATE_AS_STRINGS=",  ClickHouse::DATE_AS_STRINGS, "\n";
echo "FETCH_COLUMN=",     ClickHouse::FETCH_COLUMN, "\n";

/* Methods that must exist on the public surface. Order matches the
 * stub. Mismatches here surface as 0= immediately. */
$methods = [
    "__construct", "__destruct",
    "select", "selectStream", "selectStatement", "selectStreamCallback",
    "insert", "insertAssoc", "writeStart", "write", "writeEnd",
    "execute", "ping",
    "setSettings", "setSetting", "setDatabase",
    "setProgressCallback", "setProfileCallback", "setVerbose",
    "resetConnection", "getServerInfo", "getCurrentEndpoint",
    "getStatistics",
    "isExists", "showDatabases", "showProcesslist", "getServerVersion",
    "databaseSize", "tablesSize", "tableSize", "partitions",
    "showTables", "showCreateTable", "getServerUptime",
    "truncateTable", "dropPartition",
    "enableLogQueries", "getLogQueries",
];
$missing = [];
foreach ($methods as $m) {
    if (!method_exists("ClickHouse", $m)) $missing[] = $m;
}
echo "missing_methods=", count($missing) ? implode(",", $missing) : "none", "\n";

/* Iterator surface. */
$it_methods = ["rewind", "valid", "current", "key", "next", "count"];
$it_missing = [];
foreach ($it_methods as $m) {
    if (!method_exists("ClickHouseRowIterator", $m)) $it_missing[] = $m;
}
echo "iter_missing=", count($it_missing) ? implode(",", $it_missing) : "none", "\n";

$ri = new ReflectionClass("ClickHouseRowIterator");
echo "iter_implements_iterator=", $ri->implementsInterface("Iterator") ? 1 : 0, "\n";
echo "iter_implements_countable=", $ri->implementsInterface("Countable") ? 1 : 0, "\n";

/* Constructor on a port that no one is listening on, with a tight
 * timeout, must throw ClickHouseException -- proves the C++ -> PHP
 * exception boundary works on this build/OS. Uses port 1 (reserved,
 * always rejected) and a 50ms connect timeout. */
try {
    new ClickHouse([
        "host" => "127.0.0.1",
        "port" => 1,
        "connect_timeout_ms" => 50,
    ]);
    echo "ctor_throws=0\n";
} catch (ClickHouseException $e) {
    echo "ctor_throws=1\n";
    echo "ctor_exc_class=", get_class($e), "\n";
    /* server_code stays 0 for client-side errors per the API contract. */
    echo "ctor_server_code_zero=", ($e->server_code === 0) ? 1 : 0, "\n";
} catch (\Throwable $e) {
    echo "ctor_threw_wrong=", get_class($e), "\n";
}
?>
--EXPECT--
ext=1
class.ClickHouse=1
class.ClickHouseException=1
class.ClickHouseRowIterator=1
class.ClickHouseStatement=1
class.SeasClick=1
class.SeasClickException=1
FETCH_ONE=1
FETCH_KEY_PAIR=2
DATE_AS_STRINGS=4
FETCH_COLUMN=8
missing_methods=none
iter_missing=none
iter_implements_iterator=1
iter_implements_countable=1
ctor_throws=1
ctor_exc_class=ClickHouseException
ctor_server_code_zero=1
