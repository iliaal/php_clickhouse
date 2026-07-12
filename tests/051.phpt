--TEST--
ClickHouse offline surface: classes, constants, methods, exception path (no server)
--EXTENSIONS--
clickhouse
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
echo "JSON_AS_ARRAY=",    ClickHouse::JSON_AS_ARRAY, "\n";
echo "JSON_AS_OBJECT=",   ClickHouse::JSON_AS_OBJECT, "\n";
echo "UUID_WITH_DASHES=", ClickHouse::UUID_WITH_DASHES, "\n";
echo "FIXEDSTRING_BINARY=", ClickHouse::FIXEDSTRING_BINARY, "\n";

/* [required, total], matching clickhouse.stub.php. */
$methods = [
    "__construct" => [1, 1], "__destruct" => [0, 0],
    "select" => [1, 5], "selectWithExternalData" => [2, 6],
    "selectToStream" => [3, 6], "insert" => [3, 5],
    "insertAssoc" => [2, 4], "insertFromStream" => [3, 7],
    "writeStart" => [2, 4], "write" => [1, 1], "writeEnd" => [0, 0],
    "execute" => [1, 4], "ping" => [0, 0],
    "setSettings" => [1, 1], "setSetting" => [2, 2], "setDatabase" => [1, 1],
    "setProgressCallback" => [1, 1], "setProfileCallback" => [1, 1],
    "setVerbose" => [1, 1], "resetConnection" => [0, 0],
    "getServerInfo" => [0, 0], "getCurrentEndpoint" => [0, 0],
    "getStatistics" => [0, 0], "databaseSize" => [0, 1],
    "tablesSize" => [0, 1], "partitions" => [1, 1], "showTables" => [0, 2],
    "showCreateTable" => [1, 1], "getServerUptime" => [0, 0],
    "enableLogQueries" => [0, 1], "getLogQueries" => [0, 0],
    "selectStream" => [1, 5], "selectStatement" => [1, 5],
    "selectStreamCallback" => [2, 6], "isExists" => [2, 2],
    "showDatabases" => [0, 0], "showProcesslist" => [0, 0],
    "getServerVersion" => [0, 0], "tableSize" => [1, 1],
    "truncateTable" => [1, 1], "dropPartition" => [2, 2],
];
$missing = [];
$bad_arity = [];
foreach ($methods as $m => $arity) {
    if (!method_exists("ClickHouse", $m)) {
        $missing[] = $m;
        continue;
    }
    $rm = new ReflectionMethod("ClickHouse", $m);
    if ($rm->getNumberOfRequiredParameters() !== $arity[0]
        || $rm->getNumberOfParameters() !== $arity[1]) {
        $bad_arity[] = $m;
    }
}
echo "missing_methods=", count($missing) ? implode(",", $missing) : "none", "\n";
echo "bad_arity=", count($bad_arity) ? implode(",", $bad_arity) : "none", "\n";

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

$statement_methods = [
    "count", "rewind", "valid", "current", "key", "next",
    "offsetExists", "offsetGet", "offsetSet", "offsetUnset",
    "jsonSerialize", "toArray", "statistics", "fetchOne",
    "fetchKeyPair", "fetchColumn",
];
$statement_missing = [];
foreach ($statement_methods as $m) {
    if (!method_exists("ClickHouseStatement", $m)) $statement_missing[] = $m;
}
echo "statement_missing=", count($statement_missing) ? implode(",", $statement_missing) : "none", "\n";
$rs = new ReflectionClass("ClickHouseStatement");
foreach (["Iterator", "Countable", "ArrayAccess", "JsonSerializable"] as $interface) {
    echo "statement_implements_", strtolower($interface), "=",
        $rs->implementsInterface($interface) ? 1 : 0, "\n";
}

$exception_methods = ["getServerCode", "getServerName", "getQueryId"];
$exception_missing = [];
foreach ($exception_methods as $m) {
    if (!method_exists("ClickHouseException", $m)) $exception_missing[] = $m;
}
echo "exception_missing=", count($exception_missing) ? implode(",", $exception_missing) : "none", "\n";

/* PHP 7.4 can represent these types even though mixed/static are PHP 8-only. */
function reflection_type_name($type) {
    if ($type === null) return "none";
    $name = $type->getName();
    return $type->allowsNull() ? "?" . $name : $name;
}

$return_types = [
    "selectToStream" => "int",
    "insert" => "bool",
    "getServerInfo" => "array",
    "getServerVersion" => "string",
];
$bad_return_types = [];
foreach ($return_types as $m => $expected) {
    $rm = new ReflectionMethod("ClickHouse", $m);
    $actual = reflection_type_name($rm->getReturnType());
    if ($actual !== $expected) $bad_return_types[] = "$m:$actual";
}
echo "bad_return_types=", count($bad_return_types) ? implode(",", $bad_return_types) : "none", "\n";

$property_types = ["host" => "string", "port" => "int", "user" => "?string"];
$bad_property_types = [];
foreach ($property_types as $p => $expected) {
    $rp = new ReflectionProperty("ClickHouse", $p);
    $actual = reflection_type_name($rp->getType());
    if ($actual !== $expected) $bad_property_types[] = "$p:$actual";
}
echo "bad_property_types=", count($bad_property_types) ? implode(",", $bad_property_types) : "none", "\n";

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
JSON_AS_ARRAY=16
JSON_AS_OBJECT=32
UUID_WITH_DASHES=64
FIXEDSTRING_BINARY=128
missing_methods=none
bad_arity=none
iter_missing=none
iter_implements_iterator=1
iter_implements_countable=1
statement_missing=none
statement_implements_iterator=1
statement_implements_countable=1
statement_implements_arrayaccess=1
statement_implements_jsonserializable=1
exception_missing=none
bad_return_types=none
bad_property_types=none
ctor_throws=1
ctor_exc_class=ClickHouseException
ctor_server_code_zero=1
