--TEST--
selectWithExternalData: reset after rejected external data keeps database, query log, and statistics consistent
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
require __DIR__ . "/_clickhouse.inc";
clickhouse_skip_if_no_server();
clickhouse_skip_if_no_json();
?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$config = clickhouse_test_config();
$config["compression"] = false;
$config["receive_timeout"] = 5;
$c = new ClickHouse($config);
$c->execute("CREATE DATABASE IF NOT EXISTS t_imz_db");
$c->setDatabase("t_imz_db");
$c->setSettings(array("allow_experimental_json_type" => 1));
$c->enableLogQueries(true);

$rows = array();
for ($i = 1; $i <= 50; $i++) {
    $rows[] = array($i, array(array($i)));
}
$bad = array(array(
    "name"    => "e",
    "columns" => array("id" => "UInt32", "v" => "JSON"),
    "rows"    => $rows,
));

try {
    $c->selectWithExternalData("SELECT count() FROM e", $bad, array(), 0, "imz-rejected-1");
    echo "rejected: no exception\n";
} catch (ClickHouseException $e) {
    echo "rejected: server_code>0=", var_export($e->getServerCode() > 0, true),
        " query_id=", $e->getQueryId(), "\n";
}
$st = $c->getStatistics();
echo "stats query_id: ", $st["query_id"], "\n";

/* The reset re-applies the client's database, so unqualified names still
 * resolve against it. */
echo "database: ", $c->select("SELECT currentDatabase() AS d", array(), ClickHouse::FETCH_ONE), "\n";

/* Server-side rejection before any data is read: still an exception, and
 * the connection still works. */
try {
    $c->selectWithExternalData(
        "SELEC count() FROM e",
        array(array("name" => "e", "columns" => array("id" => "UInt32"), "rows" => array(array(1))))
    );
    echo "syntax: no exception\n";
} catch (ClickHouseException $e) {
    echo "syntax: server_code>0=", var_export($e->getServerCode() > 0, true), "\n";
}
echo "ping: ", var_export($c->ping(), true), "\n";

$log = $c->getLogQueries();
foreach ($log as $entry) {
    if ($entry["error_code"] === 0) {
        $status = "ok";
    } elseif ($entry["error_code"] > 0) {
        $status = "server error";
    } else {
        $status = "client error " . $entry["error_message"];
    }
    echo "log: ", $entry["sql"], " | ", $entry["query_id"] === "" ? "-" : $entry["query_id"], " | ", $status, "\n";
}

$c->setDatabase("default");
$c->execute("DROP DATABASE IF EXISTS t_imz_db");
?>
--EXPECT--
rejected: server_code>0=true query_id=imz-rejected-1
stats query_id: imz-rejected-1
database: t_imz_db
syntax: server_code>0=true
ping: true
log: SELECT count() FROM e | imz-rejected-1 | server error
log: SELECT currentDatabase() AS d | - | ok
log: SELEC count() FROM e | - | server error
