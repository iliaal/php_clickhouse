--TEST--
selectWithExternalData: server-rejected external data leaves the connection usable
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

/* The server rejects the JSON cells while reading the external table and
 * answers with an Exception before the session is idle again. Without a
 * reset the next request on the socket never gets a reply; the receive
 * timeout turns that hang into an exception so the test fails instead of
 * stalling the suite. */
$rows = array();
for ($i = 1; $i <= 50; $i++) {
    $rows[] = array($i, array(array($i)));
}
$external = array(array(
    "name"    => "e",
    "columns" => array("id" => "UInt32", "v" => "JSON"),
    "rows"    => $rows,
));

foreach (array(false, true) as $compression) {
    $config = clickhouse_test_config();
    $config["compression"] = $compression;
    $config["receive_timeout"] = 5;
    $c = new ClickHouse($config);
    $c->setSettings(array("allow_experimental_json_type" => 1));
    echo "compression=", $compression ? "on" : "off", "\n";

    try {
        $c->selectWithExternalData("SELECT count() FROM e", $external);
        echo "rejected: no exception\n";
    } catch (ClickHouseException $e) {
        echo "rejected: ", $e->getServerCode() > 0 ? "server error" : "client error " . $e->getMessage(), "\n";
    }

    foreach (array("ping", "select", "select2") as $step) {
        try {
            if ($step === "ping") {
                $r = $c->ping();
            } else {
                $r = $c->select($step === "select" ? "SELECT 42 AS x" : "SELECT 43 AS x");
            }
            echo "$step: ", json_encode($r), "\n";
        } catch (ClickHouseException $e) {
            echo "$step: EXC ", $e->getMessage(), "\n";
        }
    }

    $ok = $c->selectWithExternalData(
        "SELECT sum(id) AS s FROM e",
        array(array("name" => "e", "columns" => array("id" => "UInt32"), "rows" => array(array(1), array(2), array(3))))
    );
    echo "external after: ", json_encode($ok), "\n";
}
?>
--EXPECT--
compression=off
rejected: server error
ping: true
select: [{"x":42}]
select2: [{"x":43}]
external after: [{"s":6}]
compression=on
rejected: server error
ping: true
select: [{"x":42}]
select2: [{"x":43}]
external after: [{"s":6}]
