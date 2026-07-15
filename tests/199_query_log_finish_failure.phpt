--TEST--
Query logging records one caller-visible outcome when a finish observer throws
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->enableLogQueries(true);
$target = null;
$c->setVerbose(function ($event) use (&$target) {
    if ($event === $target) {
        throw new RuntimeException("finish observer failed");
    }
});

function probeFinishLog($client, $label, $event, $operation) {
    global $target;
    $target = $event;
    try {
        $operation();
        echo $label, ":accepted\n";
    } catch (RuntimeException $e) {
        echo $label, ":threw\n";
    }
    $logs = $client->getLogQueries();
    echo $label, ":count=", count($logs), ":code=", $logs[0]["error_code"], "\n";
}

probeFinishLog($c, "select", "select_finish", function () use ($c) {
    $c->select("SELECT 1");
});
probeFinishLog($c, "execute", "execute_finish", function () use ($c) {
    $c->execute("SELECT 1");
});
probeFinishLog($c, "callback", "select_finish", function () use ($c) {
    $c->selectStreamCallback("SELECT 1", function ($row) {});
});
?>
--EXPECT--
select:threw
select:count=1:code=-1
execute:threw
execute:count=1:code=-1
callback:threw
callback:count=1:code=-1
