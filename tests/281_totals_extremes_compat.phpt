--TEST--
Totals and Extremes packets retain existing exceptions, logs, and connection recovery
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
require __DIR__ . "/_clickhouse.inc";
clickhouse_skip_if_no_server();
?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$queries = [
    "totals" => "SELECT number % 2 AS k, count() AS n FROM numbers(4) GROUP BY k WITH TOTALS ORDER BY k",
    "extremes" => "SELECT number AS n FROM numbers(3) SETTINGS extremes=1",
];
$methods = [
    "select" => function ($c, $sql, $qid) {
        return $c->select($sql, [], 0, $qid);
    },
    "execute" => function ($c, $sql, $qid) {
        return $c->execute($sql, [], $qid);
    },
    "selectStatement" => function ($c, $sql, $qid) {
        return $c->selectStatement($sql, [], $qid);
    },
    "selectStream" => function ($c, $sql, $qid) {
        return $c->selectStream($sql, [], $qid);
    },
    "selectStreamCallback" => function ($c, $sql, $qid) {
        return $c->selectStreamCallback($sql, function ($row) {}, [], $qid);
    },
    "selectToStream" => function ($c, $sql, $qid) {
        $stream = fopen("php://temp", "w+");
        try {
            return $c->selectToStream($sql, [], $stream, "TabSeparated", $qid);
        } finally {
            fclose($stream);
        }
    },
    "selectWithExternalData" => function ($c, $sql, $qid) {
        $externals = [[
            "name" => "compat_external",
            "columns" => ["number" => "UInt64"],
            "rows" => [[0], [1], [2], [3]],
        ]];
        $sql = str_replace(["numbers(4)", "numbers(3)"], "compat_external", $sql);
        return $c->selectWithExternalData($sql, $externals, [], 0, $qid);
    },
];

$c = new ClickHouse(clickhouse_test_config());
$prefix = uniqid("totals-extremes-", true);
foreach ($queries as $kind => $sql) {
    foreach ($methods as $method => $invoke) {
        $label = $kind . "-" . $method;
        $qid = $prefix . "-" . $label;
        $c->enableLogQueries();
        $caught = null;
        try {
            $invoke($c, $sql, $qid);
        } catch (Throwable $error) {
            $caught = $error;
        }
        echo $label, " exception=", $caught ? get_class($caught) : "none",
            ":", $caught ? $caught->getMessage() : "none", "\n";
        if ($caught instanceof ClickHouseException) {
            echo "fields=", $caught->getCode(), ":", $caught->getServerCode(), ":",
                json_encode($caught->getServerName()), ":",
                $caught->getQueryId() === $qid ? $label : "wrong query ID", "\n";
        }
        $logs = $c->getLogQueries();
        echo "logs=", count($logs), "\n";
        foreach ($logs as $log) {
            echo $log["query_id"] === $qid ? $label : "wrong query ID", ":",
                $log["error_code"], ":", $log["error_message"], "\n";
        }
        echo "recovery=", $c->ping() ? "ping" : "failed", ":",
            $c->select("SELECT 7 AS n", [], ClickHouse::FETCH_ONE), "\n";
        $c->getLogQueries();
    }
}
?>
--EXPECT--
totals-select exception=ClickHouseException:unimplemented 7
fields=0:0:null:totals-select
logs=1
totals-select:-1:unimplemented 7
recovery=ping:7
totals-execute exception=ClickHouseException:unimplemented 7
fields=0:0:null:totals-execute
logs=1
totals-execute:-1:unimplemented 7
recovery=ping:7
totals-selectStatement exception=ClickHouseException:unimplemented 7
fields=0:0:null:totals-selectStatement
logs=1
totals-selectStatement:-1:unimplemented 7
recovery=ping:7
totals-selectStream exception=ClickHouseException:unimplemented 7
fields=0:0:null:totals-selectStream
logs=1
totals-selectStream:-1:unimplemented 7
recovery=ping:7
totals-selectStreamCallback exception=ClickHouseException:unimplemented 7
fields=0:0:null:totals-selectStreamCallback
logs=1
totals-selectStreamCallback:-1:unimplemented 7
recovery=ping:7
totals-selectToStream exception=ClickHouseException:unimplemented 7
fields=0:0:null:totals-selectToStream
logs=1
totals-selectToStream:-1:unimplemented 7
recovery=ping:7
totals-selectWithExternalData exception=ClickHouseException:unimplemented 7
fields=0:0:null:totals-selectWithExternalData
logs=1
totals-selectWithExternalData:-1:unimplemented 7
recovery=ping:7
extremes-select exception=ClickHouseException:unimplemented 8
fields=0:0:null:extremes-select
logs=1
extremes-select:-1:unimplemented 8
recovery=ping:7
extremes-execute exception=ClickHouseException:unimplemented 8
fields=0:0:null:extremes-execute
logs=1
extremes-execute:-1:unimplemented 8
recovery=ping:7
extremes-selectStatement exception=ClickHouseException:unimplemented 8
fields=0:0:null:extremes-selectStatement
logs=1
extremes-selectStatement:-1:unimplemented 8
recovery=ping:7
extremes-selectStream exception=ClickHouseException:unimplemented 8
fields=0:0:null:extremes-selectStream
logs=1
extremes-selectStream:-1:unimplemented 8
recovery=ping:7
extremes-selectStreamCallback exception=ClickHouseException:unimplemented 8
fields=0:0:null:extremes-selectStreamCallback
logs=1
extremes-selectStreamCallback:-1:unimplemented 8
recovery=ping:7
extremes-selectToStream exception=ClickHouseException:unimplemented 8
fields=0:0:null:extremes-selectToStream
logs=1
extremes-selectToStream:-1:unimplemented 8
recovery=ping:7
extremes-selectWithExternalData exception=ClickHouseException:unimplemented 8
fields=0:0:null:extremes-selectWithExternalData
logs=1
extremes-selectWithExternalData:-1:unimplemented 8
recovery=ping:7
