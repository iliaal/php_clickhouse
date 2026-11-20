<?php
/* Child of tests/287 and 288: hits memory_limit inside a ClickHouse call, then
 * queries again from a shutdown function. argv[1] shifts where the limit is
 * reached; argv[2] picks the call. */
require __DIR__ . "/_clickhouse.inc";
$c = new ClickHouse(clickhouse_test_config());
$shift = (int)$argv[1];
$mode = $argv[2];
$map = "map(concat(repeat('k', 64), toString(number)), number,
            concat(repeat('j', 64), toString(number)), number)";
$sql = "SELECT $map AS m FROM numbers(1000000) SETTINGS max_block_size = 65536";

if ($mode === "prepare") {
    /* Raise the limit in the shutdown function: the query that ran out of
     * memory keeps nothing large to release. */
    register_shutdown_function(function () use ($c) {
        ini_set("memory_limit", "-1");
        try {
            echo json_encode($c->select("SELECT 1 AS x")), "\n";
        } catch (ClickHouseException $e) {
            echo get_class($e), ": ", $e->getMessage(), "\n";
        }
    });
    $params = ["a" => str_repeat("p", 100)];
    $c->select("SELECT 1 AS x, {a:String} AS y", $params);
    /* PHP 8.0 raises a memory_limit below 2 MiB to 2 MiB. */
    $pad = str_repeat("p", 3 << 20);
    gc_collect_cycles();
    /* $shift bytes of headroom run out before, during, or after the
     * allocations select() makes outside of row conversion. */
    if (@ini_set("memory_limit", (string)(memory_get_usage() + $shift)) === false) {
        echo "limit below usage\n";
        exit;
    }
    $c->select("SELECT 1 AS x, {a:String} AS y", $params);
    echo "completed\n";
    exit;
}

if ($mode === "fiber") {
    register_shutdown_function(function () use ($c) {
        ini_set("memory_limit", "-1");
        try {
            echo json_encode($c->select("SELECT 1 AS x")), "\n";
        } catch (ClickHouseException $e) {
            echo get_class($e), ": ", $e->getMessage(), "\n";
        }
    });
    /* The verbose callback makes select_finish allocate after the row
     * callback, outside row conversion. */
    $c->setVerbose(function ($event, $ctx) {});
    /* The Fiber suspends a query on another client inside its row callback,
     * so that query's guard is armed before this client's and released
     * while this client's query still runs. */
    $other = new ClickHouse(clickhouse_test_config());
    $fiber = new Fiber(function () use ($other) {
        $other->selectStreamCallback("SELECT 1 AS x", function () {
            Fiber::suspend();
        });
    });
    $fiber->start();
    $c->selectStreamCallback("SELECT 1 AS x", function () use ($fiber, $shift) {
        $fiber->resume();
        gc_collect_cycles();
        if (@ini_set("memory_limit", (string)(memory_get_usage() + $shift)) === false) {
            echo "limit below usage\n";
        }
    });
    echo "completed\n";
    exit;
}

if ($mode === "iterator") {
    $source = new ClickHouse(clickhouse_test_config());
    $rows = $source->selectStream("SELECT map(concat(repeat('k', 64), toString(number)), number) AS m,
                                          toNullable(toString(number)) AS s
                                   FROM numbers(30000)");
}
$ballast = str_repeat("x", (1 << 20) + $shift);
register_shutdown_function(function () use ($c, &$ballast) {
    $ballast = null;
    try {
        echo json_encode($c->select("SELECT 1 AS x")), "\n";
    } catch (ClickHouseException $e) {
        echo get_class($e), ": ", $e->getMessage(), "\n";
    }
});
switch ($mode) {
    case "select":
        $result = $c->select($sql);
        break;
    case "statement":
        $result = $c->selectStatement($sql);
        break;
    case "iterator":
        /* The row iterator converts rows while a ClickHouse callback runs. */
        $kept = [];
        $c->selectStreamCallback("SELECT 1 AS x", function () use ($rows, &$kept) {
            foreach ($rows as $row) {
                $kept[] = $row;
            }
            echo "iteration finished\n";
        });
        break;
}
echo "unreachable\n";
