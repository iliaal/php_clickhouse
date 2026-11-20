<?php
/* Child of tests/287, 288 and 289: hits memory_limit inside a ClickHouse call, then
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

/* Calls that allocate outside row conversion while native state (SQL
 * strings, the Query, settings, insert blocks) is live. Each runs once
 * without a limit, so the limited run reaches the same allocations. */
$native_table = "test.memory_limit_native";
$native_qid = "memory-limit-native-" . getmypid() . "-" . str_repeat("q", 24);
$native_params = ["a" => str_repeat("p", 100), "n" => 1234567];
$native_settings = ["max_threads" => 1];
$native_sql = "SELECT number AS x, {a:String} AS y, {n} AS z FROM numbers(3)";
$native_calls = [
    "execute" => function () use ($c, $native_params, $native_qid, $native_settings) {
        $c->execute("SELECT {a:String} AS y, {n} AS z", $native_params, $native_qid, $native_settings);
    },
    "verbose" => function () use ($c, $native_sql, $native_params, $native_qid, $native_settings) {
        $c->select($native_sql, $native_params, 0, $native_qid, $native_settings);
    },
    "statement_params" => function () use ($c, $native_sql, $native_params, $native_qid, $native_settings) {
        $c->selectStatement($native_sql, $native_params, $native_qid, $native_settings);
    },
    "stream" => function () use ($c, $native_sql, $native_params, $native_qid, $native_settings) {
        foreach ($c->selectStream($native_sql, $native_params, $native_qid, $native_settings) as $row) {
        }
    },
    "to_stream" => function () use ($c, $native_sql, $native_params, $native_qid, $native_settings) {
        $fp = fopen("php://memory", "w+");
        $c->selectToStream($native_sql, $native_params, $fp, "CSVWithNames", $native_qid, $native_settings);
        fclose($fp);
    },
    "callback" => function () use ($c, $native_sql, $native_params, $native_qid, $native_settings) {
        $c->selectStreamCallback($native_sql, function ($row) {}, $native_params, $native_qid, $native_settings);
    },
    "server_error" => function () use ($c, $native_qid) {
        try {
            $c->select("SELECT throwIf(number = 0, 'native state') AS x FROM numbers(1)", [], 0, $native_qid);
        } catch (ClickHouseException $e) {
        }
    },
    "insert" => function () use ($c, $native_table, $native_qid, $native_settings) {
        $c->insert($native_table, ["a", "s", "n", "m", "t"], [
            [1, 1234567, null, ["k" => 1], [1, 2345678]],
            [2, 1.5, 3456789, [], [2, "x"]],
        ], $native_qid, $native_settings);
    },
    "insert_assoc" => function () use ($c, $native_table, $native_qid) {
        $c->insertAssoc($native_table, [["a" => 1, "s" => 1234567, "n" => null, "m" => ["k" => 1], "t" => [1, 2345678]]], $native_qid);
    },
    "write" => function () use ($c, $native_table, $native_qid) {
        $c->writeStart($native_table, ["a", "s"], $native_qid);
        $c->write([[1, 1234567], [2, 2345678]]);
        $c->writeEnd();
    },
    "from_stream" => function () use ($c, $native_table, $native_qid, $native_settings) {
        $fp = fopen("php://memory", "w+");
        fwrite($fp, "1\t1234567\n2\t2345678\n");
        rewind($fp);
        $c->insertFromStream($native_table, ["a", "s"], $fp, "TSV", 1, $native_qid, $native_settings);
        fclose($fp);
    },
    "external" => function () use ($c, $native_params, $native_qid) {
        $c->selectWithExternalData("SELECT count() AS c, {a:String} AS y, {n} AS z FROM ext", [[
            "name" => "ext",
            "columns" => ["id" => "UInt64", "s" => "String"],
            "rows" => [[1, 1234567], [2, 2345678]],
        ]], $native_params, 0, $native_qid);
    },
    "construct" => function () {
        $config = clickhouse_test_config();
        $x = new ClickHouse($config + [
            "database" => "test",
            "endpoints" => [["host" => $config["host"], "port" => $config["port"]]],
            "connect_timeout_ms" => 5000,
            "tcp_keepalive" => true,
            "tcp_keepalive_idle" => 30,
        ]);
        unset($x);
        try {
            new ClickHouse($config + ["tcp_nodelay" => "sometimes"]);
        } catch (ClickHouseException $e) {
        }
    },
    "metadata" => function () use ($c) {
        $c->tableSize("memory_limit_native");
        $c->isExists("test", "memory_limit_native");
    },
    "geo" => function () use ($c) {
        $c->select("SELECT (1.5, 2.5)::Point AS pt, [(1.5, 2.5), (3.5, 4.5)]::Ring AS r,
                           [[(1.5, 2.5), (3.5, 4.5)]]::Polygon AS p,
                           [[[(1.5, 2.5), (3.5, 4.5)]]]::MultiPolygon AS mp");
    },
    "json" => function () use ($c) {
        $c->select("SELECT '{\"a\":[1,2,3],\"b\":\"text\"}'::JSON AS j",
                   [], ClickHouse::JSON_AS_ARRAY, "",
                   ["allow_experimental_json_type" => 1, "output_format_native_write_json_as_string" => 1]);
    },
];
if (isset($native_calls[$mode])) {
    register_shutdown_function(function () use ($c, $mode) {
        ini_set("memory_limit", "-1");
        try {
            if ($mode === "write") {
                try {
                    $c->writeEnd();
                } catch (ClickHouseException $e) {
                }
            }
            echo json_encode($c->select("SELECT 1 AS x")), "\n";
        } catch (ClickHouseException $e) {
            echo get_class($e), ": ", $e->getMessage(), "\n";
        }
    });
    if ($mode === "verbose") {
        $c->setVerbose(function ($event, $ctx) {});
    }
    $call = $native_calls[$mode];
    $call();
    $pad = str_repeat("p", 3 << 20);
    gc_collect_cycles();
    if (@ini_set("memory_limit", (string)(memory_get_usage() + $shift)) === false) {
        echo "limit below usage\n";
        exit;
    }
    $call();
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
