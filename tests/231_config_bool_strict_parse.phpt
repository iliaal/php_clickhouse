--TEST--
Socket boolean options accept only strict boolean spellings
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$base = clickhouse_test_config();
$keys = ["tcp_nodelay", "tcp_keepalive", "ping_before_query"];

foreach ($keys as $key) {
    $rejected = 0;
    $bads = ["no", "off", "yes", "on", "", "2", 2, -1, 0.0, [], [1], new stdClass()];
    foreach ($bads as $bad) {
        $cfg = $base;
        $cfg[$key] = $bad;
        try {
            new ClickHouse($cfg);
            echo "$key=", json_encode($bad), ": NO EXCEPTION\n";
        } catch (ClickHouseException $e) {
            if (strpos($e->getMessage(), "$key must be a boolean") !== false) {
                $rejected++;
            } else {
                echo "$key=", json_encode($bad), ": unexpected: ", $e->getMessage(), "\n";
            }
        }
    }
    echo "$key: $rejected/", count($bads), " rejected\n";
}

foreach ([["false", "TRUE", "1"], ["0", "true", 0], [false, 1, true], [null, null, null]] as $vals) {
    $cfg = $base;
    foreach ($keys as $i => $key) {
        $cfg[$key] = $vals[$i];
    }
    $c = new ClickHouse($cfg);
    echo json_encode($vals), ": ", $c->select("SELECT 1 AS one", [], ClickHouse::FETCH_ONE), "\n";
}
?>
--EXPECT--
tcp_nodelay: 12/12 rejected
tcp_keepalive: 12/12 rejected
ping_before_query: 12/12 rejected
["false","TRUE","1"]: 1
["0","true",0]: 1
[false,1,true]: 1
[null,null,null]: 1
