--TEST--
ClickHouse Phase C: ping_before_query, resetConnection, setProfileCallback, getServerInfo, getCurrentEndpoint
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// 1. ping_before_query construction key + getServerInfo
$cfg = clickhouse_test_config();
$cfg["ping_before_query"] = true;
$c = new ClickHouse($cfg);

$si = $c->getServerInfo();
echo "server.has_name=", isset($si["name"])         && is_string($si["name"])      ? 1 : 0, "\n";
echo "server.has_tz=",   isset($si["timezone"])     && is_string($si["timezone"])  ? 1 : 0, "\n";
echo "server.major_int=", is_int($si["version_major"]) ? 1 : 0, "\n";
echo "server.revision_pos=", $si["revision"] > 0 ? 1 : 0, "\n";

// 2. getCurrentEndpoint returns the active host/port (clickhouse-cpp
//    models a single host/port as a degenerate 1-item endpoints list).
$ep0 = $c->getCurrentEndpoint();
echo "single_ep_host_str=", is_string($ep0["host"]) ? 1 : 0, "\n";
echo "single_ep_port_pos=", $ep0["port"] > 0 ? 1 : 0, "\n";

// 3. resetConnection round-trips
echo "reset=", $c->resetConnection() ? 1 : 0, "\n";
echo "ping_after_reset=", $c->ping() ? 1 : 0, "\n";

// 4. setProfileCallback fires
$got = ["count" => 0, "rows_seen" => 0];
$c->setProfileCallback(function (array $p) use (&$got) {
    $got["count"]++;
    if (isset($p["rows"])) $got["rows_seen"] += $p["rows"];
});
$c->select("SELECT number FROM system.numbers LIMIT 100");
echo "profile_fired=", $got["count"] > 0 ? 1 : 0, "\n";

// 5. setProfileCallback(null) detaches cleanly
$c->setProfileCallback(null);
$got = ["count" => 0];
$c->select("SELECT 1");
echo "profile_after_clear=", $got["count"] === 0 ? 1 : 0, "\n";

// 6. getCurrentEndpoint returns the active endpoint when endpoints[] is set
$cfg2 = clickhouse_test_config();
$cfg2["endpoints"] = [
    ["host" => "127.0.0.1", "port" => 9000],
];
$c2 = new ClickHouse($cfg2);
$ep = $c2->getCurrentEndpoint();
echo "endpoint_host=", $ep["host"], "\n";
echo "endpoint_port=", $ep["port"], "\n";
?>
--EXPECT--
server.has_name=1
server.has_tz=1
server.major_int=1
server.revision_pos=1
single_ep_host_str=1
single_ep_port_pos=1
reset=1
ping_after_reset=1
profile_fired=1
profile_after_clear=1
endpoint_host=127.0.0.1
endpoint_port=9000
