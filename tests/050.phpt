--TEST--
ClickHouse IPv4 / IPv6 read paths return canonical string form (regression)
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());

$r = $c->select("SELECT toIPv4('1.2.3.4') AS v4, toIPv6('::1') AS v6");
echo "v4=", $r[0]["v4"], "\n";
echo "v6=", $r[0]["v6"], "\n";

$r = $c->select("SELECT toIPv6('2001:db8::1') AS v6");
echo "v6_long=", $r[0]["v6"], "\n";
?>
--EXPECT--
v4=1.2.3.4
v6=::1
v6_long=2001:db8::1
