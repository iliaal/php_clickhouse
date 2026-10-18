--TEST--
IPv4 and IPv6 inserts reject text with an embedded NUL byte
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.t_85s_ip");
$c->execute("CREATE TABLE test.t_85s_ip (n UInt8, a IPv4, b IPv6, na Nullable(IPv4), arr Array(IPv6)) ENGINE=Memory");

$bad = [
    "ipv4 tail" => [1, "1.2.3.4\0junk", "::1", null, []],
    "ipv4 lead" => [2, "\x001.2.3.4", "::1", null, []],
    "ipv6 tail" => [3, "1.2.3.4", "::1\0junk", null, []],
    "nullable ipv4" => [4, "1.2.3.4", "::1", "10.0.0.1\0", []],
    "array ipv6" => [5, "1.2.3.4", "::1", null, ["::2", "fe80::1\0x"]],
];
foreach ($bad as $label => $row) {
    try {
        $c->insert("test.t_85s_ip", ["n", "a", "b", "na", "arr"], [$row]);
        echo "$label: accepted\n";
    } catch (ClickHouseException $e) {
        echo "$label: ", strpos($e->getMessage(), "embedded NUL") !== false ? "rejected" : $e->getMessage(), "\n";
    }
}

$c->insert("test.t_85s_ip", ["n", "a", "b", "na", "arr"], [
    [10, "1.2.3.4", "::1", "10.0.0.1", ["::2", "fe80::1"]],
    [11, 16909060, "::ffff:1.2.3.4", null, []],
    [12, "255.255.255.255", "2001:db8::ff00:42:8329", "0.0.0.0", ["::"]],
]);
foreach ($c->select("SELECT n, toString(a) AS a, toString(b) AS b, toString(na) AS na, arrayMap(x -> toString(x), arr) AS arr FROM test.t_85s_ip ORDER BY n") as $row) {
    echo json_encode($row), "\n";
}

try {
    $c->selectWithExternalData("SELECT count() FROM e", [
        ["name" => "e", "columns" => ["ip" => "IPv4"], "rows" => [["1.2.3.4\0junk"]]],
    ]);
    echo "external: accepted\n";
} catch (ClickHouseException $e) {
    echo "external: ", strpos($e->getMessage(), "embedded NUL") !== false ? "rejected" : $e->getMessage(), "\n";
}
$c->execute("DROP TABLE test.t_85s_ip");
?>
--EXPECT--
ipv4 tail: rejected
ipv4 lead: rejected
ipv6 tail: rejected
nullable ipv4: rejected
array ipv6: rejected
{"n":10,"a":"1.2.3.4","b":"::1","na":"10.0.0.1","arr":["::2","fe80::1"]}
{"n":11,"a":"1.2.3.4","b":"::ffff:1.2.3.4","na":null,"arr":[]}
{"n":12,"a":"255.255.255.255","b":"2001:db8::ff00:42:8329","na":"0.0.0.0","arr":["::"]}
external: rejected
