--TEST--
Malformed endpoints config throws instead of silently falling back to localhost
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$base = clickhouse_test_config();
unset($base["host"], $base["port"]);

/* A one-letter key typo used to skip the entry and silently connect to
 * 127.0.0.1:9000. Every malformed shape must now surface an exception,
 * matching how the other config keys validate. */
$cases = [
    "typo key"        => ["endpoints" => [["hosts" => "ch1.invalid", "port" => 9000]]],
    "non-array entry" => ["endpoints" => ["ch1.invalid:9000"]],
    "non-array value" => ["endpoints" => "ch1.invalid"],
    "null host"       => ["endpoints" => [["host" => null, "port" => 9000]]],
];
foreach ($cases as $label => $extra) {
    try {
        new ClickHouse($base + $extra);
        echo "$label: no throw\n";
    } catch (ClickHouseException $e) {
        echo "$label: threw\n";
    }
}

/* A well-formed endpoints list still connects and reports the endpoint. */
$ok = new ClickHouse($base + [
    "endpoints" => [["host" => (getenv("CLICKHOUSE_HOST") ?: "127.0.0.1"), "port" => (int)(getenv("CLICKHOUSE_PORT") ?: 9000)]],
]);
echo "well-formed: ", ($ok->ping() ? "ok" : "fail"), "\n";
?>
--EXPECT--
typo key: threw
non-array entry: threw
non-array value: threw
null host: threw
well-formed: ok
