--TEST--
ClickHouse non-Nullable string and UUID insert paths reject null and malformed UUIDs
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

function probe(string $label, callable $fn): void {
    try {
        $fn();
        echo "$label: NO THROW\n";
    } catch (Throwable $e) {
        echo "$label: REJECTED\n";
    }
}

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("SET allow_suspicious_low_cardinality_types = 1");
$c->execute("DROP TABLE IF EXISTS test.strict_text_uuid");
$c->execute("CREATE TABLE test.strict_text_uuid (
    s String,
    fs FixedString(4),
    lc LowCardinality(String),
    lcf LowCardinality(FixedString(4)),
    u UUID,
    ms Map(String, String),
    muk Map(UUID, String),
    mvu Map(String, UUID),
    ns Nullable(String),
    nu Nullable(UUID)
) ENGINE=Memory");

$bad_uuid = "1" . str_repeat("z", 15) . str_repeat("0", 16);

probe("String null", fn() =>
    $c->insert("test.strict_text_uuid", ["s"], [[null]]));
probe("FixedString null", fn() =>
    $c->insert("test.strict_text_uuid", ["fs"], [[null]]));
probe("LowCardinality String null", fn() =>
    $c->insert("test.strict_text_uuid", ["lc"], [[null]]));
probe("LowCardinality FixedString null", fn() =>
    $c->insert("test.strict_text_uuid", ["lcf"], [[null]]));
probe("Map String value null", fn() =>
    $c->insert("test.strict_text_uuid", ["ms"], [[[ "k" => null ]]]));
probe("UUID null", fn() =>
    $c->insert("test.strict_text_uuid", ["u"], [[null]]));
probe("UUID partial hex", fn() =>
    $c->insert("test.strict_text_uuid", ["u"], [[$bad_uuid]]));
probe("Map UUID key partial hex", fn() =>
    $c->insert("test.strict_text_uuid", ["muk"], [[[$bad_uuid => "x"]]]));
probe("Map UUID value partial hex", fn() =>
    $c->insert("test.strict_text_uuid", ["mvu"], [[["k" => $bad_uuid]]]));

$c->insert("test.strict_text_uuid", ["ns", "nu"], [[null, null]]);
$row = $c->select(
    "SELECT isNull(ns) AS ns_null, isNull(nu) AS nu_null FROM test.strict_text_uuid"
)[0];
echo "nullable nulls: ", json_encode($row), "\n";

$c->execute("DROP TABLE test.strict_text_uuid");
?>
--EXPECT--
String null: REJECTED
FixedString null: REJECTED
LowCardinality String null: REJECTED
LowCardinality FixedString null: REJECTED
Map String value null: REJECTED
UUID null: REJECTED
UUID partial hex: REJECTED
Map UUID key partial hex: REJECTED
Map UUID value partial hex: REJECTED
nullable nulls: {"ns_null":1,"nu_null":1}
