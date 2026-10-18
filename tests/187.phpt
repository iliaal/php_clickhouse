--TEST--
ClickHouse Time and Time64 string rendering handles minimum signed values
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
require __DIR__ . "/_clickhouse.inc";
clickhouse_skip_if_no_server();
$c = new ClickHouse(clickhouse_test_config());
try {
    $c->select("SELECT CAST(0 AS Time64(0)) AS x");
} catch (ClickHouseException $e) {
    print "skip server has no Time64 support";
}
?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$mode = ClickHouse::FETCH_ONE | ClickHouse::DATE_AS_STRINGS;

/* Time is int32. ClickHouse before 26.9 stores a CAST of INT32_MIN raw,
 * 26.9 and later clamp it to the Time minimum, so key the CAST leg off the
 * raw value the server delivers. */
$time_expected = array(
    /* INT32_MIN: negating it as a signed value is undefined behaviour. */
    "-2147483648" => "-596523:14:08",
    /* Server-clamped to the Time minimum. */
    "-3599999"    => "-999:59:59",
);
$raw = $c->select(
    "SELECT toInt32(CAST(-2147483648 AS Time)) AS x",
    [],
    ClickHouse::FETCH_ONE
);
$rendered = $c->select("SELECT CAST(-2147483648 AS Time) AS x", [], $mode);
$key = (string) $raw;
if (!isset($time_expected[$key])) {
    echo "unexpected raw Time value from server: $key -> $rendered\n";
} else {
    echo $rendered === $time_expected[$key] ? "ok\n" : "MISMATCH $key -> $rendered\n";
}

/* A native-protocol insert is stored without the CAST clamp, so this leg
 * reaches the INT32_MIN rendering path on every server version. */
$c->execute("DROP TABLE IF EXISTS t_owv_time");
$c->execute("CREATE TABLE t_owv_time (t Time) ENGINE = Memory");
$c->insert("t_owv_time", ["t"], [[-2147483648]]);
$raw = $c->select("SELECT toInt32(t) FROM t_owv_time", [], ClickHouse::FETCH_ONE);
$rendered = $c->select("SELECT t FROM t_owv_time", [], $mode);
echo $raw, " -> ", $rendered, "\n";
$c->execute("DROP TABLE t_owv_time");

/* Time64 is int64, and what reaches the client depends on the server:
 * ClickHouse before 26.7 stores the raw INT64_MIN, 26.7 and later clamp
 * the CAST. Key the expectation off the raw value actually delivered, so
 * the test pins the client-side rendering either way instead of pinning
 * one server version's clamping behaviour. */
$raw = $c->select(
    "SELECT toInt64(CAST(-9223372036854775808 AS Time64(0))) AS x",
    [],
    ClickHouse::FETCH_ONE
);
$rendered = $c->select(
    "SELECT CAST(-9223372036854775808 AS Time64(0)) AS x",
    [],
    $mode
);

$expected = array(
    /* INT64_MIN: negating it as a signed value is undefined behaviour. */
    "-9223372036854775808" => "-2562047788015215:30:08",
    /* Server-clamped to the Time64 minimum, in whole seconds. */
    "-3599999"             => "-999:59:59",
);

$key = (string) $raw;
if (!isset($expected[$key])) {
    echo "unexpected raw Time64 value from server: $key -> $rendered\n";
} else {
    echo $rendered === $expected[$key] ? "ok\n" : "MISMATCH $key -> $rendered\n";
}
?>
--EXPECT--
ok
-2147483648 -> -596523:14:08
ok
