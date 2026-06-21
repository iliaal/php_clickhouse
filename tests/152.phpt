--TEST--
ClickHouse UUID_WITH_DASHES flag and Time/Decimal/FixedString insert range checks
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

/* Asserts a client-side rejection by checking for $needle in the message,
 * so a regression to an opaque server-side error (which carries a
 * different message) flips the test. */
function probe_client(string $label, callable $fn, string $needle): void {
    try {
        $fn();
        echo "$label: NO THROW\n";
    } catch (Throwable $e) {
        echo "$label: ", str_contains($e->getMessage(), $needle) ? "REJECTED (client)" : "REJECTED (server)", "\n";
    }
}

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");

/* UUID read formatting: raw hex by default, dashed canonical with the flag,
 * for both standalone columns and Map keys/values. */
$c->execute("DROP TABLE IF EXISTS test.uuid_fmt");
$c->execute("CREATE TABLE test.uuid_fmt (
    u UUID,
    mk Map(UUID, String),
    mv Map(String, UUID)
) ENGINE=Memory");
$uuid = "31249a1b-7b05-4270-9f37-c609b48a9bb2";
$c->insert("test.uuid_fmt", ["u", "mk", "mv"],
    [[$uuid, [$uuid => "x"], ["k" => $uuid]]]);

$sql = "SELECT u, mk, mv FROM test.uuid_fmt";
$plain = $c->select($sql)[0];
$dashed = $c->select($sql, [], ClickHouse::UUID_WITH_DASHES)[0];

echo "standalone plain:  ", $plain["u"], "\n";
echo "standalone dashed: ", $dashed["u"], "\n";
echo "map key plain:     ", array_key_first($plain["mk"]), "\n";
echo "map key dashed:    ", array_key_first($dashed["mk"]), "\n";
echo "map val plain:     ", $plain["mv"]["k"], "\n";
echo "map val dashed:    ", $dashed["mv"]["k"], "\n";
$c->execute("DROP TABLE test.uuid_fmt");

/* Insert range / type checks. */
$c->execute("DROP TABLE IF EXISTS test.ranges");
$c->execute("CREATE TABLE test.ranges (
    t Time,
    d Decimal(18, 4),
    fs FixedString(4)
) ENGINE=Memory");

probe("Time in range", fn() =>
    $c->insert("test.ranges", ["t"], [[3600]]));
probe("Time over int32", fn() =>
    $c->insert("test.ranges", ["t"], [[2147483648]]));
probe("Time under int32", fn() =>
    $c->insert("test.ranges", ["t"], [[-2147483649]]));
probe("Decimal scalar", fn() =>
    $c->insert("test.ranges", ["d"], [["12.3456"]]));
probe_client("Decimal array", fn() =>
    $c->insert("test.ranges", ["d"], [[[1, 2]]]), "scalar value");
probe("FixedString fits", fn() =>
    $c->insert("test.ranges", ["fs"], [["abcd"]]));
probe_client("FixedString too long", fn() =>
    $c->insert("test.ranges", ["fs"], [["abcde"]]), "exceeds the declared column width");

$c->execute("DROP TABLE test.ranges");
?>
--EXPECT--
standalone plain:  31249a1b7b0542709f37c609b48a9bb2
standalone dashed: 31249a1b-7b05-4270-9f37-c609b48a9bb2
map key plain:     31249a1b7b0542709f37c609b48a9bb2
map key dashed:    31249a1b-7b05-4270-9f37-c609b48a9bb2
map val plain:     31249a1b7b0542709f37c609b48a9bb2
map val dashed:    31249a1b-7b05-4270-9f37-c609b48a9bb2
Time in range: NO THROW
Time over int32: REJECTED
Time under int32: REJECTED
Decimal scalar: NO THROW
Decimal array: REJECTED (client)
FixedString fits: NO THROW
FixedString too long: REJECTED (client)
