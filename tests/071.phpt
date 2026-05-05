--TEST--
ClickHouse hex-literal UInt insert rejects strings with embedded NUL bytes
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// Regression for CR-508: appendUIntColumnWithHex's full-consumption
// check used `*endp != '\0'`, so a PHP zend_string with embedded NUL
// followed by junk ("0xABCD\0garbage") slipped through — strtoul stops
// at the NUL, endp points to the NUL, the *endp == '\0' check passed
// and the trailing garbage was silently dropped. Same NUL-byte trap
// CR-306 fixed for Map keys: compare consumed length against
// ZSTR_LEN, not against the C-string terminator.

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.hex_nul_t");
$c->execute("CREATE TABLE test.hex_nul_t (u32 UInt32, u64 UInt64) ENGINE = Memory");

$probes = [
    "U32 hex with embedded NUL"  => [['u32'], [["0xABCD\0garbage"]]],
    "U64 hex with embedded NUL"  => [['u64'], [["0xDEADBEEF\0junk"]]],
    "U32 hex with trailing junk" => [['u32'], [["0xABCDxyz"]]],
];
foreach ($probes as $label => [$cols, $vals]) {
    try { $c->insert("test.hex_nul_t", $cols, $vals); echo "$label: NO THROW\n"; }
    catch (ClickHouseException $e) { echo "$label: REJECTED\n"; }
}

// Sanity: well-formed hex literals still land.
$c->insert("test.hex_nul_t", ['u32', 'u64'], [["0xABCD", "0xDEADBEEFCAFEBABE"]]);
$rows = $c->select("SELECT u32, toString(u64) AS u64s FROM test.hex_nul_t");
echo "ok rowcount: ", count($rows), "\n";
echo "u32: ", $rows[0]["u32"], "\n";
echo "u64: ", $rows[0]["u64s"], "\n";

$c->execute("DROP TABLE test.hex_nul_t");
?>
--EXPECT--
U32 hex with embedded NUL: REJECTED
U64 hex with embedded NUL: REJECTED
U32 hex with trailing junk: REJECTED
ok rowcount: 1
u32: 43981
u64: 16045690984503098046
