--TEST--
ClickHouse compression="zstd" engages ZSTD instead of silently downgrading to LZ4
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// Regression for CR-005: the `$compression` stub was declared `bool`,
// so the long write of 2 (zstd) was coerced to true on the way into
// the property and read back as 1, dispatching to LZ4 in __construct's
// `cv == 2` branch. ZSTD never actually engaged, even though the
// public surface accepted "zstd" as a config value. The stub is now
// `int`; the value round-trips verbatim.

$cfg = clickhouse_test_config();

$cases = [
    ["'none'", "none", 0],
    ["'lz4'",  "lz4",  1],
    ["'zstd'", "zstd", 2],
    ["true",   true,   1],
    ["false",  false,  0],
];

foreach ($cases as [$label, $input, $expected]) {
    $c = new ClickHouse(array_merge($cfg, ["compression" => $input]));
    $r = new ReflectionObject($c);
    $p = $r->getProperty("compression");
    // PHP 7.4 enforces visibility on Reflection getValue() against
    // protected properties; PHP 8.1+ accessors bypass the check
    // implicitly and PHP 8.5 deprecated the method outright.
    if (PHP_VERSION_ID < 80100) {
        $p->setAccessible(true);
    }
    $got = $p->getValue($c);
    echo "compression=$label expected=$expected got=$got\n";
}

// Round-trip a query under each compression mode to confirm the
// dispatch actually engages without crashing.
foreach (["none", "lz4", "zstd"] as $mode) {
    $c = new ClickHouse($cfg + ["compression" => $mode]);
    $r = $c->select("SELECT 42 AS x", [], ClickHouse::FETCH_ONE);
    echo "$mode round-trip: $r\n";
}
?>
--EXPECT--
compression='none' expected=0 got=0
compression='lz4' expected=1 got=1
compression='zstd' expected=2 got=2
compression=true expected=1 got=1
compression=false expected=0 got=0
none round-trip: 42
lz4 round-trip: 42
zstd round-trip: 42
