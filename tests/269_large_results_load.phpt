--TEST--
ClickHouse loads results larger than the pre-allocation step with and without compression
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
require __DIR__ . "/_clickhouse.inc";
clickhouse_skip_if_no_server();
?>
--INI--
memory_limit=512M
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

/* Every value or column body below exceeds the 1 MiB that column loading
 * allocates before data arrives, so each one has to grow while it is read;
 * the 20 MB string also exceeds eight times that, so it grows twice.
 * Compressed frames are read in steps starting at 4 MiB. */
$rows = 1048576;
$big = 20000003;
foreach ([false, "lz4", "zstd"] as $compression) {
    $config = clickhouse_test_config();
    $config["compression"] = $compression;
    $c = new ClickHouse($config);
    $label = $compression === false ? "none" : $compression;

    $s = $c->select("SELECT concat(repeat('0123456789abcdefghij', 1000000), 'xyz') AS s", [], ClickHouse::FETCH_ONE);
    echo "$label string: ", strlen($s) === $big && md5($s) === md5(str_repeat("0123456789abcdefghij", 1000000) . "xyz") ? "ok" : "wrong", "\n";

    $f = $c->select("SELECT toFixedString(repeat('yy', 1000000), 2000000) AS f", [], ClickHouse::FETCH_ONE);
    echo "$label fixedstring: ", $f === str_repeat("y", 2000000) ? "ok" : "wrong", "\n";

    $n = $c->select("SELECT number AS n FROM numbers($rows) SETTINGS max_block_size = $rows", [], ClickHouse::FETCH_COLUMN);
    echo "$label uint64: ", count($n) === $rows && $n[0] === 0 && $n[$rows - 1] === $rows - 1
        && array_sum($n) === $rows * ($rows - 1) / 2 ? "ok" : "wrong", "\n";
    unset($n);

    $a = $c->select("SELECT arrayMap(x -> toUInt64(x), range(300000)) AS a", [], ClickHouse::FETCH_ONE);
    echo "$label array: ", $a === range(0, 299999) ? "ok" : "wrong", "\n";
}
?>
--EXPECT--
none string: ok
none fixedstring: ok
none uint64: ok
none array: ok
lz4 string: ok
lz4 fixedstring: ok
lz4 uint64: ok
lz4 array: ok
zstd string: ok
zstd fixedstring: ok
zstd uint64: ok
zstd array: ok
