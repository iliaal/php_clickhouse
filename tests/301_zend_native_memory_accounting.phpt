--TEST--
Native result buffers use Zend memory accounting and outlive their connection
--EXTENSIONS--
clickhouse
--INI--
memory_limit=128M
--SKIPIF--
<?php
if (getenv("USE_ZEND_ALLOC") === "0") die("skip Zend allocator disabled");
require __DIR__ . "/_clickhouse.inc";
clickhouse_skip_if_no_server();
?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";
foreach (array("none", "lz4", "zstd") as $codec) {
    $config = clickhouse_test_config();
    $config["compression"] = $codec === "none" ? false : $codec;
    $c = new ClickHouse($config);
    if ($codec !== "none") $c->setSetting("network_compression_method", $codec);
    $c->select("SELECT 1");
    $before = memory_get_usage();
    $iterator = $c->selectStream("SELECT CAST(repeat('x', 256), 'FixedString(256)') AS payload FROM numbers(32768)");
    $retained = memory_get_usage();
    echo $codec, " counted=", $retained - $before >= 8388608 ? "yes" : "no", "\n";
    unset($c);
    $iterator->rewind();
    $row = $iterator->current();
    echo $codec, " data=", count($iterator) === 32768 && $row["payload"] === str_repeat("x", 256) ? "yes" : "no", "\n";
    unset($row, $iterator);
    gc_collect_cycles();
    echo $codec, " released=", memory_get_usage() < $before + 1048576 ? "yes" : "no", "\n";
}
?>
--EXPECT--
none counted=yes
none data=yes
none released=yes
lz4 counted=yes
lz4 data=yes
lz4 released=yes
zstd counted=yes
zstd data=yes
zstd released=yes
