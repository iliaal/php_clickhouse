--TEST--
ClickHouse Float32 reads preserve every significant source bit
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$row = $c->select("SELECT toFloat32(16777215) AS v")[0];
printf("%.17g\n", $row["v"]);
?>
--EXPECT--
16777215
