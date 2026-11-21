--TEST--
selectToStream() spells NAN, INF, signed zero, and floats as the string conversion does, without a warning
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";
/* PHP 8.5 warns when a NAN double is converted to a string. */
set_error_handler(function ($errno, $message) {
    throw new ErrorException($message, 0, $errno);
});
$c = new ClickHouse(clickhouse_test_config());
foreach (["TSV", "CSV"] as $format) {
    $fp = fopen("php://memory", "w+");
    $c->selectToStream("SELECT nan AS a, inf AS b, -inf AS c, -0. AS d, 1.5 AS e, 1e25 AS f,
                               toFloat32(0.1) AS g, toNullable(nan) AS h, toFloat32(-inf) AS i,
                               0.1 + 0.2 AS j, -1e-300 AS k", [], $fp, $format);
    rewind($fp);
    echo $format, ": ", stream_get_contents($fp);
}
?>
--EXPECT--
TSV: NAN	INF	-INF	-0	1.5	1.0E+25	0.10000000149012	NAN	-INF	0.3	-1.0E-300
CSV: NAN,INF,-INF,-0,1.5,1.0E+25,0.10000000149012,NAN,-INF,0.3,-1.0E-300
