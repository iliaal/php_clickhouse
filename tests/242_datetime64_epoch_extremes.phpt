--TEST--
DATE_AS_STRINGS formats DateTime64 values at the Int64 limits without overflow
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
if (!function_exists("proc_open")) print "skip proc_open unavailable";
if (PHP_INT_SIZE < 8) print "skip needs 64-bit integers";
?>
--FILE--
<?php
require __DIR__ . "/_wire_mock.inc";

$values = [PHP_INT_MIN, PHP_INT_MAX, 0, -1, -86400, -86401];
$body = "";
foreach ($values as $v) $body .= pack("P", $v);

foreach (["DateTime64(0)"] as $type) {
    $hex = bin2hex(wire_select_response([["c0", $type, $body]], count($values)));
    $r = wire_run(wire_default_hello_hex(), $hex, wire_select_op(ClickHouse::DATE_AS_STRINGS));
    echo "$type select: ", $r[0], "\n";
    if ($r[0] === "ok") {
        foreach ($r[1] as $row) echo "  ", $row["c0"], "\n";
    } else {
        echo "  ", json_encode($r), "\n";
    }
    $r = wire_run(wire_default_hello_hex(), $hex, function ($c) {
        $fp = fopen("php://memory", "w+");
        $c->selectToStream("SELECT * FROM t", [], $fp, "TabSeparated");
        rewind($fp);
        return stream_get_contents($fp);
    });
    echo "$type stream: ", $r[0], " ", json_encode($r[1]), "\n";
}

/* The raw Int64 stays exact when no string formatting is requested. */
$hex = bin2hex(wire_select_response([["c0", "DateTime64(0)", $body]], count($values)));
$r = wire_run(wire_default_hello_hex(), $hex, wire_select_op(0));
echo "raw: ", $r[0], " ", json_encode(array_column($r[1], "c0")), "\n";
?>
--EXPECT--
DateTime64(0) select: ok
  -292277022657-01-27 08:29:52
  292277026596-12-04 15:30:07
  1970-01-01 00:00:00
  1969-12-31 23:59:59
  1969-12-31 00:00:00
  1969-12-30 23:59:59
DateTime64(0) stream: ok "-292277022657-01-27 08:29:52\n292277026596-12-04 15:30:07\n1970-01-01 00:00:00\n1969-12-31 23:59:59\n1969-12-31 00:00:00\n1969-12-30 23:59:59\n"
raw: ok [-9223372036854775808,9223372036854775807,0,-1,-86400,-86401]
