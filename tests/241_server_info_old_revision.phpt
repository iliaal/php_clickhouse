--TEST--
getServerInfo() reports version_patch 0 when the server revision predates it
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
if (!function_exists("proc_open")) print "skip proc_open unavailable";
?>
--FILE--
<?php
require __DIR__ . "/_wire_mock.inc";

$info = function ($c) {
    $i = $c->getServerInfo();
    return [$i["revision"], $i["version_major"], $i["version_minor"], $i["version_patch"]];
};
/* Revision 732 Hello: no timezone, display name, or patch fields. The final
 * varint byte travels as the response so the hello spans two writes. */
$r = wire_run("000a436c69636b486f7573651a09dc", "05", $info);
echo "old: ", $r[0], " ", json_encode($r[1]), "\n";

$r = wire_run(wire_default_hello_hex(), "", $info);
echo "current: ", $r[0], " ", json_encode($r[1]), "\n";
?>
--EXPECT--
old: ok [732,26,9,0]
current: ok [54492,26,9,13]
