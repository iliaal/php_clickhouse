--TEST--
FixedString reads reject rows missing from a width x rows overflowed column
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
if (!function_exists("proc_open")) print "skip proc_open unavailable";
?>
--FILE--
<?php
require __DIR__ . "/_wire_mock.inc";

/* FixedString(2^62 + 1) x 4 rows wraps to a 4-byte buffer inside a Tuple or
 * Nullable whose own row count stays 4. */
$wrapped = [
    "tuple" => "0100010002ffffffff0001000263302e5475706c652855496e74382c204669786564537472696e6728343631313638363031383432373338373930352929000100010002ffffffff0001040263302e5475706c652855496e74382c204669786564537472696e672834363131363836303138343237333837393035292900010203044142434405",
    "nullable" => "0100010002ffffffff0001000263302a4e756c6c61626c65284669786564537472696e6728343631313638363031383432373338373930352929000100010002ffffffff0001040263302a4e756c6c61626c65284669786564537472696e672834363131363836303138343237333837393035292900000000004142434405",
];
$ops = [
    "select" => wire_select_op(0),
    "select-binary" => wire_select_op(ClickHouse::FIXEDSTRING_BINARY),
    "stream" => function ($c) {
        $rows = [];
        foreach ($c->selectStream("SELECT * FROM t") as $row) $rows[] = $row;
        return $rows;
    },
    "statement" => function ($c) {
        return $c->selectStatement("SELECT * FROM t")->toArray();
    },
];
foreach ($wrapped as $name => $hex) {
    foreach ($ops as $opName => $op) {
        $r = wire_run(wire_default_hello_hex(), $hex, $op);
        echo "$name/$opName: ", $r[0] === "exception" ? "rejected" : json_encode($r), "\n";
    }
}

$tuple = bin2hex(wire_select_response([
    ["c0", "Tuple(UInt8, FixedString(4))", "\x01\x02" . "ab\0\0cdef"],
], 2));
$nullable = bin2hex(wire_select_response([
    ["c0", "Nullable(FixedString(4))", "\x01\x00" . "\0\0\0\0wxyz"],
], 2));
foreach ([0, ClickHouse::FIXEDSTRING_BINARY] as $flags) {
    $r = wire_run(wire_default_hello_hex(), $tuple, wire_select_op($flags));
    echo "valid-tuple/$flags: ", $r[0], " ", json_encode($r[1]), "\n";
    $r = wire_run(wire_default_hello_hex(), $nullable, wire_select_op($flags));
    echo "valid-nullable/$flags: ", $r[0], " ", json_encode($r[1]), "\n";
}
?>
--EXPECT--
tuple/select: rejected
tuple/select-binary: rejected
tuple/stream: rejected
tuple/statement: rejected
nullable/select: rejected
nullable/select-binary: rejected
nullable/stream: rejected
nullable/statement: rejected
valid-tuple/0: ok [{"c0":[1,"ab"]},{"c0":[2,"cdef"]}]
valid-nullable/0: ok [{"c0":null},{"c0":"wxyz"}]
valid-tuple/128: ok [{"c0":[1,"ab\u0000\u0000"]},{"c0":[2,"cdef"]}]
valid-nullable/128: ok [{"c0":null},{"c0":"wxyz"}]
