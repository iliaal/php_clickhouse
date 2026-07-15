--TEST--
Typed Array parameters enforce the documented nesting limit before native amplification
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

function nestedParam($depth) {
    $type = "UInt8";
    $value = 1;
    for ($i = 0; $i < $depth; ++$i) {
        $type = "Array(" . $type . ")";
        $value = [$value];
    }
    return [$type, $value];
}

$c = new ClickHouse(clickhouse_test_config());
list($type32, $value32) = nestedParam(32);
$row = $c->select("SELECT length({p:" . $type32 . "}) AS n", ["p" => $value32])[0];
echo "depth32=", $row["n"], "\n";

list($type33, $value33) = nestedParam(33);
try {
    $c->select("SELECT length({p:" . $type33 . "})", ["p" => $value33]);
    echo "depth33=accepted\n";
} catch (ClickHouseException $e) {
    echo "depth33=", $e->getMessage(), "\n";
}

$cycle = [];
$cycle[] = &$cycle;
try {
    $c->select("SELECT length({p:" . $type33 . "})", ["p" => $cycle]);
    echo "cycle=accepted\n";
} catch (ClickHouseException $e) {
    echo "cycle=", $e->getMessage(), "\n";
}
?>
--EXPECT--
depth32=1
depth33=Array typed parameter nesting depth exceeds limit of 32
cycle=Array typed parameter nesting depth exceeds limit of 32
