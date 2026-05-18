--TEST--
ClickHouse placeholder conversion preserves __toString exceptions and nested Array params
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

class ThrowingString {
    public function __toString(): string {
        throw new RuntimeException("string conversion failed");
    }
}

$c = new ClickHouse(clickhouse_test_config());

try {
    $c->select("SELECT 1 FROM {tbl}", ["tbl" => new ThrowingString()]);
    echo "scalar placeholder: NO THROW\n";
} catch (Throwable $e) {
    echo "scalar placeholder: ", get_class($e), ": ", $e->getMessage(), "\n";
}

try {
    $c->select("SELECT {cols} FROM system.one", ["cols" => ["dummy", new ThrowingString()]]);
    echo "array placeholder: NO THROW\n";
} catch (Throwable $e) {
    echo "array placeholder: ", get_class($e), ": ", $e->getMessage(), "\n";
}

try {
    $c->select("SELECT {x:String}", ["x" => new ThrowingString()]);
    echo "typed placeholder: NO THROW\n";
} catch (Throwable $e) {
    echo "typed placeholder: ", get_class($e), ": ", $e->getMessage(), "\n";
}

$row = $c->select(
    "SELECT {xs:Array(Array(UInt32))} AS xs, {ys:Array(Array(String))} AS ys",
    ["xs" => [[1, 2], [3]], "ys" => [["a", "b"], ["c"]]]
)[0];
echo "nested arrays=", json_encode($row), "\n";
echo "after exceptions=", $c->select("SELECT 1", [], ClickHouse::FETCH_ONE), "\n";
?>
--EXPECT--
scalar placeholder: RuntimeException: string conversion failed
array placeholder: RuntimeException: string conversion failed
typed placeholder: RuntimeException: string conversion failed
nested arrays={"xs":[[1,2],[3]],"ys":[["a","b"],["c"]]}
after exceptions=1
