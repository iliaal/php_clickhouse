--TEST--
selectWithExternalData retains referenced rows while Stringable conversion mutates their owner
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());

class ExternalRowsMutator {
    public $rows;
    public $columns;

    public function __toString(): string {
        $this->rows = [["replacement-a", "replacement-b"]];
        $this->columns = ["replacement" => "String"];
        return "original-a";
    }
}

$mutator = new ExternalRowsMutator();
$columns = ["a" => "String", "b" => "String"];
$rows = [[$mutator, "original-b"]];
$mutator->rows = &$rows;
$mutator->columns = &$columns;
$externals = [[
    "name" => "ext_strings",
    "columns" => &$columns,
    "rows" => &$rows,
]];

$result = $c->selectWithExternalData(
    "SELECT a, b FROM ext_strings",
    $externals
);

echo $result[0]["a"], "\n";
echo $result[0]["b"], "\n";
echo $rows[0][0], "\n";
echo array_key_first($columns), "\n";
?>
--EXPECT--
original-a
original-b
replacement-a
replacement
