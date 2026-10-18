--TEST--
Array cells and Map rows are read as they were before an element's __toString reassigns a referenced sibling
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());

class SiblingRewriter {
    public $target;
    public $value;
    public function __construct($target, $value) {
        $this->target = $target;
        $this->value = $value;
    }
    public function __toString(): string {
        $GLOBALS[$this->target] = $this->value;
        return "a";
    }
}

function roundtrip($c, $label, $type, $cell, $target) {
    $c->execute("DROP TABLE IF EXISTS test.t_qe0_sib");
    $c->execute("CREATE TABLE test.t_qe0_sib (v $type) ENGINE = Memory");
    try {
        $c->insert("test.t_qe0_sib", array("v"), array(array($cell)));
        $rows = $c->select("SELECT v FROM test.t_qe0_sib");
        echo "$label: ", json_encode($rows[0]["v"]), ", sibling now ",
            json_encode($GLOBALS[$target]), "\n";
    } catch (ClickHouseException $e) {
        echo "$label: ", get_class($e), ": ", $e->getMessage(), "\n";
    }
}

/* The NULL mask and the child column must see the same cell contents. */
$t = "x";
roundtrip($c, "Array(Nullable(String))", "Array(Nullable(String))",
    array(new SiblingRewriter("t", null), &$t), "t");

$t = null;
roundtrip($c, "Array(Nullable(String)) null", "Array(Nullable(String))",
    array(new SiblingRewriter("t", "late"), &$t), "t");

$t = "x";
roundtrip($c, "Array(String)", "Array(String)",
    array(new SiblingRewriter("t", "late"), &$t), "t");

$t = "x";
roundtrip($c, "Array(String) to array", "Array(String)",
    array(new SiblingRewriter("t", array(1)), &$t), "t");

/* Referenced entries inside one map row. */
$t = "x";
roundtrip($c, "Map entry", "Map(String, String)",
    array("k0" => new SiblingRewriter("t", array(1)), "k1" => &$t), "t");

$t = "x";
roundtrip($c, "Array(Map) entry", "Array(Map(String, String))",
    array(array("k0" => new SiblingRewriter("t", "late"), "k1" => &$t)), "t");

/* Tuple(Map) already held the map; keep it working. */
$t = array("k1" => "x");
roundtrip($c, "Array(Tuple(String, Map))", "Array(Tuple(String, Map(String, String)))",
    array(array(new SiblingRewriter("t", null), &$t)), "t");

/* Controls without userland mutation. */
$t = "x";
roundtrip($c, "control Array(String)", "Array(String)", array("p", &$t, "q"), "t");
roundtrip($c, "control Array(Map)", "Array(Map(String, UInt8))",
    array(array("a" => 1), array(), array("b" => 2, "c" => 3)), "t");
roundtrip($c, "control Map pairs", "Map(UInt8, String)",
    array(array(2, "two"), array(1, "one")), "t");

$c->execute("DROP TABLE IF EXISTS test.t_qe0_sib");
echo "done\n";
?>
--EXPECT--
Array(Nullable(String)): ["a","x"], sibling now null
Array(Nullable(String)) null: ["a",null], sibling now "late"
Array(String): ["a","x"], sibling now "late"
Array(String) to array: ["a","x"], sibling now [1]
Map entry: {"k0":"a","k1":"x"}, sibling now [1]
Array(Map) entry: [{"k0":"a","k1":"x"}], sibling now "late"
Array(Tuple(String, Map)): [["a",{"k1":"x"}]], sibling now null
control Array(String): ["p","x","q"], sibling now "x"
control Array(Map): [{"a":1},[],{"b":2,"c":3}], sibling now "x"
control Map pairs: {"2":"two","1":"one"}, sibling now "x"
done
