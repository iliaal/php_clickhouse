--TEST--
Map ordered-pairs insert copies every pair before a key's __toString rewrites a later pair or row
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("DROP TABLE IF EXISTS test.t_o5l_pairs");
$c->execute("DROP TABLE IF EXISTS test.t_o5l_arr_pairs");
$c->execute("CREATE TABLE test.t_o5l_pairs (id UInt8, m Map(String, String)) ENGINE = Memory");
$c->execute("CREATE TABLE test.t_o5l_arr_pairs (id UInt8, m Array(Map(String, String))) ENGINE = Memory");

class PairRewriter {
    public $target;
    public $value;
    public function __construct($target, $value) {
        $this->target = $target;
        $this->value = $value;
    }
    public function __toString(): string {
        $GLOBALS[$this->target] = $this->value;
        return "k0";
    }
}

/* The 0x41.../0x42... replacements overflow a 32-bit zend_long and are
 * floats there; "long" means the variable now holds that literal's type. */
$longType = PHP_INT_SIZE > 4 ? "integer" : "double";
function replacedType($value, $longType) {
    $type = gettype($value);
    return $type === $longType ? "long" : $type;
}

/* The rewritten pair is row 1's only element and a PHP reference. */
$pair_modes = array(
    "null"   => array(),
    "long"   => 0x4141414141414141,
    "string" => str_repeat("B", 64),
);
$id = 0;
foreach ($pair_modes as $label => $replacement) {
    $pair = array("k1", "v1");
    $row0 = array(array(new PairRewriter("pair", $replacement), "v0"));
    $row1 = array(&$pair);
    $id++;
    try {
        $c->insert("test.t_o5l_pairs", array("id", "m"),
            array(array($id, $row0), array($id, $row1)));
        echo "pair/$label: inserted, pair now ", replacedType($pair, $longType), "\n";
    } catch (ClickHouseException $e) {
        echo "pair/$label: ", get_class($e), ": ", $e->getMessage(), "\n";
    }
    unset($row1, $pair);
}
foreach ($c->select("SELECT id, m FROM test.t_o5l_pairs ORDER BY id, mapKeys(m)") as $row) {
    echo $row["id"], ": ", json_encode($row["m"]), "\n";
}

/* Array(Map) pairs form: map row 1 of one Array cell is a PHP reference. */
$row_modes = array(
    "long" => 0x4242424242424242,
    "null" => null,
);
$id = 0;
foreach ($row_modes as $label => $replacement) {
    $map_row1 = array(array("k1", "v1"));
    $cell = array(array(array(new PairRewriter("map_row1", $replacement), "v0")), &$map_row1);
    $id++;
    try {
        $c->insert("test.t_o5l_arr_pairs", array("id", "m"), array(array($id, $cell)));
        echo "row/$label: inserted, row now ", replacedType($map_row1, $longType), "\n";
    } catch (ClickHouseException $e) {
        echo "row/$label: ", get_class($e), ": ", $e->getMessage(), "\n";
    }
    unset($cell, $map_row1);
}
foreach ($c->select("SELECT id, m FROM test.t_o5l_arr_pairs ORDER BY id") as $row) {
    echo $row["id"], ": ", json_encode($row["m"]), "\n";
}

/* A value that fails conversion after its row's key column was built must
 * release that key column (checked under LeakSanitizer). */
$c->execute("DROP TABLE IF EXISTS test.t_o5l_leak");
$c->execute("CREATE TABLE test.t_o5l_leak (m Map(String, UInt8)) ENGINE = Memory");
for ($i = 0; $i < 3; $i++) {
    try {
        $c->insert("test.t_o5l_leak", array("m"), array(array(array(array("k$i", "not-a-number")))));
        echo "bad value $i: inserted\n";
    } catch (ClickHouseException $e) {
        echo "bad value $i: ", get_class($e), "\n";
    }
}
$c->insert("test.t_o5l_leak", array("m"), array(array(array(array("ok", 7)))));
echo "valid after failures: ", json_encode($c->select("SELECT m FROM test.t_o5l_leak")), "\n";
$c->execute("DROP TABLE IF EXISTS test.t_o5l_leak");

/* A pair that was never a valid [key, value] array is still rejected. */
try {
    $c->insert("test.t_o5l_pairs", array("id", "m"),
        array(array(9, array(array("k", "v"), "not-a-pair"))));
    echo "invalid: inserted\n";
} catch (ClickHouseException $e) {
    echo "invalid: ", get_class($e), "\n";
}

$c->execute("DROP TABLE IF EXISTS test.t_o5l_pairs");
$c->execute("DROP TABLE IF EXISTS test.t_o5l_arr_pairs");
echo "done\n";
?>
--EXPECT--
pair/null: inserted, pair now array
pair/long: inserted, pair now long
pair/string: inserted, pair now string
1: {"k0":"v0"}
1: {"k1":"v1"}
2: {"k0":"v0"}
2: {"k1":"v1"}
3: {"k0":"v0"}
3: {"k1":"v1"}
row/long: inserted, row now long
row/null: inserted, row now NULL
1: [{"k0":"v0"},{"k1":"v1"}]
2: [{"k0":"v0"},{"k1":"v1"}]
bad value 0: ClickHouseException
bad value 1: ClickHouseException
bad value 2: ClickHouseException
valid after failures: [{"m":{"ok":7}}]
invalid: ClickHouseException
done
