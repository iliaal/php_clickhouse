--TEST--
Insert copies a $GLOBALS-derived Array cell or Map row before __toString grows it, and resolves symbol-table slots
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

/* Before PHP 8.1 a copy of $GLOBALS aliases the symbol table: it holds no
 * reference, writes through any copy resize it in place, and named slots of
 * top-level variables are IS_INDIRECT. */
class SymbolTableGrower {
    public $g;
    public $junk = array();
    public function __toString() {
        for ($i = 0; $i < 200; $i++) {
            $this->g["grow$i"] = "g";
        }
        for ($i = 0; $i < 64; $i++) {
            $this->junk[] = str_repeat("A", 2560 - 24 - 1);
        }
        return "v";
    }
}

function clear_except(&$g, $keep) {
    foreach (array_keys($g) as $k) {
        if ($k !== $keep) {
            unset($g[$k]);
        }
    }
}

function indirect_cell($c, $row) {
    $c->execute("DROP TABLE IF EXISTS test.t_qe0_glob");
    $c->execute("CREATE TABLE test.t_qe0_glob (top_level_var String) ENGINE = Memory");
    try {
        $c->insert("test.t_qe0_glob", array("top_level_var"), array($row));
        $rows = $c->select("SELECT top_level_var FROM test.t_qe0_glob");
        echo "named slot: ", json_encode($rows[0]["top_level_var"]), "\n";
    } catch (ClickHouseException $ex) {
        echo "named slot: ", get_class($ex), ": ", $ex->getMessage(), "\n";
    }
}

function grown_cell($c, $label, $type, $wrap, $g) {
    $c->execute("DROP TABLE IF EXISTS test.t_qe0_glob");
    $c->execute("CREATE TABLE test.t_qe0_glob (v $type) ENGINE = Memory");
    clear_except($g, null);
    $g["a"] = "x";
    $e = new SymbolTableGrower;
    $e->g = $g;
    $g["b"] = $e;
    for ($i = 0; $i < 6; $i++) {
        $g["t$i"] = "y$i";
    }
    try {
        $c->insert("test.t_qe0_glob", array("v"), array(array($wrap ? array($g) : $g)));
        $rows = $c->select("SELECT v FROM test.t_qe0_glob");
        echo "$label: ", json_encode($rows[0]["v"]), ", grower ran: ",
            (count($e->g) > 200 ? "yes" : "no"), "\n";
    } catch (ClickHouseException $ex) {
        echo "$label: ", get_class($ex), ": ", $ex->getMessage(), "\n";
    }
}

function run_all() {
    $c = new ClickHouse(clickhouse_test_config());
    /* On PHP < 8.1 every copy below is the live symbol table. */
    $g = $GLOBALS;
    clear_except($g, "top_level_var");
    indirect_cell($c, $g);
    grown_cell($c, "Array(String)", "Array(String)", false, $g);
    grown_cell($c, "Array(Map)", "Array(Map(String, String))", true, $g);
    $c->execute("DROP TABLE IF EXISTS test.t_qe0_glob");
}

/* Top-level variables are the ones with IS_INDIRECT symbol-table slots. */
$top_level_var = "hello";
run_all();
echo "done\n";
?>
--EXPECT--
named slot: "hello"
Array(String): ["x","v","y0","y1","y2","y3","y4","y5"], grower ran: yes
Array(Map): [{"a":"x","b":"v","t0":"y0","t1":"y1","t2":"y2","t3":"y3","t4":"y4","t5":"y5"}], grower ran: yes
done
