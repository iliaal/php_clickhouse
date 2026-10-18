--TEST--
Array(Map) insert holds each map while a value's __toString frees or grows the referenced map
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("DROP TABLE IF EXISTS test.t_qe0_map");
$c->execute("CREATE TABLE test.t_qe0_map (id UInt8, m Array(Map(String, String))) ENGINE = Memory");

class MapMutator {
    public $mode;
    public function __construct($mode) { $this->mode = $mode; }
    public function __toString(): string {
        global $m;
        if ($this->mode === "free") {
            $m = null;
            $junk = array();
            for ($i = 0; $i < 64; $i++) { $junk[] = str_repeat("A", 200); }
            $GLOBALS["junk"] = $junk;
        } else {
            for ($i = 0; $i < 64; $i++) { $m["grow$i"] = "g"; }
        }
        return "v";
    }
}

function make_map($mode) {
    $map = array();
    for ($i = 0; $i < 8; $i++) {
        $map["k$i"] = ($i == 0) ? new MapMutator($mode) : "x$i";
    }
    return $map;
}

function describe($m) {
    return is_array($m) ? "array(" . count($m) . ")" : gettype($m);
}

$id = 0;
$paths = array(
    "insert" => function ($cell) use ($c, &$id) {
        $c->insert("test.t_qe0_map", array("id", "m"), array(array(++$id, $cell)));
    },
    "insertAssoc" => function ($cell) use ($c, &$id) {
        $c->insertAssoc("test.t_qe0_map", array(array("id" => ++$id, "m" => $cell)));
    },
    "write" => function ($cell) use ($c, &$id) {
        $c->writeStart("test.t_qe0_map", array("id", "m"));
        $c->write(array(array(++$id, $cell)));
        $c->writeEnd();
    },
);

foreach (array("free", "grow") as $mode) {
    foreach ($paths as $label => $run) {
        $m = make_map($mode);
        $cell = array(&$m);
        try {
            $run($cell);
            echo "$mode/$label: inserted, map now ", describe($m), "\n";
        } catch (ClickHouseException $e) {
            echo "$mode/$label: ", get_class($e), ": ", $e->getMessage(), "\n";
        }
        unset($cell, $m);
    }
}

$expected = array("k0" => "v");
for ($i = 1; $i < 8; $i++) { $expected["k$i"] = "x$i"; }
foreach ($c->select("SELECT id, m FROM test.t_qe0_map ORDER BY id") as $row) {
    echo $row["id"], ": ", ($row["m"] === array($expected) ? "snapshot" : json_encode($row["m"])), "\n";
}

foreach (array("free", "grow") as $mode) {
    $m = make_map($mode);
    $ext = array(array(
        "name" => "ext",
        "columns" => array("m" => "Array(Map(String, String))"),
        "rows" => array(array(array(&$m))),
    ));
    try {
        $r = $c->selectWithExternalData("SELECT m FROM ext", $ext);
        echo "$mode/external: ", ($r[0]["m"] === array($expected) ? "snapshot" : json_encode($r)),
            ", map now ", describe($m), "\n";
    } catch (ClickHouseException $e) {
        echo "$mode/external: ", get_class($e), ": ", $e->getMessage(), "\n";
    }
    unset($ext, $m);
}

$c->execute("DROP TABLE IF EXISTS test.t_qe0_map");
echo "done\n";
?>
--EXPECT--
free/insert: inserted, map now NULL
free/insertAssoc: inserted, map now NULL
free/write: inserted, map now NULL
grow/insert: inserted, map now array(72)
grow/insertAssoc: inserted, map now array(72)
grow/write: inserted, map now array(72)
1: snapshot
2: snapshot
3: snapshot
4: snapshot
5: snapshot
6: snapshot
free/external: snapshot, map now NULL
grow/external: snapshot, map now array(72)
done
