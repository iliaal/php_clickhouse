--TEST--
A negative {name} value directly after '-' is rejected instead of composing a -- comment
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";
$c = new ClickHouse(clickhouse_test_config());
$c->execute("DROP TABLE IF EXISTS t_giu_docs");
$c->execute("CREATE TABLE t_giu_docs (id UInt32, owner String) ENGINE = Memory");
$c->insert("t_giu_docs", ["id", "owner"], [[1, "alice"], [2, "bob"], [3, "mallory"]]);

$marker = "giu_" . getmypid() . "_" . mt_rand();
$where = "AND owner = 'alice' ORDER BY id /* $marker */";

function attempt($c, $sql, $params) {
    try {
        $rows = $c->select($sql, $params);
        return json_encode(array_column($rows, "owner"));
    } catch (ClickHouseException $e) {
        return "rejected: " . $e->getMessage();
    }
}

echo "positive: ", attempt($c, "SELECT owner FROM t_giu_docs WHERE id < 9-{skip} $where", ["skip" => 5]), "\n";
echo "negative: ", attempt($c, "SELECT owner FROM t_giu_docs WHERE id < 9-{skip} $where", ["skip" => -5]), "\n";
echo "negative string: ", attempt($c, "SELECT owner FROM t_giu_docs WHERE id < 9-{skip} $where", ["skip" => "-5"]), "\n";
echo "second occurrence: ", attempt($c, "SELECT owner FROM t_giu_docs WHERE id < 10+{skip} AND id < 9-{skip} $where", ["skip" => "-5"]), "\n";
echo "array head: ", attempt($c, "SELECT owner FROM t_giu_docs WHERE id IN (7, 0-{ids}) $where", ["ids" => ["-1", "1"]]), "\n";
echo "spaced: ", attempt($c, "SELECT owner FROM t_giu_docs WHERE id < 9 - {skip} $where", ["skip" => -5]), "\n";
echo "parenthesized: ", attempt($c, "SELECT owner FROM t_giu_docs WHERE id < 9-({skip}) $where", ["skip" => -5]), "\n";
echo "after plus: ", attempt($c, "SELECT owner FROM t_giu_docs WHERE id < 10+{skip} $where", ["skip" => -5]), "\n";
echo "exponent: ", attempt($c, "SELECT owner FROM t_giu_docs WHERE id < 10-{skip} $where", ["skip" => "5e-1"]), "\n";

$c->execute("SYSTEM FLUSH LOGS");
$logged = function ($needle) use ($c, $marker) {
    $rows = $c->select(
        "SELECT count() AS n FROM system.query_log WHERE query LIKE {m:String} AND query LIKE {q:String}",
        ["m" => "%$marker%", "q" => "%$needle%"]);
    return (int)$rows[0]["n"];
};
echo "logged spaced: ", $logged("9 - -5") > 0 ? "yes" : "no", "\n";
echo "logged comment: ", $logged("9--5") + $logged("0--1"), "\n";

$c->execute("DROP TABLE t_giu_docs");
?>
--EXPECT--
positive: ["alice"]
negative: rejected: Placeholder value for {skip} is invalid: a negative value directly after '-' would form a SQL comment; separate them with a space or parentheses in the SQL
negative string: rejected: Placeholder value for {skip} is invalid: a negative value directly after '-' would form a SQL comment; separate them with a space or parentheses in the SQL
second occurrence: rejected: Placeholder value for {skip} is invalid: a negative value directly after '-' would form a SQL comment; separate them with a space or parentheses in the SQL
array head: rejected: Placeholder value for {ids} is invalid: a negative value directly after '-' would form a SQL comment; separate them with a space or parentheses in the SQL
spaced: ["alice"]
parenthesized: ["alice"]
after plus: ["alice"]
exponent: ["alice"]
logged spaced: yes
logged comment: 0
