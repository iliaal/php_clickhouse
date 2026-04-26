--TEST--
ClickHouseException carries server_code, server_name, query_id
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";
$c = new ClickHouse(clickhouse_test_config());

$qid = "test-structured-exc-" . bin2hex(random_bytes(4));

try {
    $c->select("SELECT * FROM no_such_db_xyz_zz.no_such_table", [], 0, $qid);
    echo "no throw\n";
} catch (ClickHouseException $e) {
    echo "code>0: ",       ($e->server_code > 0 ? "yes" : "no"), "\n";
    echo "name set: ",     (is_string($e->server_name) && $e->server_name !== "" ? "yes" : "no"), "\n";
    echo "query_id: ",     ($e->query_id === $qid ? "match" : "mismatch"), "\n";
    echo "name prefix: ",  (strpos((string)$e->server_name, "DB::") === 0 ? "DB::" : "other"), "\n";
}

// Client-side error: no server fields populated.
try {
    $c->select("SELECT 1", ["foo" => "bar"]);
} catch (ClickHouseException $e) {
    echo "client code: ",  $e->server_code, "\n";
    echo "client name: ",  var_export($e->server_name, true), "\n";
}
?>
--EXPECT--
code>0: yes
name set: yes
query_id: match
name prefix: DB::
client code: 0
client name: NULL
