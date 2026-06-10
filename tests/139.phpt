--TEST--
Placeholder array rejects an empty-string key with a clear message
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());

/* An empty-string placeholder key has no valid {name} or {name:Type}
 * spelling; it used to fall through to the generic "does not appear in
 * the SQL" path (or, for a typed form, build a nameless server param).
 * It must be rejected up front as a malformed key. */
try {
    $c->select("SELECT {:UInt8} AS x", ["" => 1], ClickHouse::FETCH_ONE);
    echo "no throw\n";
} catch (ClickHouseException $e) {
    echo "non-empty msg=", (strpos($e->getMessage(), "non-empty") !== false ? "yes" : "no"), "\n";
}

/* A well-formed typed placeholder still works. */
$v = $c->select("SELECT {v:UInt8} AS x", ["v" => 7], ClickHouse::FETCH_ONE);
echo "typed ok=", $v, "\n";
?>
--EXPECT--
non-empty msg=yes
typed ok=7
