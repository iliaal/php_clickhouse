--TEST--
DR-005 (stream paths): selectStream / selectStreamCallback / selectToStream keep session state on a clean server error
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// DR-005 was first fixed only on select() / selectStatement() / execute().
// The three stream-style Select entry points still reset the connection on
// ANY error, so a clean server ServerException destroyed session-scoped
// state (temp tables, SET) exactly like the non-stream paths used to.
// A ServerException leaves the wire clean and must not reconnect; a
// callback/OnData throw or transport fault still resets.

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");

$bad = "SELECT * FROM test.dr005_stream_no_such_table_xyz";
$i = 0;

// selectStream: SELECT -> buffered iterator
$t = "st" . $i++;
$c->execute("CREATE TEMPORARY TABLE $t (x UInt8) ENGINE = Memory");
$c->execute("INSERT INTO $t VALUES (42)");
try { $c->selectStream($bad); echo "selectStream bad: NO THROW\n"; }
catch (ClickHouseException $e) { /* expected */ }
try { $r = $c->select("SELECT x FROM $t"); echo "selectStream: temp survives (", $r[0]['x'], ")\n"; }
catch (ClickHouseException $e) { echo "selectStream: temp GONE\n"; }

// selectStreamCallback: SELECT -> per-row callback
$t = "st" . $i++;
$c->execute("CREATE TEMPORARY TABLE $t (x UInt8) ENGINE = Memory");
$c->execute("INSERT INTO $t VALUES (42)");
try { $c->selectStreamCallback($bad, function ($row) {}); echo "selectStreamCallback bad: NO THROW\n"; }
catch (ClickHouseException $e) { /* expected */ }
try { $r = $c->select("SELECT x FROM $t"); echo "selectStreamCallback: temp survives (", $r[0]['x'], ")\n"; }
catch (ClickHouseException $e) { echo "selectStreamCallback: temp GONE\n"; }

// selectToStream: SELECT -> write TSV to a PHP stream
$t = "st" . $i++;
$c->execute("CREATE TEMPORARY TABLE $t (x UInt8) ENGINE = Memory");
$c->execute("INSERT INTO $t VALUES (42)");
$out = fopen("php://memory", "r+");
try { $c->selectToStream($bad, [], $out); echo "selectToStream bad: NO THROW\n"; }
catch (ClickHouseException $e) { /* expected */ }
fclose($out);
try { $r = $c->select("SELECT x FROM $t"); echo "selectToStream: temp survives (", $r[0]['x'], ")\n"; }
catch (ClickHouseException $e) { echo "selectToStream: temp GONE\n"; }

// Handle still usable.
$r = $c->select("SELECT 1 AS ok");
echo "handle usable: ", $r[0]['ok'], "\n";
?>
--EXPECT--
selectStream: temp survives (42)
selectStreamCallback: temp survives (42)
selectToStream: temp survives (42)
handle usable: 1
