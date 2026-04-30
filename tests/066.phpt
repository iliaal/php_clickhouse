--TEST--
ClickHouse rejects non-array input on declared-array parameters with TypeError (no segfault)
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// Regression for CR-501: every PHP_METHOD whose stub declares an
// `array` parameter used Z_PARAM_ZVAL internally, so PHP's stub-type
// declaration was not enforced at the C boundary. Z_ARRVAL_P on a
// non-array zval crashed the worker (debug build assertion / release
// build SEGV) before any user-catchable exception could fire.
// Switched to Z_PARAM_ARRAY at every site so the engine raises a
// clean TypeError before the C body runs.

// Constructor type guard.
$probes_ctor = [
    "__construct(string)" => fn() => new ClickHouse("not-an-array"),
    "__construct(int)"    => fn() => new ClickHouse(42),
    "__construct(null)"   => fn() => new ClickHouse(null),
    "__construct(object)" => fn() => new ClickHouse(new stdClass()),
];
foreach ($probes_ctor as $label => $fn) {
    try { $fn(); echo "$label: NO THROW\n"; }
    catch (TypeError $e) { echo "$label: TypeError\n"; }
    catch (Throwable $e) { echo "$label: ", get_class($e), "\n"; }
}

// Method-level type guards. Build a real client so we exercise the
// per-method ZPP, not the constructor path again.
$c = new ClickHouse(clickhouse_test_config());
$probes_methods = [
    "insert values=string"      => fn() => $c->insert("t", ["c"], "not-an-array"),
    "insert columns=string"     => fn() => $c->insert("t", "not-an-array", [[1]]),
    "write(string)"             => fn() => $c->write("not-an-array"),
    "insertAssoc rows=string"   => fn() => $c->insertAssoc("t", "not-an-array"),
    "execute params=int"        => fn() => $c->execute("SELECT 1", 42),
    "select params=int"         => fn() => $c->select("SELECT 1", 42),
    "select settings=int"       => fn() => $c->select("SELECT 1", [], 0, "", 42),
    "writeStart columns=int"    => fn() => $c->writeStart("t", 42),
];
foreach ($probes_methods as $label => $fn) {
    try { $fn(); echo "$label: NO THROW\n"; }
    catch (TypeError $e) { echo "$label: TypeError\n"; }
    catch (Throwable $e) { echo "$label: ", get_class($e), "\n"; }
}
?>
--EXPECT--
__construct(string): TypeError
__construct(int): TypeError
__construct(null): TypeError
__construct(object): TypeError
insert values=string: TypeError
insert columns=string: TypeError
write(string): TypeError
insertAssoc rows=string: TypeError
execute params=int: TypeError
select params=int: TypeError
select settings=int: TypeError
writeStart columns=int: TypeError
