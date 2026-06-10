--TEST--
ClickHouse with a self-capturing callback is reclaimed by the cycle collector
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

/* A closure that captures the client forms a cycle:
 * client -> progress_callback zval -> closure -> client.
 * Without a get_gc handler exposing the callback zval the collector
 * cannot see the object->closure edge and never breaks the cycle, so
 * the client (and its socket) leaks until request shutdown. */
$ch = new ClickHouse(clickhouse_test_config());
$ch->setProgressCallback(function ($p) use ($ch) { /* keeps $ch alive */ });
$w = WeakReference::create($ch);

unset($ch);
/* ClickHouse defines __destruct. PHP < 8.0's cycle collector reclaims a
 * cycle containing a destructor-bearing object across two passes (pass one
 * runs the destructor, pass two frees), so collect twice. On 8.0+ the
 * second call is a no-op. This still proves the get_gc edge: without it the
 * collector never sees object -> closure and no pass would reclaim it. */
gc_collect_cycles();
gc_collect_cycles();

var_dump($w->get() === null);
?>
--EXPECT--
bool(true)
