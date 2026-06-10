--TEST--
Zero-arg accessors enforce arity (getServerInfo, getCurrentEndpoint, getStatistics, getLogQueries, ping, resetConnection)
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$ch = new ClickHouse(clickhouse_test_config());

/* These zero-arg methods previously skipped zend_parse_parameters_none:
 * on a release build the extra arg was silently ignored, and on a debug
 * build the call aborted with an "Arginfo / zpp mismatch" fatal. They
 * must now reject the extra arg.
 *
 * PHP 8 throws ArgumentCountError; PHP 7.4 only emits a Warning and
 * returns NULL, so promote that Warning to a throw for the duration of
 * the call to keep the catch arm uniform across the matrix. */
set_error_handler(function ($_n, $msg) { throw new RuntimeException($msg); });
foreach (["getServerInfo", "getCurrentEndpoint", "getStatistics", "getLogQueries", "ping", "resetConnection"] as $m) {
    try {
        $ch->$m("extra");
        echo "$m: no throw\n";
    } catch (Throwable $e) {
        echo "$m: REJECTED\n";
    }
}
restore_error_handler();
?>
--EXPECT--
getServerInfo: REJECTED
getCurrentEndpoint: REJECTED
getStatistics: REJECTED
getLogQueries: REJECTED
ping: REJECTED
resetConnection: REJECTED
