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
 * must raise a catchable ArgumentCountError instead. */
foreach (["getServerInfo", "getCurrentEndpoint", "getStatistics", "getLogQueries", "ping", "resetConnection"] as $m) {
    try {
        $ch->$m("extra");
        echo "$m: no throw\n";
    } catch (ArgumentCountError $e) {
        echo "$m: ArgumentCountError\n";
    }
}
?>
--EXPECT--
getServerInfo: ArgumentCountError
getCurrentEndpoint: ArgumentCountError
getStatistics: ArgumentCountError
getLogQueries: ArgumentCountError
ping: ArgumentCountError
resetConnection: ArgumentCountError
