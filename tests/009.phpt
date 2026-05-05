--TEST--
ClickHouse testDate
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";
$config = clickhouse_test_config();

clientTest($config);

function clientTest($config)
{
    $deleteTable = true;
    $client = new ClickHouse($config);
    $client->execute("CREATE DATABASE IF NOT EXISTS test");

    testDate($client, $deleteTable);
}

function testDate($client, $deleteTable = false) {
    $client->execute("DROP TABLE IF EXISTS test.date_test");
    $client->execute("CREATE TABLE test.date_test (date_c Date, datetime_c DateTime('UTC')) ENGINE = Memory");

    // Raw epoch ints, TZ-independent. 1548633600 = 2019-01-28 00:00:00 UTC,
    // 1548687925 = 2019-01-28 15:05:25 UTC.
    $client->insert("test.date_test", ['date_c', 'datetime_c'], [
        [1548633600, 1548687925],
        [1548633600, 1548687925],
    ]);

    $result = $client->select("SELECT date_c, datetime_c FROM test.date_test");
    var_dump($result);

    if ($deleteTable) {
        $client->execute("DROP TABLE test.date_test");
    }
}

?>
--EXPECT--
array(2) {
  [0]=>
  array(2) {
    ["date_c"]=>
    int(1548633600)
    ["datetime_c"]=>
    int(1548687925)
  }
  [1]=>
  array(2) {
    ["date_c"]=>
    int(1548633600)
    ["datetime_c"]=>
    int(1548687925)
  }
}
