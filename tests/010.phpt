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

    testTuple($client, $deleteTable);
}

function testTuple($client, $deleteTable = false) {
    $client->execute("CREATE TABLE IF NOT EXISTS test.tuple_test (tuple_c Tuple(id UInt64, name String), int_c UInt64, string_c String) ENGINE = Memory");

    $client->insert("test.tuple_test", [
        'tuple_c', 'int_c', 'string_c'
    ], [
        [[1, 'one'], 1, 'one'],
        [[2, 'two'], 2, 'two'],
    ]);
    $result = $client->select("SELECT {select} FROM {table}", [
        'select' => ['tuple_c', 'int_c', 'string_c'],
        'table' => 'test.tuple_test'
    ]);
    var_dump($result);

    if ($deleteTable) {
        $client->execute("DROP TABLE {table}", [
            'table' => 'test.tuple_test'
        ]);
    }
}

?>
--EXPECT--
array(2) {
  [0]=>
  array(3) {
    ["tuple_c"]=>
    array(2) {
      [0]=>
      int(1)
      [1]=>
      string(3) "one"
    }
    ["int_c"]=>
    int(1)
    ["string_c"]=>
    string(3) "one"
  }
  [1]=>
  array(3) {
    ["tuple_c"]=>
    array(2) {
      [0]=>
      int(2)
      [1]=>
      string(3) "two"
    }
    ["int_c"]=>
    int(2)
    ["string_c"]=>
    string(3) "two"
  }
}
