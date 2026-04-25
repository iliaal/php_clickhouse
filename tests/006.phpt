--TEST--
ClickHouse testInt
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

clientTest(clickhouse_test_config());

function clientTest($config)
{
    $deleteTable = true;
    $client = new ClickHouse($config);
    $client->execute("CREATE DATABASE IF NOT EXISTS test");

    testUInt($client, $deleteTable);
}

function testUInt($client, $deleteTable = false) {
    $client->execute("DROP TABLE IF EXISTS test.int_test");
    $client->execute("CREATE TABLE test.int_test (int8_c Int8, int16_c Int16, uint8_c UInt8, uint16_c UInt16) ENGINE = Memory");

    $client->insert("test.int_test",[
        'int8_c','int16_c','uint8_c','uint16_c'
    ], [
        [8, 8, 8, 8],
        [9, 9, 9, 9],
    ]);

    $client->writeStart("test.int_test", [
        'int8_c','int16_c','uint8_c'
    ]);
    $client->write([
        [8, 8, 8],
        [9, 9, 9],
    ]);
    $client->writeEnd();
    
    $result = $client->select("SELECT {select} FROM {table} ORDER BY uint16_c, int8_c", [
        'select' => 'int8_c, int16_c, uint8_c, uint16_c',
        'table' => 'test.int_test'
    ]);
    var_dump($result);
    
    if ($deleteTable) {
        $client->execute("DROP TABLE {table}", [
            'table' => 'test.int_test'
        ]);
    }
}

?>
--EXPECT--
array(4) {
  [0]=>
  array(4) {
    ["int8_c"]=>
    int(8)
    ["int16_c"]=>
    int(8)
    ["uint8_c"]=>
    int(8)
    ["uint16_c"]=>
    int(0)
  }
  [1]=>
  array(4) {
    ["int8_c"]=>
    int(9)
    ["int16_c"]=>
    int(9)
    ["uint8_c"]=>
    int(9)
    ["uint16_c"]=>
    int(0)
  }
  [2]=>
  array(4) {
    ["int8_c"]=>
    int(8)
    ["int16_c"]=>
    int(8)
    ["uint8_c"]=>
    int(8)
    ["uint16_c"]=>
    int(8)
  }
  [3]=>
  array(4) {
    ["int8_c"]=>
    int(9)
    ["int16_c"]=>
    int(9)
    ["uint8_c"]=>
    int(9)
    ["uint16_c"]=>
    int(9)
  }
}
