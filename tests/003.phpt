--TEST--
ClickHouse testEnum
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

    testEnum($client, $deleteTable);
}

function testEnum($client, $deleteTable = false) {
    $client->execute("CREATE TABLE IF NOT EXISTS test.enum_test (enum8_c Enum8('One8' = 1, 'Two8' = 2), enum16_c Enum16('One16' = 1, 'Two16' = 2)) ENGINE = Memory");

    $client->insert("test.enum_test", [
        'enum8_c', 'enum16_c'
    ],[
        [1, 'Two16'],
        ['Two8', 1]
    ]);

    $result = $client->select("SELECT {select} FROM {table}", [
        'select' => ['enum8_c', 'enum16_c'],
        'table' => 'test.enum_test'
    ]);
    var_dump($result);

    if ($deleteTable) {
        $client->execute("DROP TABLE {table}", [
            'table' => 'test.enum_test'
        ]);
    }
}

?>
--EXPECT--
array(2) {
  [0]=>
  array(2) {
    ["enum8_c"]=>
    string(4) "One8"
    ["enum16_c"]=>
    string(5) "Two16"
  }
  [1]=>
  array(2) {
    ["enum8_c"]=>
    string(4) "Two8"
    ["enum16_c"]=>
    string(5) "One16"
  }
}
