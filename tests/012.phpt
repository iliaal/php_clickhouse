--TEST--
ClickHouse Date Formatting
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";
$config = clickhouse_test_config();

$deleteTable = true;
$client = new ClickHouse($config);
$client->execute('CREATE DATABASE IF NOT EXISTS test');

$client->execute("CREATE TABLE IF NOT EXISTS test.dates (
	date_c Date,
	datetime_c DateTime
) ENGINE = Memory");

$data = [
    [
        'date_c'        => 1548633600,
        'datetime_c'    => 1548687925,
    ],
    [
        'date_c'        => 1548547200,
        'datetime_c'    => 1548513600,
    ],
];
$expected = [
    [
            'date_c'        => gmdate('Y-m-d', 1548633600),
            'datetime_c'    => gmdate('Y-m-d H:i:s', 1548687925),
        ],
        [
            'date_c'        => gmdate('Y-m-d', 1548547200),
            'datetime_c'    => gmdate('Y-m-d H:i:s', 1548513600),
        ],
];

$fields = array_keys(current($data));
$client->insert('test.dates', $fields, [array_values($data[0]), array_values($data[1])]);

$res = $client->select("SELECT * FROM test.dates", [], ClickHouse::DATE_AS_STRINGS);
var_dump($res);
if (array_diff_assoc($expected[0], $res[0]) || array_diff_assoc($expected[1], $res[1])) {
    echo "FAIL\n";
} else {
    echo "OK\n";
}

$res = $client->select("SELECT date_c FROM test.dates WHERE datetime_c = 1548687925", [], ClickHouse::FETCH_ONE|ClickHouse::DATE_AS_STRINGS);
echo $res, ' = ', $expected[0]['date_c'], ' ', ($res === $expected[0]['date_c'] ? 'OK' : 'FAIL'), "\n";

$res = $client->select("SELECT datetime_c FROM test.dates WHERE datetime_c = 1548687925", [], ClickHouse::FETCH_ONE|ClickHouse::DATE_AS_STRINGS);
echo $res, ' = ', $expected[0]['datetime_c'], ' ' , ($res === $expected[0]['datetime_c'] ? 'OK' : 'FAIL'), "\n";

$client->execute('DROP TABLE test.dates');
?>
--EXPECT--
array(2) {
  [0]=>
  array(2) {
    ["date_c"]=>
    string(10) "2019-01-28"
    ["datetime_c"]=>
    string(19) "2019-01-28 15:05:25"
  }
  [1]=>
  array(2) {
    ["date_c"]=>
    string(10) "2019-01-27"
    ["datetime_c"]=>
    string(19) "2019-01-26 14:40:00"
  }
}
OK
2019-01-28 = 2019-01-28 OK
2019-01-28 15:05:25 = 2019-01-28 15:05:25 OK
