--TEST--
ClickHouse Exception test
--SKIPIF--
<?php if (!extension_loaded("SeasClick")) print "skip"; ?>
--FILE--
<?php
$config = [
    "host"        => "clickhouse",
    "port"        => "9000",
    "compression" => true,
];

$deleteTable = true;
$client = new ClickHouse($config);
try {
    $client->execute('FOO');
} catch (ClickHouseException $e) {
    var_dump(get_class($e));
}
?>
--EXPECT--
string(18) "ClickHouseException"
