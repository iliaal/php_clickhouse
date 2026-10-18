--TEST--
ClickHouse::__construct() redacts its config array in exception traces
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php if (PHP_VERSION_ID < 80200) print "skip SensitiveParameter requires PHP 8.2+"; ?>
--INI--
zend.exception_ignore_args=0
--FILE--
<?php
$pw = "pw-" . bin2hex(random_bytes(8));
$cfg = [
    "host" => "127.0.0.1",
    "port" => 1,
    "user" => "u",
    "passwd" => $pw,
    "connect_timeout" => 1,
    "retry_timeout" => 0,
];
try {
    new ClickHouse($cfg);
    echo "connected\n";
} catch (ClickHouseException $e) {
    $frame = $e->getTrace()[0];
    echo $frame["class"], "::", $frame["function"], "\n";
    $arg = $frame["args"][0];
    echo is_object($arg) ? get_class($arg) : gettype($arg), "\n";
    echo "password in getTrace(): ", var_export(strpos(var_export($e->getTrace(), true), $pw) !== false, true), "\n";
    echo "password in print_r: ", var_export(strpos(print_r($e->getTrace(), true), $pw) !== false, true), "\n";
    echo "password in json_encode: ", var_export(strpos(json_encode($e->getTrace()), $pw) !== false, true), "\n";
}

$r = new ReflectionMethod("ClickHouse", "__construct");
echo count($r->getParameters()[0]->getAttributes("SensitiveParameter")), "\n";
?>
--EXPECT--
ClickHouse::__construct
SensitiveParameterValue
password in getTrace(): false
password in print_r: false
password in json_encode: false
1
