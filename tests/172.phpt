--TEST--
DR-C1: a throwing __toString() on the host config value surfaces as an exception, not a process abort
--EXTENSIONS--
clickhouse
--FILE--
<?php
// The host value was coerced via ZStrGuard before the constructor's main
// try block, so a throwing __toString() escaped the Zend dispatcher as a
// C++ exception and aborted the process (SIGABRT). It must surface as a
// normal PHP exception instead.

class Boom {
    public function __toString() { throw new Exception("boom in host"); }
}

try {
    $c = new ClickHouse(array("host" => new Boom(), "port" => 9000));
    echo "no throw\n";
} catch (Exception $e) {
    echo "caught: ", get_class($e), ": ", $e->getMessage(), "\n";
}
echo "process survived\n";
?>
--EXPECT--
caught: Exception: boom in host
process survived
