--TEST--
DR-013: JSON insert preserves a user jsonSerialize() exception instead of a generic wrapper
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); clickhouse_skip_if_no_json(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// When a JsonSerializable value throws during php_json_encode, the JSON
// insert path used to clear the pending exception and throw a generic
// "failed to encode value to JSON". It must instead leave EG(exception)
// set so the boundary preserves the original type and message (matching
// the String path via ZStrGuard).

class BoomSerialize implements JsonSerializable {
    #[\ReturnTypeWillChange]
    public function jsonSerialize() {
        throw new RuntimeException("boom_from_user_land");
    }
}

$c = new ClickHouse(clickhouse_test_config());
$c->setSettings([
    "allow_experimental_json_type"              => 1,
    "output_format_native_write_json_as_string" => 1,
]);
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.dr013");
$c->execute("CREATE TABLE test.dr013 (j JSON) ENGINE = Memory");

try {
    $c->insert("test.dr013", ['j'], [[new BoomSerialize()]]);
    echo "NO THROW\n";
} catch (\Throwable $e) {
    echo get_class($e), ": ", $e->getMessage(), "\n";
}

$c->execute("DROP TABLE test.dr013");
?>
--EXPECT--
RuntimeException: boom_from_user_land
