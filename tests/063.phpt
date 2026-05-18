--TEST--
ClickHouse setSettings rejects malformed keys consistently with setSetting
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// setSetting (singular) has always rejected the empty-key case; setSettings
// (bulk) used to silently skip integer keys and silently store empty-string
// keys. Now both entry points apply the same validation so a settings array
// accepted by one is also accepted by the other.

$c = new ClickHouse(clickhouse_test_config());

class ThrowingSettingString {
    public function __toString(): string {
        throw new RuntimeException("setting conversion failed");
    }
}

$probes = [
    "singular: empty key" => fn() => $c->setSetting("", "1"),
    "bulk: empty key"     => fn() => $c->setSettings(["" => "1"]),
    "bulk: numeric key"   => fn() => $c->setSettings([0 => "1", 1 => "2"]),
    "bulk: mixed numeric" => fn() => $c->setSettings(["max_threads" => "1", 5 => "x"]),
    "singular: throwing value" => fn() => $c->setSetting("max_threads", new ThrowingSettingString()),
    "bulk: throwing value" => fn() => $c->setSettings(["max_threads" => new ThrowingSettingString()]),
];
foreach ($probes as $label => $fn) {
    try { $fn(); echo "$label: NO THROW\n"; }
    catch (Throwable $e) { echo "$label: REJECTED — ", $e->getMessage(), "\n"; }
}

// Sanity: well-formed key sets still work via both entry points.
$c->setSetting("max_threads", "1");
$c->setSettings(["max_threads" => "2", "max_block_size" => "65536"]);
echo "ok\n";
?>
--EXPECT--
singular: empty key: REJECTED — setting key must not be empty
bulk: empty key: REJECTED — setting key must not be empty
bulk: numeric key: REJECTED — setting keys must be strings
bulk: mixed numeric: REJECTED — setting keys must be strings
singular: throwing value: REJECTED — setting conversion failed
bulk: throwing value: REJECTED — setting conversion failed
ok
