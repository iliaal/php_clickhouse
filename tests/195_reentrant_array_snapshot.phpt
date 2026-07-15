--TEST--
Callback-capable endpoint and typed-array coercion use stable input snapshots
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

class MutatingEndpointHost {
    public function __toString() {
        unset($GLOBALS["endpoint"]["port"]);
        $GLOBALS["endpoint"]["replacement"] = 70000;
        return clickhouse_test_config()["host"];
    }
}

$config = clickhouse_test_config();
$GLOBALS["endpoint"] = [
    "host" => new MutatingEndpointHost(),
    "port" => $config["port"],
];
unset($config["host"], $config["port"]);
$config["endpoints"] = [&$GLOBALS["endpoint"]];
$endpointClient = new ClickHouse($config);
echo "endpoint=", $endpointClient->ping() ? "ok" : "fail", "\n";

class MutatingTypedElement {
    public function __toString() {
        unset($GLOBALS["typed_values"][1]);
        $GLOBALS["typed_values"][1] = 70000;
        return "1";
    }
}

$GLOBALS["typed_values"] = [new MutatingTypedElement(), 2];
$params = ["p" => &$GLOBALS["typed_values"]];
$row = $endpointClient->select("SELECT {p:Array(UInt32)} AS v", $params)[0];
echo json_encode($row["v"]), "\n";
echo json_encode($GLOBALS["typed_values"]), "\n";
?>
--EXPECT--
endpoint=ok
[1,2]
[{},70000]
