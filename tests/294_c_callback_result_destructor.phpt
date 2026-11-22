--TEST--
Callback result destructors and JSON serializers preserve user exceptions and native error logs
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
require __DIR__ . "/_clickhouse.inc";
clickhouse_skip_if_no_server();
clickhouse_skip_if_no_json();
?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

class ThrowingCallbackResult {
    private $exception;

    public function __construct($exception) {
        $this->exception = $exception;
    }

    public function __destruct() {
        throw $this->exception;
    }
}

foreach (["select_start", "select_finish", "progress", "profile"] as $event) {
    $c = new ClickHouse(clickhouse_test_config());
    $c->enableLogQueries();
    $sentinel = new RuntimeException($event . " destructor sentinel");
    $calls = 0;
    if ($event === "progress") {
        $c->setProgressCallback(function () use ($sentinel, &$calls) {
            $calls++;
            return new ThrowingCallbackResult($sentinel);
        });
    } elseif ($event === "profile") {
        $c->setProfileCallback(function () use ($sentinel, &$calls) {
            $calls++;
            return new ThrowingCallbackResult($sentinel);
        });
    } else {
        $c->setVerbose(function ($name) use ($event, $sentinel, &$calls) {
            if ($name === $event) {
                $calls++;
                return new ThrowingCallbackResult($sentinel);
            }
        });
    }

    $caught = null;
    try {
        $c->select("SELECT number FROM numbers(1000)", [], 0, $event . "-destructor");
    } catch (Throwable $error) {
        $caught = $error;
    }
    $c->setVerbose(false);
    $c->setProgressCallback(null);
    $c->setProfileCallback(null);
    echo $event, " exception=", $caught === $sentinel ? "original" : "wrong", " calls=", $calls, "\n";
    $logs = $c->getLogQueries();
    echo $event, " logs=", count($logs), "\n";
    foreach ($logs as $log) {
        echo $log["query_id"], ":", $log["error_code"], ":", $log["error_message"], "\n";
    }
    echo $event, " recovery=", $c->ping() ? "ping" : "failed", ":",
        $c->select("SELECT 7 AS n", [], ClickHouse::FETCH_ONE), "\n";
}

class ThrowingJsonValue implements JsonSerializable {
    private $exception;

    public function __construct($exception) {
        $this->exception = $exception;
    }

    #[\ReturnTypeWillChange]
    public function jsonSerialize() {
        throw $this->exception;
    }
}

$c = new ClickHouse(clickhouse_test_config());
$c->setSettings(["allow_experimental_json_type" => 1]);
$c->execute("CREATE TEMPORARY TABLE callback_json (value JSON)");
$c->enableLogQueries();
$sentinel = new ClickHouseException("user\0message", 777);
$caught = null;
try {
    $c->insert("callback_json", ["value"], [[new ThrowingJsonValue($sentinel)]], "json-serializer");
} catch (Throwable $error) {
    $caught = $error;
}
echo "json exception=", $caught === $sentinel ? "original" : "wrong",
    " message=", bin2hex($sentinel->getMessage()), " code=", $sentinel->getCode(), "\n";
$logs = $c->getLogQueries();
echo "json logs=", count($logs), "\n";
foreach ($logs as $log) {
    echo $log["query_id"], ":", $log["error_code"], ":", $log["error_message"], "\n";
}
echo "json recovery=", $c->ping() ? "ping" : "failed", ":",
    $c->select("SELECT count() FROM callback_json", [], ClickHouse::FETCH_ONE), "\n";
$c->insert("callback_json", ["value"], [['{"ok":1}']]);
echo "json valid insert=", $c->select("SELECT count() FROM callback_json", [], ClickHouse::FETCH_ONE), "\n";
?>
--EXPECT--
select_start exception=original calls=1
select_start logs=1
select_start-destructor:-1:verbose callback aborted query
select_start recovery=ping:7
select_finish exception=original calls=1
select_finish logs=1
select_finish-destructor:-1:verbose callback aborted query
select_finish recovery=ping:7
progress exception=original calls=1
progress logs=1
progress-destructor:-1:progress callback aborted query
progress recovery=ping:7
profile exception=original calls=1
profile logs=1
profile-destructor:-1:profile callback aborted query
profile recovery=ping:7
json exception=original message=75736572006d657373616765 code=777
json logs=1
json-serializer:-1:JSON insert: value serialization threw an exception
json recovery=ping:0
json valid insert=1
