--TEST--
ClickHouse streaming selects reset the connection after callback exceptions
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());

$seen = 0;
try {
    $c->selectStreamCallback(
        "SELECT number FROM numbers(1000)",
        function (array $row) use (&$seen) {
            $seen++;
            if ($seen === 3) {
                throw new RuntimeException("stop at row 3");
            }
        }
    );
    echo "row callback: NO THROW\n";
} catch (RuntimeException $e) {
    echo "row callback: ", $e->getMessage(), "\n";
}
echo "rows seen: $seen\n";
echo "after row callback: ", $c->select("SELECT 1", [], ClickHouse::FETCH_ONE), "\n";

$c->setProgressCallback(function () {
    throw new RuntimeException("progress stop");
});
try {
    $c->selectStream("SELECT number FROM numbers(1000000)");
    echo "progress callback: NO THROW\n";
} catch (RuntimeException $e) {
    echo "progress callback: ", $e->getMessage(), "\n";
}
$c->setProgressCallback(null);
echo "after progress callback: ", $c->select("SELECT 2", [], ClickHouse::FETCH_ONE), "\n";

$c->setProfileCallback(function () {
    throw new RuntimeException("profile stop");
});
try {
    $c->selectStream("SELECT number FROM numbers(1000)");
    echo "profile callback: NO THROW\n";
} catch (RuntimeException $e) {
    echo "profile callback: ", $e->getMessage(), "\n";
}
$c->setProfileCallback(null);
echo "after profile callback: ", $c->select("SELECT 3", [], ClickHouse::FETCH_ONE), "\n";
?>
--EXPECT--
row callback: stop at row 3
rows seen: 3
after row callback: 1
progress callback: progress stop
after progress callback: 2
profile callback: profile stop
after profile callback: 3
