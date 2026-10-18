--TEST--
ClickHouse Decimal columns of different scale keep their own scale across result blocks
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
require __DIR__ . "/_clickhouse.inc";
clickhouse_skip_if_no_server();
?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.t_q9c_dec");
$c->execute("CREATE TABLE test.t_q9c_dec (n UInt32, d2 Decimal(18,2), d6 Decimal(18,6), d0 Decimal(38,0)) ENGINE = Memory");
$c->execute("INSERT INTO test.t_q9c_dec SELECT number, toDecimal64('12.34', 2), toDecimal64('12.34', 6), toDecimal128('1234', 0) FROM numbers(20000)");

$want = ["d2" => "12.34", "d6" => "12.340000", "d0" => "1234"];

function count_bad(iterable $rows, array $want, int &$seen): array
{
    $bad = ["d2" => 0, "d6" => 0, "d0" => 0];
    foreach ($rows as $r) {
        ++$seen;
        foreach ($want as $k => $v) {
            if ($r[$k] !== $v) {
                ++$bad[$k];
            }
        }
    }
    return $bad;
}

foreach (["d2, d6, d0", "d6, d0, d2", "d0, d2, d6"] as $cols) {
    $sql = "SELECT n, $cols FROM test.t_q9c_dec ORDER BY n SETTINGS max_block_size = 100";

    $seen = 0;
    $bad = count_bad($c->select($sql), $want, $seen);
    echo "select [$cols] rows=$seen bad=", json_encode($bad), "\n";

    $seen = 0;
    $bad = count_bad($c->selectStream($sql), $want, $seen);
    echo "stream [$cols] rows=$seen bad=", json_encode($bad), "\n";
}

$c->execute("DROP TABLE test.t_q9c_dec");
?>
--EXPECT--
select [d2, d6, d0] rows=20000 bad={"d2":0,"d6":0,"d0":0}
stream [d2, d6, d0] rows=20000 bad={"d2":0,"d6":0,"d0":0}
select [d6, d0, d2] rows=20000 bad={"d2":0,"d6":0,"d0":0}
stream [d6, d0, d2] rows=20000 bad={"d2":0,"d6":0,"d0":0}
select [d0, d2, d6] rows=20000 bad={"d2":0,"d6":0,"d0":0}
stream [d0, d2, d6] rows=20000 bad={"d2":0,"d6":0,"d0":0}
