--TEST--
ClickHouse Map matrix expansion: LowCardinality keys, UUID K/V, integer width K/V, Float keys, narrow ints
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.maps_matrix");
$c->execute("CREATE TABLE test.maps_matrix (
    id UInt32,
    lc_str  Map(LowCardinality(String), String),
    lc_u32  Map(LowCardinality(String), UInt32),
    uuid_str Map(UUID, String),
    str_uuid Map(String, UUID),
    i32_f64  Map(Int32, Float64),
    u8_u32   Map(UInt8,  UInt32),
    f64_str  Map(Float64, String),
    i16_i16  Map(Int16, Int16)
) ENGINE = Memory");

$U1 = "00000000-0000-0000-0000-000000000001";
$U2 = "00000000-0000-0000-0000-000000000002";

$c->insert("test.maps_matrix",
    ["id", "lc_str", "lc_u32", "uuid_str", "str_uuid", "i32_f64", "u8_u32", "f64_str", "i16_i16"],
    [
        [1,
         ["env" => "prod"],
         ["hits" => 99],
         [$U1 => "alice", $U2 => "bob"],
         ["alice" => $U1, "bob" => $U2],
         [-1 => 1.5, 2 => 2.5],
         [0 => 100, 255 => 200],
         ["1.5" => "one-and-half"],
         [-32768 => -1, 32767 => 1]],
    ]);

// Round-trip — except for LowCardinality keys: vendor read path doesn't
// decode them (clickhouse-cpp v2.6.1 issue, see vendor wiki). Verify the
// LC writes landed via server-side aggregation instead, and round-trip
// the rest.
$lc_check = $c->select("SELECT
    sum(mapContains(lc_str, 'env'))         AS env_present,
    sum(lc_u32['hits'])                     AS hits_sum
    FROM test.maps_matrix");
echo "lc env_present=", $lc_check[0]["env_present"], " hits_sum=", $lc_check[0]["hits_sum"], "\n";

foreach ($c->select("SELECT id, uuid_str, str_uuid, i32_f64, u8_u32, f64_str, i16_i16 FROM test.maps_matrix ORDER BY id") as $r) {
    ksort($r["uuid_str"]);
    ksort($r["str_uuid"]);
    ksort($r["i32_f64"]);
    ksort($r["u8_u32"]);
    ksort($r["f64_str"]);
    ksort($r["i16_i16"]);
    echo "id=", $r["id"], "\n";
    echo "  uuid_str=",  json_encode($r["uuid_str"]),  "\n";
    echo "  str_uuid=",  json_encode($r["str_uuid"]),  "\n";
    echo "  i32_f64=",   json_encode($r["i32_f64"]),   "\n";
    echo "  u8_u32=",    json_encode($r["u8_u32"]),    "\n";
    echo "  f64_str=",   json_encode($r["f64_str"]),   "\n";
    echo "  i16_i16=",   json_encode($r["i16_i16"]),   "\n";
}

$c->execute("DROP TABLE test.maps_matrix");
?>
--EXPECT--
lc env_present=1 hits_sum=99
id=1
  uuid_str={"00000000-0000-0000-0000-000000000001":"alice","00000000-0000-0000-0000-000000000002":"bob"}
  str_uuid={"alice":"00000000-0000-0000-0000-000000000001","bob":"00000000-0000-0000-0000-000000000002"}
  i32_f64={"-1":1.5,"2":2.5}
  u8_u32={"0":100,"255":200}
  f64_str={"1.5":"one-and-half"}
  i16_i16={"-32768":-1,"32767":1}
