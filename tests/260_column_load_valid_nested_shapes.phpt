--TEST--
Valid FixedString, Tuple, Nullable, Array, LowCardinality, and Map columns still load from a real server
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
require_once __DIR__ . "/_clickhouse.inc";
clickhouse_skip_if_no_server();
?>
--FILE--
<?php
require_once __DIR__ . "/_clickhouse.inc";

$client = new ClickHouse(clickhouse_test_config());
$rows = $client->select(
    "SELECT
        toFixedString(concat('ab', toString(number), 'z'), 4) AS f,
        tuple(toUInt8(number), toFixedString('wxyz', 4)) AS t,
        if(number = 1, NULL, toNullable(toFixedString('nnnn', 4))) AS n,
        toLowCardinality(toFixedString('lclc', 4)) AS lc,
        CAST(if(number = 1, NULL, toString(number)) AS LowCardinality(Nullable(String))) AS lcn,
        range(number) AS a,
        [] AS e,
        [range(number), []] AS aa,
        CAST(arrayMap(x -> toString(x), range(number)) AS Array(LowCardinality(String))) AS alc,
        CAST(arrayMap(x -> if(x = 0, NULL, toString(x)), range(number)) AS Array(Nullable(String))) AS an,
        CAST(arrayMap(x -> tuple(toUInt8(x), if(x = 1, NULL, toString(x))), range(number))
            AS Array(Tuple(UInt8, Nullable(String)))) AS at,
        map('k', number) AS m
    FROM numbers(4)
    ORDER BY number"
);
foreach ($rows as $row) {
    echo json_encode($row), "\n";
}
?>
--EXPECT--
{"f":"ab0z","t":[0,"wxyz"],"n":"nnnn","lc":"lclc","lcn":"0","a":[],"e":[],"aa":[[],[]],"alc":[],"an":[],"at":[],"m":{"k":0}}
{"f":"ab1z","t":[1,"wxyz"],"n":null,"lc":"lclc","lcn":null,"a":[0],"e":[],"aa":[[0],[]],"alc":["0"],"an":[null],"at":[[0,"0"]],"m":{"k":1}}
{"f":"ab2z","t":[2,"wxyz"],"n":"nnnn","lc":"lclc","lcn":"2","a":[0,1],"e":[],"aa":[[0,1],[]],"alc":["0","1"],"an":[null,"1"],"at":[[0,"0"],[1,null]],"m":{"k":2}}
{"f":"ab3z","t":[3,"wxyz"],"n":"nnnn","lc":"lclc","lcn":"3","a":[0,1,2],"e":[],"aa":[[0,1,2],[]],"alc":["0","1","2"],"an":[null,"1","2"],"at":[[0,"0"],[1,null],[2,"2"]],"m":{"k":3}}
