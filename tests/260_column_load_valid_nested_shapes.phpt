--TEST--
Valid FixedString, Tuple, Nullable, Array, and Map columns still load from a real server
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
        range(number) AS a,
        [] AS e,
        [range(number), []] AS aa,
        map('k', number) AS m
    FROM numbers(3)
    SETTINGS max_block_size = 1"
);
echo json_encode($rows, JSON_PRETTY_PRINT), "\n";
?>
--EXPECT--
[
    {
        "f": "ab0z",
        "t": [
            0,
            "wxyz"
        ],
        "n": "nnnn",
        "lc": "lclc",
        "a": [],
        "e": [],
        "aa": [
            [],
            []
        ],
        "m": {
            "k": 0
        }
    },
    {
        "f": "ab1z",
        "t": [
            1,
            "wxyz"
        ],
        "n": null,
        "lc": "lclc",
        "a": [
            0
        ],
        "e": [],
        "aa": [
            [
                0
            ],
            []
        ],
        "m": {
            "k": 1
        }
    },
    {
        "f": "ab2z",
        "t": [
            2,
            "wxyz"
        ],
        "n": "nnnn",
        "lc": "lclc",
        "a": [
            0,
            1
        ],
        "e": [],
        "aa": [
            [
                0,
                1
            ],
            []
        ],
        "m": {
            "k": 2
        }
    }
]
