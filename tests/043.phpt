--TEST--
ClickHouse Geo types: Point, Ring, Polygon, MultiPolygon round-trip
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.geo_t");
$c->execute("SET allow_experimental_geo_types = 1");
$c->execute("CREATE TABLE test.geo_t (
    id UInt32,
    p Point,
    r Ring,
    poly Polygon,
    mp MultiPolygon
) ENGINE = Memory");

$c->insert("test.geo_t", ["id", "p", "r", "poly", "mp"], [
    [
        1,
        [10.5, 20.5],                                                 // Point
        [[0, 0], [1, 0], [1, 1], [0, 1]],                             // Ring
        [[[0, 0], [4, 0], [4, 4], [0, 4]], [[1, 1], [2, 1], [2, 2]]], // Polygon = list of rings
        [                                                             // MultiPolygon = list of polygons
            [[[0, 0], [1, 0], [1, 1]]],
            [[[10, 10], [11, 10], [11, 11]]],
        ],
    ],
]);

$rows = $c->select("SELECT id, p, r, poly, mp FROM test.geo_t WHERE id = 1");
$row = $rows[0];
echo "id=", $row["id"], "\n";
echo "p=", json_encode($row["p"]), "\n";
echo "r=", json_encode($row["r"]), "\n";
echo "poly=", json_encode($row["poly"]), "\n";
echo "mp=", json_encode($row["mp"]), "\n";

$c->execute("DROP TABLE test.geo_t");
?>
--EXPECT--
id=1
p=[10.5,20.5]
r=[[0,0],[1,0],[1,1],[0,1]]
poly=[[[0,0],[4,0],[4,4],[0,4]],[[1,1],[2,1],[2,2]]]
mp=[[[[0,0],[1,0],[1,1]]],[[[10,10],[11,10],[11,11]]]]
