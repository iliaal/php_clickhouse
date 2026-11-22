<?php
/* Prints a fixed type corpus through one php_clickhouse build as var_export
 * text; scripts/differential-decode.sh compares two builds' output byte for
 * byte. Every value is a literal, so the output depends only on the build.
 *
 *   decode           run every read path over the corpus
 *   write TAG        insert each case's decoded rows into test.diffdec_TAG_*
 *   tables TAG       read back the tables that `write TAG` filled
 *   drop TAG         drop those tables
 */
require __DIR__ . "/../tests/_clickhouse.inc";

$queries = 0;
$exceptions = 0;
/* One line per result, so that every differing line names its case. */
function out($label, $value)
{
    echo $label, " = ", addcslashes(var_export($value, true), "\n\\"), "\n";
}
function attempt($label, callable $fn)
{
    global $queries, $exceptions;
    ++$queries;
    try {
        out($label, $fn());
    } catch (Throwable $e) {
        ++$exceptions;
        echo $label, " ! ", get_class($e), ": ", $e->getMessage(), "\n";
    }
}
function literal($v, $raw)
{
    return $v === null ? "NULL" : ($raw ? $v : "'" . addcslashes($v, "'\\") . "'");
}

$big = "9223372036854775807";
$cases = [
    ["Int8", ["-128", "0", "127"]], ["UInt8", ["0", "255"]],
    ["Int16", ["-32768", "32767"]], ["UInt16", ["0", "65535"]],
    ["Int32", ["-2147483648", "2147483647"]], ["UInt32", ["0", "4294967295"]],
    ["Int64", ["-9223372036854775808", $big]], ["UInt64", ["0", "18446744073709551615"]],
    ["Int128", ["-170141183460469231731687303715884105728", "170141183460469231731687303715884105727"]],
    ["UInt128", ["0", "340282366920938463463374607431768211455"]],
    ["Int256", ["-57896044618658097711785492504343953926634992332820282019728792003956564819968",
                "57896044618658097711785492504343953926634992332820282019728792003956564819967"]],
    ["UInt256", ["0", "115792089237316195423570985008687907853269984665640564039457584007913129639935"]],
    ["Float32", ["-1.5", "0", "3.4028235e38", "1e-45", "nan", "inf", "-inf"]],
    ["Float64", ["-0.1", "1.7976931348623157e308", "5e-324", "nan", "-inf"]],
    ["BFloat16", ["1.5", "-2"]],
    ["Bool", ["true", "false"]],
    ["String", ["", "abc", "h\u{e9}llo \u{2713} \u{65e5}\u{672c}", "tab\tnl\nquote'\"back\\slash", "\x00\x01\xff"]],
    ["FixedString(1)", ["a", ""]], ["FixedString(16)", ["abc", "0123456789abcdef"]],
    ["Date", ["1970-01-01", "2024-02-29", "2149-06-06"]],
    ["Date32", ["1900-01-01", "2299-12-31"]],
    ["DateTime('UTC')", ["1970-01-01 00:00:00", "2106-02-07 06:28:15"]],
    ["DateTime('Asia/Kolkata')", ["2024-06-01 12:34:56"]],
    ["DateTime64(0, 'UTC')", ["1900-01-01 00:00:00", "2299-12-31 23:59:59"]],
    ["DateTime64(3, 'UTC')", ["1969-12-31 23:59:59.999", "2024-02-29 12:00:00.001"]],
    ["DateTime64(6, 'UTC')", ["2001-02-03 04:05:06.789012"]],
    ["DateTime64(9, 'UTC')", ["1900-01-01 00:00:00.000000001", "2262-04-11 23:47:16.854775807"]],
    ["Time", ["00:00:00", "12:34:56", "-12:34:56", "999:59:59", "-999:59:59"]],
    ["Time64(3)", ["-01:00:00.5", "23:59:59.999"]], ["Time64(9)", ["00:00:00.000000001"]],
    ["Decimal32(4)", ["-99999.9999", "0", "1.5"]], ["Decimal64(10)", ["-99999999.9999999999"]],
    ["Decimal128(20)", ["-999999999999999999.99999999999999999999", "0.00000000000000000001"]],
    ["Decimal256(40)", ["-1.0000000000000000000000000000000000000001", "123456789012345678901234567890.1234567890123456789012345678901234567890"]],
    ["Decimal(9, 0)", ["999999999"]], ["Decimal(38, 38)", ["0.99999999999999999999999999999999999999"]],
    ["Enum8('a' = -128, 'b' = 0, '\u{e9}' = 127)", ["a", "b", "\u{e9}"]],
    ["Enum16('x' = -32768, 'y' = 32767)", ["x", "y"]],
    ["UUID", ["00000000-0000-0000-0000-000000000000", "61f0c404-5cb3-11e7-907b-a6006ad3dba0", "ffffffff-ffff-ffff-ffff-ffffffffffff"]],
    ["IPv4", ["0.0.0.0", "127.0.0.1", "255.255.255.255"]],
    ["IPv6", ["::", "::1", "2001:db8::ff00:42:8329", "::ffff:1.2.3.4"]],
    ["Nullable(Int32)", [null, "-7"]], ["Nullable(String)", [null, ""]],
    ["Nullable(DateTime64(3, 'UTC'))", [null, "2020-01-01 00:00:00.5"]],
    ["Nullable(Decimal128(10))", [null, "1.5"]], ["Nullable(UUID)", [null, "61f0c404-5cb3-11e7-907b-a6006ad3dba0"]],
    ["LowCardinality(String)", ["a", "b", "a", ""]], ["LowCardinality(Nullable(String))", [null, "x", null]],
    ["LowCardinality(FixedString(2))", ["ab", "cd"]],
    ["Array(Int32)", ["[]", "[1,-2,3]"]], ["Array(Nullable(String))", ["[NULL,'a','']"]],
    ["Array(Array(UInt8))", ["[[],[1],[2,3]]"]], ["Array(LowCardinality(String))", ["['x','y','x']"]],
    ["Array(Decimal64(2))", ["[1.25,-3.5]"]], ["Array(DateTime64(3, 'UTC'))", ["['2024-01-01 00:00:00.123']"]],
    ["Tuple(Int32, String)", ["(1,'a')"]], ["Tuple(a Int32, b Nullable(String))", ["(1,NULL)", "(2,'z')"]],
    ["Tuple(Array(UInt8), Tuple(Float64, UUID))", ["([1,2],(1.5,'61f0c404-5cb3-11e7-907b-a6006ad3dba0'))"]],
    ["Map(String, Int32)", ["map()", "map('a', 1, 'b', -2)", "map('1', 1, '01', 2)"], true],
    ["Map(Int64, Array(String))", ["map(1, ['x'], -5, [])"], true],
    ["Map(String, Nullable(UInt64))", ["map('k', NULL, 'v', 7)"], true],
    ["Map(UUID, String)", ["map(toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), 'u')"], true],
    ["Map(LowCardinality(String), Map(String, UInt8))", ["map('o', map('i', 1))"], true],
    ["Map(String, Tuple(Float64, String))", ["map('t', (1.5, 'x'))"], true],
    ["Point", ["(1.5,-2.5)"]], ["Ring", ["[(0,0),(1,0),(1,1)]"]],
    ["LineString", ["[(0,0),(2,2)]"]], ["MultiLineString", ["[[(0,0),(1,1)],[(2,2)]]"]],
    ["Polygon", ["[[(0,0),(1,0),(1,1)],[(0.2,0.2)]]"]], ["MultiPolygon", ["[[[(0,0),(1,0),(1,1)]]]"]],
    ["JSON", ["{}", "{\"a\":1,\"b\":{\"c\":[1,2,3],\"d\":\"x\"},\"e\":null}"]],
    ["Nullable(JSON)", [null, "{\"k\":\"\u{e9}\"}"]],
    ["Variant(String, UInt64, Array(UInt8))", [null, "abc", "42", "[1,2]"]],
    ["Dynamic", [null, "abc", "42"]],
    ["SimpleAggregateFunction(sum, UInt64)", ["5"]],
];
$special = [
    "nothing" => "SELECT NULL AS v, [] AS e, [NULL] AS n",
    "long_string" => "SELECT repeat('\u{e9}x', 300000) AS v",
    "many_rows" => "SELECT number AS id, toString(number) AS s, number % 3 = 0 ? NULL : number AS n FROM numbers(70000)",
    "duplicate_names" => "SELECT a, a, b FROM (SELECT 1 AS a, 'x' AS b)",
    "numeric_names" => "SELECT 1, 'one' AS `2`, 3 AS `-4`",
    "nested_flat" => "SELECT n.a, n.b, n FROM test.diffdec_nested ORDER BY id",
    "nested_whole" => "SELECT n FROM test.diffdec_nested_whole ORDER BY id",
    "empty" => "SELECT number AS v FROM numbers(0)",
];
$settings = ["allow_experimental_json_type" => 1, "allow_experimental_variant_type" => 1,
             "allow_experimental_dynamic_type" => 1, "enable_time_time64_type" => 1,
             "allow_suspicious_low_cardinality_types" => 1, "output_format_native_write_json_as_string" => 1];
$sql = function ($case) {
    $rows = [];
    foreach ($case[1] as $id => $v) {
        $rows[] = "SELECT $id AS id, CAST(" . literal($v, !empty($case[2])) . " AS " . $case[0] . ") AS v";
    }
    return "SELECT v FROM (" . implode(" UNION ALL ", $rows) . ") ORDER BY id";
};
$flags = [
    "plain" => 0, "dates" => ClickHouse::DATE_AS_STRINGS, "uuid" => ClickHouse::UUID_WITH_DASHES,
    "fixed" => ClickHouse::FIXEDSTRING_BINARY, "pairs" => ClickHouse::MAP_AS_PAIRS,
    "json_array" => ClickHouse::JSON_AS_ARRAY, "json_object" => ClickHouse::JSON_AS_OBJECT,
    "column" => ClickHouse::FETCH_COLUMN, "one" => ClickHouse::FETCH_ONE,
];
$c = new ClickHouse(clickhouse_test_config());
$c->setSettings($settings);
$mode = $argv[1] ?? "";
$tag = $argv[2] ?? "";
$all = [];
foreach ($cases as $i => $case) {
    $all["c$i " . $case[0]] = $sql($case);
}
foreach ($special as $name => $query) {
    $all[$name] = $query;
}

if ($mode === "decode") {
    foreach (["diffdec_nested" => 1, "diffdec_nested_whole" => 0] as $table => $flatten) {
        $c->execute("DROP TABLE IF EXISTS test.$table");
        $c->execute("CREATE TABLE test.$table (id UInt8, n Nested(a Int32, b String)) ENGINE = Memory",
                    [], "", ["flatten_nested" => $flatten]);
        $c->execute($flatten ? "INSERT INTO test.$table VALUES (1, [1, 2], ['x', 'y']), (2, [], [])"
                             : "INSERT INTO test.$table VALUES (1, [(1, 'x'), (2, 'y')]), (2, [])");
    }
    foreach ($all as $label => $query) {
        foreach ($flags as $name => $flag) {
            attempt("$label select $name", function () use ($c, $query, $flag) {
                return $c->select($query, [], $flag);
            });
        }
        attempt("$label stream", function () use ($c, $query) {
            $rows = [];
            $it = $c->selectStream($query);
            foreach ($it as $k => $row) {
                $rows[$k] = $row;
            }
            return [count($it), $rows];
        });
        attempt("$label callback", function () use ($c, $query) {
            $rows = [];
            $c->selectStreamCallback($query, function ($row) use (&$rows) {
                $rows[] = $row;
            });
            return $rows;
        });
        foreach (["TSV", "CSVWithNames"] as $format) {
            attempt("$label to_stream $format", function () use ($c, $query, $format) {
                $fp = fopen("php://memory", "w+");
                $c->selectToStream($query, [], $fp, $format);
                rewind($fp);
                return stream_get_contents($fp);
            });
        }
        attempt("$label statement", function () use ($c, $query) {
            $s = $c->selectStatement($query);
            $stats = $s->statistics();
            return [count($s), $s->toArray(), $s->fetchOne(), $s->fetchColumn(), $stats["rows_read"], $stats["total_rows"]];
        });
    }
    $c->execute("DROP TABLE test.diffdec_nested");
    $c->execute("DROP TABLE test.diffdec_nested_whole");
} elseif ($mode === "write" && $tag !== "") {
    foreach ($cases as $i => $case) {
        $query = $sql($case);
        $rows = [];
        attempt("c$i " . $case[0] . " read", function () use ($c, $query, &$rows) {
            return $rows = $c->select($query, [], ClickHouse::FETCH_COLUMN);
        });
        foreach (["insert", "assoc", "write", "stream"] as $path) {
            $table = "test.diffdec_{$tag}_{$i}_{$path}";
            attempt("c$i " . $case[0] . " $path", function () use ($c, $table, $case, $rows, $path, $query) {
                $c->execute("DROP TABLE IF EXISTS $table");
                $c->execute("CREATE TABLE $table (id UInt32, v " . $case[0] . ") ENGINE = Memory");
                $tuples = [];
                foreach ($rows as $id => $v) {
                    $tuples[] = $path === "assoc" ? ["id" => $id, "v" => $v] : [$id, $v];
                }
                if ($path === "insert") {
                    return $c->insert($table, ["id", "v"], $tuples);
                } elseif ($path === "assoc") {
                    return $c->insertAssoc($table, $tuples);
                } elseif ($path === "write") {
                    $c->writeStart($table, ["id", "v"]);
                    $c->write($tuples);
                    return $c->writeEnd();
                }
                $fp = fopen("php://memory", "w+");
                $c->selectToStream("SELECT toUInt32(rowNumberInAllBlocks()) AS id, v FROM ($query)", [], $fp, "TSV");
                rewind($fp);
                return $c->insertFromStream($table, ["id", "v"], $fp, "TSV");
            });
        }
    }
} elseif ($mode === "tables" && $tag !== "") {
    foreach ($cases as $i => $case) {
        foreach (["insert", "assoc", "write", "stream"] as $path) {
            $table = "test.diffdec_{$tag}_{$i}_{$path}";
            attempt("c$i " . $case[0] . " $path", function () use ($c, $table) {
                return $c->select("SELECT id, v FROM $table ORDER BY id");
            });
        }
    }
} elseif ($mode === "drop" && $tag !== "") {
    foreach ($cases as $i => $case) {
        foreach (["insert", "assoc", "write", "stream"] as $path) {
            attempt("drop c$i $path", function () use ($c, $tag, $i, $path) {
                return $c->execute("DROP TABLE IF EXISTS test.diffdec_{$tag}_{$i}_{$path}");
            });
        }
    }
} else {
    fwrite(STDERR, "usage: differential-decode.php decode | write TAG | tables TAG | drop TAG\n");
    exit(2);
}
echo "queries: $queries, exceptions: $exceptions\n";
