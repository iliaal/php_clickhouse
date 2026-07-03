--TEST--
ClickHouse placeholder substitution rejects malformed params branch by branch
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php require __DIR__ . "/_clickhouse.inc"; clickhouse_skip_if_no_server(); ?>
--FILE--
<?php
require __DIR__ . "/_clickhouse.inc";

// Regression for CR-017: exercise each rejection branch of applyPlaceholders
// (integer key, empty key, unterminated typed placeholder, key with no
// matching {name} in the SQL, empty array, nested-array element, and a
// multi-token client-side value) so a refactor cannot silently loosen one.

$c = new ClickHouse(clickhouse_test_config());

function reject($c, $sql, $params): string
{
    try {
        $c->select($sql, $params, ClickHouse::FETCH_ONE);
        return "NO THROW";
    } catch (ClickHouseException $e) {
        return $e->getMessage();
    }
}

echo reject($c, "SELECT {p}",       ["col"]),           "\n"; // integer key 0
echo reject($c, "SELECT 1",         ["" => "x"]),        "\n"; // empty key
echo reject($c, "SELECT {p:String", ["p" => "x"]),       "\n"; // unterminated type
echo reject($c, "SELECT 1",         ["p" => "x"]),       "\n"; // no {p} in SQL
echo reject($c, "SELECT {c}",       ["c" => []]),        "\n"; // empty array
echo reject($c, "SELECT {c}",       ["c" => [["x"]]]),   "\n"; // nested array element
echo reject($c, "SELECT {c}",       ["c" => "a, b"]),    "\n"; // multi-token value
?>
--EXPECT--
Placeholder array keys must be strings
Placeholder array keys must be non-empty
Unterminated typed placeholder for {p}
Placeholder {p} does not appear in the SQL
Placeholder value for {c} is invalid: empty array
Placeholder value for {c} is invalid: a list element must not be an array
Placeholder value for {c} is invalid: only one identifier or numeric literal per token; use array-valued placeholders for column lists
