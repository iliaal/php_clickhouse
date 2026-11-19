#!/usr/bin/env bash
set -euo pipefail

# Linux-only embed regression: a fatal error inside insert() conversion must
# not leave conversion state behind for the next request on the same thread.
# PHP must be built with --enable-zts --enable-embed=shared, and a ClickHouse
# server must be reachable through the CLICKHOUSE_* variables.
php_config=${PHP_CONFIG:-php-config}
module=${1:-modules/clickhouse.so}
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
module=$(realpath "$module")
[[ -f "$module" ]] || { echo "module not found: $module" >&2; exit 1; }
prefix=$("$php_config" --prefix)
read -r -a includes <<<"$("$php_config" --includes)"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

"${CC:-cc}" -D_GNU_SOURCE -Wall -Wextra -Werror -pthread "${includes[@]}" \
    "$root/scripts/test-zts-fatal-state.c" \
    -L"$prefix/lib" -Wl,-rpath,"$prefix/lib" -lphp -o "$work/test-zts-fatal-state"
printf 'extension="%s"\ndisplay_errors=1\nlog_errors=0\n' "$module" >"$work/php.ini"

cat >"$work/fatal.php" <<'PHP'
<?php
require getenv("CLICKHOUSE_TEST_INC");
class FatalValue
{
    public function __toString(): string
    {
        eval('function clickhouse_zts_dup() {} function clickhouse_zts_dup() {}');
        return "x";
    }
}
/* Thirty Tuple levels with a Nullable leaf raise both the NULL and the
 * depth state before __toString runs. */
$depth = 30;
$type = str_repeat("Tuple(", $depth) . "Nullable(String)" . str_repeat(")", $depth);
$leaf = new FatalValue;
for ($i = 0; $i < $depth; $i++) {
    $leaf = [$leaf];
}
$c = new ClickHouse(clickhouse_test_config());
$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS test.zts_fatal_state");
$c->execute("CREATE TABLE test.zts_fatal_state (s $type) ENGINE=Memory");
$c->insert("test.zts_fatal_state", ["s"], [[$leaf]]);
PHP

cat >"$work/probe.php" <<'PHP'
<?php
require getenv("CLICKHOUSE_TEST_INC");
$c = new ClickHouse(clickhouse_test_config());
$c->execute("DROP TABLE IF EXISTS test.zts_fatal_state");
try {
    $c->selectWithExternalData("SELECT v FROM ext", [
        ["name" => "ext", "columns" => ["v" => "Int32"], "rows" => [[null]]],
    ]);
    echo "probe: NULL accepted for Int32\n";
} catch (ClickHouseException $e) {
    echo "probe: ", $e->getMessage(), "\n";
}
foreach ($c->selectStream("SELECT [1, 2] AS a") as $row) {
    echo "probe: row ", json_encode($row), "\n";
}
PHP

out=$(CLICKHOUSE_TEST_INC="$root/tests/_clickhouse.inc" PHP_INI_SCAN_DIR='' \
    timeout 60 "$work/test-zts-fatal-state" "$work/php.ini" \
    "$work/fatal.php" "$work/probe.php" 2>&1) || true
printf '%s\n' "$out"
expected=$'probe: null cannot be assigned to non-Nullable column Int32\nprobe: row {"a":[1,2]}'
if ! grep -q 'Cannot redeclare' <<<"$out" || [[ "$(grep '^probe:' <<<"$out")" != "$expected" ]]; then
    echo "ZTS fatal-state regression failed" >&2
    exit 1
fi
echo "ZTS fatal-state regression passed"
