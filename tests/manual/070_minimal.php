<?php
/**
 * Minimal self-contained repro for the Map(Float64, String) + non-C locale issue.
 *
 * The original 070.phpt would occasionally produce:
 *   Tracer caught signal 11
 *   ... LeakSanitizer fatal ...
 * under the project's ASAN DEBUG PHP build, after the keys were printed correctly.
 *
 * Usage (from repo root):
 *   RTK_DISABLED=1 \
 *   CLICKHOUSE_HOST=127.0.0.1 CLICKHOUSE_PORT=9000 \
 *   CLICKHOUSE_USER=test CLICKHOUSE_PASSWD=test \
 *   ~/php-install-PHP-8.4/bin/php \
 *       -d extension=modules/clickhouse.so \
 *       tests/manual/070_minimal.php
 *
 * For better ASAN output:
 *   ASAN_OPTIONS="print_stacktrace=1:abort_on_error=1:detect_leaks=0:handle_segv=1:fast_unwind_on_fatal=0" \
 *   LSAN_OPTIONS="verbosity=1:log_threads=1" \
 *   ... same as above ...
 */

function clickhouse_test_config(): array
{
    return [
        "host"        => getenv("CLICKHOUSE_HOST") ?: "127.0.0.1",
        "port"        => (int)(getenv("CLICKHOUSE_PORT") ?: "9000"),
        "user"        => getenv("CLICKHOUSE_USER") ?: "test",
        "passwd"      => getenv("CLICKHOUSE_PASSWD") ?: "test",
        "compression" => true,
    ];
}

$prev = setlocale(LC_NUMERIC, 0);
$applied = setlocale(LC_NUMERIC, 'de_DE.UTF-8', 'de_DE.utf8', 'de_DE');
echo "locale: ", ($applied === false ? "(not set)" : $applied), "\n";

echo "php sprintf 1.5: ", sprintf('%g', 1.5), "\n";

$config = clickhouse_test_config();
$c = new ClickHouse($config);

// Use a unique table name so repeated runs (or ASAN runs that leave the
// server in odd states) do not interfere with each other.
$tbl = "test.fmap_t_min_" . getmypid() . "_" . time();

$c->execute("CREATE DATABASE IF NOT EXISTS test");
$c->execute("DROP TABLE IF EXISTS $tbl");
$c->execute("CREATE TABLE $tbl (m Map(Float64, String)) ENGINE = Memory");
$c->execute("INSERT INTO $tbl VALUES (map(1.5, 'a', 0.1, 'b'))");

$rows = $c->select("SELECT m FROM $tbl");
$keys = array_keys($rows[0]["m"]);
sort($keys, SORT_STRING);
echo "key 0: ", $keys[0], "\n";
echo "key 1: ", $keys[1], "\n";

// Force the exact destruction sequence that was suspected in the original
// failures: release the result, run GC, restore locale, DROP via the same
// client, then release the client and GC again.
$rows = null;
gc_collect_cycles();
setlocale(LC_NUMERIC, $prev);
$c->execute("DROP TABLE $tbl");
$c = null;
gc_collect_cycles();

echo "clean exit\n";
