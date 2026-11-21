#!/usr/bin/env bash
set -euo pipefail

# Runs PHPT FILE sections whose regression is a native leak after a fatal
# error, each as a plain CLI process under LeakSanitizer. run-tests.php is not
# used: the runner itself is not a reliable leak-checking process boundary.
# Needs an ASan PHP, a ClickHouse server from the CLICKHOUSE_* variables, and
# the Zend allocator enabled, since memory_limit is not enforced without it.
#
# usage: test-fatal-leaks.sh /path/to/asan/php modules/clickhouse.so tests/NNN.phpt...
php=$1
module=$(realpath "$2")
shift 2
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
export USE_ZEND_ALLOC=1
export ASAN_OPTIONS=${ASAN_OPTIONS:-detect_odr_violation=0}:detect_leaks=1
export LSAN_OPTIONS=${LSAN_OPTIONS:-}${LSAN_OPTIONS:+:}exitcode=23
failed=0
for phpt in "$@"; do
    name=$(basename "$phpt" .phpt)
    script="$root/tests/.fatal-leak-$name.php"
    trap 'rm -f "$script"' EXIT
    awk '/^--FILE--$/ {f = 1; next} /^--[A-Z_]+--$/ {f = 0} f' "$phpt" >"$script"
    ini=()
    while IFS= read -r line; do
        [[ -n "$line" ]] && ini+=(-d "$line")
    done < <(awk '/^--INI--$/ {f = 1; next} /^--[A-Z_]+--$/ {f = 0} f' "$phpt")
    set +e
    out=$("$php" -n -d extension="$module" -d display_errors=1 -d report_memleaks=0 \
        "${ini[@]}" "$script" 2>&1)
    rc=$?
    set -e
    rm -f "$script"
    if [[ $rc -eq 23 ]] || grep -q 'ERROR: LeakSanitizer' <<<"$out"; then
        echo "LEAK $name"
        grep -E 'SUMMARY|ERROR: LeakSanitizer' <<<"$out" || true
        failed=1
    elif grep -q 'ERROR: AddressSanitizer' <<<"$out"; then
        echo "ASAN $name"
        printf '%s\n' "$out"
        failed=1
    else
        echo "ok   $name"
    fi
done
exit "$failed"
