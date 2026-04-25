#!/bin/bash
# Fail if a "SeasClick" / "SEASCLICK" / "seasclick" reference shows up
# anywhere outside the documented compat surface. The compat surface is:
#
#   - php_clickhouse.h: CLICKHOUSE_RES_NAME_LEGACY / CLICKHOUSE_EXCEPTION_NAME_LEGACY
#     definitions and the file-header attribution
#   - clickhouse.cpp: alias-registration comments and the file-header
#     attribution
#   - tests/_clickhouse.inc: seasclick_test_config / seasclick_skip_if_no_server
#     back-compat function aliases
#   - tests/026.phpt: exercises the SeasClick / SeasClickException
#     BC aliases on purpose
#   - bench/bench_mark.php: file-header attribution
#   - composer.json: "Original author of SeasClick" credit string
#   - documentation files (README, CHANGELOG, CONTRIBUTING, SECURITY,
#     CREDITS): historical references are fine and expected
#
# Anywhere else, "SeasClick" usually means a rename was missed.
#
# Vendored library (lib/clickhouse-cpp/) and build artifacts are
# excluded.

set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

UNEXPECTED=$(grep -rn "SeasClick\|SEASCLICK\|seasclick" \
    --include="*.cpp" --include="*.hpp" --include="*.h" \
    --include="*.m4" --include="*.yml" --include="*.json" \
    --include="*.phpt" --include="*.inc" --include="*.php" \
    . 2>/dev/null \
    | grep -v '/lib/clickhouse-cpp/' \
    | grep -v '/scripts/' \
    | grep -v '\.dep$' \
    | grep -v '\.loT$' \
    | grep -vE '^\./php_clickhouse\.h:.*(Original SeasClick|RES_NAME_LEGACY|EXCEPTION_NAME_LEGACY|aliases for the original SeasClick)' \
    | grep -vE '^\./clickhouse\.cpp:.*(Original SeasClick|aliases for the original SeasClick)' \
    | grep -vE '^\./tests/_clickhouse\.inc:.*(seasclick_test_config|seasclick_skip_if_no_server)' \
    | grep -vE '^\./tests/026\.phpt:' \
    | grep -vE '^\./bench/bench_mark\.php:.*for SeasClick\.' \
    | grep -vE '^\./composer\.json:.*(SeasX Group \(original SeasClick\)|Original author of SeasClick)')

if [ -n "$UNEXPECTED" ]; then
    echo "::error::Unexpected SeasClick reference(s) found outside the documented compat surface:"
    echo "$UNEXPECTED"
    echo
    echo "If a reference is intentional, allowlist it in scripts/check-no-seasclick.sh."
    exit 1
fi

echo "OK: no unexpected SeasClick references."
