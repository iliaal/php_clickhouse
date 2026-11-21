#!/usr/bin/env bash
set -euo pipefail

# Differential decode check between two backends: builds base-ref (default
# 0.14.0, the clickhouse-cpp backend) and head-ref (default HEAD) with one PHP,
# runs scripts/differential-decode.php through each against the ClickHouse
# server from the CLICKHOUSE_* variables, and compares the output byte for
# byte. Each backend also reads the tables the other inserted, so an insert
# bug that its own read path mirrors still shows up.
#
# usage: differential-decode.sh /path/to/php-config [base-ref] [head-ref]
# DIFF_KEEP=1 keeps the work directory; JOBS sets make's parallelism (4).
php_config=$1
base=${2:-0.14.0}
head=${3:-HEAD}
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
prefix=$("$php_config" --prefix)
php=$("$php_config" --php-binary)
work=$(mktemp -d)
if [[ ${DIFF_KEEP:-0} != 1 ]]; then
    trap 'rm -rf "$work"' EXIT
fi

build() {
    local ref=$1 dir="$work/$2"
    mkdir -p "$dir"
    git -C "$root" archive "$ref" | tar -x -C "$dir"
    if ! (cd "$dir" && "$prefix/bin/phpize" && ./configure --with-php-config="$php_config" \
        --enable-clickhouse && make -j"${JOBS:-4}") >"$dir.log" 2>&1; then
        tail -n 30 "$dir.log" >&2
        echo "build of $ref failed" >&2
        exit 1
    fi
}

run() {
    local so="$work/$1/modules/clickhouse.so" out="$work/$2"
    shift 2
    "$php" -n -d extension="$so" -d date.timezone=UTC -d memory_limit=1G \
        "$root/scripts/differential-decode.php" "$@" >"$out"
    if ! grep -Eq '^queries: [1-9][0-9]*, exceptions: [0-9]+$' "$out"; then
        echo "$1 $* produced no complete output" >&2
        exit 1
    fi
}

build "$base" base
build "$head" head
for side in base head; do
    run "$side" "$side.decode" decode
    run "$side" "$side.write" write "$side"
done
for reader in base head; do
    for writer in base head; do
        run "$reader" "$reader-$writer.tables" tables "$writer"
    done
done
for side in base head; do
    run "$side" "$side.drop" drop "$side"
done

# Deliberate changes since 0.14.0, listed in CHANGELOG.md: Decimal256, which
# neither backend decodes, fails as an unsupported type as soon as its column
# arrives; unsupported-type errors spell types as the server does.
allowed='^(c[0-9]+ Decimal256\(40\) |c[0-9]+ Array\(Decimal64\(2\)\) (stream|to_stream [A-Za-z]+) ! .*Decimal\(18, ?2\)|nothing to_stream [A-Za-z]+ ! .*Array\((Void|Nothing)\)|queries: )'
status=0
compare() {
    local delta="$work/$1-$2.diff" unexpected
    if diff "$work/$1" "$work/$2" >"$delta"; then
        return
    fi
    unexpected=$(sed -n 's/^[<>] //p' "$delta" | grep -Ev "$allowed" || true)
    if [[ -n "$unexpected" || "$(grep -o 'queries: [0-9]*' "$work/$1")" != "$(grep -o 'queries: [0-9]*' "$work/$2")" ]]; then
        echo "DIFFERENT: $1 vs $2" >&2
        head -n 60 "$delta" >&2
        status=1
    else
        echo "$1 vs $2: $(grep -c '^<' "$delta") documented differences"
    fi
}
compare base.decode head.decode
compare base.write head.write
compare base-base.tables base-head.tables
compare base-base.tables head-base.tables
compare base-base.tables head-head.tables
for file in base.decode base.write base-base.tables; do
    printf '%s: %s\n' "$file" "$(tail -n 1 "$work/$file")"
done
if [[ $status -eq 0 ]]; then
    echo "no undocumented differences between $base and $head"
fi
exit "$status"
