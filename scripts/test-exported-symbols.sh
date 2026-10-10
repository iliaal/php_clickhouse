#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
TEMP_DIR=$(mktemp -d)
trap 'rm -rf "$TEMP_DIR"' EXIT
mkdir "$TEMP_DIR/bin"
touch "$TEMP_DIR/clickhouse.so"
cat >"$TEMP_DIR/bin/nm" <<'NM'
#!/usr/bin/env bash
printf '%s' "${NM_OUTPUT:-}"
exit "${NM_STATUS:-0}"
NM
chmod +x "$TEMP_DIR/bin/nm"
export PATH="$TEMP_DIR/bin:$PATH"

check() {
    local expected=$1 label=$2 output=$3 status=$4 diagnostic=${5:-}
    local actual=0
    NM_OUTPUT="$output" NM_STATUS="$status" \
        "$SCRIPT_DIR/check-exported-symbols.sh" "$TEMP_DIR/clickhouse.so" \
        >"$TEMP_DIR/output" 2>&1 || actual=$?
    if [[ "$actual" -ne "$expected" ]]; then
        echo "export guard self-test: $label: expected $expected, got $actual" >&2
        cat "$TEMP_DIR/output" >&2
        exit 1
    fi
    if [[ -n "$diagnostic" ]]; then
        grep -Fq "$diagnostic" "$TEMP_DIR/output"
    fi
}

check 0 'single entry point' $'00000000 T get_module\n' 0
check 0 'duplicate entry point' $'00000000 T get_module\n00000000 T get_module\n' 0
check 1 'additional export' $'00000000 T get_module\n00000001 T unexpected\n' 0 'found 2 exported symbols'
check 1 'wrong export' $'00000000 T unexpected\n' 0 'expected only get_module'
check 1 'no exports' '' 0 'found 0 exported symbols'
check 1 'inspection failure' '' 1 'failed to inspect module'
check 1 'partial output before failure' $'00000000 T get_module\n' 1 'failed to inspect module'

if "$SCRIPT_DIR/check-exported-symbols.sh" "$TEMP_DIR/missing.so" >"$TEMP_DIR/output" 2>&1; then
    echo 'export guard self-test: missing module unexpectedly passed' >&2
    exit 1
fi
grep -Fq 'module not found' "$TEMP_DIR/output"
printf 'exported-symbol guard self-test passed (8 cases)\n'
