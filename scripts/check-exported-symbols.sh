#!/usr/bin/env bash
set -euo pipefail

module=${1:-modules/clickhouse.so}
if [[ ! -f "$module" ]]; then
    echo "export guard: module not found: $module" >&2
    exit 1
fi

# Capture the pipeline directly: process substitution hides inspection failures.
if ! symbols=$(nm -D --defined-only "$module" | awk '{print $3}' | sort -u); then
    echo "export guard: failed to inspect module: $module" >&2
    exit 1
fi
exports=()
if [[ -n "$symbols" ]]; then
    mapfile -t exports <<<"$symbols"
fi
if [[ ${#exports[@]} -ne 1 || ${exports[0]} != get_module ]]; then
    echo "export guard: expected only get_module, found ${#exports[@]} exported symbols:" >&2
    printf '  %s\n' "${exports[@]:0:20}" >&2
    if [[ ${#exports[@]} -gt 20 ]]; then
        echo "  ... $(( ${#exports[@]} - 20 )) more" >&2
    fi
    exit 1
fi
