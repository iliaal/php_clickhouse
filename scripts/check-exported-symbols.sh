#!/usr/bin/env bash
set -euo pipefail

module=${1:-modules/clickhouse.so}
if [[ ! -f "$module" ]]; then
    echo "export guard: module not found: $module" >&2
    exit 1
fi

mapfile -t exports < <(nm -D --defined-only "$module" | awk '{print $3}' | sort -u)
if [[ ${#exports[@]} -ne 1 || ${exports[0]} != get_module ]]; then
    echo "export guard: expected only get_module, found ${#exports[@]} exported symbols:" >&2
    printf '  %s\n' "${exports[@]:0:20}" >&2
    if [[ ${#exports[@]} -gt 20 ]]; then
        echo "  ... $(( ${#exports[@]} - 20 )) more" >&2
    fi
    exit 1
fi
