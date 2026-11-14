#!/usr/bin/env bash
set -euo pipefail

# Linux-only embed regression. PHP must be built with --enable-zts
# --enable-embed=shared. Reuse the PHP 8.4 ZTS CI build, not a new matrix.
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
    "$root/scripts/test-zts-request-thread.c" \
    -L"$prefix/lib" -Wl,-rpath,"$prefix/lib" -lphp -o "$work/test-zts-thread"
printf 'extension="%s"\n' "$module" >"$work/php.ini"
# Never inherit a system scan directory and accidentally load other modules.
PHP_INI_SCAN_DIR= timeout 30 "$work/test-zts-thread" "$work/php.ini"
