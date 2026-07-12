#!/usr/bin/env bash
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
#   - tests/051.phpt: server-free surface smoke; verifies the legacy
#     class-alias names resolve as part of the public surface check
#   - bench/bench_mark.php: file-header attribution
#   - composer.json: "Original author of SeasClick" credit string
#   - documentation files (README, CHANGELOG, CONTRIBUTING, SECURITY,
#     CREDITS): historical references are fine and expected
#
# Anywhere else, "SeasClick" usually means a rename was missed.
#
# Vendored library (lib/clickhouse-cpp/) and build artifacts are
# excluded.

set -Eeuo pipefail
shopt -s inherit_errexit

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
readonly ROOT
_tmpfile=$(mktemp)
trap 'rm -f -- "${_tmpfile:-}"' EXIT
cd "${ROOT}"

git ls-files -z --cached --others --exclude-standard -- \
	'*.cpp' '*.hpp' '*.h' '*.m4' '*.yml' '*.json' '*.phpt' '*.inc' '*.php' \
	>"${_tmpfile}"
mapfile -d '' CANDIDATE_FILES <"${_tmpfile}"
SOURCE_FILES=()
for path in "${CANDIDATE_FILES[@]}"; do
	[[ -f "${path}" ]] && SOURCE_FILES+=("${path}")
done

RAW_MATCHES=""
if ((${#SOURCE_FILES[@]} > 0)); then
	set +e
	RAW_MATCHES=$(grep -nH -E "SeasClick|SEASCLICK|seasclick" "${SOURCE_FILES[@]}")
	GREP_STATUS=$?
	set -e
	if ((GREP_STATUS > 1)); then
		printf '%s\n' "::error::Failed to scan repository source files for legacy names." >&2
		exit "${GREP_STATUS}"
	fi
fi

UNEXPECTED=$(printf '%s\n' "${RAW_MATCHES}" |
	grep -v '^lib/clickhouse-cpp/' |
	grep -v '^scripts/' |
	grep -vE '^php_clickhouse\.h:.*(Original SeasClick|RES_NAME_LEGACY|EXCEPTION_NAME_LEGACY|aliases for the original SeasClick)' |
	grep -vE '^clickhouse\.cpp:.*(Original SeasClick|aliases for the original SeasClick)' |
	grep -vE '^tests/_clickhouse\.inc:.*(seasclick_test_config|seasclick_skip_if_no_server)' |
	grep -vE '^tests/026\.phpt:' |
	grep -vE '^tests/051\.phpt:' |
	grep -vE '^bench/bench_mark\.php:.*for SeasClick\.' |
	grep -vE '^\.github/workflows/tests\.yml:.*scripts/check-no-seasclick\.sh' |
	grep -vE '^composer\.json:.*(SeasX Group \(original SeasClick\)|Original author of SeasClick)' ||
	true)

if [[ -n "${UNEXPECTED}" ]]; then
	printf '%s\n' "::error::Unexpected SeasClick reference(s) found outside the documented compat surface:"
	printf '%s\n' "${UNEXPECTED}"
	printf '\n'
	printf '%s\n' "If a reference is intentional, allowlist it in scripts/check-no-seasclick.sh."
	exit 1
fi

printf '%s\n' "OK: no unexpected SeasClick references."
