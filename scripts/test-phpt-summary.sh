#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
TEMP_DIR=$(mktemp -d)
readonly SCRIPT_DIR TEMP_DIR
trap 'rm -rf "${TEMP_DIR}"' EXIT

# Match run-tests.php's summary format. BORKED and LEAKED lines are optional.
summary() {
	printf 'Tests skipped   : %5d (  0.0%%) --------\n' "${1:-0}"
	printf 'Tests warned    : %5d (  0.0%%) (  0.0%%)\n' "${2:-0}"
	printf 'Tests failed    : %5d (  0.0%%) (  0.0%%)\n' "${3:-0}"
	printf 'Tests passed    : %5d (100.0%%) (100.0%%)\n' "${4:-1}"
}

accept() {
	"${SCRIPT_DIR}/assert-phpt-summary.sh" "${TEMP_DIR}/summary" "$@" >"${TEMP_DIR}/stdout"
}

reject() {
	local expected=$1
	shift
	if "${SCRIPT_DIR}/assert-phpt-summary.sh" "${TEMP_DIR}/summary" "$@" \
		>"${TEMP_DIR}/stdout" 2>"${TEMP_DIR}/stderr"; then
		printf 'PHPT summary guard unexpectedly accepted: %s\n' "${expected}" >&2
		exit 1
	fi
	grep -Fq -- "${expected}" "${TEMP_DIR}/stderr"
}

summary >"${TEMP_DIR}/summary"
accept
printf 'Tests leaked    :     0 (  0.0%%) (  0.0%%)\n' >>"${TEMP_DIR}/summary"
accept

for outcome in borked leaked; do
	summary >"${TEMP_DIR}/summary"
	printf 'Tests %s    :     1 ( 50.0%%)\n' "${outcome}" >>"${TEMP_DIR}/summary"
	reject "PHPT run reported 1 ${outcome} test(s)"
done

summary 0 0 1 >"${TEMP_DIR}/summary"
reject 'PHPT run reported 1 failed test(s)'
summary 0 1 >"${TEMP_DIR}/summary"
reject 'PHPT run reported 1 warned test(s)'
summary 0 0 0 0 >"${TEMP_DIR}/summary"
reject 'PHPT run executed zero passing tests'

summary 1 >"${TEMP_DIR}/summary"
reject 'PHPT summary reports 1 skips but 0 test paths were parsed'
printf 'SKIP example [tests/027.phpt]\n' >>"${TEMP_DIR}/summary"
reject 'Unexpected skipped PHPT: tests/027.phpt'
accept tests/027.phpt

# A truncated or otherwise incomplete run must not pass the gate.
for field in passed skipped failed warned; do
	summary | grep -v "Tests ${field}" >"${TEMP_DIR}/summary"
	reject 'Could not parse PHPT'
done

printf 'PHPT summary guard self-test passed\n'
