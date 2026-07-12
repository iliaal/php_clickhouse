#!/usr/bin/env bash
set -Eeuo pipefail
shopt -s inherit_errexit

readonly EX_USAGE=64

usage() {
	printf 'Usage: %s <run-tests-output> [allowed-skip.phpt ...]\n' "${0##*/}" >&2
}

main() {
	if (($# < 1)); then
		usage
		return "${EX_USAGE}"
	fi

	local -r output_file=$1
	shift
	if [[ ! -r "${output_file}" ]]; then
		printf 'PHPT output is not readable: %s\n' "${output_file}" >&2
		return 1
	fi

	local passed="" skipped="" line
	local -a observed=()
	while IFS= read -r line; do
		if [[ -z "${passed}" && "${line}" =~ Tests[[:space:]]+passed[[:space:]]*:[[:space:]]*([0-9]+) ]]; then
			passed=${BASH_REMATCH[1]}
		fi
		if [[ -z "${skipped}" && "${line}" =~ Tests[[:space:]]+skipped[[:space:]]*:[[:space:]]*([0-9]+) ]]; then
			skipped=${BASH_REMATCH[1]}
		fi
		if [[ "${line}" =~ SKIP.*\[(tests/[^]]+\.phpt)\] ]]; then
			observed+=("${BASH_REMATCH[1]}")
		fi
	done <"${output_file}"

	if [[ ! "${passed}" =~ ^[0-9]+$ || ! "${skipped}" =~ ^[0-9]+$ ]]; then
		printf 'Could not parse PHPT passed/skipped counts from %s\n' "${output_file}" >&2
		return 1
	fi
	if ((passed == 0)); then
		printf 'PHPT run executed zero passing tests\n' >&2
		return 1
	fi

	declare -A allowed=()
	local path
	for path in "$@"; do
		allowed["${path}"]=1
	done

	if ((${#observed[@]} != skipped)); then
		printf 'PHPT summary reports %d skips but %d test paths were parsed\n' \
			"${skipped}" "${#observed[@]}" >&2
		return 1
	fi
	for path in "${observed[@]}"; do
		if [[ ! -v "allowed[${path}]" ]]; then
			printf 'Unexpected skipped PHPT: %s\n' "${path}" >&2
			return 1
		fi
	done

	printf 'PHPT summary accepted: %d passed, %d allowed skipped\n' "${passed}" "${skipped}"
}

[[ "${BASH_SOURCE[0]}" == "$0" ]] && main "$@"
