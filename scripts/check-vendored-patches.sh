#!/usr/bin/env bash
# Verify documented patches, reverse application, and pristine upstream parity.
# CLICKHOUSE_C_PRISTINE and CLICKHOUSE_CODECS_PRISTINE can reuse local checkouts.
set -Eeuo pipefail
shopt -s inherit_errexit

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
readonly root
work=""
trap 'rm -rf -- "${work:-}"' EXIT

fail() {
	printf 'check-vendored-patches: %s\n' "$1" >&2
	exit 1
}

copy_sources() {
	local destination=$1
	shift
	local rel target
	git -C "${root}" ls-files -z --cached --others --exclude-standard -- "$@" >"${work}/files"
	mapfile -d '' sources <"${work}/files"
	((${#sources[@]} > 0)) || fail "no source files under $*"
	for rel in "${sources[@]}"; do
		[[ -f "${root}/${rel}" ]] || fail "missing ${rel}"
		target="${destination}/${rel#lib/}"
		mkdir -p -- "${target%/*}"
		cp -- "${root}/${rel}" "${target}"
	done
}

reverse_patches() {
	local directory=$1 patch_dir=$2 doc=$3
	local patch heading count=0
	local -a patches=()
	[[ -d "${patch_dir}" ]] || fail "missing ${patch_dir}"
	[[ -f "${doc}" ]] || fail "missing ${doc}"
	shopt -s nullglob
	patches=("${patch_dir}"/*.patch)
	shopt -u nullglob
	((${#patches[@]} > 0)) || fail "no patch files found in ${patch_dir}"
	while IFS= read -r heading; do
		[[ "${heading}" == "## "* ]] || continue
		heading=${heading#\#\# }
		[[ -f "${patch_dir}/${heading}" ]] || fail "undocumented patch heading ${heading}"
		((count += 1))
	done <"${doc}"
	((count == ${#patches[@]})) || fail "${doc} does not document every patch"
	for patch in "${patches[@]}"; do
		grep -Fxq -- "## ${patch##*/}" "${doc}" || fail "missing heading for ${patch##*/}"
	done
	for ((count = ${#patches[@]} - 1; count >= 0; --count)); do
		patch=${patches[count]}
		if ! (cd -- "${directory}" && patch -p1 -R -f -s --no-backup-if-mismatch <"${patch}") >"${work}/patch.out" 2>&1; then
			fail "${patch##*/} does not reverse-apply against ${directory}"
		fi
	done
}

pristine_tree() {
	local destination=$1 checkout=$2 repo=$3 revision=$4
	if [[ -z "${checkout}" ]]; then
		checkout="${destination}-checkout"
		git init -q "${checkout}"
		git -C "${checkout}" remote add origin "https://github.com/${repo}.git"
		git -C "${checkout}" fetch -q --depth 1 origin "${revision}" ||
			fail "could not fetch ${repo} at ${revision}"
		git -C "${checkout}" checkout -q --detach FETCH_HEAD
	fi
	mkdir -p -- "${destination}"
	if [[ -d "${checkout}/.git" || -f "${checkout}/.git" ]]; then
		git -C "${checkout}" archive --format=tar HEAD | tar -xf - -C "${destination}"
	else
		cp -a -- "${checkout}/." "${destination}/"
	fi
}

main() {
	local pinned="" repo="" key value rel source
	local -a sources=()
	work=$(mktemp -d)
	copy_sources "${work}/source" lib/clickhouse-c
	reverse_patches "${work}/source/clickhouse-c" "${root}/patches/clickhouse-c" "${root}/lib/clickhouse-c/LOCAL_PATCHES.md"

	while IFS=' ' read -r key value; do
		case "${key}" in
		pinned:) pinned=${value} ;;
		repo:) repo=${value} ;;
		*) ;;
		esac
	done <"${root}/.upstream/clickhouse-c.yml"
	[[ "${pinned}" =~ ^[0-9a-f]{40}$ ]] || fail "invalid clickhouse-c pinned revision"
	[[ "${repo}" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]] || fail "invalid clickhouse-c repository"
	pristine_tree "${work}/protocol-pristine" "${CLICKHOUSE_C_PRISTINE:-}" "${repo}" "${pinned}"
	if ! diff -qr -- "${work}/source/clickhouse-c" "${work}/protocol-pristine" >"${work}/diff.out"; then
		cat "${work}/diff.out" >&2
		fail "clickhouse-c differs from pristine upstream after reversing every patch"
	fi

	copy_sources "${work}/source" lib/compression/lz4 lib/compression/zstd
	reverse_patches "${work}/source/compression" "${root}/lib/compression/patches" "${root}/lib/compression/LOCAL_PATCHES.md"
	pristine_tree "${work}/codec-pristine" "${CLICKHOUSE_CODECS_PRISTINE:-}" ClickHouse/clickhouse-cpp v2.6.2
	for rel in "${sources[@]}"; do
		source=${rel#lib/compression/}
		case "${source}" in
		lz4/*) source="contrib/lz4/lz4/${source#lz4/}" ;;
		zstd/*) source="contrib/zstd/zstd/${source#zstd/}" ;;
		*) fail "unexpected codec source ${rel}" ;;
		esac
		if ! cmp -s -- "${work}/source/${rel#lib/}" "${work}/codec-pristine/${source}"; then
			fail "${rel} differs from pristine upstream after reversing every patch"
		fi
	done
	printf 'check-vendored-patches: OK (protocol and codec trees reverse to pristine upstream)\n'
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
	main "$@"
fi
