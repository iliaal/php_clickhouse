#!/usr/bin/env bash
set -Eeuo pipefail
shopt -s inherit_errexit

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
readonly root
work=$(mktemp -d)
trap 'rm -rf -- "${work}"' EXIT
fixture="${work}/checkout with spaces"
pristine="${work}/pristine with spaces"
codecs="${work}/codec pristine with spaces"
mkdir -p -- "${fixture}"/{scripts,patches/clickhouse-c,.upstream,lib/clickhouse-c,lib/compression/lz4,lib/compression/patches} \
	"${pristine}" "${codecs}/contrib/lz4/lz4"
cp -- "${root}/scripts/check-vendored-patches.sh" "${fixture}/scripts/"
printf 'original\n' >"${pristine}/example.h"
printf 'second\n' >"${fixture}/lib/clickhouse-c/example.h"
printf 'extra\n' >"${fixture}/lib/clickhouse-c/compat.h"
printf '## 0001-first patch.patch\n## 0002-second patch.patch\n' >"${fixture}/lib/clickhouse-c/LOCAL_PATCHES.md"
printf 'pinned: 0000000000000000000000000000000000000000\nrepo: ClickHouse/clickhouse-c\n' >"${fixture}/.upstream/clickhouse-c.yml"
cat >"${fixture}/patches/clickhouse-c/0001-first patch.patch" <<'PATCH'
--- a/example.h
+++ b/example.h
@@ -1 +1 @@
-original
+first
PATCH
cat >"${fixture}/patches/clickhouse-c/0002-second patch.patch" <<'PATCH'
--- a/example.h
+++ b/example.h
@@ -1 +1 @@
-first
+second
--- /dev/null
+++ b/compat.h
@@ -0,0 +1 @@
+extra
--- /dev/null
+++ b/LOCAL_PATCHES.md
@@ -0,0 +1,2 @@
+## 0001-first patch.patch
+## 0002-second patch.patch
PATCH

printf 'original codec\n' >"${codecs}/contrib/lz4/lz4/example.c"
printf 'patched codec\n' >"${fixture}/lib/compression/lz4/example.c"
printf '## 0001-codec.patch\n' >"${fixture}/lib/compression/LOCAL_PATCHES.md"
cat >"${fixture}/lib/compression/patches/0001-codec.patch" <<'PATCH'
--- a/lz4/example.c
+++ b/lz4/example.c
@@ -1 +1 @@
-original codec
+patched codec
PATCH

git init -q "${fixture}"
git -C "${fixture}" add .
checker=(env "CLICKHOUSE_C_PRISTINE=${pristine}" "CLICKHOUSE_CODECS_PRISTINE=${codecs}"
	bash "${fixture}/scripts/check-vendored-patches.sh")
expect_failure() {
	local message=$1
	if "${checker[@]}" >"${work}/stdout" 2>"${work}/stderr"; then
		printf 'Expected checker failure: %s\n' "${message}" >&2
		exit 1
	fi
	grep -Fq -- "${message}" "${work}/stderr"
}

# Run outside the checkout; patch ordering and newly added files must survive.
(cd -- "${work}" && "${checker[@]}") >"${work}/stdout" 2>"${work}/stderr"
grep -Fxq 'check-vendored-patches: OK (protocol and codec trees reverse to pristine upstream)' "${work}/stdout"

printf 'unexpected\n' >"${fixture}/lib/clickhouse-c/example.h"
expect_failure '0002-second patch.patch does not reverse-apply'
printf 'second\n' >"${fixture}/lib/clickhouse-c/example.h"

printf 'different upstream\n' >"${pristine}/example.h"
expect_failure 'clickhouse-c differs from pristine upstream'
printf 'original\n' >"${pristine}/example.h"

printf '## 0002-second patch.patch\n' >"${fixture}/lib/clickhouse-c/LOCAL_PATCHES.md"
expect_failure 'does not document every patch'
printf '## 0001-first patch.patch\n## 0002-second patch.patch\n' >"${fixture}/lib/clickhouse-c/LOCAL_PATCHES.md"

printf 'unexpected source\n' >"${fixture}/lib/clickhouse-c/unlisted.h"
expect_failure 'clickhouse-c differs from pristine upstream'
rm -- "${fixture}/lib/clickhouse-c/unlisted.h"

printf 'unexpected codec\n' >"${fixture}/lib/compression/lz4/example.c"
expect_failure '0001-codec.patch does not reverse-apply'
printf 'patched codec\n' >"${fixture}/lib/compression/lz4/example.c"

printf 'different upstream codec\n' >"${codecs}/contrib/lz4/lz4/example.c"
expect_failure 'lib/compression/lz4/example.c differs from pristine upstream'

printf 'vendored patch checker self-test passed (7 cases)\n'
