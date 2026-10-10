#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
fixture="$work/checkout with spaces"
pristine="$work/pristine with spaces"
mkdir -p "$fixture"/{scripts,patches/upstream,.upstream,lib/clickhouse-cpp/clickhouse} "$pristine/clickhouse"
cp "$root/scripts/check-vendored-patches.sh" "$fixture/scripts/"
printf 'original\n' > "$pristine/clickhouse/example.cpp"
printf 'second\n' > "$fixture/lib/clickhouse-cpp/clickhouse/example.cpp"
printf '## First patch\n## Second patch\n' > "$fixture/lib/clickhouse-cpp/LOCAL_PATCHES.md"
printf 'note: "2 local patches"\n' > "$fixture/.upstream/clickhouse-cpp.yml"
cat > "$fixture/patches/upstream/0001-first patch.patch" <<'PATCH'
--- a/clickhouse/example.cpp
+++ b/clickhouse/example.cpp
@@ -1 +1 @@
-original
+first
PATCH
cat > "$fixture/patches/upstream/0002-second patch.patch" <<'PATCH'
--- a/clickhouse/example.cpp
+++ b/clickhouse/example.cpp
@@ -1 +1 @@
-first
+second
PATCH

git init -q "$fixture"
git -C "$fixture" add .
run_check() {
    CLICKHOUSE_CPP_PRISTINE="$pristine" bash "$fixture/scripts/check-vendored-patches.sh"
}

# Both checkout and patch names contain spaces. The two patches modify the
# same line, so success also proves that they were reversed in stack order.
run_check > "$work/stdout" 2> "$work/stderr"
grep -Fxq 'check-vendored-patches: OK (2 patches, tree reverses to pristine upstream)' "$work/stdout"

# A genuine patch/tree mismatch must still be rejected.
printf 'unexpected\n' > "$fixture/lib/clickhouse-cpp/clickhouse/example.cpp"
if run_check > "$work/stdout" 2> "$work/stderr"; then
    echo 'Expected a mismatched patch to fail' >&2
    exit 1
fi
grep -Fq '0002-second patch.patch does not reverse-apply' "$work/stderr"
printf 'second\n' > "$fixture/lib/clickhouse-cpp/clickhouse/example.cpp"

# Successful reverse application alone is not enough: enforce pristine parity.
printf 'different upstream\n' > "$pristine/clickhouse/example.cpp"
if run_check > "$work/stdout" 2> "$work/stderr"; then
    echo 'Expected pristine drift to fail' >&2
    exit 1
fi
grep -Fq 'example.cpp differs from pristine upstream' "$work/stderr"

printf 'vendored patch checker self-test passed (3 cases)\n'
