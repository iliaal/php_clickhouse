#!/usr/bin/env bash
#
# Verify the patch ledger, reverse application, and pristine upstream parity:
#   1. every patch file has a LOCAL_PATCHES.md heading, and vice versa
#   2. the patch stack reverse-applies against the vendored tree
#   3. what is left after the reverse-apply is pristine upstream
#
# Set CLICKHOUSE_CPP_PRISTINE to reuse an upstream checkout; otherwise clone the pinned tag.
#
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
vendored="$root/lib/clickhouse-cpp"
patch_dir="$root/patches/upstream"
manifest="$root/.upstream/clickhouse-cpp.yml"
doc="$vendored/LOCAL_PATCHES.md"

fail() { printf 'check-vendored-patches: %s\n' "$1" >&2; exit 1; }

[ -d "$patch_dir" ] || fail "missing $patch_dir"
[ -f "$doc" ] || fail "missing $doc"

mapfile -t patches < <(find "$patch_dir" -maxdepth 1 -name '*.patch' | sort)
[ "${#patches[@]}" -gt 0 ] || fail "no patch files found in $patch_dir"

# The document opens with an "Obsoleted in <version>" section for patches
# upstream has absorbed, which deliberately has no patch file.
doc_headings=$(grep -c '^## ' "$doc" || true)
obsolete_headings=$(grep -c '^## Obsoleted in ' "$doc" || true)
live_headings=$((doc_headings - obsolete_headings))
if [ "$live_headings" -ne "${#patches[@]}" ]; then
    fail "LOCAL_PATCHES.md documents $live_headings live modification(s) but $patch_dir holds ${#patches[@]} patch file(s)"
fi

if [ -f "$manifest" ]; then
    if ! grep -q "${#patches[@]} local patches" "$manifest"; then
        fail "$manifest note does not say '${#patches[@]} local patches'"
    fi
fi

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# Copy tracked sources only; in-tree build artifacts are not upstream files.
mapfile -t tracked < <(cd "$root" && git ls-files 'lib/clickhouse-cpp/clickhouse' 'lib/clickhouse-cpp/contrib')
[ "${#tracked[@]}" -gt 0 ] || fail "no tracked files under lib/clickhouse-cpp"
for rel in "${tracked[@]}"; do
    dest="$work/${rel#lib/clickhouse-cpp/}"
    mkdir -p "$(dirname "$dest")"
    cp "$root/$rel" "$dest"
done

# Reverse the sorted array without splitting paths at whitespace.
for ((i = ${#patches[@]} - 1; i >= 0; --i)); do
    p="${patches[i]}"
    if ! (cd "$work" && patch -p1 -R -f -s --no-backup-if-mismatch < "$p") >/dev/null 2>&1; then
        fail "$(basename "$p") does not reverse-apply against lib/clickhouse-cpp — the patch file and the tree have drifted"
    fi
done

pristine="${CLICKHOUSE_CPP_PRISTINE:-}"
if [ -z "$pristine" ]; then
    pinned=$(sed -n 's/^pinned: *//p' "$manifest" | head -1)
    repo=$(sed -n 's/^repo: *//p' "$manifest" | head -1)
    [ -n "$pinned" ] && [ -n "$repo" ] || fail "cannot read pinned/repo from $manifest"
    pristine="$work/pristine"
    if ! git clone -q --depth 1 --branch "v$pinned" "https://github.com/$repo.git" "$pristine" 2>/dev/null; then
        # Local offline runs may skip pristine comparison; CI must enforce it.
        if [ -n "${CI:-}" ]; then
            fail "could not clone v$pinned to compare against pristine upstream"
        fi
        printf 'check-vendored-patches: could not clone v%s; skipping the pristine comparison\n' "$pinned" >&2
        printf 'check-vendored-patches: patch/doc mapping and reverse-apply OK (%s patches)\n' "${#patches[@]}"
        exit 0
    fi
fi

# Vendoring omits upstream tests, benchmarks, and CI; compare only retained files.
for rel in "${tracked[@]}"; do
    vendored_rel="${rel#lib/clickhouse-cpp/}"
    if ! cmp -s "$work/$vendored_rel" "$pristine/$vendored_rel"; then
        fail "$vendored_rel differs from pristine upstream after reversing every patch — it was hand-edited without a patch file"
    fi
done

printf 'check-vendored-patches: OK (%s patches, tree reverses to pristine upstream)\n' "${#patches[@]}"
