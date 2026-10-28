#!/usr/bin/env bash
set -uo pipefail

cd "$(dirname "$0")" || exit 1

CC=${CC:-cc}
COVERAGE=${COVERAGE:-0}
if [[ $COVERAGE == 1 ]]; then
    command -v gcov >/dev/null || { echo "coverage requires gcov" >&2; exit 1; }
    rm -rf coverage
    mkdir coverage || exit 1
fi

# liburing present? probe once; gates the real test_async_uring body.
have_liburing() {
    local cflags=() ldflags=()
    read -r -a cflags <<< "${CFLAGS:-}"
    read -r -a ldflags <<< "${LDFLAGS:-}"
    printf '#include <liburing.h>\nint main(void){return 0;}\n' \
        | "$CC" -x c "${cflags[@]}" "${ldflags[@]}" -o /dev/null - -luring >/dev/null 2>&1
}
if have_liburing; then uring=1; else uring=0; fi

# Per-test output buffered to its own log; main thread replays logs in test
# order so parallel runs stay readable. .fail marker => that test failed.
logdir=$(mktemp -d "${TMPDIR:-/tmp}/chc_test.XXXXXX") || exit 1
trap 'rm -rf "$logdir"' EXIT

run_one() {
    local name=$1
    local src=test/test_${name}.c
    local bin=$logdir/$name
    local log=$logdir/$name.log

    if [[ ! -f $src ]]; then
        echo "no such test: $name" > "$log"
        : > "$logdir/$name.fail"
        return 2
    fi

    local libs=() defs=() cflags=() ldflags=() coverage_flags=()
    read -r -a cflags <<< "${CFLAGS:-}"
    read -r -a ldflags <<< "${LDFLAGS:-}"
    if [[ $COVERAGE == 1 ]]; then
        coverage_flags=(--coverage -O0 -g -fprofile-abs-path -fkeep-inline-functions -fkeep-static-functions)
    fi
    [[ $name == client_tcp ]] && libs+=(-llz4 -lzstd)
    [[ $name == php_live ]] && libs+=(-llz4 -lzstd)
    [[ $name == ioless ]] && libs+=(-llz4)
    [[ $name == compression_errors ]] && libs+=(-llz4 -lzstd)
    [[ $name == client_errors ]] && libs+=(-llz4)
    [[ $name == async_compressed ]] && libs+=(-llz4)
    [[ $name == sparse ]] && libs+=(-llz4)
    [[ $name == compress_no_sync ]] && libs+=(-llz4)
    [[ $name == compress_no_async ]] && libs+=(-llz4)
    [[ $name == openssl_io ]] && libs+=(-lssl -lcrypto -lpthread)
    # async_uring: enable real body when liburing available, else skip stub
    if [[ $name == async_uring && $uring == 1 ]]; then
        defs+=(-DCHC_ASYNC_URING_TEST)
        libs+=(-luring -llz4)
    fi

    {
        echo "== $name =="
        if "$CC" -std=c11 -D_POSIX_C_SOURCE=200809L -D_DARWIN_C_SOURCE \
              -O2 -Wall -Wextra -Wno-unused-parameter -I. \
              "${defs[@]}" "${cflags[@]}" "${ldflags[@]}" "${coverage_flags[@]}" \
              "$src" -o "$bin" "${libs[@]}"; then
            "$bin" || { echo "(exit $?)"; : > "$logdir/$name.fail"; }
        else
            : > "$logdir/$name.fail"
        fi
    } > "$log" 2>&1
}
export -f run_one
export uring logdir CFLAGS LDFLAGS CC COVERAGE

tests=()
if (( $# == 0 )); then
    for src in test/test_*.c; do
        n=${src#test/test_}
        tests+=("${n%.c}")
    done
else
    for t in "$@"; do
        tests+=("${t#test_}")
    done
fi

# Independent binaries, distinct ports/temp dirs => safe to build + run in
# parallel. Override width with JOBS=.
jobs=${JOBS:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)}
printf '%s\n' "${tests[@]}" \
    | xargs -P "$jobs" -n1 bash -c 'run_one "$@"' _
runner_status=$?

fails=0
for t in "${tests[@]}"; do
    [[ -f $logdir/$t.log ]] && cat "$logdir/$t.log"
    [[ -f $logdir/$t.fail ]] && fails=$((fails + 1))
done

coverage_status=0
if [[ $COVERAGE == 1 ]]; then
    sh tools/coverage.sh "$logdir" || coverage_status=$?
fi

if (( fails > 0 || runner_status != 0 )); then
    echo "$fails test(s) failed" >&2
    exit 1
fi
if (( coverage_status != 0 )); then
    exit "$coverage_status"
fi
