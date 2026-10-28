#!/usr/bin/env bash
#
# Build and run the phpt suite against a 32-bit PHP.
#
# Exercise server-dependent 32-bit paths absent from the Windows x86 CI lane.
#
#   ./scripts/run-tests-32bit.sh                 # whole suite
#   ./scripts/run-tests-32bit.sh tests/208*.phpt # selected tests
#
# Needs Docker with linux/386 emulation and a ClickHouse server the container
# can reach (defaults to the host's, via --network host).
#
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
image="${PHP32_IMAGE:-i386/debian:bookworm}"
host="${CLICKHOUSE_HOST:-127.0.0.1}"
port="${CLICKHOUSE_PORT:-9000}"
user="${CLICKHOUSE_USER:-test}"
passwd="${CLICKHOUSE_PASSWD:-test}"

command -v docker >/dev/null 2>&1 || {
	printf 'run-tests-32bit: docker is required\n' >&2
	exit 69
}

tarball=$(mktemp -t php-clickhouse-32bit-XXXXXX.tar)
trap 'rm -f "$tarball"' EXIT

# Archive the index, not the working tree: a native in-tree build leaves a
# Makefile full of absolute host paths that breaks the container build.
tree=$(git -C "${root}" write-tree)
git -C "${root}" archive --format=tar "${tree}" >"${tarball}"

targets=("$@")
((${#targets[@]} > 0)) || targets=(tests/)

docker run --rm --platform linux/386 --network host \
	-v "${tarball}:/src.tar:ro" \
	-e "CLICKHOUSE_HOST=${host}" \
	-e "CLICKHOUSE_PORT=${port}" \
	-e "CLICKHOUSE_USER=${user}" \
	-e "CLICKHOUSE_PASSWD=${passwd}" \
	-e NO_INTERACTION=1 \
	"${image}" bash -c '
set -e
apt-get update -qq >/dev/null
apt-get install -y -qq php-cli php-dev gcc make >/dev/null 2>&1
mkdir -p /work && cd /work && tar xf /src.tar
php -r "if (PHP_INT_SIZE !== 4) { fwrite(STDERR, \"not a 32-bit PHP\n\"); exit(1); }"
php -r "echo \"PHP \", PHP_VERSION, \" (PHP_INT_SIZE=\", PHP_INT_SIZE, \")\n\";"
phpize >/dev/null 2>&1
./configure --enable-clickhouse >/dev/null 2>&1
make -j"$(nproc)" >/dev/null 2>&1
echo "build ok"
TEST_PHP_EXECUTABLE=$(command -v php) exec php run-tests.php -j1 --show-diff -g FAIL,BORK,LEAK,XLEAK,SKIP \
    -d extension=/work/modules/clickhouse.so "$@"' _ "${targets[@]}"
