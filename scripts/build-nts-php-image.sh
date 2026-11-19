#!/usr/bin/env bash
# Usage: build-nts-php-image.sh VERSION TARBALL_SHA256 BASE_IMAGE
#
# Builds NTS PHP VERSION (CLI only) from the checksum-pinned php.net tarball on
# the digest-pinned Debian BASE_IMAGE and tags it local/php:VERSION-cli, the
# reference prebuilt-abi-smoke.sh accepts for a locally built gate image.
# The official php images are ZTS-only from 8.6 on (docker-library/php#1686),
# so an NTS gate image for those minors has to be built here. Build tools are
# purged afterwards so the import check sees only what a runtime image ships.
# Prints the tag.
set -Eeuo pipefail
shopt -s inherit_errexit

if [[ $# -ne 3 ]]; then
	echo "usage: $0 VERSION TARBALL_SHA256 BASE_IMAGE" >&2
	exit 2
fi

version=$1
sha256=$2
base=$3
tag="local/php:${version}-cli"

if [[ "$base" != *@sha256:* ]]; then
	echo "::error::$base is not pinned by digest" >&2
	exit 1
fi

pulled=""
path=${base#public.ecr.aws/docker/library/}
candidates=("$base")
if [[ "$path" != "$base" ]]; then
	candidates=("$base" "mirror.gcr.io/library/$path" "docker.io/library/$path")
fi
for candidate in "${candidates[@]}"; do
	for attempt in 1 2 3; do
		if docker pull -q "$candidate" >&2; then
			pulled=$candidate
			break 2
		fi
		if [[ $attempt -lt 3 ]]; then
			sleep $((attempt * 5))
		fi
	done
	echo "pull of $candidate failed; trying the next mirror" >&2
done
if [[ -z "$pulled" ]]; then
	echo "::error::could not pull $base from any mirror" >&2
	exit 1
fi

ctx=$(mktemp -d)
trap 'rm -rf "$ctx"' EXIT
curl -fsSL --retry 3 -o "$ctx/php-src.tar.xz" "https://www.php.net/distributions/php-${version}.tar.xz"
echo "$sha256  $ctx/php-src.tar.xz" | sha256sum -c - >&2

# CFLAGS, CPPFLAGS, and LDFLAGS match the official php images.
docker build -q -t "$tag" --build-arg BASE_IMAGE="$pulled" --build-arg PHP_VERSION="$version" -f - "$ctx" >&2 <<'EOF'
ARG BASE_IMAGE
FROM ${BASE_IMAGE}
ARG PHP_VERSION
COPY php-src.tar.xz /usr/src/
RUN set -eux; \
	deps="autoconf dpkg-dev file g++ gcc libc-dev make pkg-config re2c xz-utils"; \
	apt-get update -qq; \
	apt-get install -y -qq --no-install-recommends $deps > /dev/null; \
	mkdir /usr/src/php; \
	tar -xJf /usr/src/php-src.tar.xz -C /usr/src/php --strip-components=1; \
	cd /usr/src/php; \
	export CFLAGS="-fstack-protector-strong -fpic -fpie -O2 -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64"; \
	export CPPFLAGS="$CFLAGS" LDFLAGS="-Wl,-O1 -pie"; \
	./configure --prefix=/usr/local --disable-all --enable-cli \
		--disable-cgi --disable-phpdbg --without-pear > /dev/null; \
	make -j"$(nproc)" > /dev/null; \
	make install > /dev/null; \
	cd /; \
	rm -rf /usr/src/php /usr/src/php-src.tar.xz; \
	apt-mark auto $deps > /dev/null; \
	apt-get purge -y -qq --auto-remove > /dev/null; \
	rm -rf /var/lib/apt/lists/*; \
	test "$(php -n -r 'echo PHP_VERSION;')" = "$PHP_VERSION"; \
	php -n -r 'exit(PHP_ZTS || PHP_DEBUG ? 1 : 0);'
EOF
printf '%s\n' "$tag"
