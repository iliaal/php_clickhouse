dnl $Id$
dnl config.m4 for extension SeasClick

PHP_ARG_ENABLE(SeasClick, whether to enable SeasClick support,
[  --enable-SeasClick           Enable SeasClick support])

PHP_ARG_ENABLE(SeasClick-openssl, whether to enable OpenSSL/TLS support in SeasClick,
[  --enable-SeasClick-openssl   Enable TLS/SSL support (requires OpenSSL)], no, no)

if test "$PHP_SEASCLICK" != "no"; then
  PHP_REQUIRE_CXX()
  PHP_SUBST(SEASCLICK_SHARED_LIBADD)
  PHP_ADD_LIBRARY(stdc++, 1, SEASCLICK_SHARED_LIBADD)
  CXXFLAGS="$CXXFLAGS -Wall -Wno-unused-function -Wno-deprecated -Wno-deprecated-declarations -std=c++17 -DZSTD_DISABLE_ASM"
  CFLAGS="$CFLAGS -DZSTD_DISABLE_ASM"

  SEASCLICK_SSL_SRC=""
  if test "$PHP_SEASCLICK_OPENSSL" != "no"; then
    AC_MSG_CHECKING([for OpenSSL libraries])
    PHP_ADD_LIBRARY(ssl, 1, SEASCLICK_SHARED_LIBADD)
    PHP_ADD_LIBRARY(crypto, 1, SEASCLICK_SHARED_LIBADD)
    AC_DEFINE([WITH_OPENSSL], [1], [Build SeasClick with TLS/SSL support])
    CXXFLAGS="$CXXFLAGS -DWITH_OPENSSL"
    SEASCLICK_SSL_SRC="lib/clickhouse-cpp/clickhouse/base/sslsocket.cpp"
    AC_MSG_RESULT([enabled])
  fi

  SeasClick_source_file="SeasClick.cpp \
        typesToPhp.cpp \
        $SEASCLICK_SSL_SRC \
        lib/clickhouse-cpp/clickhouse/base/compressed.cpp \
        lib/clickhouse-cpp/clickhouse/base/endpoints_iterator.cpp \
        lib/clickhouse-cpp/clickhouse/base/input.cpp \
        lib/clickhouse-cpp/clickhouse/base/output.cpp \
        lib/clickhouse-cpp/clickhouse/base/platform.cpp \
        lib/clickhouse-cpp/clickhouse/base/socket.cpp \
        lib/clickhouse-cpp/clickhouse/base/wire_format.cpp \
        lib/clickhouse-cpp/clickhouse/columns/array.cpp \
        lib/clickhouse-cpp/clickhouse/columns/column.cpp \
        lib/clickhouse-cpp/clickhouse/columns/date.cpp \
        lib/clickhouse-cpp/clickhouse/columns/decimal.cpp \
        lib/clickhouse-cpp/clickhouse/columns/enum.cpp \
        lib/clickhouse-cpp/clickhouse/columns/factory.cpp \
        lib/clickhouse-cpp/clickhouse/columns/geo.cpp \
        lib/clickhouse-cpp/clickhouse/columns/ip4.cpp \
        lib/clickhouse-cpp/clickhouse/columns/ip6.cpp \
        lib/clickhouse-cpp/clickhouse/columns/itemview.cpp \
        lib/clickhouse-cpp/clickhouse/columns/lowcardinality.cpp \
        lib/clickhouse-cpp/clickhouse/columns/map.cpp \
        lib/clickhouse-cpp/clickhouse/columns/nullable.cpp \
        lib/clickhouse-cpp/clickhouse/columns/numeric.cpp \
        lib/clickhouse-cpp/clickhouse/columns/string.cpp \
        lib/clickhouse-cpp/clickhouse/columns/time.cpp \
        lib/clickhouse-cpp/clickhouse/columns/tuple.cpp \
        lib/clickhouse-cpp/clickhouse/columns/uuid.cpp \
        lib/clickhouse-cpp/clickhouse/types/type_parser.cpp \
        lib/clickhouse-cpp/clickhouse/types/types.cpp \
        lib/clickhouse-cpp/clickhouse/block.cpp \
        lib/clickhouse-cpp/clickhouse/client.cpp \
        lib/clickhouse-cpp/clickhouse/query.cpp \
        lib/clickhouse-cpp/contrib/cityhash/cityhash/city.cc \
        lib/clickhouse-cpp/contrib/lz4/lz4/lz4.c \
        lib/clickhouse-cpp/contrib/lz4/lz4/lz4hc.c \
        lib/clickhouse-cpp/contrib/absl/absl/numeric/int128.cc \
        lib/clickhouse-cpp/contrib/zstd/zstd/common/debug.c \
        lib/clickhouse-cpp/contrib/zstd/zstd/common/entropy_common.c \
        lib/clickhouse-cpp/contrib/zstd/zstd/common/error_private.c \
        lib/clickhouse-cpp/contrib/zstd/zstd/common/fse_decompress.c \
        lib/clickhouse-cpp/contrib/zstd/zstd/common/pool.c \
        lib/clickhouse-cpp/contrib/zstd/zstd/common/threading.c \
        lib/clickhouse-cpp/contrib/zstd/zstd/common/xxhash.c \
        lib/clickhouse-cpp/contrib/zstd/zstd/common/zstd_common.c \
        lib/clickhouse-cpp/contrib/zstd/zstd/compress/fse_compress.c \
        lib/clickhouse-cpp/contrib/zstd/zstd/compress/hist.c \
        lib/clickhouse-cpp/contrib/zstd/zstd/compress/huf_compress.c \
        lib/clickhouse-cpp/contrib/zstd/zstd/compress/zstd_compress.c \
        lib/clickhouse-cpp/contrib/zstd/zstd/compress/zstd_compress_literals.c \
        lib/clickhouse-cpp/contrib/zstd/zstd/compress/zstd_compress_sequences.c \
        lib/clickhouse-cpp/contrib/zstd/zstd/compress/zstd_compress_superblock.c \
        lib/clickhouse-cpp/contrib/zstd/zstd/compress/zstd_double_fast.c \
        lib/clickhouse-cpp/contrib/zstd/zstd/compress/zstd_fast.c \
        lib/clickhouse-cpp/contrib/zstd/zstd/compress/zstd_lazy.c \
        lib/clickhouse-cpp/contrib/zstd/zstd/compress/zstd_ldm.c \
        lib/clickhouse-cpp/contrib/zstd/zstd/compress/zstd_opt.c \
        lib/clickhouse-cpp/contrib/zstd/zstd/compress/zstdmt_compress.c \
        lib/clickhouse-cpp/contrib/zstd/zstd/decompress/huf_decompress.c \
        lib/clickhouse-cpp/contrib/zstd/zstd/decompress/zstd_ddict.c \
        lib/clickhouse-cpp/contrib/zstd/zstd/decompress/zstd_decompress.c \
        lib/clickhouse-cpp/contrib/zstd/zstd/decompress/zstd_decompress_block.c"

  THIS_DIR=`dirname $0`
  PHP_ADD_INCLUDE($THIS_DIR/lib/clickhouse-cpp)
  PHP_ADD_INCLUDE($THIS_DIR/lib/clickhouse-cpp/contrib)
  PHP_ADD_INCLUDE($THIS_DIR/lib/clickhouse-cpp/contrib/absl)
  PHP_ADD_INCLUDE($THIS_DIR/lib/clickhouse-cpp/contrib/cityhash)
  PHP_ADD_INCLUDE($THIS_DIR/lib/clickhouse-cpp/contrib/cityhash/cityhash)
  PHP_ADD_INCLUDE($THIS_DIR/lib/clickhouse-cpp/contrib/lz4)
  PHP_ADD_INCLUDE($THIS_DIR/lib/clickhouse-cpp/contrib/lz4/lz4)
  PHP_ADD_INCLUDE($THIS_DIR/lib/clickhouse-cpp/contrib/zstd/zstd)
  PHP_ADD_INCLUDE($THIS_DIR/lib/clickhouse-cpp/contrib/zstd/zstd/common)

  PHP_NEW_EXTENSION(SeasClick, $SeasClick_source_file, $ext_shared,,-DZEND_ENABLE_STATIC_TSRMLS_CACHE=1)
  PHP_ADD_BUILD_DIR($ext_builddir/lib/clickhouse-cpp)
  PHP_ADD_BUILD_DIR($ext_builddir/lib/clickhouse-cpp/clickhouse)
  PHP_ADD_BUILD_DIR($ext_builddir/lib/clickhouse-cpp/clickhouse/base)
  PHP_ADD_BUILD_DIR($ext_builddir/lib/clickhouse-cpp/clickhouse/types)
  PHP_ADD_BUILD_DIR($ext_builddir/lib/clickhouse-cpp/clickhouse/columns)
  PHP_ADD_BUILD_DIR($ext_builddir/lib/clickhouse-cpp/contrib)
  PHP_ADD_BUILD_DIR($ext_builddir/lib/clickhouse-cpp/contrib/cityhash)
  PHP_ADD_BUILD_DIR($ext_builddir/lib/clickhouse-cpp/contrib/cityhash/cityhash)
  PHP_ADD_BUILD_DIR($ext_builddir/lib/clickhouse-cpp/contrib/lz4)
  PHP_ADD_BUILD_DIR($ext_builddir/lib/clickhouse-cpp/contrib/lz4/lz4)
  PHP_ADD_BUILD_DIR($ext_builddir/lib/clickhouse-cpp/contrib/absl)
  PHP_ADD_BUILD_DIR($ext_builddir/lib/clickhouse-cpp/contrib/absl/absl)
  PHP_ADD_BUILD_DIR($ext_builddir/lib/clickhouse-cpp/contrib/absl/absl/numeric)
  PHP_ADD_BUILD_DIR($ext_builddir/lib/clickhouse-cpp/contrib/zstd)
  PHP_ADD_BUILD_DIR($ext_builddir/lib/clickhouse-cpp/contrib/zstd/zstd)
  PHP_ADD_BUILD_DIR($ext_builddir/lib/clickhouse-cpp/contrib/zstd/zstd/common)
  PHP_ADD_BUILD_DIR($ext_builddir/lib/clickhouse-cpp/contrib/zstd/zstd/compress)
  PHP_ADD_BUILD_DIR($ext_builddir/lib/clickhouse-cpp/contrib/zstd/zstd/decompress)
fi
