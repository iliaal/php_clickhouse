PHP_ARG_ENABLE(clickhouse, whether to enable ClickHouse support,
[  --enable-clickhouse           Enable ClickHouse client support])

PHP_ARG_ENABLE(clickhouse-openssl, whether to enable OpenSSL/TLS support in php_clickhouse,
[  --enable-clickhouse-openssl Enable TLS/SSL support (requires OpenSSL)], no, no)

if test "$PHP_CLICKHOUSE" != "no"; then
  PHP_SUBST(CLICKHOUSE_SHARED_LIBADD)
  CFLAGS="$CFLAGS -std=c11 -D_GNU_SOURCE -fvisibility=hidden -Wall -DZSTD_DISABLE_ASM"

  if test "$PHP_CLICKHOUSE_OPENSSL" != "no"; then
    AC_CHECK_HEADER([openssl/ssl.h], [], [
      AC_MSG_ERROR([--enable-clickhouse-openssl was requested but openssl/ssl.h was not found. Install the OpenSSL development package (e.g. libssl-dev / openssl-devel) or drop the flag.])
    ])
    PHP_CHECK_LIBRARY(ssl, SSL_CTX_new, [
      PHP_CHECK_LIBRARY(crypto, EVP_get_digestbyname, [
        PHP_ADD_LIBRARY(ssl, 1, CLICKHOUSE_SHARED_LIBADD)
        PHP_ADD_LIBRARY(crypto, 1, CLICKHOUSE_SHARED_LIBADD)
        AC_DEFINE([WITH_OPENSSL], [1], [Build php_clickhouse with TLS/SSL support])
      ], [
        AC_MSG_ERROR([--enable-clickhouse-openssl requires libcrypto, which was not found. Install the OpenSSL development package.])
      ])
    ], [
      AC_MSG_ERROR([--enable-clickhouse-openssl requires libssl, which was not found. Install the OpenSSL development package (e.g. libssl-dev / openssl-devel).])
    ])
  fi

  case "$host_os" in
    darwin*)
      LDFLAGS="$LDFLAGS -Wl,-exported_symbol,_get_module"
      ;;
    linux*|gnu*)
      LDFLAGS="$LDFLAGS -Wl,--version-script=$srcdir/clickhouse.exports"
      ;;
  esac

  clickhouse_source_file="clickhouse.c \
        ch_backend.c \
        ch_protocol.c \
        ch_query.c \
        ch_insert.c \
        ch_results.c \
        ch_types_read.c \
        ch_types_write.c \
        ch_wide.c \
        metadata.c \
        query_log.c \
        lib/compression/lz4/lz4.c \
        lib/compression/lz4/lz4hc.c \
        lib/compression/zstd/common/debug.c \
        lib/compression/zstd/common/entropy_common.c \
        lib/compression/zstd/common/error_private.c \
        lib/compression/zstd/common/fse_decompress.c \
        lib/compression/zstd/common/pool.c \
        lib/compression/zstd/common/threading.c \
        lib/compression/zstd/common/xxhash.c \
        lib/compression/zstd/common/zstd_common.c \
        lib/compression/zstd/compress/fse_compress.c \
        lib/compression/zstd/compress/hist.c \
        lib/compression/zstd/compress/huf_compress.c \
        lib/compression/zstd/compress/zstd_compress.c \
        lib/compression/zstd/compress/zstd_compress_literals.c \
        lib/compression/zstd/compress/zstd_compress_sequences.c \
        lib/compression/zstd/compress/zstd_compress_superblock.c \
        lib/compression/zstd/compress/zstd_double_fast.c \
        lib/compression/zstd/compress/zstd_fast.c \
        lib/compression/zstd/compress/zstd_lazy.c \
        lib/compression/zstd/compress/zstd_ldm.c \
        lib/compression/zstd/compress/zstd_opt.c \
        lib/compression/zstd/compress/zstdmt_compress.c \
        lib/compression/zstd/decompress/huf_decompress.c \
        lib/compression/zstd/decompress/zstd_ddict.c \
        lib/compression/zstd/decompress/zstd_decompress.c \
        lib/compression/zstd/decompress/zstd_decompress_block.c"

  PHP_NEW_EXTENSION(clickhouse, $clickhouse_source_file, $ext_shared,,-DZEND_ENABLE_STATIC_TSRMLS_CACHE=1)
  PHP_ADD_MAKEFILE_FRAGMENT

  PHP_ADD_INCLUDE($ext_srcdir/lib/clickhouse-c)
  PHP_ADD_INCLUDE($ext_srcdir/lib/compression/lz4)
  PHP_ADD_INCLUDE($ext_srcdir/lib/compression/zstd)
  PHP_ADD_INCLUDE($ext_srcdir/lib/compression/zstd/common)

  PHP_ADD_EXTENSION_DEP(clickhouse, json)
  PHP_ADD_BUILD_DIR($ext_builddir/lib/compression/lz4)
  PHP_ADD_BUILD_DIR($ext_builddir/lib/compression/zstd/common)
  PHP_ADD_BUILD_DIR($ext_builddir/lib/compression/zstd/compress)
  PHP_ADD_BUILD_DIR($ext_builddir/lib/compression/zstd/decompress)
fi
