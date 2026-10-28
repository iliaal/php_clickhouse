/*
  +----------------------------------------------------------------------+
  | php_clickhouse                                                       |
  +----------------------------------------------------------------------+
  | Copyright (c) 1997-2026 The PHP Group                                |
  +----------------------------------------------------------------------+
  | This source file is subject to version 3.01 of the PHP license,      |
  | that is bundled with this package in the file LICENSE, and is        |
  | available through the world-wide-web at the following url:           |
  | http://www.php.net/license/3_01.txt                                  |
  | If you did not receive a copy of the PHP license and are unable to   |
  | obtain it through the world-wide-web, please send a note to          |
  | license@php.net so we can mail you a copy immediately.               |
  +----------------------------------------------------------------------+
  | Author: Ilia Alshanetsky <ilia@ilia.ws>                              |
  +----------------------------------------------------------------------+
*/

#ifdef HAVE_CONFIG_H
# include "config.h"
#endif

#include "php_clickhouse.h"
#include <assert.h>
#ifdef PHP_WIN32
# include <windows.h>
#else
# include <unistd.h>
#endif

/* PHP 7.4's formatter caps string precision at 500 bytes. Native diagnostics
 * use the C library formatter and the adapter's explicit error-size limit. */
#undef vsnprintf

#define CHC_IMPLEMENTATION
#define CHC_PHP_COMPAT 1
#define CHC_ZSTD_COMPRESSION_LEVEL 1
#define CHC_MAX_HELLO_STRING_SIZE 0x00FFFFFFu
#define CHC_MAX_PROTOCOL_STRING_SIZE 0x00FFFFFFu
#define CHC_MAX_STRING_SIZE SIZE_MAX
#define CHC_MAX_TYPE_DEPTH 512
#define CHC_MAX_NUM_COLUMNS SIZE_MAX
#define CHC_MAX_NUM_ROWS SIZE_MAX
#include "ch_protocol.h"

/* Reject before Zend queues or raises an OOM. A new chunk may be needed even
 * for a small allocation; huge blocks also allocate a tracking node. Realloc
 * can hold both allocations at once, including when moving to a smaller bin. */
static bool ch_native_allocation_fits(size_t bytes)
{
    zend_long configured = PG(memory_limit);
    size_t padded, growth, used, alignment = ZEND_MM_CHUNK_SIZE;

    if (!bytes) bytes = 1;
    if (bytes > SIZE_MAX - (ZEND_MM_ALIGNMENT - 1) - ZEND_MM_OVERHEAD) {
        return false;
    }
#ifdef ZEND_HAS_OOM
    if (ZEND_HAS_OOM()) return false;
#endif
    padded = ZEND_MM_ALIGNED_SIZE(bytes) + ZEND_MM_OVERHEAD;
    if (configured <= 0) {
        return true;
    }
    used = zend_memory_usage(true);
    if (!is_zend_mm()) {
        return used < (size_t)configured && bytes <= (size_t)configured - used;
    }
    growth = ZEND_MM_CHUNK_SIZE;
    if (padded > ZEND_MM_MAX_LARGE_SIZE) {
#ifdef PHP_WIN32
        SYSTEM_INFO info;
        GetSystemInfo(&info);
        if (info.dwPageSize > alignment) alignment = info.dwPageSize;
#else
        long page_size = sysconf(_SC_PAGESIZE);
        if (page_size <= 0) return false;
        if ((size_t)page_size > alignment) alignment = (size_t)page_size;
#endif
        size_t remainder = padded % alignment;
        size_t extra = remainder ? alignment - remainder : 0;
        if (padded > SIZE_MAX - extra || padded + extra > SIZE_MAX - growth) {
            return false;
        }
        growth += padded + extra;
    }
    return used < (size_t)configured && growth <= (size_t)configured - used;
}

static void *ch_native_alloc(void *context, size_t bytes)
{
    (void)context;
    if (!ch_native_allocation_fits(bytes)) {
        return NULL;
    }
#ifdef ZEND_HAS_OOM
    return femalloc(bytes ? bytes : 1, true);
#else
    return emalloc(bytes ? bytes : 1);
#endif
}

static void *ch_native_realloc(void *context, void *pointer,
                              size_t old_bytes, size_t new_bytes)
{
    (void)context;
    (void)old_bytes;
    if (!ch_native_allocation_fits(new_bytes)) {
        return NULL;
    }
#ifdef ZEND_HAS_OOM
    return ferealloc(pointer, new_bytes ? new_bytes : 1, true);
#else
    return erealloc(pointer, new_bytes ? new_bytes : 1);
#endif
}

static void ch_native_free(void *context, void *pointer, size_t bytes)
{
    (void)context;
    (void)bytes;
    if (pointer) efree(pointer);
}

const chc_alloc ch_php_allocator = {
    NULL, ch_native_alloc, ch_native_realloc, ch_native_free
};
