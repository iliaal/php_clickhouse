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
#include <stdlib.h>

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

/* Native decoding must fail recoverably: selectStream() promises a catchable
 * memory_limit error, while Zend allocation failure terminates the request.
 * Retained result blocks are additionally budgeted by their iterator owner. */
static bool ch_native_allocation_fits(size_t bytes)
{
    zend_long configured = PG(memory_limit);
    size_t used;

    if (configured <= 0) {
        return true;
    }
    used = zend_memory_usage(true);
    return used < (size_t)configured && bytes <= (size_t)configured - used;
}

static void *ch_native_alloc(void *context, size_t bytes)
{
    (void)context;
    if (!ch_native_allocation_fits(bytes)) {
        return NULL;
    }
    return malloc(bytes ? bytes : 1);
}

static void *ch_native_realloc(void *context, void *pointer,
                              size_t old_bytes, size_t new_bytes)
{
    (void)context;
    (void)old_bytes;
    if (!ch_native_allocation_fits(new_bytes)) {
        return NULL;
    }
    return realloc(pointer, new_bytes ? new_bytes : 1);
}

static void ch_native_free(void *context, void *pointer, size_t bytes)
{
    (void)context;
    (void)bytes;
    free(pointer);
}

const chc_alloc ch_php_allocator = {
    NULL, ch_native_alloc, ch_native_realloc, ch_native_free
};
