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

#ifndef CH_WIDE_H
#define CH_WIDE_H

#include <stddef.h>
#include <stdint.h>

typedef struct {
	uint64_t lo;
	uint64_t hi;
} ch_uint128;

/* Parse and mul10_add return 0, -1 (invalid), or -2 (overflow).
 * Failure leaves the destination unchanged. Parsing accepts leading zeroes. */
int ch_u128_parse(const char *text, size_t length, ch_uint128 *result);
/* Writes decimal digits without a terminating NUL. */
size_t ch_u128_format(ch_uint128 value, char out[40]);
ch_uint128 ch_u128_negate(ch_uint128 value);
int ch_u128_compare(ch_uint128 left, ch_uint128 right);
ch_uint128 ch_u128_from_u64(uint64_t value);
int ch_u128_mul10_add(ch_uint128 *value, unsigned digit);
unsigned ch_u128_div10(ch_uint128 *value);
void ch_u128_load_le(ch_uint128 *value, const unsigned char bytes[16]);
void ch_u128_store_le(unsigned char bytes[16], ch_uint128 value);

#endif
