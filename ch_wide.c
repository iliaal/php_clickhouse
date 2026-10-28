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

#include <assert.h>

#include "ch_wide.h"

int ch_u128_parse(const char *text, size_t length, ch_uint128 *result)
{
	ch_uint128 value = {0, 0};

	if (text == NULL || result == NULL || length == 0) {
		return -1;
	}
	for (size_t i = 0; i < length; ++i) {
		int status;
		if (text[i] < '0' || text[i] > '9') {
			return -1;
		}
		status = ch_u128_mul10_add(&value, (unsigned)(text[i] - '0'));
		if (status != 0) {
			return status;
		}
	}
	*result = value;
	return 0;
}

size_t ch_u128_format(ch_uint128 value, char out[40])
{
	char digits[40];
	size_t length = 0;

	assert(out != NULL);
	do {
		digits[length++] = (char)('0' + ch_u128_div10(&value));
	} while (value.lo != 0 || value.hi != 0);
	for (size_t i = 0; i < length; ++i) {
		out[i] = digits[length - i - 1];
	}
	return length;
}

ch_uint128 ch_u128_negate(ch_uint128 value)
{
	ch_uint128 result;

	result.lo = ~value.lo + UINT64_C(1);
	result.hi = ~value.hi + (uint64_t)(result.lo == 0);
	return result;
}

int ch_u128_compare(ch_uint128 left, ch_uint128 right)
{
	if (left.hi != right.hi) {
		return left.hi < right.hi ? -1 : 1;
	}
	if (left.lo != right.lo) {
		return left.lo < right.lo ? -1 : 1;
	}
	return 0;
}

ch_uint128 ch_u128_from_u64(uint64_t value)
{
	ch_uint128 result = {value, 0};

	return result;
}

int ch_u128_mul10_add(ch_uint128 *value, unsigned digit)
{
	uint32_t limbs[4];
	uint64_t carry = digit;

	if (value == NULL || digit > 9) {
		return -1;
	}
	limbs[0] = (uint32_t)value->lo;
	limbs[1] = (uint32_t)(value->lo >> 32);
	limbs[2] = (uint32_t)value->hi;
	limbs[3] = (uint32_t)(value->hi >> 32);
	for (size_t i = 0; i < 4; ++i) {
		uint64_t product = (uint64_t)limbs[i] * 10 + carry;
		limbs[i] = (uint32_t)product;
		carry = product >> 32;
	}
	if (carry != 0) {
		return -2;
	}
	assert(digit <= 9);
	value->lo = (uint64_t)limbs[0] | ((uint64_t)limbs[1] << 32);
	value->hi = (uint64_t)limbs[2] | ((uint64_t)limbs[3] << 32);
	return 0;
}

unsigned ch_u128_div10(ch_uint128 *value)
{
	uint32_t limbs[4];
	uint64_t remainder = 0;

	assert(value != NULL);
	limbs[0] = (uint32_t)value->lo;
	limbs[1] = (uint32_t)(value->lo >> 32);
	limbs[2] = (uint32_t)value->hi;
	limbs[3] = (uint32_t)(value->hi >> 32);
	for (size_t i = 4; i != 0; --i) {
		uint64_t dividend = (remainder << 32) | limbs[i - 1];
		limbs[i - 1] = (uint32_t)(dividend / 10);
		remainder = dividend % 10;
	}
	value->lo = (uint64_t)limbs[0] | ((uint64_t)limbs[1] << 32);
	value->hi = (uint64_t)limbs[2] | ((uint64_t)limbs[3] << 32);
	return (unsigned)remainder;
}

void ch_u128_load_le(ch_uint128 *value, const unsigned char bytes[16])
{
	ch_uint128 result = {0, 0};

	assert(value != NULL);
	assert(bytes != NULL);
	for (unsigned i = 0; i < 8; ++i) {
		result.lo |= (uint64_t)bytes[i] << (i * 8);
		result.hi |= (uint64_t)bytes[i + 8] << (i * 8);
	}
	*value = result;
}

void ch_u128_store_le(unsigned char bytes[16], ch_uint128 value)
{
	assert(bytes != NULL);
	for (unsigned i = 0; i < 8; ++i) {
		bytes[i] = (unsigned char)(value.lo >> (i * 8));
		bytes[i + 8] = (unsigned char)(value.hi >> (i * 8));
	}
}
