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
#include "config.h"
#endif

#include "ch_types.h"
#include "ch_wide.h"
#include "Zend/zend_smart_str.h"
#include "ext/json/php_json.h"
#include <errno.h>
#include <float.h>
#include <inttypes.h>
#include <limits.h>
#include <math.h>
#include <stdarg.h>
#ifdef PHP_WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#endif

#define CH_CONVERT_DEPTH 32U
#define CH_CALLER_TYPE_DEPTH 64U
#define CH_CALLER_TYPE_LENGTH (1024U * 1024U)
#define CH_WRITE_TRY(expression) do { \
	int _ch_write_status = (expression); \
	if (_ch_write_status != SUCCESS) return _ch_write_status; \
} while (0)

struct ch_owned_column {
	chc_column column;
	chc_type *owned_type;
	const chc_type *type;
	ch_owned_column **children;
	size_t child_count;
	chc_column **child_views;
	void *data;
	size_t data_capacity;
	uint64_t *offsets;
	uint8_t *nulls;
	uint8_t *map_shapes;
	zval values;
	zval temporary;
	HashTable dictionary;
	bool dictionary_initialized;
};

/* Array cells and ordered Map rows delimit legacy insertColumn calls. */
typedef struct {
	const uint64_t *ends;
	size_t count;
} ch_value_groups;

static int ch_write_error(const char *format, ...)
{
	va_list args;
	char *message;
	if (EG(exception)) return FAILURE;
	va_start(args, format);
	vspprintf(&message, 0, format, args);
	va_end(args);
	zend_throw_exception(clickhouse_exception_ce, message, 0);
	efree(message);
	return CH_TYPE_LOCAL_ERROR;
}

static void ch_copy_values(zval *out, HashTable *values)
{
	zval *value;
	bool copy_elements = values == &EG(symbol_table);
	if (!copy_elements) {
		ZEND_HASH_FOREACH_VAL(values, value) {
			if (Z_ISREF_P(value) || Z_TYPE_P(value) == IS_INDIRECT) { copy_elements = true; break; }
		} ZEND_HASH_FOREACH_END();
	}
	if (!copy_elements) {
		ZVAL_ARR(out, values);
		Z_TRY_ADDREF_P(out);
		return;
	}
	array_init_size(out, zend_hash_num_elements(values));
	ZEND_HASH_FOREACH_VAL_IND(values, value) {
		zval copy;
		ZVAL_COPY_DEREF(&copy, value);
		zend_hash_next_index_insert(Z_ARRVAL_P(out), &copy);
	} ZEND_HASH_FOREACH_END();
}

static void ch_append_copy(zval *out, zval *value)
{
	zval copy;
	ZVAL_COPY_DEREF(&copy, value);
	zend_hash_next_index_insert(Z_ARRVAL_P(out), &copy);
}

static ch_owned_column *ch_new_column(ch_owned_column **out, size_t rows)
{
	ch_owned_column *column = ecalloc(1, sizeof(*column));
	*out = column;
	column->column.n_rows = rows;
	ZVAL_UNDEF(&column->values);
	ZVAL_UNDEF(&column->temporary);
	return column;
}

static void ch_allocate_children(ch_owned_column *column, size_t count)
{
	column->children = ecalloc(count ? count : 1, sizeof(*column->children));
	column->child_count = count;
	column->child_views = ecalloc(count ? count : 1, sizeof(*column->child_views));
}

static void ch_release_values(ch_owned_column *column)
{
	if (!Z_ISUNDEF(column->values)) {
		zval_ptr_dtor(&column->values);
		ZVAL_UNDEF(&column->values);
	}
	if (!Z_ISUNDEF(column->temporary)) {
		zval_ptr_dtor(&column->temporary);
		ZVAL_UNDEF(&column->temporary);
	}
}

const chc_column *ch_owned_column_data(const ch_owned_column *column)
{
	return column ? &column->column : NULL;
}

const chc_type *ch_owned_column_type(const ch_owned_column *column)
{
	return column ? column->type : NULL;
}

void ch_owned_column_free(ch_owned_column *column)
{
	if (!column) return;
	for (size_t i = 0; i < column->child_count; ++i) ch_owned_column_free(column->children[i]);
	ch_release_values(column);
	if (column->dictionary_initialized) zend_hash_destroy(&column->dictionary);
	if (column->children) efree(column->children);
	if (column->child_views) efree(column->child_views);
	if (column->data) efree(column->data);
	if (column->offsets) efree(column->offsets);
	if (column->nulls) efree(column->nulls);
	if (column->map_shapes) efree(column->map_shapes);
	if (column->owned_type) chc_type_destroy(column->owned_type, &ch_php_allocator);
	efree(column);
}

int ch_validate_type_name(const char *name, size_t length, const char *context)
{
	size_t depth = 0;
	if (length > CH_CALLER_TYPE_LENGTH) return ch_write_error("%s type name exceeds %u bytes", context, CH_CALLER_TYPE_LENGTH);
	for (size_t i = 0; i < length;) {
		char c = name[i++];
		if (c == '(') {
			if (++depth > CH_CALLER_TYPE_DEPTH) return ch_write_error("%s type name nesting exceeds %u levels", context, CH_CALLER_TYPE_DEPTH);
		} else if (c == ')') {
			if (depth) --depth;
		} else if (c == '\'') {
			while (i + 1 < length && name[i] != '\'') ++i;
			if (i + 1 >= length) return SUCCESS;
			++i;
		} else if (c == '"' || c == '`') {
			while (i < length) {
				if (name[i] == '\\' && i + 1 < length && name[i + 1] == c) i += 2;
				else if (name[i] == c) {
					if (i + 1 < length && name[i + 1] == c) i += 2;
					else { ++i; break; }
				} else ++i;
			}
		}
	}
	return SUCCESS;
}

zval *ch_extract_row_cell(zval *row, size_t column_index, zend_string *const *column_names, size_t columns_count)
{
	zval *cell;
	ZVAL_DEREF(row);
	if (Z_TYPE_P(row) != IS_ARRAY) {
		ch_write_error("The insert function needs to pass in a two-dimensional array");
		return NULL;
	}
	cell = zend_hash_index_find(Z_ARRVAL_P(row), column_index);
	if (!cell && column_names && column_index < columns_count) {
		zend_string *name = column_names[column_index];
		cell = zend_hash_str_find_ind(Z_ARRVAL_P(row), ZSTR_VAL(name), ZSTR_LEN(name));
	}
	if (!cell) {
		ch_write_error("The number of parameters inserted per line is inconsistent");
		return NULL;
	}
	ZVAL_DEREF(cell);
	return cell;
}

static int ch_strict_i64(zval *value, const char *label, bool allow_null, int64_t *out)
{
	ZVAL_DEREF(value);
	switch (Z_TYPE_P(value)) {
		case IS_LONG: *out = Z_LVAL_P(value); return SUCCESS;
		case IS_TRUE: *out = 1; return SUCCESS;
		case IS_FALSE: *out = 0; return SUCCESS;
		case IS_NULL:
			if (allow_null) { *out = 0; return SUCCESS; }
			return ch_write_error("null cannot be assigned to non-Nullable column %s", label);
		case IS_DOUBLE: {
			double d = Z_DVAL_P(value), whole;
			if (!isfinite(d)) return ch_write_error("non-finite double cannot be assigned to %s", label);
			if (modf(d, &whole) != 0.0) return ch_write_error("fractional double cannot be assigned to integer column %s", label);
			if (d < -9223372036854775808.0 || d >= 9223372036854775808.0) return ch_write_error("double out of range for integer column %s", label);
			*out = (int64_t)d;
			return SUCCESS;
		}
		case IS_STRING: {
			char *end;
			long long number;
			if (!Z_STRLEN_P(value)) return ch_write_error("empty string cannot be assigned to %s", label);
			errno = 0;
			number = strtoll(Z_STRVAL_P(value), &end, 10);
			if (errno == ERANGE || end == Z_STRVAL_P(value) || (size_t)(end - Z_STRVAL_P(value)) != Z_STRLEN_P(value)) return ch_write_error("invalid integer string for %s", label);
			*out = (int64_t)number;
			return SUCCESS;
		}
		default: return ch_write_error("array / object / resource cannot be assigned to integer column %s", label);
	}
}

static int ch_parse_u64(const char *text, size_t length, const char *label, uint64_t *out)
{
	int base = 10;
	char *end;
	unsigned long long number;
	if (!length) return ch_write_error("empty string cannot be assigned to %s", label);
	if (length >= 3 && text[0] == '0' && (text[1] == 'x' || text[1] == 'X')) { text += 2; length -= 2; base = 16; }
	for (size_t i = 0; i < length; ++i) {
		unsigned char c = (unsigned char)text[i];
		if (!(c >= '0' && c <= '9') && !(base == 16 && ((c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')))) return ch_write_error("invalid integer string for %s", label);
	}
	errno = 0;
	number = strtoull(text, &end, base);
	if (errno == ERANGE || end == text || (size_t)(end - text) != length) return ch_write_error("invalid integer string for %s", label);
	*out = (uint64_t)number;
	return SUCCESS;
}

static int ch_strict_u64(zval *value, const char *label, bool allow_null, uint64_t *out)
{
	ZVAL_DEREF(value);
	switch (Z_TYPE_P(value)) {
		case IS_LONG:
			if (Z_LVAL_P(value) < 0) return ch_write_error("negative value cannot fit in %s", label);
			*out = (uint64_t)Z_LVAL_P(value); return SUCCESS;
		case IS_TRUE: *out = 1; return SUCCESS;
		case IS_FALSE: *out = 0; return SUCCESS;
		case IS_NULL:
			if (allow_null) { *out = 0; return SUCCESS; }
			return ch_write_error("null cannot be assigned to non-Nullable column %s", label);
		case IS_DOUBLE: {
			double d = Z_DVAL_P(value), whole;
			if (!isfinite(d)) return ch_write_error("non-finite double cannot be assigned to %s", label);
			if (modf(d, &whole) != 0.0) return ch_write_error("fractional double cannot be assigned to integer column %s", label);
			if (d < 0.0 || d >= 18446744073709551616.0) return ch_write_error("double out of range for integer column %s", label);
			*out = (uint64_t)d; return SUCCESS;
		}
		case IS_STRING: return ch_parse_u64(Z_STRVAL_P(value), Z_STRLEN_P(value), label, out);
		default: return ch_write_error("array / object / resource cannot be assigned to integer column %s", label);
	}
}

static int ch_strict_double(zval *value, const char *label, bool allow_null, double *out)
{
	ZVAL_DEREF(value);
	switch (Z_TYPE_P(value)) {
		case IS_LONG: *out = (double)Z_LVAL_P(value); return SUCCESS;
		case IS_TRUE: *out = 1.0; return SUCCESS;
		case IS_FALSE: *out = 0.0; return SUCCESS;
		case IS_NULL:
			if (allow_null) { *out = 0.0; return SUCCESS; }
			return ch_write_error("null cannot be assigned to non-Nullable column %s", label);
		case IS_DOUBLE:
			if (!isfinite(Z_DVAL_P(value))) return ch_write_error("non-finite double cannot be assigned to %s", label);
			*out = Z_DVAL_P(value); return SUCCESS;
		case IS_STRING: {
			char *end;
			double number;
			if (!Z_STRLEN_P(value)) return ch_write_error("empty string cannot be assigned to %s", label);
			errno = 0;
			number = strtod(Z_STRVAL_P(value), &end);
			if (errno == ERANGE || end == Z_STRVAL_P(value) || (size_t)(end - Z_STRVAL_P(value)) != Z_STRLEN_P(value) || !isfinite(number)) return ch_write_error("invalid float string for %s", label);
			*out = number; return SUCCESS;
		}
		default: return ch_write_error("array / object / resource cannot be assigned to float column %s", label);
	}
}

static zend_string *ch_value_string(zval *value)
{
	zend_string *result = zval_get_string(value);
	if (EG(exception)) { zend_string_release(result); return NULL; }
	return result;
}

static int ch_strict_string(zval *value, const char *label, bool allow_null, zend_string **out)
{
	*out = NULL;
	ZVAL_DEREF(value);
	if (Z_TYPE_P(value) == IS_NULL) {
		if (allow_null) { *out = zend_string_init("", 0, 0); return SUCCESS; }
		return ch_write_error("null cannot be assigned to non-Nullable column %s", label);
	}
	if (Z_TYPE_P(value) == IS_ARRAY) {
		return ch_write_error("array cannot be assigned to string column %s (scalar or Stringable object required)", label);
	}
	if (Z_TYPE_P(value) == IS_RESOURCE) {
		return ch_write_error("resource cannot be assigned to string column %s", label);
	}
	*out = ch_value_string(value);
	return *out ? SUCCESS : FAILURE;
}

static void ch_store_le(void *out, uint64_t number, size_t width)
{
	uint8_t *bytes = out;
	for (size_t i = 0; i < width; ++i) { bytes[i] = (uint8_t)number; number >>= 8; }
}

static void ch_allocate_fixed(ch_owned_column *column, size_t width)
{
	column->data = safe_emalloc(column->column.n_rows, width, 1);
	column->column.layout = CHC_COL_FIXED;
	column->column.fixed.data = column->data;
	column->column.fixed.elem_size = width;
}

static int ch_string_append(ch_owned_column *column, size_t row, const char *text, size_t length)
{
	size_t used = row ? (size_t)column->offsets[row - 1] : 0;
	size_t needed;
	if (length > SIZE_MAX - used) return ch_write_error("String column byte size exceeds addressable memory");
	needed = used + length;
	if (needed > column->data_capacity) {
		size_t capacity = column->data_capacity ? column->data_capacity : 64;
		while (capacity < needed) {
			if (capacity > SIZE_MAX / 2) { capacity = needed; break; }
			capacity *= 2;
		}
		column->data = erealloc(column->data, capacity);
		column->data_capacity = capacity;
	}
	if (length) memcpy((uint8_t *)column->data + used, text, length);
	column->offsets[row] = (uint64_t)needed;
	column->column.str.data = column->data;
	column->column.str.bytes = needed;
	return SUCCESS;
}

static int ch_parse_wide(const char *text, size_t length, const char *label, ch_uint128 *out)
{
	int status;
	if (!length || length > 39) return ch_write_error("%s string is empty or too long", label);
	status = ch_u128_parse(text, length, out);
	if (status == -1) return ch_write_error("%s string contains non-digit characters", label);
	if (status == -2) return ch_write_error("%s string overflows the 128-bit range", label);
	return SUCCESS;
}

static int ch_wide_cell(zval *value, bool is_signed, bool allow_null, uint8_t *out)
{
	ch_uint128 number;
	const char *label = is_signed ? "Int128" : "UInt128";
	ZVAL_DEREF(value);
	if (Z_TYPE_P(value) == IS_STRING) {
		const char *text = Z_STRVAL_P(value);
		size_t length = Z_STRLEN_P(value), skip = 0;
		bool negative = false;
		if (length && (text[0] == '+' || (is_signed && text[0] == '-'))) {
			negative = text[0] == '-'; skip = 1;
		}
		CH_WRITE_TRY(ch_parse_wide(text + skip, length - skip, label, &number));
		if (is_signed) {
			ch_uint128 bound = negative ? (ch_uint128){0, UINT64_C(0x8000000000000000)} : (ch_uint128){UINT64_MAX, UINT64_C(0x7fffffffffffffff)};
			if (ch_u128_compare(number, bound) > 0) return ch_write_error(negative ? "Int128 string is below -2^127" : "Int128 string exceeds 2^127-1");
			if (negative) number = ch_u128_negate(number);
		}
	} else if (is_signed) {
		int64_t number64;
		CH_WRITE_TRY(ch_strict_i64(value, label, allow_null, &number64));
		number.lo = (uint64_t)number64;
		number.hi = number64 < 0 ? UINT64_MAX : 0;
	} else {
		uint64_t number64;
		CH_WRITE_TRY(ch_strict_u64(value, label, allow_null, &number64));
		number = ch_u128_from_u64(number64);
	}
	ch_u128_store_le(out, number);
	return SUCCESS;
}

static int ch_decimal_cell(zval *value, const chc_type *type, bool allow_null, uint8_t *out)
{
	int precision = chc_type_decimal_precision(type), scale = chc_type_decimal_scale(type);
	zend_string *string;
	ch_uint128 number = {0, 0};
	size_t i = 0, integer_start, integer_end, fractional = 0;
	bool negative;
	int result = CH_TYPE_LOCAL_ERROR;
	ZVAL_DEREF(value);
	if (precision < 1 || precision > 38 || scale < 0 || scale > precision) return ch_write_error("Decimal scale out of supported range (max 38)");
	if (Z_TYPE_P(value) == IS_NULL) {
		if (!allow_null) return ch_write_error("null cannot be assigned to non-Nullable column Decimal");
		memset(out, 0, chc_type_elem_size(type)); return SUCCESS;
	}
	if (Z_TYPE_P(value) == IS_ARRAY || Z_TYPE_P(value) == IS_OBJECT || Z_TYPE_P(value) == IS_RESOURCE) return ch_write_error("Decimal insert requires a scalar value (string, int, or float)");
	if (Z_TYPE_P(value) == IS_FALSE) { memset(out, 0, chc_type_elem_size(type)); return SUCCESS; }
	string = ch_value_string(value);
	if (!string) return FAILURE;
	negative = ZSTR_LEN(string) && ZSTR_VAL(string)[0] == '-';
	if (ZSTR_LEN(string) && (negative || ZSTR_VAL(string)[0] == '+')) ++i;
	integer_start = i;
	while (i < ZSTR_LEN(string) && ZSTR_VAL(string)[i] >= '0' && ZSTR_VAL(string)[i] <= '9') ++i;
	integer_end = i;
	if (i < ZSTR_LEN(string) && ZSTR_VAL(string)[i] == '.') {
		size_t start = ++i;
		while (i < ZSTR_LEN(string) && ZSTR_VAL(string)[i] >= '0' && ZSTR_VAL(string)[i] <= '9') ++i;
		fractional = i - start;
	}
	if (i != ZSTR_LEN(string) || (integer_start == integer_end && !fractional)) {
		result = ch_write_error("Decimal value is not a valid decimal number"); goto cleanup;
	}
	while (integer_start < integer_end && ZSTR_VAL(string)[integer_start] == '0') ++integer_start;
	if (fractional > (size_t)scale) {
		result = ch_write_error("Decimal value has more fractional digits than the column scale allows"); goto cleanup;
	}
	if (integer_end - integer_start > (size_t)(precision - scale)) {
		result = ch_write_error("Decimal value exceeds the range of its Decimal precision/scale"); goto cleanup;
	}
	/* The previous native parser rejects '+', despite the lexical validator. */
	if (ZSTR_LEN(string) && ZSTR_VAL(string)[0] == '+') {
		result = ch_write_error("unexpected symbol '+' in decimal value"); goto cleanup;
	}
	for (i = negative ? 1 : 0; i < ZSTR_LEN(string); ++i) {
		if (ZSTR_VAL(string)[i] == '.') continue;
		if (ch_u128_mul10_add(&number, (unsigned)(ZSTR_VAL(string)[i] - '0')) != 0) {
			result = ch_write_error("value is too big for 128-bit integer"); goto cleanup;
		}
	}
	for (i = fractional; i < (size_t)scale; ++i) {
		if (ch_u128_mul10_add(&number, 0) != 0) {
			result = ch_write_error("value is too big for 128-bit integer"); goto cleanup;
		}
	}
	if (negative) number = ch_u128_negate(number);
	if (chc_type_elem_size(type) == 16) ch_u128_store_le(out, number);
	else ch_store_le(out, number.lo, chc_type_elem_size(type));
	result = SUCCESS;
cleanup:
	zend_string_release(string);
	return result;
}

static bool ch_digit(char c)
{
	return c >= '0' && c <= '9';
}

static bool ch_date_shape(const char *s)
{
	return ch_digit(s[0]) && ch_digit(s[1]) && ch_digit(s[2]) && ch_digit(s[3]) &&
		s[4] == '-' && ch_digit(s[5]) && ch_digit(s[6]) && s[7] == '-' && ch_digit(s[8]) && ch_digit(s[9]);
}

static bool ch_time_shape(const char *s)
{
	return s[0] == ' ' && ch_digit(s[1]) && ch_digit(s[2]) && s[3] == ':' &&
		ch_digit(s[4]) && ch_digit(s[5]) && s[6] == ':' && ch_digit(s[7]) && ch_digit(s[8]);
}

static int ch_parse_epoch(const char *s, size_t length, bool is_date, int64_t *out)
{
	static const unsigned month_days[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
	const char *kind = is_date ? "Date" : "DateTime";
	size_t wanted = is_date ? 10 : 19;
	int64_t year, adjusted_year, era, days;
	unsigned month, day, hour = 0, minute = 0, second = 0, max_day;
	unsigned year_of_era, shifted_month, day_of_year, day_of_era;
	bool leap;
	if (length != wanted || !ch_date_shape(s) || (!is_date && !ch_time_shape(s + 10))) {
		const char *detail = length > wanted && ch_date_shape(s) && (is_date || ch_time_shape(s + 10)) ? " (trailing characters)" : "";
		return ch_write_error("Invalid %s string%s: %.*s", kind, detail, (int)length, s);
	}
	year = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + s[3] - '0';
	month = (unsigned)((s[5] - '0') * 10 + s[6] - '0');
	day = (unsigned)((s[8] - '0') * 10 + s[9] - '0');
	if (!is_date) {
		hour = (unsigned)((s[11] - '0') * 10 + s[12] - '0');
		minute = (unsigned)((s[14] - '0') * 10 + s[15] - '0');
		second = (unsigned)((s[17] - '0') * 10 + s[18] - '0');
	}
	leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
	max_day = month >= 1 && month <= 12 ? month_days[month - 1] + (month == 2 && leap ? 1U : 0U) : 0;
	if (day < 1 || day > max_day || hour > 23 || minute > 59 || second > 59) return ch_write_error("Invalid %s string (invalid civil time): %.*s", kind, (int)length, s);
	adjusted_year = year - (month <= 2 ? 1 : 0);
	era = (adjusted_year >= 0 ? adjusted_year : adjusted_year - 399) / 400;
	year_of_era = (unsigned)(adjusted_year - era * 400);
	shifted_month = month > 2 ? month - 3 : month + 9;
	day_of_year = (153 * shifted_month + 2) / 5 + day - 1;
	day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
	days = era * 146097 + (int64_t)day_of_era - 719468;
	*out = days * 86400 + (int64_t)hour * 3600 + (int64_t)minute * 60 + second;
	return SUCCESS;
}

static int ch_parse_fractional_epoch(zval *value, unsigned precision, int64_t *whole, int64_t *fraction)
{
	const char *s = Z_STRVAL_P(value), *end = s + Z_STRLEN_P(value), *dot = memchr(s, '.', Z_STRLEN_P(value));
	size_t whole_length = dot ? (size_t)(dot - s) : Z_STRLEN_P(value);
	const char *detail = NULL;
	*fraction = 0;
	CH_WRITE_TRY(ch_parse_epoch(s, whole_length, false, whole));
	if (dot) {
		const char *p = dot + 1;
		unsigned consumed = 0;
		if (!precision) return ch_write_error("Invalid DateTime64(0) string (fractional suffix on a zero-precision column): %s", s);
		if (p == end) detail = "bare dot without fractional digits";
		while (p < end && consumed < precision && ch_digit(*p)) {
			*fraction = *fraction * 10 + *p++ - '0'; ++consumed;
		}
		if (!detail && !consumed) detail = "non-digit after dot";
		for (unsigned pad = consumed; pad < precision; ++pad) *fraction *= 10;
		if (!detail && p < end) detail = "trailing characters after fraction";
		if (detail) return ch_write_error("Invalid DateTime64 string (%s): %s", detail, s);
	}
	return SUCCESS;
}

static int ch_temporal_cell(zval *value, const chc_type *type, bool allow_null, uint8_t *out)
{
	chc_kind kind = chc_type_kind(type);
	const char *label = kind == CHC_DATE ? "Date" : kind == CHC_DATE32 ? "Date32" : kind == CHC_DATETIME ? "DateTime" : kind == CHC_DATETIME64 ? "DateTime64" : kind == CHC_TIME ? "Time" : "Time64";
	int64_t number;
	ZVAL_DEREF(value);
	if (kind == CHC_DATE || kind == CHC_DATE32 || kind == CHC_DATETIME) {
		int64_t minimum = kind == CHC_DATE32 ? -25567LL * 86400 : 0;
		int64_t maximum = kind == CHC_DATE ? 65535LL * 86400 + 86399 : kind == CHC_DATE32 ? 120529LL * 86400 + 86399 : 4294967295LL;
		if (Z_TYPE_P(value) == IS_STRING) {
			CH_WRITE_TRY(ch_parse_epoch(Z_STRVAL_P(value), Z_STRLEN_P(value), kind != CHC_DATETIME, &number));
		} else { CH_WRITE_TRY(ch_strict_i64(value, label, allow_null, &number)); }
		if (number < minimum || number > maximum) return ch_write_error("%s value is outside the representable range for this column type", label);
		if (kind == CHC_DATE) number /= 86400;
		else if (kind == CHC_DATE32) number = number >= 0 ? number / 86400 : -((-number + 86399) / 86400);
	} else if (kind == CHC_TIME) {
		if (Z_TYPE_P(value) == IS_STRING) return ch_write_error("Time column inserts require numeric seconds; string formatted-time input is not currently supported");
		CH_WRITE_TRY(ch_strict_i64(value, label, allow_null, &number));
		if (number < INT32_MIN || number > INT32_MAX) return ch_write_error("Time column value out of representable int32 range");
	} else {
		int precision = chc_type_datetime64_scale(type);
		int64_t scale = 1;
		if (precision < 0 || precision > 9) return ch_write_error("%s precision out of spec range (0..9)", label);
		for (int i = 0; i < precision; ++i) scale *= 10;
		if (Z_TYPE_P(value) == IS_STRING) {
			int64_t fraction;
			if (kind == CHC_TIME64) return ch_write_error("Time64 column inserts require numeric seconds; string formatted-time input is not currently supported");
			CH_WRITE_TRY(ch_parse_fractional_epoch(value, (unsigned)precision, &number, &fraction));
			if (number > (INT64_MAX - fraction) / scale || number < INT64_MIN / scale) return ch_write_error("DateTime64 value out of representable range for this precision");
			number = number * scale + fraction;
		} else if (Z_TYPE_P(value) == IS_DOUBLE) {
			double seconds;
			if (precision >= 7) return ch_write_error(kind == CHC_DATETIME64 ? "DateTime64 precision >= 7 cannot be set from a float without precision loss; pass a formatted date string for sub-microsecond precision" : "Time64 precision >= 7 cannot be set from a float without precision loss; pass an integer number of seconds");
			CH_WRITE_TRY(ch_strict_double(value, label, allow_null, &seconds));
			seconds *= (double)scale;
			if (!(seconds >= -9223372036854775808.0 && seconds < 9223372036854775808.0)) return ch_write_error("%s value out of representable 64-bit range", label);
			number = (int64_t)seconds;
		} else {
			CH_WRITE_TRY(ch_strict_i64(value, label, allow_null, &number));
			if (number > INT64_MAX / scale || number < INT64_MIN / scale) return ch_write_error("%s seconds value out of representable range for this precision", label);
			number *= scale;
		}
	}
	ch_store_le(out, (uint64_t)number, chc_type_elem_size(type));
	return SUCCESS;
}

static int ch_uuid_cell(zval *value, bool allow_null, uint8_t *out, const char *error)
{
	zend_string *string;
	uint64_t high = 0, low = 0;
	size_t digits = 0;
	bool dashed;
	ZVAL_DEREF(value);
	if (Z_TYPE_P(value) == IS_NULL) {
		if (!allow_null) return ch_write_error("null cannot be assigned to non-Nullable column UUID");
		memset(out, 0, 16); return SUCCESS;
	}
	string = ch_value_string(value);
	if (!string) return FAILURE;
	dashed = ZSTR_LEN(string) == 36 && ZSTR_VAL(string)[8] == '-' && ZSTR_VAL(string)[13] == '-' && ZSTR_VAL(string)[18] == '-' && ZSTR_VAL(string)[23] == '-';
	if (!dashed && ZSTR_LEN(string) != 32) goto invalid;
	for (size_t i = 0; i < ZSTR_LEN(string); ++i) {
		char c = ZSTR_VAL(string)[i];
		int digit;
		if (dashed && (i == 8 || i == 13 || i == 18 || i == 23)) continue;
		digit = c >= '0' && c <= '9' ? c - '0' : c >= 'a' && c <= 'f' ? c - 'a' + 10 : c >= 'A' && c <= 'F' ? c - 'A' + 10 : -1;
		if (digit < 0) goto invalid;
		if (digits++ < 16) high = (high << 4) | (unsigned)digit;
		else low = (low << 4) | (unsigned)digit;
	}
	ch_store_le(out, high, 8);
	ch_store_le(out + 8, low, 8);
	zend_string_release(string);
	return SUCCESS;
invalid:
	zend_string_release(string);
	return ch_write_error("%s", error);
}

static int ch_ip_cell(zval *value, bool ipv6, bool allow_null, uint8_t *out)
{
	const char *label = ipv6 ? "IPv6" : "IPv4";
	zend_string *string;
	uint8_t bytes[16];
	ZVAL_DEREF(value);
	if (Z_TYPE_P(value) == IS_NULL && allow_null) { memset(out, 0, ipv6 ? 16 : 4); return SUCCESS; }
	if (!ipv6 && (Z_TYPE_P(value) == IS_LONG || Z_TYPE_P(value) == IS_DOUBLE)) {
		uint32_t number;
		if (Z_TYPE_P(value) == IS_DOUBLE) {
			double d = Z_DVAL_P(value), whole;
			if (!isfinite(d) || modf(d, &whole) != 0.0) return ch_write_error("IPv4 float input must be an integral value");
			if (d < 0.0 || d > (double)UINT32_MAX) return ch_write_error("IPv4 integer out of range (0 .. 4294967295)");
			number = (uint32_t)d;
		} else {
			if (Z_LVAL_P(value) < 0 || (uint64_t)Z_LVAL_P(value) > UINT32_MAX) return ch_write_error("IPv4 integer out of range (0 .. 4294967295)");
			number = (uint32_t)Z_LVAL_P(value);
		}
		ch_store_le(out, number, 4);
		return SUCCESS;
	}
	CH_WRITE_TRY(ch_strict_string(value, label, allow_null, &string));
	if (memchr(ZSTR_VAL(string), '\0', ZSTR_LEN(string))) {
		zend_string_release(string);
		return ch_write_error("Invalid %s value: embedded NUL byte", label);
	}
	if (inet_pton(ipv6 ? AF_INET6 : AF_INET, ZSTR_VAL(string), bytes) != 1) {
		int result = ch_write_error("Invalid %s address: %s", label, ZSTR_VAL(string));
		zend_string_release(string);
		return result;
	}
	zend_string_release(string);
	if (ipv6) memcpy(out, bytes, 16);
	else for (size_t i = 0; i < 4; ++i) out[i] = bytes[3 - i];
	return SUCCESS;
}

static int ch_enum_cell(zval *value, const chc_type *type, bool allow_null, uint8_t *out)
{
	int64_t number = 0;
	bool found = false;
	size_t count = chc_type_enum_count(type);
	ZVAL_DEREF(value);
	if (Z_TYPE_P(value) == IS_NULL) {
		if (!allow_null) return ch_write_error("null cannot be assigned to non-Nullable Enum column");
		for (size_t i = 0; i < count; ++i) {
			const char *name;
			size_t length;
			int64_t candidate;
			chc_type_enum_at(type, i, &name, &length, &candidate);
			if (!found || candidate < number) number = candidate;
			found = true;
		}
	} else if (Z_TYPE_P(value) == IS_LONG) {
		number = Z_LVAL_P(value);
		for (size_t i = 0; i < count; ++i) {
			const char *name;
			size_t length;
			int64_t candidate;
			chc_type_enum_at(type, i, &name, &length, &candidate);
			if (candidate == number) { found = true; break; }
		}
		if (!found) {
			size_t length;
			const char *name = chc_type_name(type, &length);
			return ch_write_error("Enum integer value %" PRId64 " is not declared in %.*s", number, (int)length, name);
		}
	} else {
		zend_string *string = ch_value_string(value);
		if (!string) return FAILURE;
		for (size_t i = 0; i < count; ++i) {
			const char *name;
			size_t length;
			chc_type_enum_at(type, i, &name, &length, &number);
			if (length == ZSTR_LEN(string) && !memcmp(name, ZSTR_VAL(string), length)) { found = true; break; }
		}
		int status = found ? SUCCESS : ch_write_error("Enum value '%s' is not declared", ZSTR_VAL(string));
		zend_string_release(string);
		if (status != SUCCESS) return status;
	}
	ch_store_le(out, (uint64_t)number, chc_type_elem_size(type));
	return SUCCESS;
}

static int ch_bool_cell(zval *value, bool allow_null, uint8_t *out)
{
	ZVAL_DEREF(value);
	switch (Z_TYPE_P(value)) {
		case IS_NULL:
			if (!allow_null) return ch_write_error("null cannot be assigned to non-Nullable column Bool");
			*out = 0; return SUCCESS;
		case IS_TRUE: *out = 1; return SUCCESS;
		case IS_FALSE: *out = 0; return SUCCESS;
		case IS_LONG:
			if (Z_LVAL_P(value) != 0 && Z_LVAL_P(value) != 1) break;
			*out = (uint8_t)Z_LVAL_P(value); return SUCCESS;
		case IS_STRING:
			if (zend_string_equals_literal_ci(Z_STR_P(value), "true") || zend_string_equals_literal(Z_STR_P(value), "1")) { *out = 1; return SUCCESS; }
			if (zend_string_equals_literal_ci(Z_STR_P(value), "false") || zend_string_equals_literal(Z_STR_P(value), "0")) { *out = 0; return SUCCESS; }
			break;
	}
	return ch_write_error("Bool insert requires true/false, 0/1, or \"true\"/\"false\"/\"0\"/\"1\"");
}

static int ch_fixed_cell(zval *value, const chc_type *type, bool allow_null, uint8_t *out)
{
	chc_kind kind = chc_type_kind(type);
	size_t width = chc_type_elem_size(type);
	const char *label;
	int64_t number, minimum, maximum;
	uint64_t unsigned_number;
	switch (kind) {
		case CHC_INT8: label = "Int8"; minimum = INT8_MIN; maximum = INT8_MAX; break;
		case CHC_INT16: label = "Int16"; minimum = INT16_MIN; maximum = INT16_MAX; break;
		case CHC_INT32: label = "Int32"; minimum = INT32_MIN; maximum = INT32_MAX; break;
		case CHC_INT64: label = "Int64"; minimum = INT64_MIN; maximum = INT64_MAX; break;
		case CHC_UINT8: label = "UInt8"; minimum = 0; maximum = UINT8_MAX; break;
		case CHC_UINT16: label = "UInt16"; minimum = 0; maximum = UINT16_MAX; break;
		case CHC_UINT32:
			label = "UInt32";
			ZVAL_DEREF(value);
			if (Z_TYPE_P(value) == IS_STRING && Z_STRLEN_P(value) >= 3 && Z_STRVAL_P(value)[0] == '0' && (Z_STRVAL_P(value)[1] == 'x' || Z_STRVAL_P(value)[1] == 'X')) {
				char *end;
				errno = 0;
				unsigned_number = strtoull(Z_STRVAL_P(value), &end, 0);
				if (errno == ERANGE || (size_t)(end - Z_STRVAL_P(value)) != Z_STRLEN_P(value)) return ch_write_error("invalid hex literal for UInt32");
				if (unsigned_number > UINT32_MAX) return ch_write_error("hex literal out of range for UInt32");
				ch_store_le(out, unsigned_number, width); return SUCCESS;
			}
			CH_WRITE_TRY(ch_strict_i64(value, label, allow_null, &number));
			if (number < 0) return ch_write_error("negative value cannot fit in UInt32");
			if ((uint64_t)number > UINT32_MAX) return ch_write_error("value out of range for UInt32");
			ch_store_le(out, (uint64_t)number, width); return SUCCESS;
		case CHC_UINT64:
			CH_WRITE_TRY(ch_strict_u64(value, "UInt64", allow_null, &unsigned_number));
			ch_store_le(out, unsigned_number, width); return SUCCESS;
		case CHC_INT128: return ch_wide_cell(value, true, allow_null, out);
		case CHC_UINT128: return ch_wide_cell(value, false, allow_null, out);
		case CHC_FLOAT32:
		case CHC_FLOAT64: {
			double real;
			label = kind == CHC_FLOAT32 ? "Float32" : "Float64";
			CH_WRITE_TRY(ch_strict_double(value, label, allow_null, &real));
			if (kind == CHC_FLOAT32) {
				float narrow;
				uint32_t bits;
				if (real < -(double)FLT_MAX || real > (double)FLT_MAX) return ch_write_error("value out of range for Float32");
				narrow = (float)real;
				memcpy(&bits, &narrow, sizeof(bits));
				ch_store_le(out, bits, 4);
			} else {
				uint64_t bits;
				memcpy(&bits, &real, sizeof(bits));
				ch_store_le(out, bits, 8);
			}
			return SUCCESS;
		}
		case CHC_BOOL: return ch_bool_cell(value, allow_null, out);
		case CHC_DATE: case CHC_DATE32: case CHC_DATETIME: case CHC_DATETIME64: case CHC_TIME: case CHC_TIME64:
			return ch_temporal_cell(value, type, allow_null, out);
		case CHC_UUID: return ch_uuid_cell(value, allow_null, out, "UUID format error");
		case CHC_IPV4: return ch_ip_cell(value, false, allow_null, out);
		case CHC_IPV6: return ch_ip_cell(value, true, allow_null, out);
		case CHC_ENUM8: case CHC_ENUM16: return ch_enum_cell(value, type, allow_null, out);
		case CHC_DECIMAL32: case CHC_DECIMAL64: case CHC_DECIMAL128: return ch_decimal_cell(value, type, allow_null, out);
		case CHC_FIXED_STRING: {
			zend_string *string;
			CH_WRITE_TRY(ch_strict_string(value, "FixedString", allow_null, &string));
			if (ZSTR_LEN(string) > width) {
				zend_string_release(string);
				return ch_write_error("FixedString value exceeds the declared column width");
			}
			memcpy(out, ZSTR_VAL(string), ZSTR_LEN(string));
			memset(out + ZSTR_LEN(string), 0, width - ZSTR_LEN(string));
			zend_string_release(string);
			return SUCCESS;
		}
		default: return ch_write_error("insertColumn: unsupported fixed type");
	}
	CH_WRITE_TRY(ch_strict_i64(value, label, allow_null, &number));
	if (number < minimum || number > maximum) return ch_write_error("value out of range for %s", label);
	ch_store_le(out, (uint64_t)number, width);
	return SUCCESS;
}

static int ch_build_strings(ch_owned_column *column, bool json, bool allow_null)
{
	zval *value;
	size_t row = 0;
	column->offsets = safe_emalloc(column->column.n_rows, sizeof(*column->offsets), 1);
	column->column.layout = CHC_COL_STRING;
	column->column.str.offsets = column->offsets;
	ZEND_HASH_FOREACH_VAL(Z_ARRVAL(column->values), value) {
		zend_string *string = NULL;
		int result;
		if (!json) { CH_WRITE_TRY(ch_strict_string(value, "String", allow_null, &string)); }
		else if (Z_TYPE_P(value) == IS_ARRAY || Z_TYPE_P(value) == IS_OBJECT) {
			smart_str buffer = {0};
			if (php_json_encode(&buffer, value, 0) == FAILURE || EG(exception)) {
				smart_str_free(&buffer);
				if (EG(exception)) return CH_TYPE_JSON_CALLBACK_ERROR;
				return ch_write_error("JSON insert: failed to encode value to JSON");
			}
			smart_str_0(&buffer);
			string = buffer.s;
		} else if (Z_TYPE_P(value) == IS_STRING) {
#if PHP_VERSION_ID >= 80300
			if (!php_json_validate_ex(Z_STRVAL_P(value), Z_STRLEN_P(value), 0, PHP_JSON_PARSER_DEFAULT_DEPTH)) {
				if (EG(exception)) zend_clear_exception();
				return ch_write_error("JSON insert: string value is not valid JSON");
			}
#else
			zval probe;
			ZVAL_UNDEF(&probe);
			if (php_json_decode(&probe, Z_STRVAL_P(value), Z_STRLEN_P(value), true, PHP_JSON_PARSER_DEFAULT_DEPTH) == FAILURE) {
				if (!Z_ISUNDEF(probe)) zval_ptr_dtor(&probe);
				if (EG(exception)) zend_clear_exception();
				return ch_write_error("JSON insert: string value is not valid JSON");
			}
			zval_ptr_dtor(&probe);
#endif
			string = zend_string_copy(Z_STR_P(value));
		} else if (Z_TYPE_P(value) == IS_NULL) {
			if (!allow_null) return ch_write_error("null cannot be assigned to non-Nullable column JSON");
			string = zend_string_init("{}", 2, 0);
		} else return ch_write_error("JSON insert requires an array, object, or JSON string value");
		if (!string) return FAILURE;
		result = ch_string_append(column, row++, ZSTR_VAL(string), ZSTR_LEN(string));
		zend_string_release(string);
		if (result != SUCCESS) return result;
	} ZEND_HASH_FOREACH_END();
	return SUCCESS;
}

static int ch_build_column_inner(ch_owned_column **out, const chc_type *type,
	HashTable *values, unsigned depth, bool allow_null, const ch_value_groups *groups);

static int ch_build_array(ch_owned_column *column, const chc_type *type, unsigned depth, bool allow_null)
{
	const chc_type *inner = chc_type_child(type, 0);
	zval *value;
	size_t row = 0;
	ch_value_groups groups;
	if (!inner) return ch_write_error("Array: type metadata mismatch");
	if (chc_type_kind(inner) == CHC_ARRAY) return ch_write_error("Multidimensional Arrays are not supported for insert (Array(Array(...))); select of nested arrays works. Flatten the data or use a single-level Array column.");
	column->offsets = safe_emalloc(column->column.n_rows, sizeof(*column->offsets), 1);
	ch_allocate_children(column, 1);
	array_init(&column->temporary);
	ZEND_HASH_FOREACH_VAL(Z_ARRVAL(column->values), value) {
		zval *entry;
		ZVAL_DEREF(value);
		if (Z_TYPE_P(value) != IS_ARRAY) return ch_write_error("The inserted data is not an array type");
		ZEND_HASH_FOREACH_VAL_IND(Z_ARRVAL_P(value), entry) {
			ch_append_copy(&column->temporary, entry);
		} ZEND_HASH_FOREACH_END();
		column->offsets[row++] = zend_hash_num_elements(Z_ARRVAL(column->temporary));
	} ZEND_HASH_FOREACH_END();
	groups.ends = column->offsets;
	groups.count = column->column.n_rows;
	CH_WRITE_TRY(ch_build_column_inner(&column->children[0], inner, Z_ARRVAL(column->temporary), depth + 1, allow_null, &groups));
	column->column.layout = CHC_COL_ARRAY;
	column->column.array.offsets = column->offsets;
	column->column.array.values = &column->children[0]->column;
	return SUCCESS;
}

static int ch_build_tuple(ch_owned_column *column, const chc_type *type, unsigned depth, bool allow_null, const ch_value_groups *groups)
{
	size_t arity = chc_type_n_children(type);
	zval *value;
	ch_allocate_children(column, arity);
	array_init_size(&column->temporary, (uint32_t)arity);
	for (size_t field = 0; field < arity; ++field) {
		zval fields;
		array_init_size(&fields, (uint32_t)column->column.n_rows);
		zend_hash_next_index_insert(Z_ARRVAL(column->temporary), &fields);
	}
	ZEND_HASH_FOREACH_VAL(Z_ARRVAL(column->values), value) {
		ZVAL_DEREF(value);
		if (Z_TYPE_P(value) != IS_ARRAY) return ch_write_error("Tuple row must be a PHP array");
		if (zend_hash_num_elements(Z_ARRVAL_P(value)) != arity) return ch_write_error("Tuple row arity does not match the column type");
		for (size_t field = 0; field < arity; ++field) {
			zval *cell = zend_hash_index_find(Z_ARRVAL_P(value), field);
			zval *fields = zend_hash_index_find(Z_ARRVAL(column->temporary), field);
			if (!cell) return ch_write_error("Tuple row is missing a field value");
			ch_append_copy(fields, cell);
		}
	} ZEND_HASH_FOREACH_END();
	for (size_t field = 0; field < arity; ++field) {
		zval *fields = zend_hash_index_find(Z_ARRVAL(column->temporary), field);
		CH_WRITE_TRY(ch_build_column_inner(&column->children[field], chc_type_child(type, field), Z_ARRVAL_P(fields), depth + 1, allow_null, groups));
		column->child_views[field] = &column->children[field]->column;
	}
	column->column.layout = CHC_COL_TUPLE;
	column->column.tuple.arity = arity;
	column->column.tuple.children = column->child_views;
	return SUCCESS;
}

static int ch_build_low_cardinality(ch_owned_column *column, const chc_type *type, bool allow_null)
{
	const chc_type *inner = chc_type_child(type, 0), *leaf;
	bool nullable;
	chc_kind kind;
	ch_owned_column *dictionary;
	zval *value;
	size_t row = 0, width;
	uint32_t *keys;
	const char *label;
	if (!inner) return ch_write_error("LowCardinality: type metadata mismatch");
	nullable = chc_type_kind(inner) == CHC_NULLABLE;
	leaf = nullable ? chc_type_child(inner, 0) : inner;
	kind = chc_type_kind(leaf);
	if (kind != CHC_STRING && kind != CHC_FIXED_STRING) return ch_write_error("LowCardinality only supported over String / FixedString (Nullable allowed)");
	width = kind == CHC_FIXED_STRING ? chc_type_elem_size(leaf) : 0;
	if (kind == CHC_FIXED_STRING && !width) return ch_write_error("Invalid FixedString width");
	label = kind == CHC_STRING ? (nullable ? "LowCardinality Nullable String" : "LowCardinality String") : (nullable ? "LowCardinality Nullable FixedString" : "LowCardinality FixedString");
	ch_allocate_children(column, 1);
	column->data = safe_emalloc(column->column.n_rows, sizeof(*keys), 1);
	keys = column->data;
	array_init(&column->temporary);
	{
		zval initial;
		ZVAL_EMPTY_STRING(&initial);
		zend_hash_next_index_insert(Z_ARRVAL(column->temporary), &initial);
	}
	zend_hash_init(&column->dictionary, 8, NULL, NULL, 0);
	column->dictionary_initialized = true;
	if (!nullable) {
		zval index;
		zend_string *initial = zend_string_alloc(width, 0);
		memset(ZSTR_VAL(initial), 0, width + 1);
		ZVAL_LONG(&index, 0);
		zend_hash_add_new(&column->dictionary, initial, &index);
		zend_string_release(initial);
	}
	ZEND_HASH_FOREACH_VAL(Z_ARRVAL(column->values), value) {
		zend_string *string, *key;
		zval *known;
		ZVAL_DEREF(value);
		if (nullable && Z_TYPE_P(value) == IS_NULL) { keys[row++] = 0; continue; }
		CH_WRITE_TRY(ch_strict_string(value, label, allow_null, &string));
		if (kind == CHC_FIXED_STRING) {
			if (ZSTR_LEN(string) > width) {
				zend_string_release(string);
				return ch_write_error("FixedString value exceeds the declared column width");
			}
			key = zend_string_alloc(width, 0);
			memcpy(ZSTR_VAL(key), ZSTR_VAL(string), ZSTR_LEN(string));
			memset(ZSTR_VAL(key) + ZSTR_LEN(string), 0, width - ZSTR_LEN(string) + 1);
		} else key = zend_string_copy(string);
		known = zend_hash_find(&column->dictionary, key);
		if (known) keys[row++] = (uint32_t)Z_LVAL_P(known);
		else {
			uint32_t next = zend_hash_num_elements(Z_ARRVAL(column->temporary));
			zval index, entry;
			ZVAL_LONG(&index, (zend_long)next);
			zend_hash_add_new(&column->dictionary, key, &index);
			ZVAL_STR_COPY(&entry, string);
			zend_hash_next_index_insert(Z_ARRVAL(column->temporary), &entry);
			keys[row++] = next;
		}
		zend_string_release(key);
		zend_string_release(string);
	} ZEND_HASH_FOREACH_END();
	dictionary = ch_new_column(&column->children[0], zend_hash_num_elements(Z_ARRVAL(column->temporary)));
	ch_copy_values(&dictionary->values, Z_ARRVAL(column->temporary));
	if (kind == CHC_STRING) {
		CH_WRITE_TRY(ch_build_strings(dictionary, false, false));
	} else {
		ch_allocate_fixed(dictionary, width);
		row = 0;
		ZEND_HASH_FOREACH_VAL(Z_ARRVAL(dictionary->values), value) {
			CH_WRITE_TRY(ch_fixed_cell(value, leaf, false, (uint8_t *)dictionary->data + row++ * width));
		} ZEND_HASH_FOREACH_END();
	}
	ch_release_values(dictionary);
	column->column.layout = CHC_COL_LOW_CARDINALITY;
	column->column.lc.key_size = dictionary->column.n_rows <= 256 ? 1 : dictionary->column.n_rows <= 65536 ? 2 : 4;
	if (column->column.lc.key_size == 1) {
		for (row = 0; row < column->column.n_rows; ++row) ((uint8_t *)column->data)[row] = (uint8_t)keys[row];
	} else if (column->column.lc.key_size == 2) {
		for (row = 0; row < column->column.n_rows; ++row) ((uint16_t *)column->data)[row] = (uint16_t)keys[row];
	}
	column->column.lc.keys = column->data;
	column->column.lc.dict = &dictionary->column;
	column->column.lc.dict_n = dictionary->column.n_rows;
	zend_hash_destroy(&column->dictionary);
	column->dictionary_initialized = false;
	return SUCCESS;
}

static bool ch_map_pairs(HashTable *values)
{
	zend_ulong expected = 0, index;
	zend_string *key;
	zval *value;
	if (!zend_hash_num_elements(values)) return false;
	ZEND_HASH_FOREACH_KEY_VAL_IND(values, index, key, value) {
		ZVAL_DEREF(value);
		if (key || index != expected++ || Z_TYPE_P(value) != IS_ARRAY || zend_hash_num_elements(Z_ARRVAL_P(value)) != 2 || !zend_hash_index_exists(Z_ARRVAL_P(value), 0) || !zend_hash_index_exists(Z_ARRVAL_P(value), 1)) return false;
	} ZEND_HASH_FOREACH_END();
	return true;
}

static bool ch_map_associative_type(const chc_type *type)
{
	switch (chc_type_kind(type)) {
		case CHC_STRING: case CHC_UUID:
		case CHC_INT8: case CHC_INT16: case CHC_INT32: case CHC_INT64:
		case CHC_UINT8: case CHC_UINT16: case CHC_UINT32: case CHC_UINT64:
		case CHC_FLOAT32: case CHC_FLOAT64: return true;
		case CHC_LOW_CARDINALITY: {
			const chc_type *inner = chc_type_child(type, 0);
			return chc_type_kind(inner) == CHC_STRING;
		}
		default: return false;
	}
}

static int ch_map_key(zval *out, const chc_type *type, zend_string *key, zend_ulong index)
{
	chc_kind kind = chc_type_kind(type);
	if (kind == CHC_STRING || kind == CHC_LOW_CARDINALITY || kind == CHC_UUID) {
		if (!key) return ch_write_error(kind == CHC_UUID ? "Map(UUID, *) row entry must have a string key" : "Map(String, *) row entry must have a string key");
		ZVAL_STR_COPY(out, key);
		return SUCCESS;
	}
	if (kind == CHC_FLOAT32 || kind == CHC_FLOAT64) {
		double number;
		if (!key) number = (double)(zend_long)index;
		else {
			char *end;
			errno = 0;
			number = strtod(ZSTR_VAL(key), &end);
			if (errno == ERANGE || end == ZSTR_VAL(key) || (size_t)(end - ZSTR_VAL(key)) != ZSTR_LEN(key)) return ch_write_error("Map float key is not a valid number: %s", ZSTR_VAL(key));
		}
		if (kind == CHC_FLOAT32 && (number < -(double)FLT_MAX || number > (double)FLT_MAX)) return ch_write_error("Map key out of range for Float32");
		ZVAL_DOUBLE(out, number);
		return SUCCESS;
	}
	if (kind == CHC_UINT8 || kind == CHC_UINT16 || kind == CHC_UINT32 || kind == CHC_UINT64) {
		uint64_t number;
		char digits[21];
		int length;
		if (key) {
			CH_WRITE_TRY(ch_parse_u64(ZSTR_VAL(key), ZSTR_LEN(key), "Map UInt64 key", &number));
		} else {
			if ((zend_long)index < 0) return ch_write_error("Map unsigned key cannot be negative");
			number = (uint64_t)index;
		}
		length = snprintf(digits, sizeof(digits), "%" PRIu64, number);
		ZVAL_STRINGL(out, digits, (size_t)length);
		return SUCCESS;
	}
	if (!key) { ZVAL_LONG(out, (zend_long)index); }
	else {
		char *end;
		int64_t number;
		errno = 0;
		number = strtoll(ZSTR_VAL(key), &end, 10);
		if (errno == ERANGE || end == ZSTR_VAL(key) || (size_t)(end - ZSTR_VAL(key)) != ZSTR_LEN(key)) return ch_write_error("Map integer key is not a valid number: %s", ZSTR_VAL(key));
		ZVAL_STR_COPY(out, key);
		(void)number;
	}
	return SUCCESS;
}

static int ch_build_map_leaf(ch_owned_column **out, const chc_type *type, HashTable *values, unsigned depth, bool allow_null, bool key, const ch_owned_column *map)
{
	chc_kind kind = chc_type_kind(type);
	ch_owned_column *column;
	zval *value;
	size_t row = 0, map_row = 0, width = chc_type_elem_size(type);
	if (map && depth + 1 >= CH_CONVERT_DEPTH) return ch_write_error("ClickHouse column nested-type depth exceeds limit");
	if (kind == CHC_STRING || kind == CHC_LOW_CARDINALITY) return ch_build_column_inner(out, type, values, depth, allow_null, NULL);
	column = ch_new_column(out, zend_hash_num_elements(values));
	column->type = type;
	ch_copy_values(&column->values, values);
	ch_allocate_fixed(column, width);
	ZEND_HASH_FOREACH_VAL(Z_ARRVAL(column->values), value) {
		uint8_t *target = (uint8_t *)column->data + row * width;
		if (map) {
			while (row >= map->offsets[map_row]) ++map_row;
		}
		++row;
		if (map && map->map_shapes[map_row]) {
			CH_WRITE_TRY(ch_fixed_cell(value, type, allow_null, target));
		} else if (kind == CHC_UINT8 || kind == CHC_UINT16 || kind == CHC_UINT32 || kind == CHC_UINT64) {
			uint64_t number, maximum = kind == CHC_UINT8 ? UINT8_MAX : kind == CHC_UINT16 ? UINT16_MAX : kind == CHC_UINT32 ? UINT32_MAX : UINT64_MAX;
			const char *label = kind == CHC_UINT8 ? "UInt8" : kind == CHC_UINT16 ? "UInt16" : kind == CHC_UINT32 ? "UInt32" : "Map value UInt64";
			CH_WRITE_TRY(ch_strict_u64(value, label, allow_null, &number));
			if (number > maximum) return ch_write_error("Map %s out of range for %s", key ? "key" : "value", label);
			ch_store_le(target, number, width);
		} else if (key && (kind == CHC_FLOAT32 || kind == CHC_FLOAT64)) {
			if (kind == CHC_FLOAT32) {
				float number = (float)Z_DVAL_P(value);
				uint32_t bits;
				memcpy(&bits, &number, 4); ch_store_le(target, bits, 4);
			} else {
				uint64_t bits;
				double number = Z_DVAL_P(value);
				memcpy(&bits, &number, 8); ch_store_le(target, bits, 8);
			}
		} else if (kind == CHC_UUID) {
			CH_WRITE_TRY(ch_uuid_cell(value, allow_null, target, key ? "UUID key format error" : "UUID format error"));
		} else { CH_WRITE_TRY(ch_fixed_cell(value, type, allow_null, target)); }
	} ZEND_HASH_FOREACH_END();
	ch_release_values(column);
	return SUCCESS;
}

static int ch_map_associative_check(const chc_type *type)
{
	const chc_type *key_type = chc_type_child(type, 0), *value_type = chc_type_child(type, 1);
	size_t length;
	const char *name;
	if (!ch_map_associative_type(key_type)) {
		if (chc_type_kind(key_type) == CHC_LOW_CARDINALITY) {
			name = chc_type_name(chc_type_child(key_type, 0), &length);
			return ch_write_error("Unsupported Map key type LowCardinality(%.*s)", (int)length, name);
		}
		name = chc_type_name(type, &length);
		return ch_write_error("Unsupported Map(K, V) for row write: %.*s", (int)length, name);
	}
	if (!ch_map_associative_type(value_type)) {
		if (chc_type_kind(value_type) == CHC_LOW_CARDINALITY) {
			name = chc_type_name(chc_type_child(value_type, 0), &length);
			return ch_write_error("Unsupported Map value type LowCardinality(%.*s)", (int)length, name);
		}
		name = chc_type_name(value_type, &length);
		return ch_write_error("Unsupported Map value type: %.*s", (int)length, name);
	}
	return SUCCESS;
}

static int ch_build_map(ch_owned_column *column, const chc_type *type, unsigned depth, bool allow_null, const ch_value_groups *groups)
{
	const chc_type *key_type = chc_type_child(type, 0), *value_type = chc_type_child(type, 1);
	zval *row, *keys, *values;
	bool any_pairs = false, any_assoc = false;
	size_t row_index = 0, group_count = groups ? groups->count : 1;
	ch_owned_column *tuple;
	ch_value_groups child_groups;
	HashPosition position;
	if (!key_type || !value_type) return ch_write_error("Map: type metadata mismatch");
	column->map_shapes = safe_emalloc(column->column.n_rows, 1, 1);
	zend_hash_internal_pointer_reset_ex(Z_ARRVAL(column->values), &position);
	for (size_t group = 0; group < group_count; ++group) {
		size_t begin = row_index, end = groups ? (size_t)groups->ends[group] : column->column.n_rows;
		bool saw_pairs = false, saw_assoc = false;
		for (; row_index < end; ++row_index) {
			row = zend_hash_get_current_data_ex(Z_ARRVAL(column->values), &position);
			ZVAL_DEREF(row);
			if (Z_TYPE_P(row) != IS_ARRAY) return ch_write_error("Map row must be a PHP array");
			if (zend_hash_num_elements(Z_ARRVAL_P(row))) {
				if (ch_map_pairs(Z_ARRVAL_P(row))) saw_pairs = true;
				else saw_assoc = true;
			}
			zend_hash_move_forward_ex(Z_ARRVAL(column->values), &position);
		}
		if (saw_pairs && saw_assoc) return ch_write_error("Map rows must consistently use associative arrays or ordered key/value pairs");
		if (!saw_pairs) {
			CH_WRITE_TRY(ch_map_associative_check(type));
			any_assoc = true;
		} else any_pairs = true;
		memset(column->map_shapes + begin, saw_pairs, end - begin);
	}
	row_index = 0;
	column->offsets = safe_emalloc(column->column.n_rows, sizeof(*column->offsets), 1);
	array_init_size(&column->temporary, 2);
	{
		zval table;
		array_init(&table); zend_hash_next_index_insert(Z_ARRVAL(column->temporary), &table);
		array_init(&table); zend_hash_next_index_insert(Z_ARRVAL(column->temporary), &table);
	}
	keys = zend_hash_index_find(Z_ARRVAL(column->temporary), 0);
	values = zend_hash_index_find(Z_ARRVAL(column->temporary), 1);
	ZEND_HASH_FOREACH_VAL(Z_ARRVAL(column->values), row) {
		zval *entry;
		zend_string *name;
		zend_ulong index;
		ZVAL_DEREF(row);
		ZEND_HASH_FOREACH_KEY_VAL_IND(Z_ARRVAL_P(row), index, name, entry) {
			if (column->map_shapes[row_index]) {
				zval *key, *value;
				ZVAL_DEREF(entry);
				key = Z_TYPE_P(entry) == IS_ARRAY ? zend_hash_index_find(Z_ARRVAL_P(entry), 0) : NULL;
				value = Z_TYPE_P(entry) == IS_ARRAY ? zend_hash_index_find(Z_ARRVAL_P(entry), 1) : NULL;
				if (!key || !value) return ch_write_error("Map ordered pair must be an array of [key, value]");
				ch_append_copy(keys, key); ch_append_copy(values, value);
			} else {
				zval key;
				CH_WRITE_TRY(ch_map_key(&key, key_type, name, index));
				zend_hash_next_index_insert(Z_ARRVAL_P(keys), &key);
				ch_append_copy(values, entry);
			}
		} ZEND_HASH_FOREACH_END();
		column->offsets[row_index++] = zend_hash_num_elements(Z_ARRVAL_P(keys));
	} ZEND_HASH_FOREACH_END();
	ch_allocate_children(column, 1);
	tuple = ch_new_column(&column->children[0], zend_hash_num_elements(Z_ARRVAL_P(keys)));
	ch_allocate_children(tuple, 2);
	if (!any_assoc) {
		child_groups.ends = column->offsets;
		child_groups.count = column->column.n_rows;
		CH_WRITE_TRY(ch_build_column_inner(&tuple->children[0], key_type, Z_ARRVAL_P(keys), depth + 1, allow_null, &child_groups));
		CH_WRITE_TRY(ch_build_column_inner(&tuple->children[1], value_type, Z_ARRVAL_P(values), depth + 1, allow_null, &child_groups));
	} else {
		CH_WRITE_TRY(ch_build_map_leaf(&tuple->children[0], key_type, Z_ARRVAL_P(keys), depth, allow_null, true, any_pairs ? column : NULL));
		CH_WRITE_TRY(ch_build_map_leaf(&tuple->children[1], value_type, Z_ARRVAL_P(values), depth, allow_null, false, any_pairs ? column : NULL));
	}
	tuple->column.layout = CHC_COL_TUPLE;
	tuple->column.tuple.arity = 2;
	tuple->column.tuple.children = tuple->child_views;
	tuple->child_views[0] = &tuple->children[0]->column;
	tuple->child_views[1] = &tuple->children[1]->column;
	column->column.layout = CHC_COL_ARRAY;
	column->column.array.offsets = column->offsets;
	column->column.array.values = &tuple->column;
	return SUCCESS;
}

static int ch_build_geo(ch_owned_column *column, unsigned level, bool allow_null)
{
	zval *value;
	size_t row = 0;
	if (!level) {
		ch_allocate_children(column, 2);
		for (size_t i = 0; i < 2; ++i) {
			ch_new_column(&column->children[i], column->column.n_rows);
			ch_allocate_fixed(column->children[i], 8);
			column->child_views[i] = &column->children[i]->column;
		}
		ZEND_HASH_FOREACH_VAL(Z_ARRVAL(column->values), value) {
			ZVAL_DEREF(value);
			if (Z_TYPE_P(value) != IS_ARRAY) return ch_write_error("Point must be a PHP array of 2 numbers");
			if (zend_hash_num_elements(Z_ARRVAL_P(value)) != 2) return ch_write_error("Point must have exactly 2 elements");
			for (size_t i = 0; i < 2; ++i) {
				zval *coordinate = zend_hash_index_find(Z_ARRVAL_P(value), i);
				double number;
				uint64_t bits;
				if (!coordinate) return ch_write_error("Point is missing an element");
				CH_WRITE_TRY(ch_strict_double(coordinate, "Point coordinate", allow_null, &number));
				memcpy(&bits, &number, 8);
				ch_store_le((uint8_t *)column->children[i]->data + row * 8, bits, 8);
			}
			++row;
		} ZEND_HASH_FOREACH_END();
		column->column.layout = CHC_COL_TUPLE;
		column->column.tuple.arity = 2;
		column->column.tuple.children = column->child_views;
	} else {
		ch_owned_column *child;
		const char *error = level == 1 ? "Ring must be a PHP array of points" : level == 2 ? "Polygon must be a PHP array of rings" : "MultiPolygon must be a PHP array of polygons";
		ch_allocate_children(column, 1);
		column->offsets = safe_emalloc(column->column.n_rows, sizeof(*column->offsets), 1);
		array_init(&column->temporary);
		ZEND_HASH_FOREACH_VAL(Z_ARRVAL(column->values), value) {
			zval *entry;
			ZVAL_DEREF(value);
			if (Z_TYPE_P(value) != IS_ARRAY) return ch_write_error("%s", error);
			ZEND_HASH_FOREACH_VAL_IND(Z_ARRVAL_P(value), entry) {
				ch_append_copy(&column->temporary, entry);
			} ZEND_HASH_FOREACH_END();
			column->offsets[row++] = zend_hash_num_elements(Z_ARRVAL(column->temporary));
		} ZEND_HASH_FOREACH_END();
		child = ch_new_column(&column->children[0], zend_hash_num_elements(Z_ARRVAL(column->temporary)));
		ch_copy_values(&child->values, Z_ARRVAL(column->temporary));
		CH_WRITE_TRY(ch_build_geo(child, level - 1, allow_null));
		ch_release_values(child);
		column->column.layout = CHC_COL_ARRAY;
		column->column.array.offsets = column->offsets;
		column->column.array.values = &child->column;
	}
	return SUCCESS;
}

static int ch_fill_column(ch_owned_column *column, const chc_type *type,
	HashTable *values, unsigned depth, bool allow_null, const ch_value_groups *groups)
{
	chc_kind kind;
	int result = SUCCESS;
	zval *value;
	size_t row = 0;
	if (depth >= CH_CONVERT_DEPTH) return ch_write_error("ClickHouse column nested-type depth exceeds limit");
	if (!type) return ch_write_error("insertColumn: missing type metadata");
	kind = chc_type_kind(type);
	column->type = type;
	ch_copy_values(&column->values, values);
	switch (kind) {
		case CHC_INT8: case CHC_INT16: case CHC_INT32: case CHC_INT64: case CHC_INT128:
		case CHC_UINT8: case CHC_UINT16: case CHC_UINT32: case CHC_UINT64: case CHC_UINT128:
		case CHC_FLOAT32: case CHC_FLOAT64: case CHC_BOOL:
		case CHC_DATE: case CHC_DATE32: case CHC_DATETIME: case CHC_DATETIME64: case CHC_TIME: case CHC_TIME64:
		case CHC_UUID: case CHC_IPV4: case CHC_IPV6:
		case CHC_ENUM8: case CHC_ENUM16:
		case CHC_DECIMAL32: case CHC_DECIMAL64: case CHC_DECIMAL128:
		case CHC_FIXED_STRING: {
			size_t width = chc_type_elem_size(type);
			if (!width) { result = ch_write_error("Invalid FixedString width"); break; }
			ch_allocate_fixed(column, width);
			ZEND_HASH_FOREACH_VAL(Z_ARRVAL(column->values), value) {
				result = ch_fixed_cell(value, type, allow_null, (uint8_t *)column->data + row++ * width);
				if (result != SUCCESS) break;
			} ZEND_HASH_FOREACH_END();
			break;
		}
		case CHC_STRING: result = ch_build_strings(column, false, allow_null); break;
		case CHC_JSON: result = ch_build_strings(column, true, allow_null); break;
		case CHC_ARRAY: result = ch_build_array(column, type, depth, allow_null); break;
		case CHC_TUPLE: result = ch_build_tuple(column, type, depth, allow_null, groups); break;
		case CHC_MAP: result = ch_build_map(column, type, depth, allow_null, groups); break;
		case CHC_LOW_CARDINALITY: result = ch_build_low_cardinality(column, type, allow_null); break;
		case CHC_NULLABLE:
			ch_allocate_children(column, 1);
			column->nulls = safe_emalloc(column->column.n_rows, 1, 1);
			ZEND_HASH_FOREACH_VAL(Z_ARRVAL(column->values), value) {
				ZVAL_DEREF(value);
				column->nulls[row++] = Z_TYPE_P(value) == IS_NULL;
			} ZEND_HASH_FOREACH_END();
			result = ch_build_column_inner(&column->children[0], chc_type_child(type, 0), Z_ARRVAL(column->values), depth + 1, true, groups);
			if (result == SUCCESS) {
				column->column.layout = CHC_COL_NULLABLE;
				column->column.nullable.null_map = column->nulls;
				column->column.nullable.inner = &column->children[0]->column;
			}
			break;
		case CHC_POINT: result = ch_build_geo(column, 0, allow_null); break;
		case CHC_RING: result = ch_build_geo(column, 1, allow_null); break;
		case CHC_POLYGON: result = ch_build_geo(column, 2, allow_null); break;
		case CHC_MULTI_POLYGON: result = ch_build_geo(column, 3, allow_null); break;
		case CHC_NOTHING: case CHC_VOID: result = ch_write_error("can't support Void"); break;
		default: {
			size_t length;
			const char *name = chc_type_name(type, &length);
			result = ch_write_error("insertColumn: unsupported type code: %.*s", (int)length, name);
			break;
		}
	}
	if (result == SUCCESS) ch_release_values(column);
	return result;
}

static int ch_build_column_inner(ch_owned_column **out, const chc_type *type,
	HashTable *values, unsigned depth, bool allow_null, const ch_value_groups *groups)
{
	ch_owned_column *column = ch_new_column(out, zend_hash_num_elements(values));
	return ch_fill_column(column, type, values, depth, allow_null, groups);
}

static bool ch_saf_terminal(const chc_type *type)
{
	switch (chc_type_kind(type)) {
		case CHC_INT8: case CHC_INT16: case CHC_INT32: case CHC_INT64: case CHC_INT128:
		case CHC_UINT8: case CHC_UINT16: case CHC_UINT32: case CHC_UINT64: case CHC_UINT128:
		case CHC_FLOAT32: case CHC_FLOAT64: case CHC_BOOL:
		case CHC_DATE: case CHC_DATE32: case CHC_DATETIME: case CHC_DATETIME64:
		case CHC_TIME: case CHC_TIME64: case CHC_UUID: case CHC_IPV4: case CHC_IPV6:
		case CHC_STRING: case CHC_FIXED_STRING: case CHC_JSON: case CHC_NOTHING:
		case CHC_DECIMAL32: case CHC_DECIMAL64: case CHC_DECIMAL128:
		case CHC_POINT: case CHC_RING: case CHC_POLYGON: case CHC_MULTI_POLYGON:
			return true;
		default: return false;
	}
}

static bool ch_type_supported_at(const chc_type *type, unsigned depth)
{
	chc_kind kind;
	size_t children;
	if (!type || depth > CH_CALLER_TYPE_DEPTH) return false;
	kind = chc_type_kind(type);
	children = chc_type_n_children(type);
	switch (kind) {
		case CHC_INT8: case CHC_INT16: case CHC_INT32: case CHC_INT64: case CHC_INT128:
		case CHC_UINT8: case CHC_UINT16: case CHC_UINT32: case CHC_UINT64: case CHC_UINT128:
		case CHC_FLOAT32: case CHC_FLOAT64: case CHC_BOOL:
		case CHC_DATE: case CHC_DATE32: case CHC_DATETIME: case CHC_TIME:
		case CHC_UUID: case CHC_IPV4: case CHC_IPV6: case CHC_STRING: case CHC_JSON:
		case CHC_ENUM8: case CHC_ENUM16:
		case CHC_POINT: case CHC_RING: case CHC_POLYGON: case CHC_MULTI_POLYGON:
		case CHC_NOTHING:
			return children == 0;
		case CHC_FIXED_STRING:
		case CHC_DATETIME64: case CHC_TIME64:
		case CHC_DECIMAL32: case CHC_DECIMAL64: case CHC_DECIMAL128: {
			size_t length;
			const char *name = chc_type_name(type, &length);
			return children == 0 && name && memchr(name, '(', length) != NULL;
		}
		case CHC_SIMPLE_AGGREGATE_FUNCTION:
			return children == 1 && ch_saf_terminal(chc_type_child(type, 0)) && ch_type_supported_at(chc_type_child(type, 0), depth + 1);
		case CHC_ARRAY: case CHC_NULLABLE:
			return children == 1 && ch_type_supported_at(chc_type_child(type, 0), depth + 1);
		case CHC_LOW_CARDINALITY: {
			const chc_type *inner;
			if (children != 1) return false;
			inner = chc_type_child(type, 0);
			if (chc_type_kind(inner) == CHC_NULLABLE) {
				if (chc_type_n_children(inner) != 1) return false;
				inner = chc_type_child(inner, 0);
			}
			return ch_type_supported_at(inner, depth + 1) &&
				(chc_type_kind(inner) == CHC_STRING || chc_type_kind(inner) == CHC_FIXED_STRING);
		}
		case CHC_MAP:
			if (children != 2) return false;
			break;
		case CHC_TUPLE:
			if (!children) {
				ch_write_error("AST child element index out of bounds: 0");
				return false;
			}
			break;
		default: return false;
	}
	for (size_t i = 0; i < children; ++i) {
		if (!ch_type_supported_at(chc_type_child(type, i), depth + 1)) return false;
	}
	return true;
}

bool ch_insert_type_supported(const chc_type *type)
{
	return ch_type_supported_at(type, 0);
}

static bool ch_type_needs_normalization(const chc_type *type)
{
	chc_kind kind = chc_type_kind(type);
	size_t count = chc_type_n_children(type);
	if (kind == CHC_SIMPLE_AGGREGATE_FUNCTION) return true;
	/* The native factory can unwrap a child while retaining its parent's spelling. */
	if (kind == CHC_ARRAY || kind == CHC_TUPLE || kind == CHC_NULLABLE ||
		kind == CHC_MAP || kind == CHC_LOW_CARDINALITY) return true;
	if (kind == CHC_DATETIME || kind == CHC_DATETIME64) {
		size_t length;
		chc_type_timezone(type, &length);
		if (length) return true;
	}
	for (size_t i = 0; i < count; ++i) {
		if (kind == CHC_TUPLE && chc_type_tuple_field_name(type, i, NULL)) return true;
		if (ch_type_needs_normalization(chc_type_child(type, i))) return true;
	}
	return false;
}

static void ch_normalized_type_name(smart_str *name, const chc_type *type)
{
	chc_kind kind = chc_type_kind(type);
	size_t count = chc_type_n_children(type), length;
	const char *original;
	char scale[24];
	int written;
	if (!ch_type_needs_normalization(type)) {
		original = chc_type_name(type, &length);
		smart_str_appendl(name, original, length);
		return;
	}
	switch (kind) {
		case CHC_SIMPLE_AGGREGATE_FUNCTION:
			ch_normalized_type_name(name, chc_type_child(type, 0));
			return;
		case CHC_DATETIME:
			smart_str_appends(name, "DateTime");
			return;
		case CHC_DATETIME64:
			written = snprintf(scale, sizeof(scale), "DateTime64(%d)", chc_type_datetime64_scale(type));
			smart_str_appendl(name, scale, (size_t)written);
			return;
		case CHC_TUPLE: smart_str_appends(name, "Tuple("); break;
		case CHC_ARRAY: smart_str_appends(name, "Array("); break;
		case CHC_NULLABLE: smart_str_appends(name, "Nullable("); break;
		case CHC_MAP: smart_str_appends(name, "Map("); break;
		case CHC_LOW_CARDINALITY: smart_str_appends(name, "LowCardinality("); break;
		default:
			original = chc_type_name(type, &length);
			smart_str_appendl(name, original, length);
			return;
	}
	for (size_t i = 0; i < count; ++i) {
		if (i) smart_str_appends(name, ", ");
		ch_normalized_type_name(name, chc_type_child(type, i));
	}
	smart_str_appendc(name, ')');
}

int ch_build_column(ch_owned_column **out, const chc_type *type,
	HashTable *values, unsigned depth, bool allow_null)
{
	ch_owned_column *column;
	smart_str name = {0};
	chc_err error = {{0}};
	int status;
	*out = NULL;
	if (EG(exception)) return FAILURE;
	if (depth >= CH_CONVERT_DEPTH) return ch_write_error("ClickHouse column nested-type depth exceeds limit");
	if (!ch_insert_type_supported(type)) {
		size_t length = 0;
		const char *original = type ? chc_type_name(type, &length) : "";
		if (EG(exception)) return CH_TYPE_LOCAL_ERROR;
		return ch_write_error("insertColumn: unsupported type code: %.*s", (int)length, original);
	}
	column = ch_new_column(out, zend_hash_num_elements(values));
	ch_normalized_type_name(&name, type);
	smart_str_0(&name);
	status = chc_type_parse(ZSTR_VAL(name.s), ZSTR_LEN(name.s), &ch_php_allocator, &column->owned_type, &error);
	smart_str_free(&name);
	if (status != CHC_OK) return ch_write_error("%s", error.msg);
	return ch_fill_column(column, column->owned_type, values, depth, allow_null, NULL);
}
