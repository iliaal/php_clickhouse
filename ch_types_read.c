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

#include <inttypes.h>
#include <string.h>

#include "ch_types.h"
#include "ch_wide.h"
#include "ext/json/php_json.h"
#include "main/snprintf.h"

#ifdef PHP_WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#endif

enum { CH_READ_MAX_DEPTH = 32, CH_READ_MAX_JSON_BYTES = 16 * 1024 * 1024 };

static int ch_read_value_context(zval *out, const chc_type *type, const chc_column *column,
	size_t row, zend_long flags, unsigned depth, bool sliced);

static int ch_read_error(const char *message)
{
	if (!EG(exception)) {
		zend_throw_exception(clickhouse_exception_ce, message, 0);
	}
	return FAILURE;
}

static int ch_read_type_error(const char *prefix, const chc_type *type)
{
	size_t length = 0;
	const char *name = chc_type_name(type, &length);
	if (!EG(exception)) {
		zend_string *message = zend_string_alloc(strlen(prefix) + length, 0);
		memcpy(ZSTR_VAL(message), prefix, strlen(prefix));
		if (length) memcpy(ZSTR_VAL(message) + strlen(prefix), name, length);
		ZSTR_VAL(message)[ZSTR_LEN(message)] = '\0';
		zend_throw_exception(clickhouse_exception_ce, ZSTR_VAL(message), 0);
		zend_string_release(message);
	}
	return FAILURE;
}

static void ch_read_signed(zval *out, int64_t value)
{
	if (value >= (int64_t)ZEND_LONG_MIN && value <= (int64_t)ZEND_LONG_MAX) {
		ZVAL_LONG(out, (zend_long)value);
	} else {
		char buffer[32];
		int length = snprintf(buffer, sizeof(buffer), "%" PRId64, value);
		ZVAL_STRINGL(out, buffer, (size_t)length);
	}
}

static void ch_read_unsigned(zval *out, uint64_t value)
{
	if (value <= (uint64_t)ZEND_LONG_MAX) {
		ZVAL_LONG(out, (zend_long)value);
	} else {
		char buffer[32];
		int length = snprintf(buffer, sizeof(buffer), "%" PRIu64, value);
		ZVAL_STRINGL(out, buffer, (size_t)length);
	}
}

static uint64_t ch_read_le(const unsigned char *bytes, size_t width)
{
	uint64_t value = 0;
	for (size_t i = 0; i < width; ++i) value |= (uint64_t)bytes[i] << (i * 8);
	return value;
}

static int64_t ch_read_i64(const unsigned char *bytes, size_t width)
{
	uint64_t value = ch_read_le(bytes, width);
	uint64_t sign = UINT64_C(1) << (width * 8 - 1);
	if (!(value & sign)) return (int64_t)value;
	uint64_t mask = width == 8 ? UINT64_MAX : (UINT64_C(1) << (width * 8)) - 1;
	return -1 - (int64_t)((~value) & mask);
}

static int ch_read_fixed(const chc_column *column, size_t row, size_t width,
	const unsigned char **bytes)
{
	if (column->layout != CHC_COL_FIXED || column->fixed.elem_size != width ||
		row >= column->n_rows || (width && (!column->fixed.data || row > SIZE_MAX / width))) {
		return ch_read_error("Fixed column read: row or element width is outside the column data");
	}
	*bytes = width ? (const unsigned char *)column->fixed.data + row * width : (const unsigned char *)"";
	return SUCCESS;
}

static int ch_read_string_view(const chc_column *column, size_t row,
	const char **bytes, size_t *length)
{
	if (column->layout != CHC_COL_STRING || row >= column->n_rows || !column->str.offsets) {
		return ch_read_error("String read: row is outside the column data");
	}
	uint64_t begin = row ? column->str.offsets[row - 1] : 0;
	uint64_t end = column->str.offsets[row];
	if (end < begin || end > column->str.bytes || (end && !column->str.data)) {
		return ch_read_error("String read: element offsets are out of range");
	}
	*bytes = end ? (const char *)column->str.data + (size_t)begin : "";
	*length = (size_t)(end - begin);
	return SUCCESS;
}

static int ch_read_string(zval *out, const chc_type *type, const chc_column *column,
	size_t row, zend_long flags)
{
	const char *bytes;
	size_t length;
	if (chc_type_kind(type) == CHC_FIXED_STRING) {
		const unsigned char *fixed;
		length = chc_type_elem_size(type);
		if (ch_read_fixed(column, row, length, &fixed) == FAILURE) return FAILURE;
		bytes = (const char *)fixed;
		if (!(flags & SC_FETCH_FIXEDSTRING_BINARY)) {
			while (length && bytes[length - 1] == '\0') --length;
		}
	} else if (ch_read_string_view(column, row, &bytes, &length) == FAILURE) {
		return FAILURE;
	}
	ZVAL_STRINGL(out, bytes, length);
	return SUCCESS;
}

static int ch_read_json(zval *out, const chc_column *column, size_t row, zend_long flags)
{
	const char *bytes;
	size_t length;
	if (ch_read_string_view(column, row, &bytes, &length) == FAILURE) return FAILURE;
	if (!(flags & (SC_FETCH_JSON_AS_ARRAY | SC_FETCH_JSON_AS_OBJECT))) {
		ZVAL_STRINGL(out, bytes, length);
		return SUCCESS;
	}
	if (length > CH_READ_MAX_JSON_BYTES) {
		return ch_read_error("JSON read: cell exceeds maximum JSON decode size (16 MiB)");
	}
	/* The PHP scanner requires a NUL beyond the explicit string length. */
	zend_string *json = zend_string_init(bytes, length, 0);
	int result = php_json_decode(out, ZSTR_VAL(json), length,
		(flags & SC_FETCH_JSON_AS_ARRAY) != 0, PHP_JSON_PARSER_DEFAULT_DEPTH);
	zend_string_release(json);
	if (result == FAILURE) return ch_read_error("JSON read: failed to decode server JSON value");
	return SUCCESS;
}

static int ch_read_epoch(zval *out, int64_t epoch, bool with_time)
{
	int64_t days = epoch / 86400;
	int64_t seconds = epoch % 86400;
	if (seconds < 0) { seconds += 86400; --days; }
	days += 719468;
	int64_t era = (days >= 0 ? days : days - 146096) / 146097;
	unsigned doe = (unsigned)(days - era * 146097);
	unsigned yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
	int64_t year = (int64_t)yoe + era * 400;
	unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
	unsigned mp = (5 * doy + 2) / 153;
	unsigned day = doy - (153 * mp + 2) / 5 + 1;
	unsigned month = mp < 10 ? mp + 3 : mp - 9;
	year += month <= 2;
	char buffer[64];
	int length;
	if (with_time) {
		length = snprintf(buffer, sizeof(buffer), "%04" PRId64 "-%02u-%02u %02u:%02u:%02u",
			year, month, day, (unsigned)(seconds / 3600), (unsigned)((seconds / 60) % 60), (unsigned)(seconds % 60));
	} else {
		length = snprintf(buffer, sizeof(buffer), "%04" PRId64 "-%02u-%02u", year, month, day);
	}
	if (length < 0 || (size_t)length >= sizeof(buffer)) return ch_read_error("date formatting failed");
	ZVAL_STRINGL(out, buffer, (size_t)length);
	return SUCCESS;
}

static int ch_read_temporal(zval *out, const chc_type *type, const unsigned char *bytes, zend_long flags)
{
	chc_kind kind = chc_type_kind(type);
	int64_t raw = (kind == CHC_DATE || kind == CHC_DATETIME)
		? (int64_t)ch_read_le(bytes, chc_type_elem_size(type))
		: ch_read_i64(bytes, chc_type_elem_size(type));
	int precision = (kind == CHC_DATETIME64 || kind == CHC_TIME64) ? chc_type_datetime64_scale(type) : 0;
	if (precision < 0 || precision > 9) {
		return ch_read_error(kind == CHC_DATETIME64 ? "DateTime64 precision out of spec range (0..9)" : "Time64 precision out of spec range (0..9)");
	}
	if (kind == CHC_DATE || kind == CHC_DATE32) raw *= 86400;
	if (!(flags & SC_FETCH_DATE_AS_STRINGS)) {
		ch_read_signed(out, raw);
		return SUCCESS;
	}
	if (kind == CHC_DATE || kind == CHC_DATE32 || kind == CHC_DATETIME) {
		return ch_read_epoch(out, raw, kind == CHC_DATETIME);
	}
	static const int64_t powers[] = {1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000};
	int64_t scale = powers[precision];
	uint64_t fraction;
	char buffer[80];
	size_t length;
	if (kind == CHC_DATETIME64) {
		int64_t whole = raw / scale;
		int64_t rem = raw % scale;
		if (rem < 0) { rem += scale; --whole; }
		if (ch_read_epoch(out, whole, true) == FAILURE) return FAILURE;
		length = Z_STRLEN_P(out);
		memcpy(buffer, Z_STRVAL_P(out), length);
		zval_ptr_dtor(out);
		ZVAL_UNDEF(out);
		fraction = (uint64_t)rem;
	} else {
		uint64_t magnitude = raw < 0 ? UINT64_C(0) - (uint64_t)raw : (uint64_t)raw;
		uint64_t whole = magnitude / (uint64_t)scale;
		fraction = magnitude % (uint64_t)scale;
		int written = snprintf(buffer, sizeof(buffer), "%s%02" PRIu64 ":%02" PRIu64 ":%02" PRIu64,
			raw < 0 ? "-" : "", whole / 3600, (whole / 60) % 60, whole % 60);
		if (written < 0 || (size_t)written >= sizeof(buffer)) return ch_read_error("time formatting failed");
		length = (size_t)written;
	}
	if (precision) {
		int written = snprintf(buffer + length, sizeof(buffer) - length, ".%0*" PRIu64, precision, fraction);
		if (written < 0 || (size_t)written >= sizeof(buffer) - length) return ch_read_error("time formatting failed");
		length += (size_t)written;
	}
	ZVAL_STRINGL(out, buffer, length);
	return SUCCESS;
}

static int ch_read_decimal(zval *out, const chc_type *type, const unsigned char *bytes)
{
	size_t width = chc_type_elem_size(type);
	ch_uint128 value;
	bool negative;
	if (width == 16) {
		ch_u128_load_le(&value, bytes);
		negative = (value.hi >> 63) != 0;
	} else {
		int64_t signed_value = ch_read_i64(bytes, width);
		negative = signed_value < 0;
		value.lo = (uint64_t)signed_value;
		value.hi = negative ? UINT64_MAX : 0;
	}
	if (negative) value = ch_u128_negate(value);
	char buffer[64];
	size_t sign = negative ? 1 : 0;
	if (negative) buffer[0] = '-';
	size_t length = sign + ch_u128_format(value, buffer + sign);
	int scale = chc_type_decimal_scale(type);
	if (scale < 0 || scale > 38) return ch_read_error("Decimal scale out of supported range (max 38)");
	if (scale) {
		size_t digits = length - sign;
		if (digits <= (size_t)scale) {
			size_t padding = (size_t)scale + 1 - digits;
			memmove(buffer + sign + padding, buffer + sign, digits);
			memset(buffer + sign, '0', padding);
			length += padding;
		}
		size_t dot = length - (size_t)scale;
		memmove(buffer + dot + 1, buffer + dot, (size_t)scale);
		buffer[dot] = '.';
		++length;
	}
	ZVAL_STRINGL(out, buffer, length);
	return SUCCESS;
}

static void ch_read_uuid(zval *out, const unsigned char *bytes, zend_long flags)
{
	static const char hex[] = "0123456789abcdef";
	char buffer[36];
	size_t length = 0;
	for (size_t i = 0; i < 16; ++i) {
		if ((flags & SC_FETCH_UUID_WITH_DASHES) && (i == 4 || i == 6 || i == 8 || i == 10)) buffer[length++] = '-';
		size_t offset = i < 8 ? 7 - i : 23 - i;
		buffer[length++] = hex[bytes[offset] >> 4];
		buffer[length++] = hex[bytes[offset] & 15];
	}
	ZVAL_STRINGL(out, buffer, length);
}

static int ch_read_ip(zval *out, const unsigned char *bytes, bool ipv6)
{
	unsigned char address[16];
	if (ipv6) {
		memcpy(address, bytes, 16);
	} else {
		for (size_t i = 0; i < 4; ++i) address[i] = bytes[3 - i];
	}
	char buffer[INET6_ADDRSTRLEN];
	if (!inet_ntop(ipv6 ? AF_INET6 : AF_INET, address, buffer, sizeof(buffer))) {
		return ch_read_error(ipv6 ? "Invalid IPv6 data" : "Invalid IPv4 data");
	}
	ZVAL_STRING(out, buffer);
	return SUCCESS;
}

static int ch_read_scalar(zval *out, const chc_type *type, const chc_column *column,
	size_t row, zend_long flags)
{
	const unsigned char *bytes;
	size_t width = chc_type_elem_size(type);
	if (ch_read_fixed(column, row, width, &bytes) == FAILURE) return FAILURE;
	switch (chc_type_kind(type)) {
	case CHC_INT8: case CHC_INT16: case CHC_INT32: case CHC_INT64:
		ch_read_signed(out, ch_read_i64(bytes, width));
		break;
	case CHC_UINT8: case CHC_UINT16: case CHC_UINT32: case CHC_UINT64:
		ch_read_unsigned(out, ch_read_le(bytes, width));
		break;
	case CHC_FLOAT32: {
		uint32_t bits = (uint32_t)ch_read_le(bytes, 4);
		float value;
		memcpy(&value, &bits, sizeof(value));
		ZVAL_DOUBLE(out, (double)value);
		break;
	}
	case CHC_FLOAT64: {
		uint64_t bits = ch_read_le(bytes, 8);
		double value;
		memcpy(&value, &bits, sizeof(value));
		ZVAL_DOUBLE(out, value);
		break;
	}
	case CHC_BOOL:
		ZVAL_BOOL(out, bytes[0] != 0);
		break;
	case CHC_INT128: case CHC_UINT128: {
		ch_uint128 value;
		ch_u128_load_le(&value, bytes);
		bool negative = chc_type_kind(type) == CHC_INT128 && (value.hi >> 63);
		char buffer[41];
		size_t sign = negative ? 1 : 0;
		if (negative) { buffer[0] = '-'; value = ch_u128_negate(value); }
		size_t length = sign + ch_u128_format(value, buffer + sign);
		ZVAL_STRINGL(out, buffer, length);
		break;
	}
	case CHC_DECIMAL32: case CHC_DECIMAL64: case CHC_DECIMAL128:
		return ch_read_decimal(out, type, bytes);
	case CHC_DATE: case CHC_DATE32: case CHC_DATETIME: case CHC_DATETIME64: case CHC_TIME: case CHC_TIME64:
		return ch_read_temporal(out, type, bytes, flags);
	case CHC_UUID:
		ch_read_uuid(out, bytes, flags);
		break;
	case CHC_IPV4: case CHC_IPV6:
		return ch_read_ip(out, bytes, chc_type_kind(type) == CHC_IPV6);
	case CHC_ENUM8: case CHC_ENUM16: {
		int64_t value = ch_read_i64(bytes, width);
		size_t count = chc_type_enum_count(type);
		for (size_t i = 0; i < count; ++i) {
			const char *name;
			size_t length;
			int64_t candidate;
			chc_type_enum_at(type, i, &name, &length, &candidate);
			if (candidate == value) { ZVAL_STRINGL(out, name, length); return SUCCESS; }
		}
		return ch_read_error("Invalid Enum value");
	}
	default:
		return ch_read_type_error("convertToZval: unsupported type code: ", type);
	}
	return SUCCESS;
}

static int ch_read_array_init(zval *out, size_t count, const char *what)
{
	if (count >= (size_t)HT_MAX_SIZE) {
		if (!EG(exception)) zend_throw_exception_ex(clickhouse_exception_ce, 0,
			"%s: element count exceeds the PHP array limit", what);
		return FAILURE;
	}
	array_init_size(out, (uint32_t)count);
	return SUCCESS;
}

static int ch_read_array_range(const chc_column *column, size_t row, size_t *begin, size_t *end)
{
	if (column->layout != CHC_COL_ARRAY || row >= column->n_rows ||
		!column->array.offsets || !column->array.values) {
		return ch_read_error("Array read: row is outside the column data");
	}
	uint64_t first = row ? column->array.offsets[row - 1] : 0;
	uint64_t last = column->array.offsets[row];
	if (last < first || last > column->array.values->n_rows) {
		return ch_read_error("Array read: element offsets are out of range");
	}
	*begin = (size_t)first;
	*end = (size_t)last;
	return SUCCESS;
}

static int ch_read_array(zval *out, const chc_type *type, const chc_column *column,
	size_t row, zend_long flags, unsigned depth)
{
	size_t begin, end;
	if (ch_read_array_range(column, row, &begin, &end) == FAILURE) return FAILURE;
	if (ch_read_array_init(out, end - begin, "Array read") == FAILURE) return FAILURE;
	for (size_t i = begin; i < end; ++i) {
		zval value;
		if (ch_read_value_context(&value, chc_type_child(type, 0), column->array.values, i,
			flags & SC_FETCH_VALUE_FLAGS, depth + 1, true) == FAILURE) return FAILURE;
		add_next_index_zval(out, &value);
	}
	return SUCCESS;
}

static int ch_read_tuple(zval *out, const chc_type *type, const chc_column *column,
	size_t row, zend_long flags, unsigned depth, bool sliced)
{
	size_t arity = chc_type_n_children(type);
	if (column->layout != CHC_COL_TUPLE || column->tuple.arity != arity ||
		(arity && !column->tuple.children)) return ch_read_error("Tuple read: column type mismatch");
	if (ch_read_array_init(out, arity, "Tuple read") == FAILURE) return FAILURE;
	for (size_t i = 0; i < arity; ++i) {
		zval value;
		if (ch_read_value_context(&value, chc_type_child(type, i), column->tuple.children[i], row,
			flags & SC_FETCH_VALUE_FLAGS, depth + 1, sliced) == FAILURE) return FAILURE;
		add_next_index_zval(out, &value);
	}
	return SUCCESS;
}

static int ch_read_lc(zval *out, const chc_type *type, const chc_column *column,
	size_t row, zend_long flags, bool sliced)
{
	const chc_type *inner = chc_type_child(type, 0);
	bool nullable = chc_type_kind(inner) == CHC_NULLABLE;
	if (nullable) inner = chc_type_child(inner, 0);
	if (chc_type_kind(inner) != CHC_STRING && chc_type_kind(inner) != CHC_FIXED_STRING) {
		return ch_read_error("LowCardinality read only supports String / FixedString");
	}
	if (column->layout != CHC_COL_LOW_CARDINALITY || !column->lc.keys || !column->lc.dict) {
		return ch_read_error("LowCardinality column downcast failed");
	}
	uint64_t key = 0;
	const unsigned char *keys = column->lc.keys;
	switch (column->lc.key_size) {
	case 1: key = keys[row]; break;
	case 2: { uint16_t value; memcpy(&value, keys + row * 2, 2); key = value; break; }
	case 4: { uint32_t value; memcpy(&value, keys + row * 4, 4); key = value; break; }
	case 8: memcpy(&key, keys + row * 8, 8); break;
	default: return ch_read_error("LowCardinality read: invalid key size");
	}
	const chc_column *dictionary = column->lc.dict;
	if (key >= dictionary->n_rows) return ch_read_error("LowCardinality read: dictionary index is out of range");
	if (nullable && key == 0) { ZVAL_NULL(out); return SUCCESS; }
	if (nullable && dictionary->layout == CHC_COL_NULLABLE) dictionary = dictionary->nullable.inner;
	if (!dictionary) return ch_read_error("LowCardinality read: missing dictionary");
	/* The previous client's slice dictionary mapped its empty String default
	 * to the nullable sentinel; top-level and FixedString values are distinct. */
	if (sliced && nullable && chc_type_kind(inner) == CHC_STRING) {
		const char *bytes;
		size_t length;
		if (ch_read_string_view(dictionary, (size_t)key, &bytes, &length) == FAILURE) return FAILURE;
		if (!length) { ZVAL_NULL(out); return SUCCESS; }
	}
	return ch_read_string(out, inner, dictionary, (size_t)key, flags);
}

static bool ch_read_map_scalar(chc_kind kind)
{
	switch (kind) {
	case CHC_STRING: case CHC_INT8: case CHC_INT16: case CHC_INT32: case CHC_INT64:
	case CHC_UINT8: case CHC_UINT16: case CHC_UINT32: case CHC_UINT64:
	case CHC_FLOAT32: case CHC_FLOAT64: case CHC_UUID:
		return true;
	default:
		return false;
	}
}

static zval *ch_read_map_add(HashTable *table, zval *key, zval *value)
{
	if (Z_TYPE_P(key) == IS_LONG) return zend_hash_index_add(table, (zend_ulong)Z_LVAL_P(key), value);
	char buffer[64];
	const char *bytes;
	size_t length;
	if (Z_TYPE_P(key) == IS_DOUBLE) {
		php_gcvt(Z_DVAL_P(key), 17, '.', 'e', buffer);
		bytes = buffer;
		length = strlen(buffer);
	} else {
		bytes = Z_STRVAL_P(key);
		length = Z_STRLEN_P(key);
	}
	zend_ulong index;
	if (ZEND_HANDLE_NUMERIC_STR(bytes, length, index)) return zend_hash_index_add(table, index, value);
	return zend_hash_str_add(table, bytes, length, value);
}

static int ch_read_map(zval *out, const chc_type *type, const chc_column *column,
	size_t row, zend_long flags, unsigned depth)
{
	size_t begin, end;
	if (ch_read_array_range(column, row, &begin, &end) == FAILURE) return FAILURE;
	const chc_column *tuple = column->array.values;
	if (tuple->layout != CHC_COL_TUPLE || tuple->tuple.arity != 2 || !tuple->tuple.children ||
		!tuple->tuple.children[0] || !tuple->tuple.children[1]) return ch_read_error("Map read: inner tuple type mismatch");
	const chc_column *keys = tuple->tuple.children[0], *values = tuple->tuple.children[1];
	if (keys->n_rows != values->n_rows || end > keys->n_rows) return ch_read_error("Map column key/value size mismatch");
	const chc_type *key_type = chc_type_child(type, 0), *value_type = chc_type_child(type, 1);
	if (!(flags & SC_FETCH_MAP_AS_PAIRS)) {
		if (!ch_read_map_scalar(chc_type_kind(key_type))) return ch_read_type_error("Map read: unsupported key type ", key_type);
		if (!ch_read_map_scalar(chc_type_kind(value_type))) return ch_read_type_error("Map read: unsupported value type ", value_type);
	}
	if (ch_read_array_init(out, end - begin, "Map read") == FAILURE) return FAILURE;
	for (size_t i = begin; i < end; ++i) {
		zval key, value;
		unsigned child_depth = (flags & SC_FETCH_MAP_AS_PAIRS) ? depth + 1 : depth;
		if (ch_read_value_context(&key, key_type, keys, i, flags & SC_FETCH_VALUE_FLAGS, child_depth, true) == FAILURE) return FAILURE;
		if (ch_read_value_context(&value, value_type, values, i, flags & SC_FETCH_VALUE_FLAGS, child_depth, true) == FAILURE) {
			zval_ptr_dtor(&key);
			return FAILURE;
		}
		if (flags & SC_FETCH_MAP_AS_PAIRS) {
			zval pair;
			array_init_size(&pair, 2);
			add_next_index_zval(&pair, &key);
			add_next_index_zval(&pair, &value);
			add_next_index_zval(out, &pair);
		} else {
			zval *inserted = ch_read_map_add(Z_ARRVAL_P(out), &key, &value);
			zval_ptr_dtor(&key);
			if (!inserted) {
				zval_ptr_dtor(&value);
				return ch_read_error("Map decoding would collapse two entries onto the same PHP array key; pass ClickHouse::MAP_AS_PAIRS for an ordered lossless result");
			}
		}
	}
	return SUCCESS;
}

/* Geometry aliases contribute one level to the existing conversion limit. */
static int ch_read_geo(zval *out, const chc_column *column, size_t row, unsigned arrays)
{
	if (arrays) {
		size_t begin, end;
		if (ch_read_array_range(column, row, &begin, &end) == FAILURE) return FAILURE;
		if (ch_read_array_init(out, end - begin, "Geometry read") == FAILURE) return FAILURE;
		for (size_t i = begin; i < end; ++i) {
			zval value;
			ZVAL_UNDEF(&value);
			if (ch_read_geo(&value, column->array.values, i, arrays - 1) == FAILURE) {
				if (!Z_ISUNDEF(value)) zval_ptr_dtor(&value);
				return FAILURE;
			}
			add_next_index_zval(out, &value);
		}
		return SUCCESS;
	}
	if (column->layout != CHC_COL_TUPLE || column->tuple.arity != 2 || !column->tuple.children) {
		return ch_read_error("Point column downcast failed");
	}
	array_init_size(out, 2);
	for (size_t i = 0; i < 2; ++i) {
		const unsigned char *bytes;
		if (!column->tuple.children[i]) return ch_read_error("Point column downcast failed");
		if (ch_read_fixed(column->tuple.children[i], row, 8, &bytes) == FAILURE) return FAILURE;
		uint64_t bits = ch_read_le(bytes, 8);
		double value;
		memcpy(&value, &bits, sizeof(value));
		add_next_index_double(out, value);
	}
	return SUCCESS;
}

static int ch_read_dispatch(zval *out, const chc_type *type, const chc_column *column,
	size_t row, zend_long flags, unsigned depth, bool sliced)
{
	while (type && chc_type_kind(type) == CHC_SIMPLE_AGGREGATE_FUNCTION) type = chc_type_child(type, 0);
	if (!type || !column || row >= column->n_rows) return ch_read_error("Column read: row or type is outside the column data");
	if (depth >= CH_READ_MAX_DEPTH) return ch_read_error("ClickHouse column nested-type depth exceeds limit");
	switch (chc_type_kind(type)) {
	case CHC_STRING: case CHC_FIXED_STRING:
		return ch_read_string(out, type, column, row, flags);
	case CHC_JSON:
		return ch_read_json(out, column, row, flags);
	case CHC_NULLABLE:
		if (column->layout != CHC_COL_NULLABLE || !column->nullable.null_map) return ch_read_error("Nullable read: column type mismatch");
		if (column->nullable.null_map[row]) { ZVAL_NULL(out); return SUCCESS; }
		return ch_read_value_context(out, chc_type_child(type, 0), column->nullable.inner, row, flags, depth + 1, sliced);
	case CHC_ARRAY:
		return ch_read_array(out, type, column, row, flags, depth);
	case CHC_TUPLE:
		return ch_read_tuple(out, type, column, row, flags, depth, sliced);
	case CHC_LOW_CARDINALITY:
		return ch_read_lc(out, type, column, row, flags, sliced);
	case CHC_MAP:
		return ch_read_map(out, type, column, row, flags, depth);
	case CHC_POINT:
		return ch_read_geo(out, column, row, 0);
	case CHC_RING:
		return ch_read_geo(out, column, row, 1);
	case CHC_POLYGON:
		return ch_read_geo(out, column, row, 2);
	case CHC_MULTI_POLYGON:
		return ch_read_geo(out, column, row, 3);
	case CHC_NOTHING: case CHC_VOID:
		return ch_read_error("can't support Void");
	case CHC_INT8: case CHC_INT16: case CHC_INT32: case CHC_INT64: case CHC_INT128:
	case CHC_UINT8: case CHC_UINT16: case CHC_UINT32: case CHC_UINT64: case CHC_UINT128:
	case CHC_FLOAT32: case CHC_FLOAT64: case CHC_BOOL:
	case CHC_DATE: case CHC_DATE32: case CHC_DATETIME: case CHC_DATETIME64: case CHC_TIME: case CHC_TIME64:
	case CHC_DECIMAL32: case CHC_DECIMAL64: case CHC_DECIMAL128:
	case CHC_UUID: case CHC_IPV4: case CHC_IPV6: case CHC_ENUM8: case CHC_ENUM16:
		return ch_read_scalar(out, type, column, row, flags);
	default:
		return ch_read_type_error("convertToZval: unsupported type code: ", type);
	}
}

static int ch_read_value_context(zval *out, const chc_type *type, const chc_column *column,
	size_t row, zend_long flags, unsigned depth, bool sliced)
{
	ZVAL_UNDEF(out);
	if (EG(exception)) return FAILURE;
	int result = ch_read_dispatch(out, type, column, row, flags, depth, sliced);
	if (result == FAILURE && !Z_ISUNDEF_P(out)) {
		zval_ptr_dtor(out);
		ZVAL_UNDEF(out);
	}
	return result;
}

int ch_read_value(zval *out, const chc_type *type, const chc_column *column,
	size_t row, zend_long flags, unsigned depth)
{
	return ch_read_value_context(out, type, column, row, flags, depth, false);
}
