/*
  +----------------------------------------------------------------------+
  | php_clickhouse                                                       |
  +----------------------------------------------------------------------+
  | Copyright (c) 1997-2026 The PHP Group                                  |
  +----------------------------------------------------------------------+
  | This source file is subject to version 3.01 of the PHP license,        |
  | that is bundled with this package in the file LICENSE, and is         |
  | available through the world-wide-web at the following url:            |
  | http://www.php.net/license/3_01.txt                                    |
  | If you did not receive a copy of the PHP license and are unable to     |
  | obtain it through the world-wide-web, please send a note to            |
  | license@php.net so we can mail you a copy immediately.                 |
  +----------------------------------------------------------------------+
  | Author: Ilia Alshanetsky <ilia@ilia.ws>                                |
  +----------------------------------------------------------------------+
*/

#include "clickhouse_c_internal.h"
#include "ext/json/php_json.h"
#include "ext/standard/php_math.h"
#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#ifdef PHP_WIN32
#include <windows.h>
#else
#include <time.h>
#endif

#define CH_ERROR_MAX_LEN 4096
#define CH_PARAM_ARRAY_MAX_DEPTH 32

void ch_annotate_local_exception(zend_string *query_id)
{
	zval exception, rv;
	if (!EG(exception)) return;
	ZVAL_OBJ(&exception, EG(exception));
	zval *message = sc_zend_read_property(zend_ce_exception, &exception,
		"message", sizeof("message") - 1, 1, &rv);
	if (message && Z_TYPE_P(message) == IS_STRING) {
		zend_string *safe = ch_sanitize_error(Z_STRVAL_P(message), Z_STRLEN_P(message));
		sc_zend_update_property_stringl(zend_ce_exception, &exception,
			"message", sizeof("message") - 1, ZSTR_VAL(safe), ZSTR_LEN(safe));
		zend_string_release(safe);
	}
	if (query_id && ZSTR_LEN(query_id)) {
		sc_zend_update_property_stringl(clickhouse_exception_ce, &exception,
			"query_id", sizeof("query_id") - 1, ZSTR_VAL(query_id), ZSTR_LEN(query_id));
	}
}

static int ch_fail_qid(const char *message, zend_string *query_id)
{
	if (!EG(exception)) {
		zend_throw_exception(clickhouse_exception_ce, message, 0);
		ch_annotate_local_exception(query_id);
	}
	return FAILURE;
}

static int ch_fail(const char *message)
{
	return ch_fail_qid(message, NULL);
}

static const char *ch_find(const char *text, size_t length,
	const char *needle, size_t needle_length)
{
	if (needle_length > length) return NULL;
	if (!needle_length) return text;
	for (size_t i = 0; i <= length - needle_length; ++i) {
		if (text[i] == needle[0] && !memcmp(text + i, needle, needle_length)) return text + i;
	}
	return NULL;
}

static const char *ch_find_ascii_ci(const char *text, size_t length,
	const char *needle, size_t needle_length)
{
	if (needle_length > length) return NULL;
	for (size_t i = 0; i <= length - needle_length; ++i) {
		size_t j;
		for (j = 0; j < needle_length; ++j) {
			unsigned char c = (unsigned char)text[i + j];
			if (c >= 'A' && c <= 'Z') c = (unsigned char)(c + ('a' - 'A'));
			if (c != (unsigned char)needle[j]) break;
		}
		if (j == needle_length) return text + i;
	}
	return NULL;
}

zend_string *ch_sanitize_error(const char *text, size_t length)
{
	static const char *markers[] = {"while executing", "in query: ", "while processing"};
	static const char suffix[] = "... (truncated)";
	const char *value, *parse;
	zend_string *message;
	if (!text) return zend_string_init("", 0, 0);
	value = ch_find_ascii_ci(text, length, "value ", sizeof("value ") - 1);
	parse = ch_find_ascii_ci(text, length, " cannot be parsed", sizeof(" cannot be parsed") - 1);
	if (value && parse && value + 6 <= parse) {
		smart_str redacted = {0};
		smart_str_appendl(&redacted, text, (size_t)(value + 6 - text));
		smart_str_appends(&redacted, "<redacted>");
		smart_str_appendl(&redacted, parse, length - (size_t)(parse - text));
		smart_str_0(&redacted);
		message = redacted.s;
	} else {
		message = zend_string_init(text, length, 0);
	}
	length = ZSTR_LEN(message);
	for (size_t i = 0; i < sizeof(markers) / sizeof(markers[0]); ++i) {
		const char *found = ch_find_ascii_ci(ZSTR_VAL(message), length, markers[i], strlen(markers[i]));
		if (found) length = (size_t)(found - ZSTR_VAL(message));
	}
	if (length != ZSTR_LEN(message)) {
		while (length && (ZSTR_VAL(message)[length - 1] == ' ' || ZSTR_VAL(message)[length - 1] == ','
			|| ZSTR_VAL(message)[length - 1] == ':' || ZSTR_VAL(message)[length - 1] == '.')) --length;
		ZSTR_LEN(message) = length;
		ZSTR_VAL(message)[length] = '\0';
	}
	if (length > CH_ERROR_MAX_LEN) {
		size_t prefix = CH_ERROR_MAX_LEN - (sizeof(suffix) - 1);
		memcpy(ZSTR_VAL(message) + prefix, suffix, sizeof(suffix));
		ZSTR_LEN(message) = CH_ERROR_MAX_LEN;
	}
	return message;
}

static zend_string *ch_native_error_message(int status, const chc_err *error,
	const chc_exception *server_error)
{
	const char *text;
	size_t length;
	if (server_error) {
		text = server_error->display_text;
		length = server_error->display_text_len;
	} else if (status == CHC_ERR_OOM) {
		text = "ClickHouse allocation exceeds memory_limit or available memory";
		length = strlen(text);
	} else if (error && error->msg[0]) {
		text = error->msg;
		length = strlen(text);
	} else {
		switch (status) {
			case CHC_ERR_EOF: text = "Unexpected end of ClickHouse connection"; break;
			case CHC_ERR_CANCELLED: text = "ClickHouse query was cancelled"; break;
			case CHC_ERR_IO: text = "ClickHouse transport error"; break;
			default: text = "ClickHouse protocol error"; break;
		}
		length = strlen(text);
	}
	return ch_sanitize_error(text, length);
}

void ch_failure_clear(clickhouse_object *obj)
{
	if (obj->failure_message) zend_string_release(obj->failure_message);
	obj->failure_message = NULL;
	obj->failure_code = -1;
}

void ch_failure_set(clickhouse_object *obj, zend_long code, const char *message, size_t length)
{
	zend_string *safe = ch_sanitize_error(message, length);
	ch_failure_clear(obj);
	obj->failure_message = safe;
	obj->failure_code = code;
}

void ch_failure_native(clickhouse_object *obj, int status, const chc_err *error,
	const chc_exception *server_error)
{
	zend_string *message = ch_native_error_message(status, error, server_error);
	ch_failure_clear(obj);
	obj->failure_message = message;
	obj->failure_code = server_error ? (zend_long)server_error->code : -1;
}

void ch_throw_error(int status, const chc_err *error,
	const chc_exception *server_error, zend_string *query_id)
{
	zend_string *message;
	zval exception;
	if (EG(exception)) return;
	message = ch_native_error_message(status, error, server_error);
	object_init_ex(&exception, clickhouse_exception_ce);
	if (server_error) {
		sc_zend_update_property_long(clickhouse_exception_ce, &exception,
			"server_code", sizeof("server_code") - 1, (zend_long)server_error->code);
		if (server_error->name_len) {
			sc_zend_update_property_stringl(clickhouse_exception_ce, &exception,
				"server_name", sizeof("server_name") - 1, server_error->name, server_error->name_len);
		}
	}
	if (query_id && ZSTR_LEN(query_id)) {
		sc_zend_update_property_stringl(clickhouse_exception_ce, &exception,
			"query_id", sizeof("query_id") - 1, ZSTR_VAL(query_id), ZSTR_LEN(query_id));
	}
	sc_zend_update_property_stringl(clickhouse_exception_ce, &exception,
		"message", sizeof("message") - 1, ZSTR_VAL(message), ZSTR_LEN(message));
	zend_string_release(message);
	zend_throw_exception_object(&exception);
}

int ch_operation_begin(clickhouse_object *obj, bool allow_insert)
{
	if (obj->query_active) {
		return ch_fail("Reentrant operation: another query is already in progress "
			"on this ClickHouse instance. Use a separate ClickHouse instance from "
			"inside row / progress / profile / verbose callbacks.");
	}
	if (!obj->backend) return ch_fail("ClickHouse client is not initialized");
	if (!allow_insert && obj->insert_active) return ch_fail("The insert operation is now in progress");
	ch_failure_clear(obj);
	obj->query_active = true;
	return SUCCESS;
}

void ch_operation_end(clickhouse_object *obj)
{
	obj->query_active = false;
	ch_failure_clear(obj);
}

void ch_stats_reset(ch_stats *stats)
{
	if (stats->last_query_id) zend_string_release(stats->last_query_id);
	memset(stats, 0, sizeof(*stats));
}

void ch_add_assoc_uint64(zval *out, const char *name, uint64_t value)
{
	if (value <= (uint64_t)ZEND_LONG_MAX) {
		add_assoc_long(out, name, (zend_long)value);
	} else {
		char text[21];
		int length = snprintf(text, sizeof(text), "%" PRIu64, value);
		add_assoc_stringl(out, name, text, (size_t)length);
	}
}

void ch_stats_array(zval *out, const ch_stats *stats)
{
	array_init(out);
	ch_add_assoc_uint64(out, "rows_read", stats->rows_read);
	ch_add_assoc_uint64(out, "bytes_read", stats->bytes_read);
	ch_add_assoc_uint64(out, "total_rows", stats->total_rows);
	ch_add_assoc_uint64(out, "written_rows", stats->written_rows);
	ch_add_assoc_uint64(out, "written_bytes", stats->written_bytes);
	ch_add_assoc_uint64(out, "blocks", stats->blocks);
	ch_add_assoc_uint64(out, "rows_before_limit", stats->rows_before_limit);
	add_assoc_bool(out, "applied_limit", stats->applied_limit);
	add_assoc_double(out, "elapsed_ms", stats->elapsed_ms);
	if (stats->last_query_id) add_assoc_str(out, "query_id", zend_string_copy(stats->last_query_id));
	else add_assoc_stringl(out, "query_id", "", 0);
}

uint64_t ch_monotonic_ns(void)
{
#ifdef PHP_WIN32
	LARGE_INTEGER counter, frequency;
	if (!QueryPerformanceFrequency(&frequency) || !QueryPerformanceCounter(&counter)) return 0;
	return (uint64_t)(counter.QuadPart / frequency.QuadPart) * UINT64_C(1000000000)
		+ (uint64_t)((long double)(counter.QuadPart % frequency.QuadPart)
			* 1000000000.0L / (long double)frequency.QuadPart);
#else
	struct timespec time;
	if (clock_gettime(CLOCK_MONOTONIC, &time) != 0) return 0;
	return (uint64_t)time.tv_sec * UINT64_C(1000000000) + (uint64_t)time.tv_nsec;
#endif
}

static int ch_format_scalar_qid(zend_string **out, zval *value, zend_string *query_id,
	clickhouse_object *obj)
{
	zval held;
	char buffer[64];
	*out = NULL;
	ZVAL_COPY_DEREF(&held, value);
	switch (Z_TYPE(held)) {
		case IS_NULL: *out = zend_string_init("", 0, 0); break;
		case IS_TRUE: *out = zend_string_init("true", 4, 0); break;
		case IS_FALSE: *out = zend_string_init("false", 5, 0); break;
		case IS_LONG: {
			int length = snprintf(buffer, sizeof(buffer), ZEND_LONG_FMT, Z_LVAL(held));
			*out = zend_string_init(buffer, (size_t)length, 0);
			break;
		}
		case IS_DOUBLE:
			php_gcvt(Z_DVAL(held), 17, '.', 'e', buffer);
			*out = zend_string_init(buffer, strlen(buffer), 0);
			break;
		case IS_ARRAY:
		case IS_RESOURCE:
			zval_ptr_dtor(&held);
			return ch_fail_qid("setting/parameter value must be a scalar (string, int, float, bool, or null), not an array or resource", query_id);
		default: *out = zval_get_string(&held); break;
	}
	zval_ptr_dtor(&held);
	if (EG(exception) || !*out) {
		if (*out) zend_string_release(*out);
		*out = NULL;
		if (obj) ch_failure_set(obj, -1, ZEND_STRL("PHP string conversion failed"));
		return ch_fail_qid("PHP string conversion failed", query_id);
	}
	return SUCCESS;
}

int ch_format_scalar(zend_string **out, zval *value)
{
	return ch_format_scalar_qid(out, value, NULL, NULL);
}

static bool ch_unwrap(const char **type, size_t *length, const char *prefix, size_t prefix_length)
{
	if (*length > prefix_length + 1 && !memcmp(*type, prefix, prefix_length) && (*type)[*length - 1] == ')') {
		*type += prefix_length;
		*length -= prefix_length + 1;
		return true;
	}
	return false;
}

static bool ch_array_inner(const char **type, size_t *length)
{
	while (ch_unwrap(type, length, "LowCardinality(", 15) || ch_unwrap(type, length, "Nullable(", 9)) {}
	return ch_unwrap(type, length, "Array(", 6);
}

static bool ch_type_quotes(const char *type, size_t length)
{
	static const char *bare[] = {"Int", "UInt", "Float", "Decimal", "Bool"};
	while (ch_unwrap(&type, &length, "Nullable(", 9) || ch_unwrap(&type, &length, "LowCardinality(", 15)) {}
	for (size_t i = 0; i < sizeof(bare) / sizeof(bare[0]); ++i) {
		size_t n = strlen(bare[i]);
		if (length >= n && !memcmp(type, bare[i], n)) return false;
	}
	return true;
}

static bool ch_type_null(const char *type, size_t length)
{
	while (ch_unwrap(&type, &length, "LowCardinality(", 15)) {}
	return ch_unwrap(&type, &length, "Nullable(", 9);
}

static bool ch_bare_number(const zend_string *value)
{
	const char *text = ZSTR_VAL(value);
	size_t length = ZSTR_LEN(value), start = 0;
	bool digits = false;
	if (!length) return false;
	if (text[0] == '+' || text[0] == '-') start = 1;
	if (length - start == 3 && (ch_find_ascii_ci(text + start, 3, "inf", 3)
		|| ch_find_ascii_ci(text + start, 3, "nan", 3))) return true;
	for (size_t i = 0; i < length; ++i) {
		char c = text[i];
		if (c >= '0' && c <= '9') { digits = true; continue; }
		if (c != '+' && c != '-' && c != '.' && c != 'e' && c != 'E') return false;
	}
	return digits;
}

static int ch_format_param(zend_string **out, zval *value,
	const char *type, size_t type_length, unsigned depth, zend_string *query_id,
	clickhouse_object *obj)
{
	zval held, *item;
	smart_str buffer = {0};
	int result = FAILURE;
	bool first = true;
	*out = NULL;
	ZVAL_COPY_DEREF(&held, value);
	if (Z_TYPE(held) != IS_ARRAY) {
		result = ch_format_scalar_qid(out, &held, query_id, obj);
		zval_ptr_dtor(&held);
		return result;
	}
	if (depth >= CH_PARAM_ARRAY_MAX_DEPTH) {
		ch_fail_qid("Array typed parameter nesting depth exceeds limit of 32", query_id);
		goto cleanup;
	}
	if (!ch_array_inner(&type, &type_length)) {
		ch_fail_qid("PHP array value requires an Array(...) typed parameter "
			"(Map/Tuple/other composites are not supported as bound "
			"parameters; use a string representation or external data)", query_id);
		goto cleanup;
	}
	bool quote = ch_type_quotes(type, type_length);
	bool nullable = ch_type_null(type, type_length);
	const char *nested = type;
	size_t nested_length = type_length;
	bool nested_array = ch_array_inner(&nested, &nested_length);
	smart_str_appendc(&buffer, '[');
	ZEND_HASH_FOREACH_VAL(Z_ARRVAL(held), item) {
		zval cell;
		zend_string *scalar = NULL;
		ZVAL_COPY_DEREF(&cell, item);
		if (!first) smart_str_appendc(&buffer, ',');
		first = false;
		if (Z_TYPE(cell) == IS_NULL) {
			zval_ptr_dtor(&cell);
			if (!nullable) { ch_fail_qid("NULL element in non-Nullable Array typed parameter", query_id); goto cleanup; }
			smart_str_appends(&buffer, "NULL");
			continue;
		}
		if (nested_array) {
			if (Z_TYPE(cell) != IS_ARRAY) {
				zval_ptr_dtor(&cell);
				ch_fail_qid("Array typed parameter element must be an array", query_id);
				goto cleanup;
			}
			if (ch_format_param(&scalar, &cell, type, type_length, depth + 1, query_id, obj) != SUCCESS) {
				zval_ptr_dtor(&cell);
				goto cleanup;
			}
			smart_str_append(&buffer, scalar);
		} else {
			if (ch_format_scalar_qid(&scalar, &cell, query_id, obj) != SUCCESS) { zval_ptr_dtor(&cell); goto cleanup; }
			if (quote) {
				smart_str_appendc(&buffer, '\'');
				for (size_t i = 0; i < ZSTR_LEN(scalar); ++i) {
					if (ZSTR_VAL(scalar)[i] == '\'') smart_str_appendc(&buffer, '\'');
					smart_str_appendc(&buffer, ZSTR_VAL(scalar)[i]);
				}
				smart_str_appendc(&buffer, '\'');
			} else {
				if (Z_TYPE(cell) != IS_LONG && Z_TYPE(cell) != IS_DOUBLE
					&& Z_TYPE(cell) != IS_TRUE && Z_TYPE(cell) != IS_FALSE && !ch_bare_number(scalar)) {
					zend_string_release(scalar);
					zval_ptr_dtor(&cell);
					ch_fail_qid("non-numeric element in a numeric Array typed parameter "
						"(each string/object element must be a single numeric literal)", query_id);
					goto cleanup;
				}
				smart_str_append(&buffer, scalar);
			}
		}
		zend_string_release(scalar);
		zval_ptr_dtor(&cell);
	} ZEND_HASH_FOREACH_END();
	smart_str_appendc(&buffer, ']');
	smart_str_0(&buffer);
	*out = buffer.s;
	buffer.s = NULL;
	result = SUCCESS;
cleanup:
	smart_str_free(&buffer);
	zval_ptr_dtor(&held);
	return result;
}

static zend_string *ch_quote_field(zend_string *value)
{
	smart_str output = {0};
	smart_str_appendc(&output, '\'');
	for (size_t i = 0; i < ZSTR_LEN(value); ++i) {
		switch (ZSTR_VAL(value)[i]) {
			case '\0': smart_str_appendl(&output, "\\x00", 4); break;
			case '\b': smart_str_appendl(&output, "\\x08", 4); break;
			case '\t': smart_str_appendl(&output, "\\\\\\t", 4); break;
			case '\n': smart_str_appendl(&output, "\\\\\\n", 4); break;
			case '\'': smart_str_appendl(&output, "\\x27", 4); break;
			case '\\': smart_str_appendl(&output, "\\\\\\\\", 4); break;
			default: smart_str_appendc(&output, ZSTR_VAL(value)[i]); break;
		}
	}
	smart_str_appendc(&output, '\'');
	smart_str_0(&output);
	return output.s;
}

static bool ch_alpha(unsigned char c)
{
	return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_';
}

static bool ch_digit(unsigned char c)
{
	return c >= '0' && c <= '9';
}

static int ch_placeholder_error(zend_string *name, const char *why, zend_string *query_id)
{
	smart_str error = {0};
	smart_str_appends(&error, "Placeholder value for {");
	smart_str_append(&error, name);
	smart_str_appends(&error, "} is invalid: ");
	smart_str_appends(&error, why);
	smart_str_0(&error);
	ch_fail_qid(ZSTR_VAL(error.s), query_id);
	smart_str_free(&error);
	return FAILURE;
}

static int ch_validate_token(zend_string *value, zend_string *name, zend_string *query_id)
{
	const char *text = ZSTR_VAL(value);
	size_t length = ZSTR_LEN(value), i = 0;
	if (!length) return ch_placeholder_error(name, "empty", query_id);
	unsigned char first = (unsigned char)text[0];
	if (ch_digit(first) || first == '+' || first == '-') {
		if (first == '+' || first == '-') {
			++i;
			if (i == length || !ch_digit((unsigned char)text[i])) return ch_placeholder_error(name, "sign without digits", query_id);
		}
		while (i < length && ch_digit((unsigned char)text[i])) ++i;
		if (i < length && text[i] == '.') {
			++i;
			while (i < length && ch_digit((unsigned char)text[i])) ++i;
		}
		if (i < length && (text[i] == 'e' || text[i] == 'E')) {
			++i;
			if (i < length && (text[i] == '+' || text[i] == '-')) ++i;
			if (i == length || !ch_digit((unsigned char)text[i])) return ch_placeholder_error(name, "malformed exponent", query_id);
			while (i < length && ch_digit((unsigned char)text[i])) ++i;
		}
	} else if (ch_alpha(first)) {
		bool dot = false;
		++i;
		while (i < length) {
			unsigned char c = (unsigned char)text[i];
			if (ch_alpha(c) || ch_digit(c)) { ++i; continue; }
			if (c == '.' && !dot) {
				dot = true;
				if (++i == length) return ch_placeholder_error(name, "trailing dot", query_id);
				if (!ch_alpha((unsigned char)text[i])) return ch_placeholder_error(name, "segment after dot must start with a letter or underscore", query_id);
				++i;
				continue;
			}
			break;
		}
	} else {
		return ch_placeholder_error(name, "must start with a digit, sign, letter, or underscore", query_id);
	}
	if (i != length) return ch_placeholder_error(name, "only one identifier or numeric literal per token; use array-valued placeholders for column lists", query_id);
	return SUCCESS;
}

static int ch_token_string(zend_string **out, zval *value, zend_string *name,
	zend_string *query_id, clickhouse_object *obj)
{
	*out = zval_get_string(value);
	if (!*out || EG(exception)) {
		if (*out) zend_string_release(*out);
		*out = NULL;
		ch_failure_set(obj, -1, ZEND_STRL("PHP string conversion failed"));
		return ch_fail_qid("PHP string conversion failed", query_id);
	}
	if (ch_validate_token(*out, name, query_id) != SUCCESS) {
		zend_string_release(*out);
		*out = NULL;
		return FAILURE;
	}
	return SUCCESS;
}

static int ch_placeholder_value(zend_string **out, zval *value, zend_string *name,
	zend_string *query_id, clickhouse_object *obj)
{
	zval held, *item;
	smart_str buffer = {0};
	bool first = true;
	int result = FAILURE;
	*out = NULL;
	ZVAL_COPY_DEREF(&held, value);
	if (Z_TYPE(held) != IS_ARRAY) {
		result = ch_token_string(out, &held, name, query_id, obj);
		zval_ptr_dtor(&held);
		return result;
	}
	if (!zend_hash_num_elements(Z_ARRVAL(held))) {
		ch_placeholder_error(name, "empty array", query_id);
		goto cleanup;
	}
	ZEND_HASH_FOREACH_VAL(Z_ARRVAL(held), item) {
		zval cell;
		zend_string *token = NULL;
		ZVAL_COPY_DEREF(&cell, item);
		if (Z_TYPE(cell) == IS_ARRAY) {
			zval_ptr_dtor(&cell);
			ch_placeholder_error(name, "a list element must not be an array", query_id);
			goto cleanup;
		}
		if (ch_token_string(&token, &cell, name, query_id, obj) != SUCCESS) { zval_ptr_dtor(&cell); goto cleanup; }
		zval_ptr_dtor(&cell);
		if (!first) smart_str_appends(&buffer, ", ");
		first = false;
		smart_str_append(&buffer, token);
		zend_string_release(token);
	} ZEND_HASH_FOREACH_END();
	smart_str_0(&buffer);
	*out = buffer.s;
	buffer.s = NULL;
	result = SUCCESS;
cleanup:
	smart_str_free(&buffer);
	zval_ptr_dtor(&held);
	return result;
}

static int ch_replace_placeholder(ch_query *query, zend_string *name, zend_string *replacement)
{
	smart_str needle = {0}, output = {0};
	const char *sql = ZSTR_VAL(query->sql);
	size_t length = ZSTR_LEN(query->sql), cursor = 0;
	bool found = false;
	int result = FAILURE;
	smart_str_appendc(&needle, '{');
	smart_str_append(&needle, name);
	smart_str_appendc(&needle, '}');
	smart_str_0(&needle);
	while (cursor <= length) {
		const char *match = ch_find(sql + cursor, length - cursor, ZSTR_VAL(needle.s), ZSTR_LEN(needle.s));
		if (!match) break;
		size_t position = (size_t)(match - sql);
		size_t word_start = position;
		found = true;
		if (ZSTR_VAL(replacement)[0] == '-' && position && sql[position - 1] == '-') {
			ch_placeholder_error(name, "a negative value directly after '-' would form a SQL comment; separate them with a space or parentheses in the SQL", query->query_id);
			goto cleanup;
		}
		while (word_start && (ch_alpha((unsigned char)sql[word_start - 1]) || ch_digit((unsigned char)sql[word_start - 1]))) --word_start;
		if (word_start && sql[word_start - 1] == '$') {
			ch_placeholder_error(name, "a value after '$' would become part of a $tag$ heredoc delimiter", query->query_id);
			goto cleanup;
		}
		smart_str_appendl(&output, sql + cursor, position - cursor);
		smart_str_append(&output, replacement);
		cursor = position + ZSTR_LEN(needle.s);
	}
	if (!found) {
		smart_str error = {0};
		smart_str_appends(&error, "Placeholder {");
		smart_str_append(&error, name);
		smart_str_appends(&error, "} does not appear in the SQL");
		smart_str_0(&error);
		ch_fail_qid(ZSTR_VAL(error.s), query->query_id);
		smart_str_free(&error);
		goto cleanup;
	}
	smart_str_appendl(&output, sql + cursor, length - cursor);
	smart_str_0(&output);
	zend_string_release(query->sql);
	query->sql = output.s;
	output.s = NULL;
	result = SUCCESS;
cleanup:
	smart_str_free(&needle);
	smart_str_free(&output);
	return result;
}

static int ch_apply_placeholders(ch_query *query, zval *params, clickhouse_object *obj)
{
	zval held, *value;
	zend_string *name;
	int result = FAILURE;
	ZVAL_COPY_DEREF(&held, params);
	ZEND_HASH_FOREACH_STR_KEY(Z_ARRVAL(held), name) {
		if (name && (memchr(ZSTR_VAL(name), ':', ZSTR_LEN(name))
			|| memchr(ZSTR_VAL(name), '{', ZSTR_LEN(name)) || memchr(ZSTR_VAL(name), '}', ZSTR_LEN(name)))) {
			query->distrust_braces = true;
			ch_fail_qid("Placeholder array keys must not contain ':', '{' or '}'", query->query_id);
			goto cleanup;
		}
	} ZEND_HASH_FOREACH_END();
	ZEND_HASH_FOREACH_STR_KEY_VAL(Z_ARRVAL(held), name, value) {
		smart_str prefix = {0};
		zend_string *formatted = NULL;
		zval cell;
		if (!name) { ch_fail_qid("Placeholder array keys must be strings", query->query_id); goto cleanup; }
		if (!ZSTR_LEN(name)) { ch_fail_qid("Placeholder array keys must be non-empty", query->query_id); goto cleanup; }
		ZVAL_COPY_DEREF(&cell, value);
		smart_str_appendc(&prefix, '{');
		smart_str_append(&prefix, name);
		smart_str_appendc(&prefix, ':');
		smart_str_0(&prefix);
		const char *match = ch_find(ZSTR_VAL(query->sql), ZSTR_LEN(query->sql), ZSTR_VAL(prefix.s), ZSTR_LEN(prefix.s));
		if (match) {
			const char *type = match + ZSTR_LEN(prefix.s);
			size_t remaining = ZSTR_LEN(query->sql) - (size_t)(type - ZSTR_VAL(query->sql));
			const char *end = memchr(type, '}', remaining);
			smart_str_free(&prefix);
			if (!end) {
				smart_str error = {0};
				smart_str_appends(&error, "Unterminated typed placeholder for {");
				smart_str_append(&error, name);
				smart_str_appendc(&error, '}');
				smart_str_0(&error);
				ch_fail_qid(ZSTR_VAL(error.s), query->query_id);
				smart_str_free(&error);
				zval_ptr_dtor(&cell);
				goto cleanup;
			}
			if (Z_TYPE(cell) == IS_NULL) {
				formatted = zend_string_init("'\\\\N'", 5, 0);
			} else {
				zend_string *raw = NULL;
				if (ch_format_param(&raw, &cell, type, (size_t)(end - type), 0, query->query_id, obj) != SUCCESS) { zval_ptr_dtor(&cell); goto cleanup; }
				formatted = ch_quote_field(raw);
				zend_string_release(raw);
			}
			zval wire_value;
			ZVAL_STR(&wire_value, formatted);
			zend_hash_update(&query->params, name, &wire_value);
			zval_ptr_dtor(&cell);
			continue;
		}
		smart_str_free(&prefix);
		if (ch_placeholder_value(&formatted, &cell, name, query->query_id, obj) != SUCCESS) { zval_ptr_dtor(&cell); goto cleanup; }
		zval_ptr_dtor(&cell);
		int replaced = ch_replace_placeholder(query, name, formatted);
		zend_string_release(formatted);
		if (replaced != SUCCESS) goto cleanup;
	} ZEND_HASH_FOREACH_END();
	result = SUCCESS;
cleanup:
	zval_ptr_dtor(&held);
	return result;
}

static int ch_merge_settings(ch_query *query, clickhouse_object *obj, zval *settings)
{
	zval held, *value;
	zend_string *name;
	zend_hash_copy(&query->settings, &obj->settings, zval_add_ref);
	if (!settings || Z_TYPE_P(settings) != IS_ARRAY) return SUCCESS;
	ZVAL_COPY_DEREF(&held, settings);
	ZEND_HASH_FOREACH_STR_KEY_VAL(Z_ARRVAL(held), name, value) {
		zend_string *formatted = NULL;
		zval entry;
		if (!name || !ZSTR_LEN(name)) {
			zval_ptr_dtor(&held);
			return ch_fail_qid(!name ? "setting keys must be strings" : "setting key must not be empty", query->query_id);
		}
		if (ch_format_scalar_qid(&formatted, value, query->query_id, obj) != SUCCESS) { zval_ptr_dtor(&held); return FAILURE; }
		ZVAL_STR(&entry, formatted);
		zend_hash_update(&query->settings, name, &entry);
	} ZEND_HASH_FOREACH_END();
	zval_ptr_dtor(&held);
	return SUCCESS;
}

void ch_query_init(ch_query *query)
{
	memset(query, 0, sizeof(*query));
	zend_hash_init(&query->settings, 0, NULL, ZVAL_PTR_DTOR, 0);
	zend_hash_init(&query->params, 0, NULL, ZVAL_PTR_DTOR, 0);
}

void ch_query_clear(ch_query *query)
{
	if (query->sql) zend_string_release(query->sql);
	if (query->log_sql) zend_string_release(query->log_sql);
	if (query->query_id) zend_string_release(query->query_id);
	if (query->wire_settings) efree(query->wire_settings);
	if (query->wire_params) efree(query->wire_params);
	zend_hash_destroy(&query->settings);
	zend_hash_destroy(&query->params);
	memset(query, 0, sizeof(*query));
}

int ch_query_prepare(clickhouse_object *obj, ch_query *query,
	zend_string *sql, zval *params, zend_string *query_id,
	zval *settings, const char *params_error)
{
	zend_string *name;
	zval *value;
	size_t index;
	query->sql = zend_string_copy(sql);
	query->log_sql = zend_string_copy(sql);
	query->query_id = query_id ? zend_string_copy(query_id) : zend_string_init("", 0, 0);
	ch_stats_reset(&obj->stats);
	obj->stats.last_query_id = zend_string_copy(query->query_id);
	if (obj->insert_active) return ch_fail_qid("The insert operation is now in progress", query->query_id);
	if (params && Z_TYPE_P(params) == IS_ARRAY) {
		if (ch_apply_placeholders(query, params, obj) != SUCCESS) return FAILURE;
	} else if (params && params_error) {
		return ch_fail_qid(params_error, query->query_id);
	}
	if (ch_merge_settings(query, obj, settings) != SUCCESS) return FAILURE;
	query->wire.n_settings = zend_hash_num_elements(&query->settings);
	query->wire.n_params = zend_hash_num_elements(&query->params);
	if (query->wire.n_settings) {
		query->wire_settings = safe_emalloc(query->wire.n_settings, sizeof(*query->wire_settings), 0);
		memset(query->wire_settings, 0, query->wire.n_settings * sizeof(*query->wire_settings));
	}
	if (query->wire.n_params) {
		query->wire_params = safe_emalloc(query->wire.n_params, sizeof(*query->wire_params), 0);
		memset(query->wire_params, 0, query->wire.n_params * sizeof(*query->wire_params));
	}
	index = 0;
	ZEND_HASH_FOREACH_STR_KEY_VAL(&query->settings, name, value) {
		chc_query_setting *entry = &query->wire_settings[index++];
		entry->name = ZSTR_VAL(name);
		entry->name_len = ZSTR_LEN(name);
		entry->value = Z_STRVAL_P(value);
		entry->value_len = Z_STRLEN_P(value);
		entry->strings_have_lengths = true;
	} ZEND_HASH_FOREACH_END();
	index = 0;
	ZEND_HASH_FOREACH_STR_KEY_VAL(&query->params, name, value) {
		chc_query_param *entry = &query->wire_params[index++];
		entry->name = ZSTR_VAL(name);
		entry->name_len = ZSTR_LEN(name);
		entry->value = Z_STRVAL_P(value);
		entry->value_len = Z_STRLEN_P(value);
		entry->strings_have_lengths = true;
	} ZEND_HASH_FOREACH_END();
	query->wire.query_id = ZSTR_VAL(query->query_id);
	query->wire.query_id_len = ZSTR_LEN(query->query_id);
	query->wire.settings = query->wire_settings;
	query->wire.params = query->wire_params;
	return SUCCESS;
}

static int ch_invoke(zval *callback, zval *args, uint32_t argc, const char *failure,
	zend_string *query_id, clickhouse_object *obj)
{
	zval copy, retval;
	int status;
	ZVAL_COPY(&copy, callback);
	ZVAL_UNDEF(&retval);
	status = call_user_function(NULL, NULL, &copy, &retval, argc, args);
	zval_ptr_dtor(&copy);
	bool has_retval = !Z_ISUNDEF(retval);
	if (has_retval) zval_ptr_dtor(&retval);
	if (status != SUCCESS || EG(exception) || !has_retval) {
		ch_failure_set(obj, -1, failure, strlen(failure));
		return ch_fail_qid(failure, query_id);
	}
	return SUCCESS;
}

int ch_emit_verbose(clickhouse_object *obj, const char *event, zval *context)
{
	zval payload;
	int result = SUCCESS;
	if (!obj->verbose_to_stderr && Z_ISUNDEF(obj->verbose_callback)) {
		if (context) { zval_ptr_dtor(context); ZVAL_UNDEF(context); }
		return SUCCESS;
	}
	if (context) {
		ZVAL_COPY_VALUE(&payload, context);
		ZVAL_UNDEF(context);
	} else {
		array_init(&payload);
	}
	if (obj->verbose_to_stderr) {
		smart_str output = {0};
		zend_object *previous = EG(exception);
		php_json_encode(&output, &payload, 0);
		smart_str_0(&output);
		if (EG(exception) && EG(exception) != previous) zend_clear_exception();
		fprintf(stderr, "[clickhouse] %s ", event);
		if (output.s) fwrite(ZSTR_VAL(output.s), 1, ZSTR_LEN(output.s), stderr);
		else fwrite("{}", 1, 2, stderr);
		fputc('\n', stderr);
		smart_str_free(&output);
	} else if (!Z_ISUNDEF(obj->verbose_callback)) {
		zval args[2];
		ZVAL_STRING(&args[0], event);
		ZVAL_COPY(&args[1], &payload);
		result = ch_invoke(&obj->verbose_callback, args, 2, "verbose callback aborted query", obj->stats.last_query_id, obj);
		zval_ptr_dtor(&args[0]);
		zval_ptr_dtor(&args[1]);
	}
	zval_ptr_dtor(&payload);
	return result;
}

static int ch_server_exception_event(clickhouse_object *obj, const chc_exception *exception)
{
	zval context;
	if (!obj->verbose_to_stderr && Z_ISUNDEF(obj->verbose_callback)) return SUCCESS;
	zend_string *message = ch_sanitize_error(exception->display_text, exception->display_text_len);
	array_init(&context);
	add_assoc_long(&context, "code", (zend_long)exception->code);
	add_assoc_stringl(&context, "name", exception->name, exception->name_len);
	add_assoc_str(&context, "message", message);
	return ch_emit_verbose(obj, "server_exception", &context);
}

int ch_query_event(clickhouse_object *obj, const chc_packet *packet, zend_string *query_id, bool callbacks)
{
	ch_stats *stats = &obj->stats;
	zval context;
	int result;
	switch (packet->kind) {
		case CHC_PKT_PROGRESS:
			stats->rows_read += packet->progress.rows;
			stats->bytes_read += packet->progress.bytes;
			if (packet->progress.total_rows > stats->total_rows) stats->total_rows = packet->progress.total_rows;
			stats->written_rows += packet->progress.written_rows;
			stats->written_bytes += packet->progress.written_bytes;
			if (!callbacks || Z_ISUNDEF(obj->progress_callback)) return SUCCESS;
			array_init(&context);
			ch_add_assoc_uint64(&context, "rows", packet->progress.rows);
			ch_add_assoc_uint64(&context, "bytes", packet->progress.bytes);
			ch_add_assoc_uint64(&context, "total_rows", packet->progress.total_rows);
			ch_add_assoc_uint64(&context, "written_rows", packet->progress.written_rows);
			ch_add_assoc_uint64(&context, "written_bytes", packet->progress.written_bytes);
			result = ch_invoke(&obj->progress_callback, &context, 1, "progress callback aborted query", query_id, obj);
			zval_ptr_dtor(&context);
			return result;
		case CHC_PKT_PROFILE_INFO:
			stats->blocks = packet->profile.blocks;
			if (packet->profile.calculated_rows_before_limit) {
				stats->rows_before_limit = packet->profile.rows_before_limit;
				stats->applied_limit = packet->profile.applied_limit != 0;
			}
			if (packet->profile.bytes > stats->bytes_read) stats->bytes_read = packet->profile.bytes;
			if (packet->profile.rows > stats->rows_read) stats->rows_read = packet->profile.rows;
			if (!callbacks || Z_ISUNDEF(obj->profile_callback)) return SUCCESS;
			array_init(&context);
			ch_add_assoc_uint64(&context, "rows", packet->profile.rows);
			ch_add_assoc_uint64(&context, "blocks", packet->profile.blocks);
			ch_add_assoc_uint64(&context, "bytes", packet->profile.bytes);
			ch_add_assoc_uint64(&context, "rows_before_limit", packet->profile.rows_before_limit);
			add_assoc_bool(&context, "applied_limit", packet->profile.applied_limit != 0);
			add_assoc_bool(&context, "calculated_rows_before_limit", packet->profile.calculated_rows_before_limit != 0);
			result = ch_invoke(&obj->profile_callback, &context, 1, "profile callback aborted query", query_id, obj);
			zval_ptr_dtor(&context);
			return result;
		case CHC_PKT_EXCEPTION:
			if (callbacks && ch_server_exception_event(obj, packet->exception) != SUCCESS) return FAILURE;
			ch_failure_native(obj, CHC_ERR_SERVER, NULL, packet->exception);
			ch_throw_error(CHC_ERR_SERVER, NULL, packet->exception, query_id);
			return FAILURE;
		default: return SUCCESS;
	}
}

int ch_validate_block(const chc_block *block, chc_err *error)
{
	size_t columns = chc_block_n_columns(block);
	for (size_t i = 0; i < columns; ++i) {
		int status = chc_column_validate(chc_block_column(block, i), error);
		if (status != CHC_OK) return status;
	}
	return CHC_OK;
}

int ch_reset_after_error(clickhouse_object *obj)
{
	chc_exception *exception = NULL;
	chc_err error = {{0}};
	int status;
	if (obj->backend) {
		status = ch_backend_reset(obj->backend, &exception, &error);
	} else {
		status = CHC_ERR_USAGE;
		snprintf(error.msg, sizeof(error.msg), "%s", "ClickHouse client is not initialized");
	}
	if (status != CHC_OK && obj->log_enabled) {
		zend_string *saved_message = obj->failure_message;
		zend_long saved_code = obj->failure_code;
		obj->failure_message = NULL;
		ch_failure_native(obj, status, &error, exception);
		ch_record_query_error(obj, obj->insert_sql, obj->insert_query_id, false);
		ch_failure_clear(obj);
		obj->failure_message = saved_message;
		obj->failure_code = saved_code;
	}
	if (exception) chc_exception_free(exception, &ch_php_allocator);
	return status == CHC_OK ? SUCCESS : FAILURE;
}

int ch_query_run(clickhouse_object *obj, ch_query *query,
	const ch_external_table *externals, size_t external_count, ch_on_block on_block, void *context)
{
	chc_err error = {{0}};
	chc_exception *exception = NULL;
	chc_client *client;
	uint64_t started = ch_monotonic_ns();
	bool cancelled = false, reset = true;
	int status = ch_backend_before_query(obj->backend, &exception, &error);
	if (status != CHC_OK) {
		ch_failure_native(obj, status, &error, exception);
		ch_throw_error(status, &error, exception, query->query_id);
		if (exception) chc_exception_free(exception, &ch_php_allocator);
		goto failed;
	}
	client = ch_backend_client(obj->backend);
	query->wire.defer_data_terminator = external_count != 0;
	status = chc_client_send_query_ex(client, ZSTR_VAL(query->sql), ZSTR_LEN(query->sql), &query->wire, &error);
	if (status != CHC_OK) goto native_failed;
	for (size_t i = 0; i < external_count; ++i) {
		status = chc_client_send_data_named(client, externals[i].block,
			ZSTR_VAL(externals[i].name), ZSTR_LEN(externals[i].name), &error);
		if (status != CHC_OK) goto native_failed;
	}
	if (external_count) {
		status = chc_client_send_data(client, NULL, &error);
		if (status != CHC_OK) goto native_failed;
	}
	for (;;) {
		chc_packet packet = {0};
		status = chc_client_recv_packet(client, &packet, &error);
		if (status != CHC_OK) goto native_failed;
		if (packet.kind == CHC_PKT_END_OF_STREAM) {
			chc_packet_clear(client, &packet);
			obj->stats.elapsed_ms = (double)(ch_monotonic_ns() - started) / 1000000.0;
			return SUCCESS;
		}
		if (packet.kind == CHC_PKT_DATA || packet.kind == CHC_PKT_TOTALS || packet.kind == CHC_PKT_EXTREMES
			|| packet.kind == CHC_PKT_LOG || packet.kind == CHC_PKT_PROFILE_EVENTS) {
			status = ch_validate_block(packet.block, &error);
			if (status != CHC_OK) { chc_packet_clear(client, &packet); goto native_failed; }
		}
		if (packet.kind == CHC_PKT_DATA && !cancelled && on_block) {
			ch_block_action action = on_block(obj, &packet.block, context);
			chc_packet_clear(client, &packet);
			if (action == CH_BLOCK_FAIL || EG(exception)) { ch_fail_qid("data callback aborted query", query->query_id); goto failed; }
			if (action == CH_BLOCK_CANCEL) {
				cancelled = true;
				status = chc_client_send_cancel(client, &error);
				if (status != CHC_OK) goto native_failed;
			}
			continue;
		}
		if (packet.kind == CHC_PKT_EXCEPTION) {
			if (ch_server_exception_event(obj, packet.exception) != SUCCESS) {
				chc_packet_clear(client, &packet);
				goto failed;
			}
			ch_failure_native(obj, CHC_ERR_SERVER, NULL, packet.exception);
			ch_throw_error(CHC_ERR_SERVER, NULL, packet.exception, query->query_id);
			chc_packet_clear(client, &packet);
			reset = external_count != 0;
			goto failed;
		}
		int event_result = ch_query_event(obj, &packet, query->query_id, true);
		chc_packet_clear(client, &packet);
		if (event_result != SUCCESS) goto failed;
	}
native_failed:
	ch_failure_native(obj, status, &error, NULL);
	ch_throw_error(status, &error, NULL, query->query_id);
failed:
	obj->stats.elapsed_ms = (double)(ch_monotonic_ns() - started) / 1000000.0;
	if (reset) ch_reset_after_error(obj);
	return FAILURE;
}
