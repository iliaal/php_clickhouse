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

#include "clickhouse_c_internal.h"
#include "main/php_streams.h"
#include "ext/standard/file.h"

#include <limits.h>
#include <string.h>

typedef struct {
	chc_block_builder builder;
	chc_block_col *columns;
	ch_owned_column **owned;
	size_t count;
} ch_insert_block;

typedef struct {
	ch_insert_block block;
	zend_string **names;
	chc_type **types;
	size_t count;
} ch_external_owned;

struct ch_external_tables {
	ch_external_table *tables;
	ch_external_owned *owned;
	size_t count;
};

static int ch_insert_error(const char *message)
{
	if (!EG(exception)) {
		zend_throw_exception(clickhouse_exception_ce, message, 0);
		return CH_TYPE_LOCAL_ERROR;
	}
	return FAILURE;
}

static int ch_insert_checked(int result, zend_string *query_id)
{
	if (result == CH_TYPE_LOCAL_ERROR) ch_annotate_local_exception(query_id);
	return result;
}

static void ch_insert_conversion_failure(clickhouse_object *obj, int result)
{
	if (result == CH_TYPE_JSON_CALLBACK_ERROR) {
		ch_failure_set(obj, -1, ZEND_STRL("JSON insert: value serialization threw an exception"));
	} else if (result == FAILURE) {
		ch_failure_set(obj, -1, ZEND_STRL("PHP string conversion failed"));
	}
}

static void ch_insert_stats(clickhouse_object *obj, zend_string *query_id)
{
	ch_stats_reset(&obj->stats);
	obj->stats.last_query_id = query_id ? zend_string_copy(query_id) : zend_string_init("", 0, 0);
}

static int ch_insert_guard(clickhouse_object *obj, zend_string *query_id)
{
	if (ch_operation_begin(obj, true) != SUCCESS) {
		ch_annotate_local_exception(query_id);
		ch_record_query_local_error(obj, NULL, query_id, false);
		return FAILURE;
	}
	ch_insert_stats(obj, query_id);
	return SUCCESS;
}

static void ch_insert_elapsed(clickhouse_object *obj)
{
	if (obj->insert_started_ns) {
		obj->stats.elapsed_ms = (double)(ch_monotonic_ns() - obj->insert_started_ns) / 1000000.0;
	}
}

void ch_insert_clear(clickhouse_object *obj)
{
	if (obj->insert_schema) {
		chc_block_destroy(obj->insert_schema, &ch_php_allocator);
		obj->insert_schema = NULL;
	}
	if (obj->insert_sql) {
		zend_string_release(obj->insert_sql);
		obj->insert_sql = NULL;
	}
	if (obj->insert_query_id) {
		zend_string_release(obj->insert_query_id);
		obj->insert_query_id = NULL;
	}
	obj->insert_started_ns = 0;
	obj->insert_active = false;
}

int ch_insert_begin(clickhouse_object *obj, ch_query *query)
{
	chc_err error = {0};
	chc_exception *exception = NULL;
	chc_client *client;
	int status;

	status = ch_backend_before_query(obj->backend, &exception, &error);
	if (status != CHC_OK) {
		if (!EG(exception)) ch_failure_native(obj, status, &error, exception);
		ch_throw_error(status, &error, exception, query->query_id);
		chc_exception_free(exception, &ch_php_allocator);
		return FAILURE;
	}
	client = ch_backend_client(obj->backend);
	obj->insert_active = true;
	obj->insert_sql = zend_string_copy(query->log_sql);
	obj->insert_query_id = zend_string_copy(query->query_id);
	obj->insert_started_ns = ch_monotonic_ns();
	status = chc_client_send_query_ex(client, ZSTR_VAL(query->sql), ZSTR_LEN(query->sql), &query->wire, &error);
	if (status != CHC_OK) {
		if (!EG(exception)) ch_failure_native(obj, status, &error, NULL);
		ch_throw_error(status, &error, NULL, query->query_id);
		return FAILURE;
	}
	for (;;) {
		chc_packet packet = {0};
		status = chc_client_recv_packet(client, &packet, &error);
		if (status != CHC_OK) {
			if (!EG(exception)) ch_failure_native(obj, status, &error, NULL);
			ch_throw_error(status, &error, NULL, query->query_id);
			chc_packet_clear(client, &packet);
			return FAILURE;
		}
		if (packet.kind == CHC_PKT_DATA) {
			if (ch_validate_block(packet.block, &error) != CHC_OK) {
				if (!EG(exception)) ch_failure_native(obj, CHC_ERR_PROTOCOL, &error, NULL);
				ch_throw_error(CHC_ERR_PROTOCOL, &error, NULL, query->query_id);
				chc_packet_clear(client, &packet);
				return FAILURE;
			}
			obj->insert_schema = packet.block;
			packet.block = NULL;
			chc_packet_clear(client, &packet);
			return SUCCESS;
		}
		if (packet.kind == CHC_PKT_END_OF_STREAM) {
			chc_packet_clear(client, &packet);
			return ch_insert_checked(ch_insert_error("fail to receive data packet"), query->query_id);
		}
		status = ch_query_event(obj, &packet, query->query_id, true);
		chc_packet_clear(client, &packet);
		if (status != SUCCESS) {
			return FAILURE;
		}
	}
}

int ch_insert_send(clickhouse_object *obj, const chc_block_builder *block)
{
	chc_err error = {0};
	int status;
	if (!obj->insert_active || !ch_backend_client(obj->backend)) {
		return ch_insert_checked(ch_insert_error("illegal to send insert data without first calling BeginInsert"), obj->insert_query_id);
	}
	status = chc_client_send_data(ch_backend_client(obj->backend), block, &error);
	if (status != CHC_OK) {
		if (!EG(exception)) ch_failure_native(obj, status, &error, NULL);
		ch_throw_error(status, &error, NULL, obj->insert_query_id);
		return FAILURE;
	}
	return SUCCESS;
}

static int ch_insert_end_impl(clickhouse_object *obj, bool report_error)
{
	chc_err error = {0};
	chc_client *client;
	int status;
	if (!obj->insert_active) {
		return SUCCESS;
	}
	client = ch_backend_client(obj->backend);
	if (!client) {
		if (report_error) ch_insert_checked(ch_insert_error("ClickHouse client is not initialized"), obj->insert_query_id);
		return FAILURE;
	}
	status = chc_client_send_data(client, NULL, &error);
	if (status != CHC_OK) {
		if (report_error) {
			if (!EG(exception)) ch_failure_native(obj, status, &error, NULL);
			ch_throw_error(status, &error, NULL, obj->insert_query_id);
		}
		return FAILURE;
	}
	for (;;) {
		chc_packet packet = {0};
		status = chc_client_recv_packet(client, &packet, &error);
		if (status != CHC_OK) {
			if (report_error) {
				if (!EG(exception)) ch_failure_native(obj, status, &error, NULL);
				ch_throw_error(status, &error, NULL, obj->insert_query_id);
			}
			chc_packet_clear(client, &packet);
			return FAILURE;
		}
		if (packet.kind == CHC_PKT_EXCEPTION) {
			if (report_error) {
				if (!EG(exception)) ch_failure_native(obj, CHC_ERR_SERVER, &error, packet.exception);
				ch_throw_error(CHC_ERR_SERVER, &error, packet.exception, obj->insert_query_id);
			}
			chc_packet_clear(client, &packet);
			return FAILURE;
		}
		if (packet.kind == CHC_PKT_END_OF_STREAM) {
			chc_packet_clear(client, &packet);
			ch_insert_elapsed(obj);
			ch_insert_clear(obj);
			return SUCCESS;
		}
		/* BeginInsert detaches events before the caller supplies any rows. */
		chc_packet_clear(client, &packet);
	}
}

int ch_insert_end(clickhouse_object *obj)
{
	return ch_insert_end_impl(obj, true);
}

int ch_insert_recover(clickhouse_object *obj, bool finalize)
{
	int result = FAILURE;
	zend_string *failure_message = obj->failure_message ? zend_string_copy(obj->failure_message) : NULL;
	zend_long failure_code = obj->failure_code;
	ch_insert_elapsed(obj);
	if (finalize && obj->insert_active && ch_backend_client(obj->backend)) {
		result = ch_insert_end(obj);
	}
	if (result != SUCCESS) {
		result = ch_reset_after_error(obj);
		if (result == SUCCESS) {
			ch_insert_clear(obj);
		}
	}
	ch_failure_clear(obj);
	obj->failure_message = failure_message;
	obj->failure_code = failure_code;
	return result;
}

void ch_insert_abandon(clickhouse_object *obj)
{
	chc_err error = {0};
	if (!obj->insert_active || !obj->backend || !ch_backend_client(obj->backend)) {
		return;
	}
	if (ch_backend_limit_io_timeouts(obj->backend, 5000, &error) == CHC_OK) {
		(void)ch_insert_end_impl(obj, false);
	}
}

static void ch_rows_snapshot(zval *out, HashTable *rows)
{
	zval *row;
	array_init_size(out, zend_hash_num_elements(rows));
	ZEND_HASH_FOREACH_VAL(rows, row) {
		zval copy;
		ZVAL_COPY_DEREF(&copy, row);
		add_next_index_zval(out, &copy);
	} ZEND_HASH_FOREACH_END();
}

static int ch_validate_rows(HashTable *rows, size_t count)
{
	zval *row;
	ZEND_HASH_FOREACH_VAL(rows, row) {
		size_t cells;
		ZVAL_DEREF(row);
		if (Z_TYPE_P(row) != IS_ARRAY) {
			return ch_insert_error("The insert function needs to pass in a two-dimensional array");
		}
		cells = zend_hash_num_elements(Z_ARRVAL_P(row));
		if (cells > count) {
			zend_throw_exception_ex(clickhouse_exception_ce, 0,
				"row has %zu cells but only %zu columns were declared; extra cells would be silently dropped", cells, count);
			return CH_TYPE_LOCAL_ERROR;
		}
	} ZEND_HASH_FOREACH_END();
	return SUCCESS;
}

static void ch_names_free(zend_string **names, size_t count)
{
	if (names) {
		for (size_t i = 0; i < count; ++i) {
			if (names[i]) zend_string_release(names[i]);
		}
		efree(names);
	}
}

static int ch_names_copy(zend_string ***out, HashTable *columns, const char *error)
{
	size_t index = 0;
	zval *value;
	*out = safe_emalloc(zend_hash_num_elements(columns), sizeof(**out), 0);
	memset(*out, 0, zend_hash_num_elements(columns) * sizeof(**out));
	ZEND_HASH_FOREACH_VAL(columns, value) {
		ZVAL_DEREF(value);
		if (Z_TYPE_P(value) != IS_STRING) {
			return ch_insert_error(error);
		}
		(*out)[index++] = zend_string_copy(Z_STR_P(value));
	} ZEND_HASH_FOREACH_END();
	return SUCCESS;
}

static int ch_insert_sql(clickhouse_object *obj, zend_string **out, zend_string *table, zval *columns)
{
	smart_str sql = {0};
	zend_string *identifier = NULL;
	zval hold, *value;
	bool first = true;
	int result = CH_TYPE_LOCAL_ERROR;
	*out = NULL;
	ZVAL_COPY_DEREF(&hold, columns);
	if (ch_parse_identifier(&identifier, ZSTR_VAL(table), ZSTR_LEN(table), "table name", true) != SUCCESS) {
		goto failure;
	}
	smart_str_appends(&sql, "INSERT INTO ");
	smart_str_append(&sql, identifier);
	zend_string_release(identifier);
	identifier = NULL;
	if (zend_hash_num_elements(Z_ARRVAL(hold)) == 0) {
		ch_insert_error("Column list must not be empty");
		goto failure;
	}
	smart_str_appends(&sql, " ( ");
	ZEND_HASH_FOREACH_VAL(Z_ARRVAL(hold), value) {
		zend_string *name = zval_get_string(value);
		int status;
		if (EG(exception)) {
			if (name) zend_string_release(name);
			ch_failure_set(obj, -1, ZEND_STRL("PHP string conversion failed"));
			result = FAILURE;
			goto failure;
		}
		status = ch_parse_identifier(&identifier, ZSTR_VAL(name), ZSTR_LEN(name), "column name", false);
		zend_string_release(name);
		if (status != SUCCESS) goto failure;
		if (!first) smart_str_appendc(&sql, ',');
		smart_str_append(&sql, identifier);
		zend_string_release(identifier);
		identifier = NULL;
		first = false;
	} ZEND_HASH_FOREACH_END();
	smart_str_appends(&sql, " ) VALUES");
	smart_str_0(&sql);
	*out = sql.s;
	zval_ptr_dtor(&hold);
	return SUCCESS;
failure:
	if (identifier) zend_string_release(identifier);
	smart_str_free(&sql);
	zval_ptr_dtor(&hold);
	return result;
}

static void ch_insert_block_init(ch_insert_block *block, size_t count)
{
	memset(block, 0, sizeof(*block));
	block->count = count;
	block->columns = ecalloc(count, sizeof(*block->columns));
	block->owned = ecalloc(count, sizeof(*block->owned));
	chc_block_builder_init(&block->builder, block->columns);
}

static void ch_insert_block_clear(ch_insert_block *block)
{
	if (block->owned) {
		for (size_t i = 0; i < block->count; ++i) {
			ch_owned_column_free(block->owned[i]);
		}
		efree(block->owned);
	}
	if (block->columns) efree(block->columns);
	memset(block, 0, sizeof(*block));
}

static int ch_gather_column(zval *out, HashTable *rows, size_t index,
	zend_string *const *names, size_t count)
{
	zval *row;
	array_init_size(out, zend_hash_num_elements(rows));
	ZEND_HASH_FOREACH_VAL(rows, row) {
		zval copy;
		zval *value = ch_extract_row_cell(row, index, names, count);
		if (!value || EG(exception)) {
			zval_ptr_dtor(out);
			ZVAL_UNDEF(out);
			return CH_TYPE_LOCAL_ERROR;
		}
		ZVAL_COPY(&copy, value);
		add_next_index_zval(out, &copy);
	} ZEND_HASH_FOREACH_END();
	return SUCCESS;
}

static int ch_build_rows(clickhouse_object *obj, ch_insert_block *block, const chc_block *schema,
	HashTable *rows, zend_string *const *names)
{
	size_t count = chc_block_n_columns(schema);
	ch_insert_block_init(block, count);
	for (size_t i = 0; i < count; ++i) {
		zval values;
		size_t name_len;
		const char *name = chc_block_column_name(schema, i, &name_len);
		const chc_type *type = chc_block_column_type(schema, i);
		int result;
		result = ch_gather_column(&values, rows, i, names, count);
		if (result != SUCCESS) return result;
		result = ch_build_column(&block->owned[i], type, Z_ARRVAL(values), 0, false);
		zval_ptr_dtor(&values);
		if (result != SUCCESS) {
			ch_insert_conversion_failure(obj, result);
			return result;
		}
		chc_block_builder_append(&block->builder, name, name_len,
			ch_owned_column_type(block->owned[i]), ch_owned_column_data(block->owned[i]));
	}
	return SUCCESS;
}

void ch_external_tables_destroy(ch_external_tables *tables)
{
	if (!tables) return;
	for (size_t i = 0; i < tables->count; ++i) {
		ch_external_owned *entry = tables->owned ? &tables->owned[i] : NULL;
		if (entry) {
			ch_insert_block_clear(&entry->block);
			ch_names_free(entry->names, entry->count);
			if (entry->types) {
				for (size_t j = 0; j < entry->count; ++j) chc_type_destroy(entry->types[j], &ch_php_allocator);
				efree(entry->types);
			}
		}
		if (tables->tables && tables->tables[i].name) zend_string_release(tables->tables[i].name);
	}
	if (tables->owned) efree(tables->owned);
	if (tables->tables) efree(tables->tables);
	efree(tables);
}

const ch_external_table *ch_external_tables_data(const ch_external_tables *tables)
{
	return tables ? tables->tables : NULL;
}

size_t ch_external_tables_count(const ch_external_tables *tables)
{
	return tables ? tables->count : 0;
}

static const char *ch_type_head(const char *text, const char *end, size_t *length)
{
	const char *start;
	while (text < end && (*text == ' ' || *text == '\t' || *text == '\n' || *text == '\r')) ++text;
	start = text;
	if (text < end && ((*text >= 'A' && *text <= 'Z') || (*text >= 'a' && *text <= 'z') || *text == '_')) {
		do {
			++text;
		} while (text < end && ((*text >= 'A' && *text <= 'Z') || (*text >= 'a' && *text <= 'z') ||
			(*text >= '0' && *text <= '9') || *text == '_'));
	}
	*length = (size_t)(text - start);
	return start;
}

static bool ch_external_low_cardinality_error(zend_string *type_name)
{
	const char *end = ZSTR_VAL(type_name) + ZSTR_LEN(type_name);
	size_t length;
	const char *name = ch_type_head(ZSTR_VAL(type_name), end, &length);
	if (length != sizeof("LowCardinality") - 1 || memcmp(name, "LowCardinality", length)) return false;
	name += length;
	while (name < end && (*name == ' ' || *name == '\t' || *name == '\n' || *name == '\r')) ++name;
	if (name == end || *name++ != '(') return false;
	name = ch_type_head(name, end, &length);
	if (length == sizeof("Nullable") - 1 && !memcmp(name, "Nullable", length)) {
		name += length;
		while (name < end && (*name == ' ' || *name == '\t' || *name == '\n' || *name == '\r')) ++name;
		if (name == end || *name++ != '(') return false;
		name = ch_type_head(name, end, &length);
		zend_throw_exception_ex(clickhouse_exception_ce, 0,
			"LowCardinality(Nullable(%.*s)) is not supported", (int)length, name);
	} else {
		zend_throw_exception_ex(clickhouse_exception_ce, 0,
			"LowCardinality(%.*s) is not supported", (int)length, name);
	}
	return true;
}

static int ch_external_build(ch_external_table *view, ch_external_owned *owned, zval *descriptor)
{
	zval *name, *columns, *rows, *value;
	zval columns_hold, rows_hold;
	zend_string *key;
	size_t index = 0;
	int result = CH_TYPE_LOCAL_ERROR;
	ZVAL_UNDEF(&columns_hold);
	ZVAL_UNDEF(&rows_hold);
	ZVAL_DEREF(descriptor);
	if (Z_TYPE_P(descriptor) != IS_ARRAY) return ch_insert_error("externals must be a list of arrays");
	name = zend_hash_str_find(Z_ARRVAL_P(descriptor), "name", sizeof("name") - 1);
	columns = zend_hash_str_find(Z_ARRVAL_P(descriptor), "columns", sizeof("columns") - 1);
	rows = zend_hash_str_find(Z_ARRVAL_P(descriptor), "rows", sizeof("rows") - 1);
	if (name) ZVAL_DEREF(name);
	if (columns) ZVAL_DEREF(columns);
	if (rows) ZVAL_DEREF(rows);
	if (!name || Z_TYPE_P(name) != IS_STRING) return ch_insert_error("external table requires string 'name'");
	if (!columns || Z_TYPE_P(columns) != IS_ARRAY) return ch_insert_error("external table requires array 'columns'");
	if (!rows || Z_TYPE_P(rows) != IS_ARRAY) return ch_insert_error("external table requires array 'rows'");
	if (ch_validate_identifier(Z_STRVAL_P(name), Z_STRLEN_P(name), "external table name", false) != SUCCESS) return CH_TYPE_LOCAL_ERROR;
	view->name = zend_string_copy(Z_STR_P(name));
	ZVAL_COPY_DEREF(&columns_hold, columns);
	ch_rows_snapshot(&rows_hold, Z_ARRVAL_P(rows));
	owned->count = zend_hash_num_elements(Z_ARRVAL(columns_hold));
	if (!owned->count) {
		zend_throw_exception_ex(clickhouse_exception_ce, 0, "external table '%s' has no columns", ZSTR_VAL(view->name));
		goto cleanup;
	}
	owned->names = ecalloc(owned->count, sizeof(*owned->names));
	owned->types = ecalloc(owned->count, sizeof(*owned->types));
	ZEND_HASH_FOREACH_STR_KEY_VAL(Z_ARRVAL(columns_hold), key, value) {
		chc_err error = {0};
		zend_string *context;
		int status;
		if (!key) {
			zend_throw_exception_ex(clickhouse_exception_ce, 0,
				"external table '%s' columns must be an associative array of name => type", ZSTR_VAL(view->name));
			goto cleanup;
		}
		ZVAL_DEREF(value);
		if (Z_TYPE_P(value) != IS_STRING) {
			zend_throw_exception_ex(clickhouse_exception_ce, 0,
				"external table '%s' column '%s' type must be a string", ZSTR_VAL(view->name), ZSTR_VAL(key));
			goto cleanup;
		}
		if (ch_validate_identifier(ZSTR_VAL(key), ZSTR_LEN(key), "external column name", false) != SUCCESS) goto cleanup;
		context = strpprintf(0, "external table '%s' column '%s'", ZSTR_VAL(view->name), ZSTR_VAL(key));
		status = ch_validate_type_name(Z_STRVAL_P(value), Z_STRLEN_P(value), ZSTR_VAL(context));
		zend_string_release(context);
		if (status != SUCCESS) {
			result = status;
			goto cleanup;
		}
		status = chc_type_parse(Z_STRVAL_P(value), Z_STRLEN_P(value), &ch_php_allocator, &owned->types[index], &error);
		if (status != CHC_OK || !ch_insert_type_supported(owned->types[index])) {
			if (status == CHC_ERR_USAGE && !EG(exception)) {
				zend_throw_exception(clickhouse_exception_ce, error.msg, 0);
			}
			if (EG(exception)) {
				result = CH_TYPE_LOCAL_ERROR;
				goto cleanup;
			}
			if (ch_external_low_cardinality_error(Z_STR_P(value))) goto cleanup;
			zend_throw_exception_ex(clickhouse_exception_ce, 0,
				"external table '%s' column '%s' has unsupported type '%s'",
				ZSTR_VAL(view->name), ZSTR_VAL(key), Z_STRVAL_P(value));
			goto cleanup;
		}
		owned->names[index++] = zend_string_copy(key);
	} ZEND_HASH_FOREACH_END();
	if (ch_validate_rows(Z_ARRVAL(rows_hold), owned->count) != SUCCESS) goto cleanup;
	if (zend_hash_num_elements(Z_ARRVAL(rows_hold)) == 0) {
		zend_throw_exception_ex(clickhouse_exception_ce, 0,
			"external table '%s' has no rows; the native protocol cannot carry an empty named external table — guard the call in userland and skip the query when the filter set is empty",
			ZSTR_VAL(view->name));
		goto cleanup;
	}
	ch_insert_block_init(&owned->block, owned->count);
	view->block = &owned->block.builder;
	for (size_t i = 0; i < owned->count; ++i) {
		zval values;
		int status;
		if (ch_gather_column(&values, Z_ARRVAL(rows_hold), i, owned->names, owned->count) != SUCCESS) goto cleanup;
		status = ch_build_column(&owned->block.owned[i], owned->types[i], Z_ARRVAL(values), 0, false);
		zval_ptr_dtor(&values);
		if (status != SUCCESS) {
			result = status;
			goto cleanup;
		}
		chc_block_builder_append(&owned->block.builder, ZSTR_VAL(owned->names[i]), ZSTR_LEN(owned->names[i]),
			ch_owned_column_type(owned->block.owned[i]), ch_owned_column_data(owned->block.owned[i]));
	}
	result = SUCCESS;
cleanup:
	if (Z_TYPE(columns_hold) != IS_UNDEF) zval_ptr_dtor(&columns_hold);
	if (Z_TYPE(rows_hold) != IS_UNDEF) zval_ptr_dtor(&rows_hold);
	return result;
}

int ch_external_tables_create(ch_external_tables **out, zval *descriptors)
{
	zval hold, *entry;
	size_t index = 0;
	ch_external_tables *tables;
	*out = ecalloc(1, sizeof(**out));
	tables = *out;
	ch_rows_snapshot(&hold, Z_ARRVAL_P(descriptors));
	tables->count = zend_hash_num_elements(Z_ARRVAL(hold));
	if (!tables->count) {
		zval_ptr_dtor(&hold);
		return ch_insert_error("selectWithExternalData requires at least one external table; use select() when no external data is needed");
	}
	tables->tables = ecalloc(tables->count, sizeof(*tables->tables));
	tables->owned = ecalloc(tables->count, sizeof(*tables->owned));
	ZEND_HASH_FOREACH_VAL(Z_ARRVAL(hold), entry) {
		int result = ch_external_build(&tables->tables[index], &tables->owned[index], entry);
		if (result != SUCCESS) {
			zval_ptr_dtor(&hold);
			return result;
		}
		++index;
	} ZEND_HASH_FOREACH_END();
	zval_ptr_dtor(&hold);
	return SUCCESS;
}

static int ch_do_insert(zval *this_obj, zend_string *table, zval *columns,
	zval *values, zend_string *query_id, zval *settings)
{
	clickhouse_object *obj = Z_CLICKHOUSE_P(this_obj);
	ch_query query;
	ch_insert_block block = {0};
	zend_string *sql = NULL;
	zend_string **names = NULL;
	zval rows;
	size_t count = zend_hash_num_elements(Z_ARRVAL_P(columns));
	bool sent = false;
	int result = FAILURE;
	ZVAL_UNDEF(&rows);
	if (ch_insert_guard(obj, query_id) != SUCCESS) return FAILURE;
	ch_query_init(&query);
	if (obj->insert_active) {
		ch_insert_checked(ch_insert_error("The insert operation is now in progress"), query_id);
		goto cleanup;
	}
	ch_rows_snapshot(&rows, Z_ARRVAL_P(values));
	if (ch_insert_checked(ch_names_copy(&names, Z_ARRVAL_P(columns), "The columns array must be a list of column-name strings"), query_id) != SUCCESS) goto cleanup;
	if (ch_insert_checked(ch_validate_rows(Z_ARRVAL(rows), count), query_id) != SUCCESS) goto cleanup;
	if (ch_insert_checked(ch_insert_sql(obj, &sql, table, columns), query_id) != SUCCESS) goto cleanup;
	if (ch_query_prepare(obj, &query, sql, NULL, query_id, settings, NULL) != SUCCESS) goto cleanup;
	if (ch_insert_begin(obj, &query) != SUCCESS) {
		if (obj->insert_active) (void)ch_insert_recover(obj, false);
		goto cleanup;
	}
	if (chc_block_n_columns(obj->insert_schema) != count) {
		ch_insert_checked(ch_insert_error("INSERT sample block does not match the declared column count"), query_id);
		goto recover;
	}
	if (ch_insert_checked(ch_build_rows(obj, &block, obj->insert_schema, Z_ARRVAL(rows), names), query_id) != SUCCESS) goto recover;
	sent = true;
	if (ch_insert_send(obj, &block.builder) != SUCCESS) goto recover;
	ch_insert_block_clear(&block);
	if (ch_insert_end(obj) != SUCCESS) goto recover;
	result = SUCCESS;
	goto cleanup;
recover:
	ch_insert_block_clear(&block);
	(void)ch_insert_recover(obj, !sent);
cleanup:
	if (result == SUCCESS) ch_record_query_success(obj, sql, query.query_id, false);
	else ch_record_query_error(obj, sql, query_id, false);
	ch_insert_block_clear(&block);
	ch_names_free(names, count);
	if (Z_TYPE(rows) != IS_UNDEF) zval_ptr_dtor(&rows);
	if (sql) zend_string_release(sql);
	ch_query_clear(&query);
	ch_operation_end(obj);
	return result;
}

PHP_METHOD(ClickHouse, insert)
{
	zend_string *table, *query_id = NULL;
	zval *columns, *values, *settings = NULL;
	ZEND_PARSE_PARAMETERS_START(3, 5)
		Z_PARAM_STR(table)
		Z_PARAM_ARRAY(columns)
		Z_PARAM_ARRAY(values)
		Z_PARAM_OPTIONAL
		Z_PARAM_STR(query_id)
		Z_PARAM_ARRAY(settings)
	ZEND_PARSE_PARAMETERS_END();
	if (ch_do_insert(getThis(), table, columns, values, query_id, settings) != SUCCESS) return;
	RETURN_TRUE;
}

PHP_METHOD(ClickHouse, writeStart)
{
	zend_string *table, *query_id = NULL, *sql = NULL;
	zval *columns, *settings = NULL;
	clickhouse_object *obj;
	ch_query query;
	int result = FAILURE;
	ZEND_PARSE_PARAMETERS_START(2, 4)
		Z_PARAM_STR(table)
		Z_PARAM_ARRAY(columns)
		Z_PARAM_OPTIONAL
		Z_PARAM_STR(query_id)
		Z_PARAM_ARRAY(settings)
	ZEND_PARSE_PARAMETERS_END();
	obj = Z_CLICKHOUSE_P(getThis());
	if (ch_insert_guard(obj, query_id) != SUCCESS) return;
	ch_query_init(&query);
	if (obj->insert_active) {
		ch_insert_checked(ch_insert_error("The insert operation is now in progress"), query_id);
		goto cleanup;
	}
	if (ch_insert_checked(ch_insert_sql(obj, &sql, table, columns), query_id) != SUCCESS) goto cleanup;
	if (ch_query_prepare(obj, &query, sql, NULL, query_id, settings, NULL) != SUCCESS) goto cleanup;
	if (ch_insert_begin(obj, &query) != SUCCESS) {
		if (obj->insert_active) (void)ch_insert_recover(obj, false);
		goto cleanup;
	}
	result = SUCCESS;
cleanup:
	if (result != SUCCESS) ch_record_query_error(obj, sql, query_id, false);
	if (sql) zend_string_release(sql);
	ch_query_clear(&query);
	ch_operation_end(obj);
	if (result == SUCCESS) RETVAL_TRUE;
}

PHP_METHOD(ClickHouse, write)
{
	zval *values, rows;
	ch_insert_block block = {0};
	clickhouse_object *obj;
	zend_string *sql = NULL, *query_id = NULL;
	size_t count;
	bool sent = false;
	int result = FAILURE;
	ZEND_PARSE_PARAMETERS_START(1, 1)
		Z_PARAM_ARRAY(values)
	ZEND_PARSE_PARAMETERS_END();
	obj = Z_CLICKHOUSE_P(getThis());
	if (ch_operation_begin(obj, true) != SUCCESS) {
		ch_annotate_local_exception(NULL);
		return;
	}
	ZVAL_UNDEF(&rows);
	if (!obj->insert_active || !obj->insert_schema) {
		ch_insert_checked(ch_insert_error("write() called without a matching writeStart()"), NULL);
		goto cleanup;
	}
	sql = zend_string_copy(obj->insert_sql);
	query_id = zend_string_copy(obj->insert_query_id);
	ch_rows_snapshot(&rows, Z_ARRVAL_P(values));
	if (!zend_hash_num_elements(Z_ARRVAL(rows))) {
		result = SUCCESS;
		goto cleanup;
	}
	count = chc_block_n_columns(obj->insert_schema);
	if (!count) {
		ch_insert_checked(ch_insert_error("writeStart() block has no columns; cannot stream rows"), query_id);
		goto recover;
	}
	if (ch_insert_checked(ch_validate_rows(Z_ARRVAL(rows), count), query_id) != SUCCESS) goto recover;
	if (ch_insert_checked(ch_build_rows(obj, &block, obj->insert_schema, Z_ARRVAL(rows), NULL), query_id) != SUCCESS) goto recover;
	sent = true;
	if (ch_insert_send(obj, &block.builder) != SUCCESS) goto recover;
	result = SUCCESS;
	goto cleanup;
recover:
	ch_insert_block_clear(&block);
	(void)ch_insert_recover(obj, !sent);
cleanup:
	if (result != SUCCESS && (sql || query_id)) ch_record_query_error(obj, sql, query_id, false);
	ch_insert_block_clear(&block);
	if (Z_TYPE(rows) != IS_UNDEF) zval_ptr_dtor(&rows);
	if (sql) zend_string_release(sql);
	if (query_id) zend_string_release(query_id);
	ch_operation_end(obj);
	if (result == SUCCESS) RETVAL_TRUE;
}

PHP_METHOD(ClickHouse, writeEnd)
{
	clickhouse_object *obj;
	zend_string *sql = NULL, *query_id = NULL;
	int result = FAILURE;
	if (zend_parse_parameters_none() != SUCCESS) return;
	obj = Z_CLICKHOUSE_P(getThis());
	if (ch_operation_begin(obj, true) != SUCCESS) {
		ch_annotate_local_exception(NULL);
		return;
	}
	if (!obj->insert_active) {
		ch_insert_checked(ch_insert_error("writeEnd() called without a matching writeStart()"), NULL);
		goto cleanup;
	}
	sql = zend_string_copy(obj->insert_sql);
	query_id = zend_string_copy(obj->insert_query_id);
	if (ch_insert_end(obj) != SUCCESS) {
		(void)ch_insert_recover(obj, false);
		ch_record_query_error(obj, sql, query_id, false);
		goto cleanup;
	}
	ch_record_query_success(obj, sql, query_id, false);
	result = SUCCESS;
cleanup:
	if (sql) zend_string_release(sql);
	if (query_id) zend_string_release(query_id);
	ch_operation_end(obj);
	if (result == SUCCESS) RETVAL_TRUE;
}

PHP_METHOD(ClickHouse, insertAssoc)
{
	zend_string *table, *query_id = NULL;
	zval *rows, *settings = NULL, *row, *first = NULL, *value;
	zval columns;
	zend_string *key;
	HashTable *first_table;
	size_t count;
	int result = FAILURE;
	ZEND_PARSE_PARAMETERS_START(2, 4)
		Z_PARAM_STR(table)
		Z_PARAM_ARRAY(rows)
		Z_PARAM_OPTIONAL
		Z_PARAM_STR(query_id)
		Z_PARAM_ARRAY(settings)
	ZEND_PARSE_PARAMETERS_END();
	if (!zend_hash_num_elements(Z_ARRVAL_P(rows))) {
		ch_insert_checked(ch_insert_error("insertAssoc: rows is empty"), query_id);
		return;
	}
	ZEND_HASH_FOREACH_VAL(Z_ARRVAL_P(rows), row) {
		first = row;
		break;
	} ZEND_HASH_FOREACH_END();
	ZVAL_DEREF(first);
	if (Z_TYPE_P(first) != IS_ARRAY) {
		ch_insert_checked(ch_insert_error("insertAssoc: each row must be an associative array"), query_id);
		return;
	}
	first_table = Z_ARRVAL_P(first);
	count = zend_hash_num_elements(first_table);
	array_init_size(&columns, count);
	ZEND_HASH_FOREACH_STR_KEY_VAL(first_table, key, value) {
		(void)value;
		if (!key) {
			ch_insert_error("insertAssoc: each row must have string keys (column names)");
			goto cleanup;
		}
		add_next_index_str(&columns, zend_string_copy(key));
	} ZEND_HASH_FOREACH_END();
	ZEND_HASH_FOREACH_VAL(Z_ARRVAL_P(rows), row) {
		ZVAL_DEREF(row);
		if (Z_TYPE_P(row) != IS_ARRAY) {
			ch_insert_error("insertAssoc: each row must be an associative array");
			goto cleanup;
		}
		if (zend_hash_num_elements(Z_ARRVAL_P(row)) != count) {
			ch_insert_error("insertAssoc: row key count differs from first row");
			goto cleanup;
		}
		ZEND_HASH_FOREACH_STR_KEY_VAL(Z_ARRVAL_P(row), key, value) {
			(void)value;
			if (!key) {
				ch_insert_error("insertAssoc: each row must have string keys (column names)");
				goto cleanup;
			}
			if (!zend_hash_exists(first_table, key)) {
				zend_throw_exception_ex(clickhouse_exception_ce, 0,
					"insertAssoc: unexpected key '%s' (not in first row's column set)", ZSTR_VAL(key));
				goto cleanup;
			}
		} ZEND_HASH_FOREACH_END();
	} ZEND_HASH_FOREACH_END();
	result = ch_do_insert(getThis(), table, &columns, rows, query_id, settings);
	zval_ptr_dtor(&columns);
	if (result == SUCCESS) RETVAL_TRUE;
	return;
cleanup:
	ch_annotate_local_exception(query_id);
	zval_ptr_dtor(&columns);
}

typedef enum { CH_TSV, CH_TSV_NAMES, CH_CSV, CH_CSV_NAMES } ch_stream_format;
typedef enum { CH_CELL_START, CH_IN_CELL, CH_IN_QUOTED, CH_QUOTE_PENDING } ch_cell_state;

typedef struct {
	clickhouse_object *obj;
	zend_string **names;
	zval *columns;
	size_t count;
	size_t pending;
	size_t batch_rows;
	zend_long total;
	bool sent;
} ch_stream_batch;

typedef struct {
	ch_stream_format format;
	ch_cell_state state;
	char *cell;
	size_t length;
	size_t capacity;
	zval row;
	bool quoted;
	bool null_cell;
	bool header_seen;
	bool previous_cr;
	bool pending_backslash;
	ch_stream_batch *batch;
} ch_stream_parser;

static bool ch_stream_csv(ch_stream_format format)
{
	return format == CH_CSV || format == CH_CSV_NAMES;
}

static bool ch_stream_header(ch_stream_format format)
{
	return format == CH_TSV_NAMES || format == CH_CSV_NAMES;
}

static int ch_stream_format_parse(zend_string *value, ch_stream_format *format)
{
	if (!value || zend_string_equals_literal(value, "TabSeparated") || zend_string_equals_literal(value, "TSV")) *format = CH_TSV;
	else if (zend_string_equals_literal(value, "TabSeparatedWithNames") || zend_string_equals_literal(value, "TSVWithNames")) *format = CH_TSV_NAMES;
	else if (zend_string_equals_literal(value, "CSV")) *format = CH_CSV;
	else if (zend_string_equals_literal(value, "CSVWithNames")) *format = CH_CSV_NAMES;
	else return ch_insert_error("insertFromStream: unknown format; expected TabSeparated, TabSeparatedWithNames, CSV, or CSVWithNames");
	return SUCCESS;
}

static bool ch_stream_nullable(const chc_type *type)
{
	unsigned depth = 0;
	while (type && chc_type_kind(type) == CHC_LOW_CARDINALITY && depth++ < 32) type = chc_type_child(type, 0);
	return type && chc_type_kind(type) == CHC_NULLABLE;
}

static int ch_stream_flush(ch_stream_batch *batch)
{
	ch_insert_block block = {0};
	int result = FAILURE;
	if (!batch->pending) return SUCCESS;
	ch_insert_block_init(&block, batch->count);
	for (size_t i = 0; i < batch->count; ++i) {
		size_t length;
		int status;
		const chc_type *type = chc_block_column_type(batch->obj->insert_schema, i);
		const char *name = chc_block_column_name(batch->obj->insert_schema, i, &length);
		status = ch_build_column(&block.owned[i], type, Z_ARRVAL(batch->columns[i]), 0, false);
		if (status != SUCCESS) {
			ch_insert_conversion_failure(batch->obj, status);
			result = status;
			goto cleanup;
		}
		chc_block_builder_append(&block.builder, name, length,
			ch_owned_column_type(block.owned[i]), ch_owned_column_data(block.owned[i]));
		zval_ptr_dtor(&batch->columns[i]);
		array_init(&batch->columns[i]);
	}
	batch->sent = true;
	if (ch_insert_send(batch->obj, &block.builder) != SUCCESS) goto cleanup;
	batch->pending = 0;
	result = SUCCESS;
cleanup:
	ch_insert_block_clear(&block);
	return result;
}

static int ch_stream_row(ch_stream_parser *parser)
{
	ch_stream_batch *batch = parser->batch;
	size_t cells = zend_hash_num_elements(Z_ARRVAL(parser->row));
	if (ch_stream_header(parser->format) && !parser->header_seen) {
		if (cells != batch->count) {
			zend_throw_exception_ex(clickhouse_exception_ce, 0,
				"insertFromStream: header has %zu cells but %zu columns were declared", cells, batch->count);
			return CH_TYPE_LOCAL_ERROR;
		}
		for (size_t i = 0; i < cells; ++i) {
			zval *cell = zend_hash_index_find(Z_ARRVAL(parser->row), i);
			zend_string *wanted = batch->names[i];
			if (Z_TYPE_P(cell) == IS_STRING && zend_string_equals(Z_STR_P(cell), wanted)) continue;
			zend_throw_exception_ex(clickhouse_exception_ce, 0,
				"insertFromStream: header column %zu is '%s' but '%s' was declared",
				i + 1, Z_TYPE_P(cell) == IS_STRING ? Z_STRVAL_P(cell) : "\\N", ZSTR_VAL(wanted));
			return CH_TYPE_LOCAL_ERROR;
		}
		parser->header_seen = true;
	} else {
		if (cells != batch->count) {
			zend_throw_exception_ex(clickhouse_exception_ce, 0,
				"insertFromStream: row has %zu cells but %zu columns were declared", cells, batch->count);
			return CH_TYPE_LOCAL_ERROR;
		}
		for (size_t i = 0; i < cells; ++i) {
			zval *cell = zend_hash_index_find(Z_ARRVAL(parser->row), i);
			if (Z_TYPE_P(cell) == IS_NULL && !ch_stream_nullable(chc_block_column_type(batch->obj->insert_schema, i))) {
				size_t length;
				const char *name = chc_block_column_name(batch->obj->insert_schema, i, &length);
				zend_throw_exception_ex(clickhouse_exception_ce, 0,
					"insertFromStream: NULL (`\\N`) value for column '%.*s' which is not Nullable", (int)length, name);
				return CH_TYPE_LOCAL_ERROR;
			}
			add_next_index_zval(&batch->columns[i], cell);
			ZVAL_UNDEF(cell);
		}
		if (batch->total == ZEND_LONG_MAX) return ch_insert_error("insertFromStream: inserted row count exceeds PHP integer range");
		++batch->pending;
		++batch->total;
		if (batch->pending >= batch->batch_rows) {
			int status = ch_stream_flush(batch);
			if (status != SUCCESS) return status;
		}
	}
	zend_hash_clean(Z_ARRVAL(parser->row));
	return SUCCESS;
}

static int ch_stream_cell_limit(ch_stream_parser *parser, size_t capacity, size_t copy_size)
{
	if (capacity >= 64 * 1024 && PG(memory_limit) >= 0) {
		size_t used = zend_memory_usage(true);
		size_t limit = (size_t)PG(memory_limit);
		/* Current storage is already charged by Zend; reserve only its growth. */
		size_t extra = capacity > parser->capacity ? capacity - parser->capacity : 0;
		if (used >= limit || extra > limit - used || copy_size > limit - used - extra) {
			return ch_insert_error("insertFromStream: cell exceeds the available PHP memory_limit");
		}
	}
	return SUCCESS;
}

static int ch_stream_append(ch_stream_parser *parser, char value)
{
	if (parser->length == SIZE_MAX) return ch_insert_error("insertFromStream: cell exceeds the available PHP memory_limit");
	if (parser->length == parser->capacity) {
		size_t required = parser->length + 1;
		size_t capacity = parser->capacity > SIZE_MAX / 2 ? required : parser->capacity * 2;
		if (capacity < required) capacity = required;
		if (ch_stream_cell_limit(parser, capacity, required) != SUCCESS) return CH_TYPE_LOCAL_ERROR;
		parser->cell = erealloc(parser->cell, capacity);
		parser->capacity = capacity;
	}
	parser->cell[parser->length++] = value;
	return SUCCESS;
}

static int ch_stream_cell(ch_stream_parser *parser)
{
	zval value;
	if (parser->null_cell || (ch_stream_csv(parser->format) && !parser->quoted && parser->length == 2 &&
		parser->cell[0] == '\\' && parser->cell[1] == 'N')) {
		ZVAL_NULL(&value);
	} else {
		if (ch_stream_cell_limit(parser, parser->capacity, parser->length) != SUCCESS) return CH_TYPE_LOCAL_ERROR;
		ZVAL_STRINGL(&value, parser->cell, parser->length);
	}
	add_next_index_zval(&parser->row, &value);
	parser->length = 0;
	parser->quoted = false;
	parser->null_cell = false;
	return SUCCESS;
}

/* Return 1 when consumed, 0 to reprocess the byte, or a negative status. */
static int ch_stream_escape(ch_stream_parser *parser, char value)
{
	char decoded;
	switch (value) {
		case '\\': decoded = '\\'; break;
		case 't': decoded = '\t'; break;
		case 'n': decoded = '\n'; break;
		case 'r': decoded = '\r'; break;
		case '0': decoded = '\0'; break;
		case 'b': decoded = '\b'; break;
		case 'f': decoded = '\f'; break;
		case 'a': decoded = '\a'; break;
		case 'v': decoded = '\v'; break;
		case 'N':
			if (ch_stream_append(parser, '\\') != SUCCESS) return CH_TYPE_LOCAL_ERROR;
			if (parser->length != 1) return 0;
			if (ch_stream_append(parser, 'N') != SUCCESS) return CH_TYPE_LOCAL_ERROR;
			parser->null_cell = true;
			return 1;
		default: decoded = value; break;
	}
	return ch_stream_append(parser, decoded) == SUCCESS ? 1 : CH_TYPE_LOCAL_ERROR;
}

static int ch_stream_feed(ch_stream_parser *parser, const char *data, size_t length)
{
	bool csv = ch_stream_csv(parser->format);
	char separator = csv ? ',' : '\t';
	for (size_t i = 0; i < length; ++i) {
		char c = data[i];
		if (c != '\n') parser->previous_cr = false;
		if (parser->pending_backslash) {
			int consumed;
			parser->pending_backslash = false;
			if (parser->state == CH_CELL_START) parser->state = CH_IN_CELL;
			consumed = ch_stream_escape(parser, c);
			if (consumed < 0) return consumed;
			if (consumed) continue;
		}
		if (csv && parser->state == CH_IN_QUOTED) {
			if (c == '"') parser->state = CH_QUOTE_PENDING;
			else if (ch_stream_append(parser, c) != SUCCESS) return CH_TYPE_LOCAL_ERROR;
			continue;
		}
		if (csv && parser->state == CH_QUOTE_PENDING) {
			if (c == '"') {
				if (ch_stream_append(parser, '"') != SUCCESS) return CH_TYPE_LOCAL_ERROR;
				parser->state = CH_IN_QUOTED;
				continue;
			}
			if (c != separator && c != '\n' && c != '\r') return ch_insert_error("insertFromStream: malformed CSV - byte after closing quote must be ',', newline, or end of input");
			parser->state = CH_IN_CELL;
		}
		if (c == separator) {
			if (ch_stream_cell(parser) != SUCCESS) return CH_TYPE_LOCAL_ERROR;
			parser->state = CH_CELL_START;
			continue;
		}
		if (c == '\n' || c == '\r') {
			if (c == '\r') parser->previous_cr = true;
			else if (parser->previous_cr) {
				parser->previous_cr = false;
				continue;
			}
			if (parser->state == CH_CELL_START && zend_hash_num_elements(Z_ARRVAL(parser->row)) == 0 && parser->length == 0) continue;
			if (ch_stream_cell(parser) != SUCCESS) return CH_TYPE_LOCAL_ERROR;
			int status = ch_stream_row(parser);
			if (status != SUCCESS) return status;
			parser->state = CH_CELL_START;
			continue;
		}
		parser->previous_cr = false;
		if (parser->null_cell) return ch_insert_error("insertFromStream: TSV `\\N` is the whole-cell NULL marker and cannot be followed by other data");
		if (parser->state == CH_CELL_START) {
			if (csv && c == '"') {
				parser->state = CH_IN_QUOTED;
				parser->quoted = true;
				continue;
			}
			parser->state = CH_IN_CELL;
		}
		if (!csv && c == '\\') {
			int consumed;
			if (i + 1 == length) {
				parser->pending_backslash = true;
				break;
			}
			consumed = ch_stream_escape(parser, data[i + 1]);
			if (consumed < 0) return consumed;
			if (consumed) ++i;
			continue;
		}
		if (ch_stream_append(parser, c) != SUCCESS) return CH_TYPE_LOCAL_ERROR;
	}
	return SUCCESS;
}

static int ch_stream_finish(ch_stream_parser *parser)
{
	if (parser->pending_backslash) {
		if (ch_stream_append(parser, '\\') != SUCCESS) return CH_TYPE_LOCAL_ERROR;
		parser->pending_backslash = false;
		if (parser->state == CH_CELL_START) parser->state = CH_IN_CELL;
	}
	if (parser->state == CH_QUOTE_PENDING) parser->state = CH_IN_CELL;
	if (parser->state == CH_IN_QUOTED) return ch_insert_error("insertFromStream: unterminated quoted CSV cell at EOF");
	if (parser->length || zend_hash_num_elements(Z_ARRVAL(parser->row)) || parser->quoted) {
		if (ch_stream_cell(parser) != SUCCESS) return CH_TYPE_LOCAL_ERROR;
		return ch_stream_row(parser);
	}
	return SUCCESS;
}

PHP_METHOD(ClickHouse, insertFromStream)
{
	zend_string *table, *format = NULL, *query_id = NULL, *sql = NULL;
	zval *columns, *stream_value, *settings = NULL;
	zval stream_hold;
	zend_long batch_rows = 10000;
	php_stream *stream = NULL;
	clickhouse_object *obj;
	ch_query query;
	ch_stream_batch batch = {0};
	ch_stream_parser parser = {0};
	int result = FAILURE;
	ZEND_PARSE_PARAMETERS_START(3, 7)
		Z_PARAM_STR(table)
		Z_PARAM_ARRAY(columns)
		Z_PARAM_ZVAL(stream_value)
		Z_PARAM_OPTIONAL
		Z_PARAM_STR(format)
		Z_PARAM_LONG(batch_rows)
		Z_PARAM_STR(query_id)
		Z_PARAM_ARRAY(settings)
	ZEND_PARSE_PARAMETERS_END();
	if (ch_stream_format_parse(format, &parser.format) != SUCCESS) return;
	if (batch_rows < 1) {
		ch_insert_error("insertFromStream: batch_rows must be >= 1");
		return;
	}
	if (batch_rows > 10000000) {
		ch_insert_error("insertFromStream: batch_rows exceeds the maximum of 10000000 (a larger value would buffer the whole stream in memory, defeating batching)");
		return;
	}
	ZVAL_DEREF(stream_value);
	if (Z_TYPE_P(stream_value) == IS_RESOURCE &&
		(Z_RES_P(stream_value)->type == php_file_le_stream() || Z_RES_P(stream_value)->type == php_file_le_pstream())) {
		stream = (php_stream *)Z_RES_P(stream_value)->ptr;
	}
	if (!stream) {
		ch_insert_error("insertFromStream: argument 3 must be an open stream resource");
		return;
	}
	batch.count = zend_hash_num_elements(Z_ARRVAL_P(columns));
	if (!batch.count) {
		ch_insert_error("insertFromStream: columns list cannot be empty");
		return;
	}
	obj = Z_CLICKHOUSE_P(getThis());
	if (ch_insert_guard(obj, query_id) != SUCCESS) return;
	ch_query_init(&query);
	ZVAL_COPY_DEREF(&stream_hold, stream_value);
	ZVAL_UNDEF(&parser.row);
	batch.obj = obj;
	batch.batch_rows = (size_t)batch_rows;
	parser.batch = &batch;
	if (obj->insert_active) {
		ch_insert_checked(ch_insert_error("The insert operation is now in progress"), query_id);
		goto cleanup;
	}
	if (ch_insert_checked(ch_names_copy(&batch.names, Z_ARRVAL_P(columns), "insertFromStream: columns must be a list of column-name strings"), query_id) != SUCCESS) goto cleanup;
	if (ch_insert_checked(ch_insert_sql(obj, &sql, table, columns), query_id) != SUCCESS) goto cleanup;
	if (ch_query_prepare(obj, &query, sql, NULL, query_id, settings, NULL) != SUCCESS) goto cleanup;
	if (ch_insert_begin(obj, &query) != SUCCESS) {
		if (obj->insert_active) (void)ch_insert_recover(obj, false);
		goto cleanup;
	}
	if (chc_block_n_columns(obj->insert_schema) != batch.count) {
		ch_insert_checked(ch_insert_error("INSERT sample block does not match the declared column count"), query_id);
		goto recover;
	}
	batch.columns = ecalloc(batch.count, sizeof(*batch.columns));
	for (size_t i = 0; i < batch.count; ++i) array_init(&batch.columns[i]);
	array_init_size(&parser.row, batch.count);
	parser.cell = emalloc(256);
	parser.capacity = 256;
	stream = NULL;
	if (Z_TYPE(stream_hold) == IS_RESOURCE &&
		(Z_RES(stream_hold)->type == php_file_le_stream() || Z_RES(stream_hold)->type == php_file_le_pstream())) {
		stream = (php_stream *)Z_RES(stream_hold)->ptr;
	}
	if (!stream) {
		ch_insert_checked(ch_insert_error("insertFromStream: argument 3 must remain an open stream resource"), query_id);
		goto recover;
	}
	for (;;) {
		char buffer[64 * 1024];
		ssize_t length = php_stream_read(stream, buffer, sizeof(buffer));
		if (EG(exception)) goto recover;
		if (length < 0) {
			ch_insert_checked(ch_insert_error("insertFromStream: stream read returned an error"), query_id);
			goto recover;
		}
		if (!length) {
			if (php_stream_eof(stream)) break;
			ch_insert_checked(ch_insert_error("insertFromStream: stream returned 0 bytes without reaching EOF"), query_id);
			goto recover;
		}
		if (ch_insert_checked(ch_stream_feed(&parser, buffer, (size_t)length), query_id) != SUCCESS) goto recover;
	}
	if (ch_insert_checked(ch_stream_finish(&parser), query_id) != SUCCESS ||
		ch_insert_checked(ch_stream_flush(&batch), query_id) != SUCCESS) goto recover;
	if (ch_insert_end(obj) != SUCCESS) {
		(void)ch_insert_recover(obj, false);
		goto cleanup;
	}
	result = SUCCESS;
	goto cleanup;
recover:
	(void)ch_insert_recover(obj, !batch.sent);
cleanup:
	if (result == SUCCESS) ch_record_query_success(obj, sql, query.query_id, false);
	else ch_record_query_error(obj, sql, query_id, false);
	if (parser.cell) efree(parser.cell);
	if (Z_TYPE(parser.row) != IS_UNDEF) zval_ptr_dtor(&parser.row);
	if (batch.columns) {
		for (size_t i = 0; i < batch.count; ++i) zval_ptr_dtor(&batch.columns[i]);
		efree(batch.columns);
	}
	ch_names_free(batch.names, batch.count);
	if (sql) zend_string_release(sql);
	zval_ptr_dtor(&stream_hold);
	ch_query_clear(&query);
	ch_operation_end(obj);
	if (result == SUCCESS) RETVAL_LONG(batch.total);
}
