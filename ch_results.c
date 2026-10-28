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
#include "clickhouse_c_internal.h"
#include "main/php_streams.h"
#include "ext/standard/file.h"
#include <limits.h>
#include <string.h>

typedef struct {
	chc_block **blocks;
	size_t block_count, block_capacity, block_idx, row_idx, buffered_estimate;
	uint64_t cumulative_row_idx, total_rows;
	zend_long fetch_mode;
	zend_object std;
} clickhouse_iter_object;

typedef struct {
	zval rows, positional_rows, statistics;
	HashPosition pos;
#if PHP_VERSION_ID < 80000
	zval gc_buf[3];
#endif
	zend_object std;
} clickhouse_statement_object;

static zend_object_handlers clickhouse_iter_object_handlers;
static zend_object_handlers clickhouse_statement_object_handlers;

static inline clickhouse_iter_object *clickhouse_iter_from_obj(zend_object *obj)
{
	return (clickhouse_iter_object *)((char *)obj - offsetof(clickhouse_iter_object, std));
}

static inline clickhouse_statement_object *clickhouse_statement_from_obj(zend_object *obj)
{
	return (clickhouse_statement_object *)((char *)obj - offsetof(clickhouse_statement_object, std));
}

#define Z_CLICKHOUSE_ITER_P(zv) clickhouse_iter_from_obj(Z_OBJ_P(zv))
#define Z_CLICKHOUSE_STATEMENT_P(zv) clickhouse_statement_from_obj(Z_OBJ_P(zv))

static zend_object *clickhouse_iter_create_object(zend_class_entry *ce)
{
	clickhouse_iter_object *iter = zend_object_alloc(sizeof(*iter), ce);
	memset(iter, 0, offsetof(clickhouse_iter_object, std));
	zend_object_std_init(&iter->std, ce);
	object_properties_init(&iter->std, ce);
	iter->std.handlers = &clickhouse_iter_object_handlers;
	return &iter->std;
}

static void clickhouse_iter_free_obj(zend_object *object)
{
	clickhouse_iter_object *iter = clickhouse_iter_from_obj(object);
	for (size_t i = 0; i < iter->block_count; ++i) chc_block_destroy(iter->blocks[i], &ch_php_allocator);
	if (iter->blocks) efree(iter->blocks);
	zend_object_std_dtor(&iter->std);
}

static zend_object *clickhouse_statement_create_object(zend_class_entry *ce)
{
	clickhouse_statement_object *stmt = zend_object_alloc(sizeof(*stmt), ce);
	ZVAL_UNDEF(&stmt->rows);
	ZVAL_UNDEF(&stmt->positional_rows);
	ZVAL_UNDEF(&stmt->statistics);
	stmt->pos = 0;
	zend_object_std_init(&stmt->std, ce);
	object_properties_init(&stmt->std, ce);
	stmt->std.handlers = &clickhouse_statement_object_handlers;
	return &stmt->std;
}

static void clickhouse_statement_free_obj(zend_object *object)
{
	clickhouse_statement_object *stmt = clickhouse_statement_from_obj(object);
	zval_ptr_dtor(&stmt->rows);
	zval_ptr_dtor(&stmt->positional_rows);
	zval_ptr_dtor(&stmt->statistics);
	zend_object_std_dtor(&stmt->std);
}

#if PHP_VERSION_ID >= 80000
static HashTable *clickhouse_statement_get_gc(zend_object *object, zval **table, int *n)
{
	clickhouse_statement_object *stmt = clickhouse_statement_from_obj(object);
	zend_get_gc_buffer *buf = zend_get_gc_buffer_create();
	zend_get_gc_buffer_add_zval(buf, &stmt->rows);
	zend_get_gc_buffer_add_zval(buf, &stmt->positional_rows);
	zend_get_gc_buffer_add_zval(buf, &stmt->statistics);
	zend_get_gc_buffer_use(buf, table, n);
	return zend_std_get_properties(object);
}
#else
static HashTable *clickhouse_statement_get_gc(zval *object, zval **table, int *n)
{
	clickhouse_statement_object *stmt = Z_CLICKHOUSE_STATEMENT_P(object);
	int count = 0;
	if (Z_TYPE(stmt->rows) != IS_UNDEF) ZVAL_COPY_VALUE(&stmt->gc_buf[count++], &stmt->rows);
	if (Z_TYPE(stmt->positional_rows) != IS_UNDEF) ZVAL_COPY_VALUE(&stmt->gc_buf[count++], &stmt->positional_rows);
	if (Z_TYPE(stmt->statistics) != IS_UNDEF) ZVAL_COPY_VALUE(&stmt->gc_buf[count++], &stmt->statistics);
	*table = stmt->gc_buf;
	*n = count;
	return zend_std_get_properties(object);
}
#endif

int ch_results_minit(INIT_FUNC_ARGS)
{
	(void)type;
	(void)module_number;
	memcpy(&clickhouse_iter_object_handlers, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
	clickhouse_iter_object_handlers.offset = offsetof(clickhouse_iter_object, std);
	clickhouse_iter_object_handlers.free_obj = clickhouse_iter_free_obj;
	clickhouse_iter_object_handlers.clone_obj = NULL;
	clickhouse_iter_ce->create_object = clickhouse_iter_create_object;
	memcpy(&clickhouse_statement_object_handlers, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
	clickhouse_statement_object_handlers.offset = offsetof(clickhouse_statement_object, std);
	clickhouse_statement_object_handlers.free_obj = clickhouse_statement_free_obj;
	clickhouse_statement_object_handlers.get_gc = clickhouse_statement_get_gc;
	clickhouse_statement_object_handlers.clone_obj = NULL;
	clickhouse_statement_ce->create_object = clickhouse_statement_create_object;
#if PHP_VERSION_ID >= 80400
	clickhouse_iter_ce->default_object_handlers = &clickhouse_iter_object_handlers;
	clickhouse_statement_ce->default_object_handlers = &clickhouse_statement_object_handlers;
#endif
	return SUCCESS;
}

static int ch_result_error(const char *message, zend_string *query_id)
{
	if (!EG(exception)) {
		zend_string *safe = ch_sanitize_error(message, strlen(message));
		zend_throw_exception(clickhouse_exception_ce, ZSTR_VAL(safe), 0);
		zend_string_release(safe);
		ch_annotate_local_exception(query_id);
	}
	return FAILURE;
}

static int ch_result_begin(clickhouse_object *obj, zend_string *sql, zend_string *query_id)
{
	bool had_exception = EG(exception) != NULL;
	if (ch_operation_begin(obj, false) == SUCCESS) return SUCCESS;
	if (!had_exception) ch_annotate_local_exception(query_id);
	ch_record_query_local_error(obj, sql, query_id, false);
	return FAILURE;
}

/* Read conversion invokes no user code, so its errors belong to this query. */
static int ch_result_read(zval *out, const chc_type *type, const chc_column *column,
	size_t row, zend_long flags, zend_string *query_id)
{
	int status = ch_read_value(out, type, column, row, flags, 0);
	if (status != SUCCESS) ch_annotate_local_exception(query_id);
	return status;
}

static void ch_result_clear(zval *value)
{
	if (Z_TYPE_P(value) != IS_UNDEF) zval_ptr_dtor(value);
	ZVAL_UNDEF(value);
}

static bool ch_verbose_active(const clickhouse_object *obj)
{
	return obj->verbose_to_stderr || Z_TYPE(obj->verbose_callback) != IS_UNDEF;
}

static int ch_result_start(clickhouse_object *obj, ch_query *query,
	const char *event, zend_long flags, bool has_flags)
{
	zval context;
	zend_string *redacted;
	if (!ch_verbose_active(obj)) return SUCCESS;
	array_init(&context);
	redacted = ch_redact_sql_literals(query->log_sql, query->distrust_braces);
	if (!redacted) { zval_ptr_dtor(&context); return FAILURE; }
	add_assoc_str(&context, "sql", redacted);
	add_assoc_str(&context, "query_id", zend_string_copy(query->query_id));
	add_assoc_long(&context, "settings_count", zend_hash_num_elements(&obj->settings));
	if (has_flags) add_assoc_long(&context, "fetch_mode", flags);
	return ch_emit_verbose(obj, event, &context);
}

static int ch_result_block(clickhouse_object *obj, const chc_block *block, size_t *index)
{
	zval context;
	if (!ch_verbose_active(obj)) return SUCCESS;
	array_init(&context);
	add_assoc_long(&context, "rows", (zend_long)chc_block_n_rows(block));
	add_assoc_long(&context, "columns", (zend_long)chc_block_n_columns(block));
	add_assoc_long(&context, "block_index", (zend_long)(*index)++);
	return ch_emit_verbose(obj, "data_block", &context);
}

static int ch_result_finish(clickhouse_object *obj, const char *event, size_t blocks, bool select)
{
	zval context;
	if (!ch_verbose_active(obj)) return SUCCESS;
	array_init(&context);
	add_assoc_double(&context, "elapsed_ms", obj->stats.elapsed_ms);
	if (select) {
		ch_add_assoc_uint64(&context, "rows_read", obj->stats.rows_read);
		ch_add_assoc_uint64(&context, "bytes_read", obj->stats.bytes_read);
		ch_add_assoc_uint64(&context, "blocks", blocks);
	}
	return ch_emit_verbose(obj, event, &context);
}

static void ch_result_record(clickhouse_object *obj, ch_query *query,
	zend_string *sql, zend_string *query_id, int status)
{
	zend_string *empty = NULL;
	if (!query_id && !query->query_id) empty = zend_string_init("", 0, 0);
	if (status == SUCCESS) {
		ch_record_query_success(obj, query->log_sql ? query->log_sql : sql,
			query->query_id ? query->query_id : (query_id ? query_id : empty), query->distrust_braces);
	} else {
		ch_record_query_error(obj, query->log_sql ? query->log_sql : sql,
			query->query_id ? query->query_id : (query_id ? query_id : empty), query->distrust_braces);
	}
	if (empty) zend_string_release(empty);
}

static int ch_result_row(zval *out, const chc_block *block, size_t row,
	zend_long flags, zval *positional, zend_string *query_id)
{
	size_t columns = chc_block_n_columns(block);
	array_init_size(out, (uint32_t)columns);
	if (positional) array_init_size(positional, (uint32_t)columns);
	for (size_t col = 0; col < columns; ++col) {
		zval cell;
		size_t length;
		const char *name = chc_block_column_name(block, col, &length);
		ZVAL_UNDEF(&cell);
		if (ch_result_read(&cell, chc_block_column_type(block, col),
			chc_block_column(block, col), row, flags & SC_FETCH_VALUE_FLAGS, query_id) != SUCCESS) {
			ch_result_clear(out);
			if (positional) ch_result_clear(positional);
			return FAILURE;
		}
		if (positional) {
			zval copy;
			ZVAL_COPY(&copy, &cell);
			add_next_index_zval(positional, &copy);
		}
		add_assoc_zval_ex(out, name, length, &cell);
	}
	return SUCCESS;
}

typedef struct {
	zval *out, *positional;
	zend_string *query_id;
	zend_long flags;
	size_t blocks;
	bool fetched_one, positional_decided, positional_active;
} ch_select_context;

static int ch_result_pair(zval *out, const chc_block *block, size_t row,
	zend_long flags, zend_string *query_id)
{
	zval key, value;
	zend_string *coerced;
	ZVAL_UNDEF(&key);
	ZVAL_UNDEF(&value);
	if (chc_block_n_columns(block) < 2) return ch_result_error("Key pair mode requires at least 2 columns to be present", query_id);
	if (ch_result_read(&key, chc_block_column_type(block, 0), chc_block_column(block, 0), row, flags, query_id) != SUCCESS ||
		ch_result_read(&value, chc_block_column_type(block, 1), chc_block_column(block, 1), row, flags, query_id) != SUCCESS) goto fail;
	if (Z_TYPE(key) == IS_ARRAY || Z_TYPE(key) == IS_OBJECT) {
		ch_result_error("Key pair mode requires a scalar key column", query_id);
		goto fail;
	}
	if (Z_TYPE(key) == IS_LONG) zend_hash_index_update(Z_ARRVAL_P(out), Z_LVAL(key), &value);
	else {
		coerced = zval_get_string(&key);
		if (EG(exception)) { if (coerced) zend_string_release(coerced); goto fail; }
		zend_symtable_update(Z_ARRVAL_P(out), coerced, &value);
		zend_string_release(coerced);
	}
	zval_ptr_dtor(&key);
	return SUCCESS;
fail:
	ch_result_clear(&key);
	ch_result_clear(&value);
	return FAILURE;
}

static ch_block_action ch_collect_rows(clickhouse_object *obj, chc_block **owned, void *context)
{
	ch_select_context *result = context;
	const chc_block *block = *owned;
	size_t columns = chc_block_n_columns(block), rows = chc_block_n_rows(block);
	if (ch_result_block(obj, block, &result->blocks) == FAILURE) return CH_BLOCK_FAIL;
	if (result->flags & SC_FETCH_ONE) {
		if (!result->fetched_one && rows && columns) {
			if (ch_result_read(result->out, chc_block_column_type(block, 0),
				chc_block_column(block, 0), 0, result->flags, result->query_id) != SUCCESS) return CH_BLOCK_FAIL;
			result->fetched_one = true;
		}
		return CH_BLOCK_CONTINUE;
	}
	if (result->positional && !result->positional_decided) {
		HashTable names;
		zval marker;
		result->positional_decided = true;
		zend_hash_init(&names, (uint32_t)columns, NULL, NULL, 0);
		ZVAL_TRUE(&marker);
		for (size_t col = 0; col < columns; ++col) {
			size_t length;
			const char *name = chc_block_column_name(block, col, &length);
			if (!zend_hash_str_add(&names, name, length, &marker)) { result->positional_active = true; break; }
		}
		zend_hash_destroy(&names);
		if (result->positional_active) array_init(result->positional);
	}
	for (size_t row = 0; row < rows; ++row) {
		zval value, positional;
		ZVAL_UNDEF(&value);
		ZVAL_UNDEF(&positional);
		if (result->flags & SC_FETCH_KEY_PAIR) {
			if (ch_result_pair(result->out, block, row, result->flags, result->query_id) == FAILURE) return CH_BLOCK_FAIL;
			continue;
		}
		if (result->flags & SC_FETCH_COLUMN) {
			if (columns && ch_result_read(&value, chc_block_column_type(block, 0),
				chc_block_column(block, 0), row, result->flags, result->query_id) != SUCCESS) return CH_BLOCK_FAIL;
			if (!columns) ZVAL_NULL(&value);
		} else if (ch_result_row(&value, block, row, result->flags,
			result->positional_active ? &positional : NULL, result->query_id) == FAILURE) return CH_BLOCK_FAIL;
		if (result->positional_active && !(result->flags & SC_FETCH_COLUMN)) add_next_index_zval(result->positional, &positional);
		add_next_index_zval(result->out, &value);
	}
	return CH_BLOCK_CONTINUE;
}

static int ch_select_guarded(zval *out, clickhouse_object *obj, zend_string *sql,
	zval *params, zend_long flags, zend_string *query_id, zval *settings,
	const ch_external_table *externals, size_t external_count, zval *positional_out)
{
	ch_query query;
	ch_select_context result = {0};
	int status = FAILURE;
	result.out = out; result.positional = positional_out; result.flags = flags;
	ZVAL_UNDEF(out);
	if (positional_out) ZVAL_UNDEF(positional_out);
	ch_query_init(&query);
	if (ch_query_prepare(obj, &query, sql, params, query_id, settings,
		"The second argument to the select function must be an array") == FAILURE) goto done;
	result.query_id = query.query_id;
	if (ch_result_start(obj, &query, "select_start", flags, true) == FAILURE) goto done;
	if (!(flags & SC_FETCH_ONE)) array_init(out);
	if (ch_query_run(obj, &query, externals, external_count, ch_collect_rows, &result) == FAILURE) goto done;
	if (ch_result_finish(obj, "select_finish", result.blocks, true) == FAILURE) goto done;
	status = SUCCESS;
done:
	ch_result_record(obj, &query, sql, query_id, status);
	ch_query_clear(&query);
	if (status == FAILURE || EG(exception)) {
		ch_result_clear(out);
		if (positional_out) ch_result_clear(positional_out);
		return FAILURE;
	}
	if (Z_TYPE_P(out) == IS_UNDEF) ZVAL_NULL(out);
	return SUCCESS;
}

int ch_select_into(zval *out, zval *this_obj, zend_string *sql,
	zval *params, zend_long flags, zend_string *query_id, zval *settings,
	const ch_external_table *externals, size_t external_count, zval *positional_out)
{
	clickhouse_object *obj = Z_CLICKHOUSE_P(this_obj);
	int status;
	ZVAL_UNDEF(out);
	if (positional_out) ZVAL_UNDEF(positional_out);
	if (ch_result_begin(obj, sql, query_id) == FAILURE) return FAILURE;
	status = ch_select_guarded(out, obj, sql, params, flags, query_id, settings, externals, external_count, positional_out);
	ch_operation_end(obj);
	return status;
}

int ch_execute_into(zval *this_obj, zend_string *sql, zval *params, zend_string *query_id, zval *settings)
{
	clickhouse_object *obj = Z_CLICKHOUSE_P(this_obj);
	ch_query query;
	int status = FAILURE;
	if (ch_result_begin(obj, sql, query_id) == FAILURE) return FAILURE;
	ch_query_init(&query);
	if (ch_query_prepare(obj, &query, sql, params, query_id, settings,
		"The second argument to execute must be an array") == FAILURE) goto done;
	if (ch_result_start(obj, &query, "execute_start", 0, false) == FAILURE) goto done;
	if (ch_query_run(obj, &query, NULL, 0, NULL, NULL) == FAILURE) goto done;
	if (ch_result_finish(obj, "execute_finish", 0, false) == FAILURE) goto done;
	status = SUCCESS;
done:
	ch_result_record(obj, &query, sql, query_id, status);
	ch_query_clear(&query);
	ch_operation_end(obj);
	return EG(exception) ? FAILURE : status;
}

PHP_METHOD(ClickHouse, select)
{
	zend_string *sql, *query_id = NULL;
	zval *params = NULL, *settings = NULL;
	zend_long flags = 0;
	ZEND_PARSE_PARAMETERS_START(1, 5)
		Z_PARAM_STR(sql)
		Z_PARAM_OPTIONAL
		Z_PARAM_ARRAY(params)
		Z_PARAM_LONG(flags)
		Z_PARAM_STR(query_id)
		Z_PARAM_ARRAY(settings)
	ZEND_PARSE_PARAMETERS_END();
	ch_select_into(return_value, getThis(), sql, params, flags, query_id, settings, NULL, 0, NULL);
}

PHP_METHOD(ClickHouse, execute)
{
	zend_string *sql, *query_id = NULL;
	zval *params = NULL, *settings = NULL;
	ZEND_PARSE_PARAMETERS_START(1, 4)
		Z_PARAM_STR(sql)
		Z_PARAM_OPTIONAL
		Z_PARAM_ARRAY(params)
		Z_PARAM_STR(query_id)
		Z_PARAM_ARRAY(settings)
	ZEND_PARSE_PARAMETERS_END();
	if (ch_execute_into(getThis(), sql, params, query_id, settings) == FAILURE) return;
	RETURN_TRUE;
}

PHP_METHOD(ClickHouse, selectWithExternalData)
{
	zend_string *sql, *query_id = NULL;
	zval *externals, *params = NULL, *settings = NULL;
	zend_long flags = 0;
	ch_external_tables *tables = NULL;
	int status;
	ZEND_PARSE_PARAMETERS_START(2, 6)
		Z_PARAM_STR(sql)
		Z_PARAM_ARRAY(externals)
		Z_PARAM_OPTIONAL
		Z_PARAM_ARRAY(params)
		Z_PARAM_LONG(flags)
		Z_PARAM_STR(query_id)
		Z_PARAM_ARRAY(settings)
	ZEND_PARSE_PARAMETERS_END();
	if (!zend_hash_num_elements(Z_ARRVAL_P(externals))) {
		ch_result_error("selectWithExternalData requires at least one external table; use select() when no external data is needed", NULL);
		return;
	}
	status = ch_external_tables_create(&tables, externals);
	if (status == SUCCESS) {
		ch_select_into(return_value, getThis(), sql, params, flags, query_id, settings,
			ch_external_tables_data(tables), ch_external_tables_count(tables), NULL);
	} else if (status == CH_TYPE_LOCAL_ERROR) {
		ch_annotate_local_exception(NULL);
	}
	ch_external_tables_destroy(tables);
}

PHP_METHOD(ClickHouse, selectStatement)
{
	zend_string *sql, *query_id = NULL;
	zval *params = NULL, *settings = NULL;
	zend_long flags = 0;
	clickhouse_statement_object *stmt;
	ZEND_PARSE_PARAMETERS_START(1, 5)
		Z_PARAM_STR(sql)
		Z_PARAM_OPTIONAL
		Z_PARAM_ARRAY(params)
		Z_PARAM_STR(query_id)
		Z_PARAM_ARRAY(settings)
		Z_PARAM_LONG(flags)
	ZEND_PARSE_PARAMETERS_END();
	object_init_ex(return_value, clickhouse_statement_ce);
	stmt = Z_CLICKHOUSE_STATEMENT_P(return_value);
	if (ch_select_into(&stmt->rows, getThis(), sql, params, flags & SC_FETCH_VALUE_FLAGS,
		query_id, settings, NULL, 0, &stmt->positional_rows) == FAILURE) { ch_result_clear(return_value); return; }
	ch_stats_array(&stmt->statistics, &Z_CLICKHOUSE_P(getThis())->stats);
}
static size_t ch_size_add(size_t left, size_t right)
{
	return right > SIZE_MAX - left ? SIZE_MAX : left + right;
}

static size_t ch_size_mul(size_t left, size_t right)
{
	return right && left > SIZE_MAX / right ? SIZE_MAX : left * right;
}

static size_t ch_column_bytes(const chc_column *column)
{
	size_t bytes = sizeof(*column);
	switch (column->layout) {
		case CHC_COL_FIXED:
			return ch_size_add(bytes, ch_size_mul(column->n_rows, column->fixed.elem_size));
		case CHC_COL_STRING:
			return ch_size_add(bytes, ch_size_add(ch_size_mul(column->n_rows, 2 * sizeof(size_t)), column->str.bytes));
		case CHC_COL_NULLABLE:
			return ch_size_add(bytes, ch_size_add(column->n_rows, ch_column_bytes(column->nullable.inner)));
		case CHC_COL_ARRAY:
			return ch_size_add(bytes, ch_size_add(ch_size_mul(column->n_rows, sizeof(uint64_t)), ch_column_bytes(column->array.values)));
		case CHC_COL_TUPLE:
			bytes = ch_size_add(bytes, ch_size_mul(column->tuple.arity, sizeof(chc_column *)));
			for (size_t i = 0; i < column->tuple.arity; ++i) bytes = ch_size_add(bytes, ch_column_bytes(column->tuple.children[i]));
			return bytes;
		case CHC_COL_LOW_CARDINALITY:
			return ch_size_add(bytes, ch_size_add(ch_size_mul(column->n_rows, (size_t)column->lc.key_size), ch_column_bytes(column->lc.dict)));
		case CHC_COL_NOTHING:
			return bytes;
	}
	return SIZE_MAX;
}

typedef struct {
	clickhouse_iter_object *iterator;
	zend_string *query_id;
	size_t blocks;
} ch_iterator_context;

static ch_block_action ch_collect_blocks(clickhouse_object *obj, chc_block **owned, void *context)
{
	ch_iterator_context *result = context;
	clickhouse_iter_object *iter = result->iterator;
	const chc_block *block = *owned;
	size_t rows = chc_block_n_rows(block), columns = chc_block_n_columns(block);
	size_t incoming = 0;
	if (!rows || !columns) return CH_BLOCK_CONTINUE;
	if (ch_result_block(obj, block, &result->blocks) == FAILURE) return CH_BLOCK_FAIL;
	if (PG(memory_limit) > 0) {
		size_t floor = ch_size_mul(ch_size_mul(rows, columns), 32);
		size_t limit = (size_t)PG(memory_limit), used = zend_memory_usage(1);
		for (size_t col = 0; col < columns; ++col) incoming = ch_size_add(incoming, ch_column_bytes(chc_block_column(block, col)));
		if (floor > incoming) incoming = floor;
		if (used >= limit || iter->buffered_estimate > limit - used || incoming > limit - used - iter->buffered_estimate) {
			ch_result_error("selectStream: buffered result exceeds PHP memory_limit; use selectStreamCallback() for unbounded streaming or raise memory_limit", result->query_id);
			return CH_BLOCK_FAIL;
		}
	}
	if (iter->block_count == iter->block_capacity) {
		size_t capacity = iter->block_capacity ? ch_size_mul(iter->block_capacity, 2) : 8;
		iter->blocks = safe_erealloc(iter->blocks, capacity, sizeof(*iter->blocks), 0);
		iter->block_capacity = capacity;
	}
	iter->blocks[iter->block_count++] = *owned;
	*owned = NULL;
	iter->buffered_estimate = ch_size_add(iter->buffered_estimate, incoming);
	iter->total_rows += rows;
	return CH_BLOCK_CONTINUE;
}

PHP_METHOD(ClickHouse, selectStream)
{
	zend_string *sql, *query_id = NULL;
	zval *params = NULL, *settings = NULL;
	zend_long flags = 0;
	clickhouse_object *obj;
	ch_iterator_context context = {0};
	ch_query query;
	int status = FAILURE;
	ZEND_PARSE_PARAMETERS_START(1, 5)
		Z_PARAM_STR(sql)
		Z_PARAM_OPTIONAL
		Z_PARAM_ARRAY(params)
		Z_PARAM_STR(query_id)
		Z_PARAM_ARRAY(settings)
		Z_PARAM_LONG(flags)
	ZEND_PARSE_PARAMETERS_END();
	obj = Z_CLICKHOUSE_P(getThis());
	if (ch_result_begin(obj, sql, query_id) == FAILURE) return;
	object_init_ex(return_value, clickhouse_iter_ce);
	context.iterator = Z_CLICKHOUSE_ITER_P(return_value);
	context.iterator->fetch_mode = flags;
	ch_query_init(&query);
	if (ch_query_prepare(obj, &query, sql, params, query_id, settings, NULL) == FAILURE) goto done;
	context.query_id = query.query_id;
	if (ch_result_start(obj, &query, "select_start", flags, true) == FAILURE) goto done;
	if (ch_query_run(obj, &query, NULL, 0, ch_collect_blocks, &context) == FAILURE) goto done;
	if (ch_result_finish(obj, "select_finish", context.blocks, true) == FAILURE) goto done;
	status = SUCCESS;
done:
	ch_result_record(obj, &query, sql, query_id, status);
	ch_query_clear(&query);
	ch_operation_end(obj);
	if (status == FAILURE || EG(exception)) ch_result_clear(return_value);
}

typedef struct {
	zval callback;
	zend_string *query_id;
	zend_long flags;
	size_t blocks;
} ch_callback_context;

static ch_block_action ch_callback_rows(clickhouse_object *obj, chc_block **owned, void *context)
{
	ch_callback_context *result = context;
	const chc_block *block = *owned;
	size_t rows = chc_block_n_rows(block);
	if (!rows || !chc_block_n_columns(block)) return CH_BLOCK_CONTINUE;
	if (ch_result_block(obj, block, &result->blocks) == FAILURE) return CH_BLOCK_FAIL;
	for (size_t row = 0; row < rows; ++row) {
		zval argument, retval;
		int status;
		bool stop;
		ZVAL_UNDEF(&argument);
		ZVAL_UNDEF(&retval);
		if (ch_result_row(&argument, block, row, result->flags, NULL, result->query_id) == FAILURE) return CH_BLOCK_FAIL;
		status = call_user_function(NULL, NULL, &result->callback, &retval, 1, &argument);
		stop = Z_TYPE(retval) == IS_FALSE;
		zval_ptr_dtor(&argument);
		if (Z_TYPE(retval) != IS_UNDEF) zval_ptr_dtor(&retval);
		if (status == FAILURE || EG(exception) || Z_TYPE(retval) == IS_UNDEF) {
			ch_failure_set(obj, -1, ZEND_STRL("row callback aborted query"));
			ch_result_error("row callback aborted query", result->query_id);
			return CH_BLOCK_FAIL;
		}
		if (stop) return CH_BLOCK_CANCEL;
	}
	return CH_BLOCK_CONTINUE;
}

PHP_METHOD(ClickHouse, selectStreamCallback)
{
	zend_string *sql, *query_id = NULL;
	zval *callback, *params = NULL, *settings = NULL;
	zend_long flags = 0;
	clickhouse_object *obj;
	ch_callback_context context = {0};
	ch_query query;
	int status = FAILURE;
	ZEND_PARSE_PARAMETERS_START(2, 6)
		Z_PARAM_STR(sql)
		Z_PARAM_ZVAL(callback)
		Z_PARAM_OPTIONAL
		Z_PARAM_ARRAY(params)
		Z_PARAM_STR(query_id)
		Z_PARAM_ARRAY(settings)
		Z_PARAM_LONG(flags)
	ZEND_PARSE_PARAMETERS_END();
	if (!zend_is_callable(callback, 0, NULL)) { ch_result_error("Argument 2 passed to selectStreamCallback must be callable", NULL); return; }
	obj = Z_CLICKHOUSE_P(getThis());
	if (ch_result_begin(obj, sql, query_id) == FAILURE) return;
	ZVAL_COPY_DEREF(&context.callback, callback);
	context.flags = flags;
	ch_query_init(&query);
	if (ch_query_prepare(obj, &query, sql, params, query_id, settings, NULL) == FAILURE) goto done;
	context.query_id = query.query_id;
	if (ch_result_start(obj, &query, "select_start", flags, false) == FAILURE) goto done;
	if (ch_query_run(obj, &query, NULL, 0, ch_callback_rows, &context) == FAILURE) goto done;
	if (ch_result_finish(obj, "select_finish", context.blocks, true) == FAILURE) goto done;
	status = SUCCESS;
done:
	ch_result_record(obj, &query, sql, query_id, status);
	ch_query_clear(&query);
	zval_ptr_dtor(&context.callback);
	ch_operation_end(obj);
	if (status == FAILURE || EG(exception)) return;
	RETURN_TRUE;
}

PHP_METHOD(ClickHouseRowIterator, rewind)
{
	clickhouse_iter_object *iter;
	if (zend_parse_parameters_none() == FAILURE) return;
	iter = Z_CLICKHOUSE_ITER_P(getThis());
	iter->block_idx = iter->row_idx = 0;
	iter->cumulative_row_idx = 0;
}

PHP_METHOD(ClickHouseRowIterator, valid)
{
	clickhouse_iter_object *iter;
	if (zend_parse_parameters_none() == FAILURE) RETURN_FALSE;
	iter = Z_CLICKHOUSE_ITER_P(getThis());
	RETURN_BOOL(iter->block_idx < iter->block_count && iter->row_idx < chc_block_n_rows(iter->blocks[iter->block_idx]));
}

PHP_METHOD(ClickHouseRowIterator, current)
{
	clickhouse_iter_object *iter;
	if (zend_parse_parameters_none() == FAILURE) return;
	iter = Z_CLICKHOUSE_ITER_P(getThis());
	if (iter->block_idx >= iter->block_count || iter->row_idx >= chc_block_n_rows(iter->blocks[iter->block_idx])) RETURN_NULL();
	ch_result_row(return_value, iter->blocks[iter->block_idx], iter->row_idx, iter->fetch_mode, NULL, NULL);
}

PHP_METHOD(ClickHouseRowIterator, key)
{
	if (zend_parse_parameters_none() == FAILURE) RETURN_LONG(0);
	RETURN_LONG((zend_long)Z_CLICKHOUSE_ITER_P(getThis())->cumulative_row_idx);
}

PHP_METHOD(ClickHouseRowIterator, next)
{
	clickhouse_iter_object *iter;
	if (zend_parse_parameters_none() == FAILURE) return;
	iter = Z_CLICKHOUSE_ITER_P(getThis());
	if (iter->block_idx >= iter->block_count) return;
	++iter->row_idx;
	++iter->cumulative_row_idx;
	while (iter->block_idx < iter->block_count && iter->row_idx >= chc_block_n_rows(iter->blocks[iter->block_idx])) {
		++iter->block_idx;
		iter->row_idx = 0;
	}
}

PHP_METHOD(ClickHouseRowIterator, count)
{
	if (zend_parse_parameters_none() == FAILURE) RETURN_LONG(0);
	RETURN_LONG((zend_long)Z_CLICKHOUSE_ITER_P(getThis())->total_rows);
}

typedef enum {
	CH_STREAM_TSV, CH_STREAM_TSV_NAMES, CH_STREAM_CSV, CH_STREAM_CSV_NAMES
} ch_stream_format;

static int ch_stream_format_parse(zend_string *name, ch_stream_format *format)
{
	if (!name || zend_string_equals_literal(name, "TabSeparated") || zend_string_equals_literal(name, "TSV")) *format = CH_STREAM_TSV;
	else if (zend_string_equals_literal(name, "TabSeparatedWithNames") || zend_string_equals_literal(name, "TSVWithNames")) *format = CH_STREAM_TSV_NAMES;
	else if (zend_string_equals_literal(name, "CSV")) *format = CH_STREAM_CSV;
	else if (zend_string_equals_literal(name, "CSVWithNames")) *format = CH_STREAM_CSV_NAMES;
	else return FAILURE;
	return SUCCESS;
}

static bool ch_stream_csv(ch_stream_format format)
{
	return format == CH_STREAM_CSV || format == CH_STREAM_CSV_NAMES;
}

static const chc_type *ch_stream_unsupported(const chc_type *type)
{
	chc_kind kind;
	while ((kind = chc_type_kind(type)) == CHC_NULLABLE || kind == CHC_LOW_CARDINALITY || kind == CHC_SIMPLE_AGGREGATE_FUNCTION) {
		type = chc_type_child(type, 0);
	}
	switch (kind) {
		case CHC_ARRAY: case CHC_TUPLE: case CHC_MAP:
		case CHC_POINT: case CHC_RING: case CHC_POLYGON: case CHC_MULTI_POLYGON:
			return type;
		default:
			return NULL;
	}
}

static void ch_tsv_escape(smart_str *buffer, const char *data, size_t length)
{
	for (size_t i = 0; i < length; ++i) {
		switch (data[i]) {
			case '\\': smart_str_appendl(buffer, "\\\\", 2); break;
			case '\t': smart_str_appendl(buffer, "\\t", 2); break;
			case '\n': smart_str_appendl(buffer, "\\n", 2); break;
			case '\r': smart_str_appendl(buffer, "\\r", 2); break;
			case '\0': smart_str_appendl(buffer, "\\0", 2); break;
			default: smart_str_appendc(buffer, data[i]);
		}
	}
}

static void ch_csv_escape(smart_str *buffer, const char *data, size_t length)
{
	bool quote = length == 2 && data[0] == '\\' && data[1] == 'N';
	for (size_t i = 0; i < length; ++i) {
		if (data[i] == '"' || data[i] == ',' || data[i] == '\n' || data[i] == '\r') { quote = true; break; }
	}
	if (!quote) { smart_str_appendl(buffer, data, length); return; }
	smart_str_appendc(buffer, '"');
	for (size_t i = 0; i < length; ++i) {
		if (data[i] == '"') smart_str_appendc(buffer, '"');
		smart_str_appendc(buffer, data[i]);
	}
	smart_str_appendc(buffer, '"');
}

static int ch_stream_append_cell(smart_str *buffer, zval *cell, ch_stream_format format)
{
	zend_string *text;
	if (Z_TYPE_P(cell) == IS_NULL) { smart_str_appendl(buffer, "\\N", 2); return SUCCESS; }
	text = zval_get_string(cell);
	if (EG(exception)) { if (text) zend_string_release(text); return FAILURE; }
	if (ch_stream_csv(format)) ch_csv_escape(buffer, ZSTR_VAL(text), ZSTR_LEN(text));
	else ch_tsv_escape(buffer, ZSTR_VAL(text), ZSTR_LEN(text));
	zend_string_release(text);
	return SUCCESS;
}

static int ch_stream_flush(smart_str *buffer, zval *resource,
	zend_string *query_id, clickhouse_object *obj)
{
	size_t written = 0, length;
	if (!buffer->s || !(length = ZSTR_LEN(buffer->s))) return SUCCESS;
	while (written < length) {
		php_stream *stream = zend_fetch_resource2_ex(resource, NULL, php_file_le_stream(), php_file_le_pstream());
		ssize_t count;
		if (!stream) return ch_result_error("selectToStream: stream was closed during the query", query_id);
		count = php_stream_write(stream, ZSTR_VAL(buffer->s) + written, length - written);
		if (EG(exception) || count <= 0 || (size_t)count > length - written) {
			ch_failure_set(obj, -1, ZEND_STRL("selectToStream: write to PHP stream failed"));
			return ch_result_error("selectToStream: write to PHP stream failed", query_id);
		}
		written += (size_t)count;
	}
	smart_str_free(buffer);
	return SUCCESS;
}

typedef struct {
	zval resource;
	zend_string *query_id;
	smart_str buffer;
	ch_stream_format format;
	zend_long total_rows;
	size_t blocks;
	bool header_written;
} ch_stream_context;

static ch_block_action ch_stream_rows(clickhouse_object *obj, chc_block **owned, void *context)
{
	ch_stream_context *result = context;
	const chc_block *block = *owned;
	size_t columns = chc_block_n_columns(block), rows = chc_block_n_rows(block);
	bool csv = ch_stream_csv(result->format);
	char separator = csv ? ',' : '\t';
	const char *terminator = csv ? "\r\n" : "\n";
	size_t terminator_length = csv ? 2 : 1;
	if (!columns) return CH_BLOCK_CONTINUE;
	if (ch_result_block(obj, block, &result->blocks) == FAILURE) return CH_BLOCK_FAIL;
	if (!result->header_written) {
		for (size_t col = 0; col < columns; ++col) {
			const chc_type *bad = ch_stream_unsupported(chc_block_column_type(block, col));
			if (bad) {
				size_t name_length, type_length;
				const char *name = chc_block_column_name(block, col, &name_length);
				const char *type = chc_type_name(bad, &type_length);
				zend_string *message = strpprintf(0, "selectToStream: column '%.*s' has unsupported type '%.*s'; TSV/CSV cannot represent it",
					(int)name_length, name, (int)type_length, type);
				ch_result_error(ZSTR_VAL(message), result->query_id);
				zend_string_release(message);
				return CH_BLOCK_FAIL;
			}
		}
		if (result->format == CH_STREAM_TSV_NAMES || result->format == CH_STREAM_CSV_NAMES) {
			for (size_t col = 0; col < columns; ++col) {
				size_t length;
				const char *name = chc_block_column_name(block, col, &length);
				if (col) smart_str_appendc(&result->buffer, separator);
				if (csv) ch_csv_escape(&result->buffer, name, length);
				else ch_tsv_escape(&result->buffer, name, length);
			}
			smart_str_appendl(&result->buffer, terminator, terminator_length);
		}
		result->header_written = true;
	}
	for (size_t row = 0; row < rows; ++row) {
		for (size_t col = 0; col < columns; ++col) {
			zval cell;
			int status;
			ZVAL_UNDEF(&cell);
			if (col) smart_str_appendc(&result->buffer, separator);
			if (ch_result_read(&cell, chc_block_column_type(block, col), chc_block_column(block, col),
				row, SC_FETCH_DATE_AS_STRINGS, result->query_id) != SUCCESS) return CH_BLOCK_FAIL;
			status = ch_stream_append_cell(&result->buffer, &cell, result->format);
			zval_ptr_dtor(&cell);
			if (status == FAILURE) return CH_BLOCK_FAIL;
		}
		smart_str_appendl(&result->buffer, terminator, terminator_length);
		if (result->total_rows == ZEND_LONG_MAX) { ch_result_error("selectToStream: row count exceeds PHP integer range", result->query_id); return CH_BLOCK_FAIL; }
		++result->total_rows;
	}
	return ch_stream_flush(&result->buffer, &result->resource, result->query_id, obj) == SUCCESS ? CH_BLOCK_CONTINUE : CH_BLOCK_FAIL;
}

PHP_METHOD(ClickHouse, selectToStream)
{
	zend_string *sql, *format = NULL, *query_id = NULL;
	zval *params, *stream, *settings = NULL;
	clickhouse_object *obj;
	php_stream *native_stream;
	ch_stream_context context = {0};
	ch_query query;
	int status = FAILURE;
	ZEND_PARSE_PARAMETERS_START(3, 6)
		Z_PARAM_STR(sql)
		Z_PARAM_ARRAY(params)
		Z_PARAM_ZVAL(stream)
		Z_PARAM_OPTIONAL
		Z_PARAM_STR(format)
		Z_PARAM_STR(query_id)
		Z_PARAM_ARRAY(settings)
	ZEND_PARSE_PARAMETERS_END();
	if (ch_stream_format_parse(format, &context.format) == FAILURE) {
		ch_result_error("selectToStream: unknown format; expected TabSeparated, TabSeparatedWithNames, CSV, or CSVWithNames", NULL);
		return;
	}
	native_stream = zend_fetch_resource2_ex(stream, NULL, php_file_le_stream(), php_file_le_pstream());
	if (!native_stream) { ch_result_error("selectToStream: argument 3 must be an open stream resource", NULL); return; }
	obj = Z_CLICKHOUSE_P(getThis());
	if (ch_result_begin(obj, sql, query_id) == FAILURE) return;
	ZVAL_COPY_DEREF(&context.resource, stream);
	ch_query_init(&query);
	if (ch_query_prepare(obj, &query, sql, params, query_id, settings,
		"The second argument to selectToStream must be an array") == FAILURE) goto done;
	context.query_id = query.query_id;
	if (ch_result_start(obj, &query, "select_start", 0, false) == FAILURE) goto done;
	if (ch_query_run(obj, &query, NULL, 0, ch_stream_rows, &context) == FAILURE) goto done;
	if (ch_stream_flush(&context.buffer, &context.resource, context.query_id, obj) == FAILURE) goto done;
	if (ch_result_finish(obj, "select_finish", context.blocks, true) == FAILURE) goto done;
	status = SUCCESS;
done:
	ch_result_record(obj, &query, sql, query_id, status);
	ch_query_clear(&query);
	smart_str_free(&context.buffer);
	zval_ptr_dtor(&context.resource);
	ch_operation_end(obj);
	if (status == FAILURE || EG(exception)) return;
	RETURN_LONG(context.total_rows);
}

PHP_METHOD(ClickHouseStatement, __construct)
{
    zend_throw_exception(clickhouse_exception_ce,
        "ClickHouseStatement is constructed by ClickHouse::selectStatement(); direct construction is not allowed",
        0);
    return;
}

PHP_METHOD(ClickHouseStatement, count)
{
    if (zend_parse_parameters_none() == FAILURE) return;
    clickhouse_statement_object *stmt = Z_CLICKHOUSE_STATEMENT_P(getThis());
    if (Z_TYPE(stmt->rows) != IS_ARRAY) {
        RETURN_LONG(0);
    }
    RETURN_LONG((zend_long)zend_hash_num_elements(Z_ARRVAL(stmt->rows)));
}

PHP_METHOD(ClickHouseStatement, rewind)
{
    if (zend_parse_parameters_none() == FAILURE) return;
    clickhouse_statement_object *stmt = Z_CLICKHOUSE_STATEMENT_P(getThis());
    if (Z_TYPE(stmt->rows) == IS_ARRAY) {
        zend_hash_internal_pointer_reset_ex(Z_ARRVAL(stmt->rows), &stmt->pos);
    }
}

PHP_METHOD(ClickHouseStatement, valid)
{
    if (zend_parse_parameters_none() == FAILURE) return;
    clickhouse_statement_object *stmt = Z_CLICKHOUSE_STATEMENT_P(getThis());
    if (Z_TYPE(stmt->rows) != IS_ARRAY) {
        RETURN_FALSE;
    }
    RETURN_BOOL(zend_hash_get_current_data_ex(Z_ARRVAL(stmt->rows), &stmt->pos) != NULL);
}

PHP_METHOD(ClickHouseStatement, current)
{
    if (zend_parse_parameters_none() == FAILURE) return;
    clickhouse_statement_object *stmt = Z_CLICKHOUSE_STATEMENT_P(getThis());
    if (Z_TYPE(stmt->rows) != IS_ARRAY) {
        RETURN_NULL();
    }
    zval *cur = zend_hash_get_current_data_ex(Z_ARRVAL(stmt->rows), &stmt->pos);
    if (!cur) {
        RETURN_NULL();
    }
    ZVAL_COPY(return_value, cur);
}

PHP_METHOD(ClickHouseStatement, key)
{
    if (zend_parse_parameters_none() == FAILURE) return;
    clickhouse_statement_object *stmt = Z_CLICKHOUSE_STATEMENT_P(getThis());
    if (Z_TYPE(stmt->rows) != IS_ARRAY) {
        RETURN_NULL();
    }
    zend_string *str_key;
    zend_ulong num_key;
    int t = zend_hash_get_current_key_ex(Z_ARRVAL(stmt->rows), &str_key, &num_key, &stmt->pos);
    if (t == HASH_KEY_IS_STRING) {
        RETURN_STR_COPY(str_key);
    } else if (t == HASH_KEY_IS_LONG) {
        RETURN_LONG((zend_long)num_key);
    }
    RETURN_NULL();
}

PHP_METHOD(ClickHouseStatement, next)
{
    if (zend_parse_parameters_none() == FAILURE) return;
    clickhouse_statement_object *stmt = Z_CLICKHOUSE_STATEMENT_P(getThis());
    if (Z_TYPE(stmt->rows) == IS_ARRAY) {
        zend_hash_move_forward_ex(Z_ARRVAL(stmt->rows), &stmt->pos);
    }
}

PHP_METHOD(ClickHouseStatement, offsetExists)
{
    zval *offset;
    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_ZVAL(offset)
    ZEND_PARSE_PARAMETERS_END();
    clickhouse_statement_object *stmt = Z_CLICKHOUSE_STATEMENT_P(getThis());
    if (Z_TYPE(stmt->rows) != IS_ARRAY) {
        RETURN_FALSE;
    }
    HashTable *ht = Z_ARRVAL(stmt->rows);
    if (Z_TYPE_P(offset) == IS_LONG) {
        RETURN_BOOL(zend_hash_index_exists(ht, Z_LVAL_P(offset)));
    }
    if (Z_TYPE_P(offset) == IS_STRING) {
        RETURN_BOOL(zend_symtable_exists(ht, Z_STR_P(offset)));
    }
    RETURN_FALSE;
}

PHP_METHOD(ClickHouseStatement, offsetGet)
{
    zval *offset;
    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_ZVAL(offset)
    ZEND_PARSE_PARAMETERS_END();
    clickhouse_statement_object *stmt = Z_CLICKHOUSE_STATEMENT_P(getThis());
    if (Z_TYPE(stmt->rows) != IS_ARRAY) {
        RETURN_NULL();
    }
    HashTable *ht = Z_ARRVAL(stmt->rows);
    zval *v = NULL;
    if (Z_TYPE_P(offset) == IS_LONG) {
        v = zend_hash_index_find(ht, Z_LVAL_P(offset));
    } else if (Z_TYPE_P(offset) == IS_STRING) {
        v = zend_symtable_find(ht, Z_STR_P(offset));
    }
    if (!v) {
        RETURN_NULL();
    }
    ZVAL_COPY(return_value, v);
}

PHP_METHOD(ClickHouseStatement, offsetSet)
{
    zend_throw_exception(clickhouse_exception_ce,
        "ClickHouseStatement is read-only; offsetSet is not supported", 0);
}

PHP_METHOD(ClickHouseStatement, offsetUnset)
{
    zend_throw_exception(clickhouse_exception_ce,
        "ClickHouseStatement is read-only; offsetUnset is not supported", 0);
}

static void statement_emit_rows(zval *return_value, zval *this_obj)
{
    clickhouse_statement_object *stmt = Z_CLICKHOUSE_STATEMENT_P(this_obj);
    if (Z_TYPE(stmt->rows) == IS_ARRAY) {
        ZVAL_COPY(return_value, &stmt->rows);
    } else {
        array_init(return_value);
    }
}

PHP_METHOD(ClickHouseStatement, jsonSerialize)
{
    if (zend_parse_parameters_none() == FAILURE) return;
    statement_emit_rows(return_value, getThis());
}

PHP_METHOD(ClickHouseStatement, toArray)
{
    if (zend_parse_parameters_none() == FAILURE) return;
    statement_emit_rows(return_value, getThis());
}

PHP_METHOD(ClickHouseStatement, statistics)
{
    if (zend_parse_parameters_none() == FAILURE) return;
    clickhouse_statement_object *stmt = Z_CLICKHOUSE_STATEMENT_P(getThis());
    if (Z_TYPE(stmt->statistics) == IS_ARRAY) {
        ZVAL_COPY(return_value, &stmt->statistics);
    } else {
        array_init(return_value);
    }
}

PHP_METHOD(ClickHouseStatement, fetchOne)
{
    if (zend_parse_parameters_none() == FAILURE) return;
    clickhouse_statement_object *stmt = Z_CLICKHOUSE_STATEMENT_P(getThis());
    if (Z_TYPE(stmt->rows) != IS_ARRAY || zend_hash_num_elements(Z_ARRVAL(stmt->rows)) == 0) {
        RETURN_NULL();
    }
    HashPosition pos;
    zend_hash_internal_pointer_reset_ex(Z_ARRVAL(stmt->rows), &pos);
    zval *first = zend_hash_get_current_data_ex(Z_ARRVAL(stmt->rows), &pos);
    if (!first) {
        RETURN_NULL();
    }
    /* Assoc rows collapse duplicate names; use positional rows for true column count. */
    size_t col_count = (Z_TYPE_P(first) == IS_ARRAY)
        ? zend_hash_num_elements(Z_ARRVAL_P(first)) : 0;
    if (Z_TYPE(stmt->positional_rows) == IS_ARRAY) {
        HashPosition ppos;
        zend_hash_internal_pointer_reset_ex(Z_ARRVAL(stmt->positional_rows), &ppos);
        zval *pfirst = zend_hash_get_current_data_ex(Z_ARRVAL(stmt->positional_rows), &ppos);
        if (pfirst && Z_TYPE_P(pfirst) == IS_ARRAY) {
            col_count = zend_hash_num_elements(Z_ARRVAL_P(pfirst));
        }
    }
    if (Z_TYPE_P(first) == IS_ARRAY && col_count == 1) {
        zend_hash_internal_pointer_reset_ex(Z_ARRVAL_P(first), &pos);
        zval *only = zend_hash_get_current_data_ex(Z_ARRVAL_P(first), &pos);
        if (only) {
            ZVAL_COPY(return_value, only);
            return;
        }
    }
    ZVAL_COPY(return_value, first);
}

PHP_METHOD(ClickHouseStatement, fetchKeyPair)
{
    if (zend_parse_parameters_none() == FAILURE) return;
    clickhouse_statement_object *stmt = Z_CLICKHOUSE_STATEMENT_P(getThis());
    array_init(return_value);
    if (Z_TYPE(stmt->rows) != IS_ARRAY) {
        return;
    }
    zval *source_rows = (Z_TYPE(stmt->positional_rows) == IS_ARRAY)
        ? &stmt->positional_rows
        : &stmt->rows;
    zval *row;
    ZEND_HASH_FOREACH_VAL(Z_ARRVAL_P(source_rows), row) {
        if (Z_TYPE_P(row) != IS_ARRAY || zend_hash_num_elements(Z_ARRVAL_P(row)) < 2) {
            zend_throw_exception(clickhouse_exception_ce,
                "fetchKeyPair requires each row to have at least 2 columns", 0);
            zend_array_destroy(Z_ARR_P(return_value));
            ZVAL_UNDEF(return_value);
            return;
        }
        HashPosition pos;
        zend_hash_internal_pointer_reset_ex(Z_ARRVAL_P(row), &pos);
        zval *kv = zend_hash_get_current_data_ex(Z_ARRVAL_P(row), &pos);
        zend_hash_move_forward_ex(Z_ARRVAL_P(row), &pos);
        zval *vv = zend_hash_get_current_data_ex(Z_ARRVAL_P(row), &pos);
        if (!kv || !vv) continue;

        if (Z_TYPE_P(kv) == IS_ARRAY || Z_TYPE_P(kv) == IS_OBJECT) {
            /* Array keys would stringify to "Array", collapsing distinct rows. */
            zend_throw_exception(clickhouse_exception_ce,
                "fetchKeyPair requires a scalar key column", 0);
            zend_array_destroy(Z_ARR_P(return_value));
            ZVAL_UNDEF(return_value);
            return;
        }
        zval val_copy;
        ZVAL_COPY(&val_copy, vv);
        if (Z_TYPE_P(kv) == IS_LONG) {
            zend_hash_index_update(Z_ARRVAL_P(return_value), Z_LVAL_P(kv), &val_copy);
        } else {
            zend_string *coerced = zval_get_string(kv);
            if (EG(exception)) {
                zval_ptr_dtor(&val_copy);
                if (coerced) zend_string_release(coerced);
                return;
            }
            zend_symtable_update(Z_ARRVAL_P(return_value), coerced, &val_copy);
            zend_string_release(coerced);
        }
    } ZEND_HASH_FOREACH_END();
}
PHP_METHOD(ClickHouseStatement, fetchColumn)
{
    if (zend_parse_parameters_none() == FAILURE) return;
    clickhouse_statement_object *stmt = Z_CLICKHOUSE_STATEMENT_P(getThis());
    array_init(return_value);
    if (Z_TYPE(stmt->rows) != IS_ARRAY) {
        return;
    }
    zval *source_rows = (Z_TYPE(stmt->positional_rows) == IS_ARRAY)
        ? &stmt->positional_rows
        : &stmt->rows;
    zval *row;
    ZEND_HASH_FOREACH_VAL(Z_ARRVAL_P(source_rows), row) {
        if (Z_TYPE_P(row) != IS_ARRAY) {
            zval c;
            ZVAL_COPY(&c, row);
            add_next_index_zval(return_value, &c);
            continue;
        }
        HashPosition pos;
        zend_hash_internal_pointer_reset_ex(Z_ARRVAL_P(row), &pos);
        zval *first = zend_hash_get_current_data_ex(Z_ARRVAL_P(row), &pos);
        if (first) {
            zval c;
            ZVAL_COPY(&c, first);
            add_next_index_zval(return_value, &c);
        }
    } ZEND_HASH_FOREACH_END();
}
