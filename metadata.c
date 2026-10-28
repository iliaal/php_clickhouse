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

static void ch_append_sql_string(smart_str *out, zend_string *value)
{
	smart_str_appendc(out, '\'');
	for (size_t i = 0; i < ZSTR_LEN(value); ++i) {
		char c = ZSTR_VAL(value)[i];
		if (c == '\'' || c == '\\') {
			smart_str_appendc(out, '\\');
			smart_str_appendc(out, c);
		} else if (c == '\0') {
			smart_str_appendl(out, "\\0", 2);
		} else {
			smart_str_appendc(out, c);
		}
	}
	smart_str_appendc(out, '\'');
}

int ch_sql_string_literal(zend_string **out, zend_string *value)
{
	smart_str result = {0};
	*out = NULL;
	ch_append_sql_string(&result, value);
	smart_str_0(&result);
	*out = result.s;
	return SUCCESS;
}

int ch_sql_quoted_identifier(zend_string **out, zend_string *value)
{
	smart_str result = {0};
	*out = NULL;
	smart_str_appendc(&result, '`');
	for (size_t i = 0; i < ZSTR_LEN(value); ++i) {
		char c = ZSTR_VAL(value)[i];
		if (c == '`' || c == '\\') smart_str_appendc(&result, '\\');
		smart_str_appendc(&result, c);
	}
	smart_str_appendc(&result, '`');
	smart_str_0(&result);
	*out = result.s;
	return SUCCESS;
}

static int ch_identifier_error(const char *what, const char *reason)
{
	if (!EG(exception)) zend_throw_exception_ex(clickhouse_exception_ce, 0, "%s%s", what, reason);
	return FAILURE;
}

/* Bare or backtick-quoted segments; dots inside quotes are literal.
 * allow_dot permits one database separator. A NULL output validates only. */
int ch_parse_identifier(zend_string **out, const char *value, size_t length,
	const char *what, bool allow_dot)
{
	smart_str result = {0};
	size_t i = 0;
	bool dot_seen = false;
	const char *reason = NULL;
	if (out) *out = NULL;
	if (length == 0) return ch_identifier_error(what, " must not be empty");
	for (;;) {
		if (i >= length) { reason = " has an empty segment"; goto invalid; }
		if (value[i] == '`') {
			size_t j = i + 1;
			size_t inner_length = 0;
			bool closed = false;
			if (out) smart_str_appendc(&result, '`');
			while (j < length) {
				char c = value[j];
				if (c == '\\' && j + 1 < length && (value[j + 1] == '`' || value[j + 1] == '\\')) {
					c = value[j + 1];
					j += 2;
				} else if (c == '`') {
					closed = true;
					break;
				} else {
					if ((unsigned char)c < 0x20) { reason = " contains an invalid character"; goto invalid; }
					++j;
				}
				++inner_length;
				if (out) {
					if (c == '`' || c == '\\') smart_str_appendc(&result, '\\');
					smart_str_appendc(&result, c);
				}
			}
			if (!closed) { reason = " has an unterminated quoted segment"; goto invalid; }
			if (inner_length == 0) { reason = " has an empty segment"; goto invalid; }
			if (out) smart_str_appendc(&result, '`');
			i = j + 1;
		} else {
			unsigned char c = (unsigned char)value[i];
			if (!((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_')) {
				reason = " must start with a letter or underscore";
				goto invalid;
			}
			size_t start = i++;
			while (i < length) {
				c = (unsigned char)value[i];
				if (!((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_')) break;
				++i;
			}
			if (out) smart_str_appendl(&result, value + start, i - start);
		}
		if (i == length) break;
		if (value[i] != '.' || !allow_dot || dot_seen) { reason = " contains an invalid character"; goto invalid; }
		dot_seen = true;
		if (out) smart_str_appendc(&result, '.');
		++i;
	}
	if (out) { smart_str_0(&result); *out = result.s; }
	return SUCCESS;
invalid:
	smart_str_free(&result);
	return ch_identifier_error(what, reason);
}

int ch_validate_identifier(const char *value, size_t length, const char *what, bool allow_dot)
{
	return ch_parse_identifier(NULL, value, length, what, allow_dot);
}

int ch_validate_metadata_filter_name(zend_string *value, const char *what)
{
	return ZSTR_LEN(value) ? SUCCESS : ch_identifier_error(what, " must not be empty");
}

int ch_current_database(zend_string **out, zval *this_obj)
{
	zval rv;
	*out = NULL;
	ZVAL_UNDEF(&rv);
	zval *db = sc_zend_read_property(clickhouse_ce, this_obj, "database", sizeof("database") - 1, 0, &rv);
	if (!EG(exception)) {
		if (db) ZVAL_DEREF(db);
		*out = db && Z_TYPE_P(db) == IS_STRING ? zend_string_copy(Z_STR_P(db))
			: zend_string_init("default", sizeof("default") - 1, 0);
	}
	if (!Z_ISUNDEF(rv)) zval_ptr_dtor(&rv);
	return *out ? SUCCESS : FAILURE;
}

int ch_helper_select(zval *out, zval *this_obj, zend_string *sql, zend_long flags)
{
	return ch_select_into(out, this_obj, sql, NULL, flags, NULL, NULL, NULL, 0, NULL);
}

int ch_helper_execute(zval *this_obj, zend_string *sql)
{
	return ch_execute_into(this_obj, sql, NULL, NULL, NULL);
}

int ch_helper_select_first_row(zval *out, zval *this_obj, zend_string *sql)
{
	zval rows;
	ZVAL_UNDEF(&rows);
	if (ch_helper_select(&rows, this_obj, sql, 0) == FAILURE || EG(exception)) {
		if (!Z_ISUNDEF(rows)) zval_ptr_dtor(&rows);
		return FAILURE;
	}
	if (Z_TYPE(rows) == IS_ARRAY && zend_hash_num_elements(Z_ARRVAL(rows)) > 0) {
		zval *first;
		ZEND_HASH_FOREACH_VAL(Z_ARRVAL(rows), first) {
			if (Z_TYPE_P(first) == IS_ARRAY) {
				ZVAL_COPY(out, first);
				zval_ptr_dtor(&rows);
				return SUCCESS;
			}
			break;
		} ZEND_HASH_FOREACH_END();
	}
	array_init(out);
	if (!Z_ISUNDEF(rows)) zval_ptr_dtor(&rows);
	return SUCCESS;
}

static int ch_metadata_database(zend_string **out, zval *this_obj, zend_string *db)
{
	if (db && ZSTR_LEN(db)) { *out = zend_string_copy(db); return SUCCESS; }
	return ch_current_database(out, this_obj);
}

static int ch_metadata_select(zval *out, zval *this_obj, smart_str *sql, zend_long flags, bool first_row)
{
	smart_str_0(sql);
	int status = first_row ? ch_helper_select_first_row(out, this_obj, sql->s)
		: ch_helper_select(out, this_obj, sql->s, flags);
	smart_str_free(sql);
	return status;
}

static int ch_metadata_literal_select(zval *out, zval *this_obj, const char *sql, size_t length, zend_long flags)
{
	zend_string *query = zend_string_init(sql, length, 0);
	int status = ch_helper_select(out, this_obj, query, flags);
	zend_string_release(query);
	return status;
}

static int ch_metadata_table_names(zend_string **db, zend_string **table, zval *this_obj, zend_string *value)
{
	*db = NULL;
	*table = NULL;
	if (ch_current_database(db, this_obj) == FAILURE) return FAILURE;
	const char *dot = NULL;
	for (size_t i = 0; i < ZSTR_LEN(value); ++i) if (ZSTR_VAL(value)[i] == '.') dot = ZSTR_VAL(value) + i;
	if (dot) {
		zend_string_release(*db);
		*db = zend_string_init(ZSTR_VAL(value), (size_t)(dot - ZSTR_VAL(value)), 0);
		*table = zend_string_init(dot + 1, ZSTR_LEN(value) - (size_t)(dot + 1 - ZSTR_VAL(value)), 0);
	} else {
		*table = zend_string_copy(value);
	}
	if (ch_validate_metadata_filter_name(*db, "database name") == FAILURE ||
		ch_validate_metadata_filter_name(*table, "table name") == FAILURE) goto failure;
	if (memchr(ZSTR_VAL(*db), '.', ZSTR_LEN(*db)) || memchr(ZSTR_VAL(*table), '.', ZSTR_LEN(*table))) {
		zend_throw_exception(clickhouse_exception_ce, "table argument must be `table` or `db.table` (at most one dot)", 0);
		goto failure;
	}
	return SUCCESS;
failure:
	zend_string_release(*db);
	zend_string_release(*table);
	*db = NULL;
	*table = NULL;
	return FAILURE;
}

PHP_METHOD(ClickHouse, databaseSize)
{
	zend_string *db = NULL, *dbname = NULL;
	ZEND_PARSE_PARAMETERS_START(0, 1)
		Z_PARAM_OPTIONAL
		Z_PARAM_STR_OR_NULL(db)
	ZEND_PARSE_PARAMETERS_END();
	if (ch_metadata_database(&dbname, getThis(), db) == FAILURE) return;
	if (ch_validate_metadata_filter_name(dbname, "database name") == FAILURE) {
		zend_string_release(dbname);
		return;
	}
	smart_str sql = {0};
	smart_str_appends(&sql, "SELECT sum(bytes_on_disk) AS bytes_on_disk, sum(rows) AS rows "
		"FROM system.parts WHERE active AND database = ");
	ch_append_sql_string(&sql, dbname);
	zend_string_release(dbname);
	ch_metadata_select(return_value, getThis(), &sql, 0, true);
}

PHP_METHOD(ClickHouse, tablesSize)
{
	zend_string *db = NULL, *dbname = NULL;
	ZEND_PARSE_PARAMETERS_START(0, 1)
		Z_PARAM_OPTIONAL
		Z_PARAM_STR_OR_NULL(db)
	ZEND_PARSE_PARAMETERS_END();
	if (ch_metadata_database(&dbname, getThis(), db) == FAILURE) return;
	if (ch_validate_metadata_filter_name(dbname, "database name") == FAILURE) {
		zend_string_release(dbname);
		return;
	}
	smart_str sql = {0};
	smart_str_appends(&sql, "SELECT table, sum(bytes_on_disk) AS bytes_on_disk, sum(rows) AS rows, "
		"max(modification_time) AS modification_time "
		"FROM system.parts WHERE active AND database = ");
	ch_append_sql_string(&sql, dbname);
	smart_str_appends(&sql, " GROUP BY table ORDER BY table");
	zend_string_release(dbname);
	ch_metadata_select(return_value, getThis(), &sql, 0, false);
}

PHP_METHOD(ClickHouse, partitions)
{
	zend_string *table = NULL, *dbname = NULL, *tname = NULL;
	ZEND_PARSE_PARAMETERS_START(1, 1)
		Z_PARAM_STR(table)
	ZEND_PARSE_PARAMETERS_END();
	/* Permit one db.table separator; reject residual dots in either component. */
	if (ch_metadata_table_names(&dbname, &tname, getThis(), table) == FAILURE) return;
	smart_str sql = {0};
	smart_str_appends(&sql, "SELECT partition, count() AS parts, sum(rows) AS rows, "
		"sum(bytes_on_disk) AS bytes_on_disk, "
		"min(min_time) AS min_time, max(max_time) AS max_time "
		"FROM system.parts WHERE active AND database = ");
	ch_append_sql_string(&sql, dbname);
	smart_str_appends(&sql, " AND table = ");
	ch_append_sql_string(&sql, tname);
	smart_str_appends(&sql, " GROUP BY partition ORDER BY partition");
	zend_string_release(dbname);
	zend_string_release(tname);
	ch_metadata_select(return_value, getThis(), &sql, 0, false);
}

PHP_METHOD(ClickHouse, showTables)
{
	zend_string *db = NULL, *like = NULL, *dbname = NULL;
	ZEND_PARSE_PARAMETERS_START(0, 2)
		Z_PARAM_OPTIONAL
		Z_PARAM_STR_OR_NULL(db)
		Z_PARAM_STR_OR_NULL(like)
	ZEND_PARSE_PARAMETERS_END();
	if (ch_metadata_database(&dbname, getThis(), db) == FAILURE) return;
	smart_str sql = {0};
	smart_str_appends(&sql, "SELECT name FROM system.tables WHERE database = ");
	ch_append_sql_string(&sql, dbname);
	if (like && ZSTR_LEN(like)) {
		smart_str_appends(&sql, " AND name LIKE ");
		ch_append_sql_string(&sql, like);
	}
	smart_str_appends(&sql, " ORDER BY name");
	zend_string_release(dbname);
	ch_metadata_select(return_value, getThis(), &sql, SC_FETCH_COLUMN, false);
}

PHP_METHOD(ClickHouse, showCreateTable)
{
	zend_string *table = NULL;
	ZEND_PARSE_PARAMETERS_START(1, 1)
		Z_PARAM_STR(table)
	ZEND_PARSE_PARAMETERS_END();
	if (ch_validate_identifier(ZSTR_VAL(table), ZSTR_LEN(table), "table name", true) == FAILURE) return;
	smart_str sql = {0};
	smart_str_appends(&sql, "SHOW CREATE TABLE ");
	smart_str_append(&sql, table);
	ch_metadata_select(return_value, getThis(), &sql, SC_FETCH_ONE | SC_FETCH_COLUMN, false);
}

PHP_METHOD(ClickHouse, getServerUptime)
{
	if (zend_parse_parameters_none() == FAILURE) return;
	ch_metadata_literal_select(return_value, getThis(), ZEND_STRL("SELECT uptime() AS uptime"),
		SC_FETCH_ONE | SC_FETCH_COLUMN);
}

PHP_METHOD(ClickHouse, isExists)
{
	zend_string *db = NULL, *table = NULL;
	ZEND_PARSE_PARAMETERS_START(2, 2)
		Z_PARAM_STR(db)
		Z_PARAM_STR(table)
	ZEND_PARSE_PARAMETERS_END();
	/* Compare names as string values so special-character identifiers remain valid. */
	smart_str sql = {0};
	smart_str_appends(&sql, "SELECT count() AS c FROM system.tables WHERE database = ");
	ch_append_sql_string(&sql, db);
	smart_str_appends(&sql, " AND name = ");
	ch_append_sql_string(&sql, table);
	zval row;
	ZVAL_UNDEF(&row);
	if (ch_metadata_select(&row, getThis(), &sql, 0, true) == FAILURE) return;
	bool exists = false;
	if (Z_TYPE(row) == IS_ARRAY) {
		zval *count = zend_hash_str_find(Z_ARRVAL(row), ZEND_STRL("c"));
		if (count) exists = zval_get_long(count) > 0;
	}
	zval_ptr_dtor(&row);
	RETURN_BOOL(exists);
}

PHP_METHOD(ClickHouse, showDatabases)
{
	if (zend_parse_parameters_none() == FAILURE) return;
	ch_metadata_literal_select(return_value, getThis(),
		ZEND_STRL("SELECT name FROM system.databases ORDER BY name"), SC_FETCH_COLUMN);
}

/* The fixed projection avoids the wider Map(LowCardinality(String), ...)
 * fields from system.processes, preserving the existing return shape. */
PHP_METHOD(ClickHouse, showProcesslist)
{
	if (zend_parse_parameters_none() == FAILURE) return;
	ch_metadata_literal_select(return_value, getThis(), ZEND_STRL(
		"SELECT query_id, user, address, port, initial_user, initial_query_id, "
		"initial_address, interface, os_user, client_hostname, client_name, "
		"client_revision, client_version_major, client_version_minor, "
		"client_version_patch, http_method, http_user_agent, http_referer, "
		"forwarded_for, query, elapsed, read_rows, read_bytes, total_rows_approx, "
		"memory_usage, peak_memory_usage FROM system.processes"), 0);
}

PHP_METHOD(ClickHouse, getServerVersion)
{
	if (zend_parse_parameters_none() == FAILURE) return;
	zend_string *sql = zend_string_init(ZEND_STRL("SELECT version() AS v"), 0);
	zval row;
	ZVAL_UNDEF(&row);
	int status = ch_helper_select_first_row(&row, getThis(), sql);
	zend_string_release(sql);
	if (status == FAILURE) return;
	if (Z_TYPE(row) == IS_ARRAY) {
		zval *version = zend_hash_str_find(Z_ARRVAL(row), ZEND_STRL("v"));
		if (version && Z_TYPE_P(version) == IS_STRING) {
			ZVAL_STR_COPY(return_value, Z_STR_P(version));
			zval_ptr_dtor(&row);
			return;
		}
	}
	zval_ptr_dtor(&row);
	RETURN_EMPTY_STRING();
}

PHP_METHOD(ClickHouse, tableSize)
{
	zend_string *table = NULL, *dbname = NULL, *tname = NULL;
	ZEND_PARSE_PARAMETERS_START(1, 1)
		Z_PARAM_STR(table)
	ZEND_PARSE_PARAMETERS_END();
	if (ch_metadata_table_names(&dbname, &tname, getThis(), table) == FAILURE) return;
	smart_str sql = {0};
	smart_str_appends(&sql, "SELECT sum(rows) AS rows, sum(bytes_on_disk) AS bytes_on_disk, "
		"uniqExact(partition) AS partitions, max(modification_time) AS modification_time "
		"FROM system.parts WHERE active AND database = ");
	ch_append_sql_string(&sql, dbname);
	smart_str_appends(&sql, " AND table = ");
	ch_append_sql_string(&sql, tname);
	smart_str_appends(&sql, " GROUP BY database, table");
	zend_string_release(dbname);
	zend_string_release(tname);
	ch_metadata_select(return_value, getThis(), &sql, 0, true);
}

PHP_METHOD(ClickHouse, truncateTable)
{
	zend_string *table = NULL;
	ZEND_PARSE_PARAMETERS_START(1, 1)
		Z_PARAM_STR(table)
	ZEND_PARSE_PARAMETERS_END();
	if (ch_validate_identifier(ZSTR_VAL(table), ZSTR_LEN(table), "table name", true) == FAILURE) return;
	smart_str sql = {0};
	smart_str_appends(&sql, "TRUNCATE TABLE ");
	smart_str_append(&sql, table);
	smart_str_0(&sql);
	int status = ch_helper_execute(getThis(), sql.s);
	smart_str_free(&sql);
	RETURN_BOOL(status == SUCCESS && !EG(exception));
}

/* The partition argument is a quoted string value; integer partitions and
 * partition IDs remain the caller's responsibility through execute(). */
PHP_METHOD(ClickHouse, dropPartition)
{
	zend_string *table = NULL, *part = NULL;
	ZEND_PARSE_PARAMETERS_START(2, 2)
		Z_PARAM_STR(table)
		Z_PARAM_STR(part)
	ZEND_PARSE_PARAMETERS_END();
	if (ch_validate_identifier(ZSTR_VAL(table), ZSTR_LEN(table), "table name", true) == FAILURE) return;
	for (size_t i = 0; i < ZSTR_LEN(part); ++i) {
		if ((unsigned char)ZSTR_VAL(part)[i] < 0x20) {
			zend_throw_exception(clickhouse_exception_ce, "dropPartition: partition value contains a control character", 0);
			return;
		}
	}
	smart_str sql = {0};
	smart_str_appends(&sql, "ALTER TABLE ");
	smart_str_append(&sql, table);
	smart_str_appends(&sql, " DROP PARTITION ");
	ch_append_sql_string(&sql, part);
	smart_str_0(&sql);
	int status = ch_helper_execute(getThis(), sql.s);
	smart_str_free(&sql);
	RETURN_BOOL(status == SUCCESS && !EG(exception));
}
