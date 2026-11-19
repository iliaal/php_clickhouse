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

extern "C" {
#include "php.h"
#include "php_ini.h"
#include "ext/standard/info.h"
#include "Zend/zend_exceptions.h"
#include "php7_wrapper.h"
}

#include "php_clickhouse.h"

#include "lib/clickhouse-cpp/clickhouse/client.h"
#include "clickhouse_internal.h"
#include <stdexcept>

using namespace clickhouse;
using namespace std;

std::string sqlStringLiteral(const std::string &s)
{
    std::string out;
    out.reserve(s.size() + 2);
    out.push_back('\'');
    for (char c : s) {
        switch (c) {
            case '\'':
            case '\\':
                out.push_back('\\');
                out.push_back(c);
                break;
            case '\0':
                out.append("\\0", 2);
                break;
            default:
                out.push_back(c);
                break;
        }
    }
    out.push_back('\'');
    return out;
}

std::string sqlQuotedIdentifier(const std::string &s)
{
    std::string out;
    out.reserve(s.size() + 2);
    out.push_back('`');
    for (char c : s) {
        if (c == '`' || c == '\\') {
            out.push_back('\\');
        }
        out.push_back(c);
    }
    out.push_back('`');
    return out;
}

/* Bare or backtick-quoted segments; dots inside quotes are literal.
 * allow_dot permits one database separator. emit_out == nullptr validates
 * only; emit mode preserves bare segments and re-quotes quoted ones. */
void parseIdentifier(const char *s, size_t len, const char *what,
                     bool allow_dot, std::string *emit_out)
{
    auto invalid = [&](const char *why) {
        throw std::runtime_error(std::string(what) + why);
    };
    if (len == 0) {
        invalid(" must not be empty");
    }
    size_t i = 0;
    bool dot_seen = false;
    while (true) {
        if (i >= len) {
            invalid(" has an empty segment");
        }
        if (s[i] == '`') {
            std::string inner;
            size_t j = i + 1;
            bool closed = false;
            while (j < len) {
                if (s[j] == '\\' && j + 1 < len &&
                    (s[j + 1] == '`' || s[j + 1] == '\\')) {
                    inner.push_back(s[j + 1]);
                    j += 2;
                    continue;
                }
                if (s[j] == '`') {
                    closed = true;
                    break;
                }
                if ((unsigned char)s[j] < 0x20) {
                    invalid(" contains an invalid character");
                }
                inner.push_back(s[j]);
                ++j;
            }
            if (!closed) {
                invalid(" has an unterminated quoted segment");
            }
            if (inner.empty()) {
                invalid(" has an empty segment");
            }
            if (emit_out) {
                emit_out->append(sqlQuotedIdentifier(inner));
            }
            i = j + 1;
        } else {
            unsigned char c0 = (unsigned char)s[i];
            if (!((c0 >= 'A' && c0 <= 'Z') || (c0 >= 'a' && c0 <= 'z') || c0 == '_')) {
                invalid(" must start with a letter or underscore");
            }
            size_t start = i++;
            while (i < len) {
                unsigned char c = (unsigned char)s[i];
                if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
                    (c >= '0' && c <= '9') || c == '_') {
                    ++i;
                    continue;
                }
                break;
            }
            if (emit_out) {
                emit_out->append(s + start, i - start);
            }
        }
        if (i == len) {
            break;
        }
        if (s[i] != '.' || !allow_dot || dot_seen) {
            invalid(" contains an invalid character");
        }
        dot_seen = true;
        if (emit_out) {
            emit_out->push_back('.');
        }
        ++i;
    }
}

void validateIdentifier(const char *s, size_t len, const char *what, bool allow_dot)
{
    parseIdentifier(s, len, what, allow_dot, nullptr);
}

void validateMetadataFilterName(const std::string &s, const char *what)
{
    if (s.empty()) {
        throw std::runtime_error(std::string(what) + " must not be empty");
    }
}

std::string currentDatabase(zval *this_obj)
{
    zval rv;
    zval *db = sc_zend_read_property(clickhouse_ce, this_obj, "database", sizeof("database") - 1, 0, &rv);
    if (db) ZVAL_DEREF(db);
    if (db && Z_TYPE_P(db) == IS_STRING) {
        return std::string(Z_STRVAL_P(db), Z_STRLEN_P(db));
    }
    return std::string("default");
}

void runHelperSelect(zval *return_value, zval *this_obj, const std::string &sql, zend_long fetch_mode)
{
    do_select_into(return_value, this_obj, sql.c_str(), sql.size(),
                   /*params=*/NULL, fetch_mode, /*qid=*/std::string(),
                   /*settings=*/NULL, /*external_tables=*/NULL,
                   /*positional_out=*/NULL);
}

bool runHelperExec(zval *this_obj, const std::string &sql)
{
    do_execute_into(this_obj, sql.c_str(), sql.size(),
                    /*params=*/NULL, /*qid=*/std::string(), /*settings=*/NULL);
    return !EG(exception);
}

void runHelperSelectFirstRow(zval *return_value, zval *this_obj, const std::string &sql)
{
    zval rows;
    ZVAL_UNDEF(&rows);
    runHelperSelect(&rows, this_obj, sql, 0);
    if (EG(exception)) {
        /* Failure may leave a partially populated rows array, or UNDEF before initialization. */
        if (Z_TYPE(rows) != IS_UNDEF) {
            zval_ptr_dtor(&rows);
        }
        return;
    }
    if (Z_TYPE(rows) == IS_ARRAY && zend_hash_num_elements(Z_ARRVAL(rows)) > 0) {
        zval *first = NULL;
        zval *fz;
        ZEND_HASH_FOREACH_VAL(Z_ARRVAL(rows), fz) {
            first = fz;
            break;
        } ZEND_HASH_FOREACH_END();
        if (first && Z_TYPE_P(first) == IS_ARRAY) {
            ZVAL_COPY(return_value, first);
            zval_ptr_dtor(&rows);
            return;
        }
    }
    array_init(return_value);
    zval_ptr_dtor(&rows);
}
/* {{{ proto array databaseSize(?string database)
 */
CLICKHOUSE_METHOD(databaseSize)
{
    zend_string *db = NULL;
    ZEND_PARSE_PARAMETERS_START(0, 1)
        Z_PARAM_OPTIONAL
        Z_PARAM_STR_OR_NULL(db)
    ZEND_PARSE_PARAMETERS_END();
    std::string dbname = (db && ZSTR_LEN(db) > 0) ? std::string(ZSTR_VAL(db), ZSTR_LEN(db)) : currentDatabase(getThis());
    try {
        validateMetadataFilterName(dbname, "database name");
    } catch (const std::exception &e) {
        throwClickHouseError(e);
        return;
    }
    std::string sql =
        "SELECT sum(bytes_on_disk) AS bytes_on_disk, sum(rows) AS rows "
        "FROM system.parts WHERE active AND database = " + sqlStringLiteral(dbname);
    runHelperSelectFirstRow(return_value, getThis(), sql);
}
/* }}} */

/* {{{ proto array tablesSize(?string database)
 */
CLICKHOUSE_METHOD(tablesSize)
{
    zend_string *db = NULL;
    ZEND_PARSE_PARAMETERS_START(0, 1)
        Z_PARAM_OPTIONAL
        Z_PARAM_STR_OR_NULL(db)
    ZEND_PARSE_PARAMETERS_END();
    std::string dbname = (db && ZSTR_LEN(db) > 0) ? std::string(ZSTR_VAL(db), ZSTR_LEN(db)) : currentDatabase(getThis());
    try {
        validateMetadataFilterName(dbname, "database name");
    } catch (const std::exception &e) {
        throwClickHouseError(e);
        return;
    }
    std::string sql =
        "SELECT table, sum(bytes_on_disk) AS bytes_on_disk, sum(rows) AS rows, "
        "max(modification_time) AS modification_time "
        "FROM system.parts WHERE active AND database = " + sqlStringLiteral(dbname) + " "
        "GROUP BY table ORDER BY table";
    runHelperSelect(return_value, getThis(), sql, 0);
}
/* }}} */

/* {{{ proto array partitions(string table)
 */
CLICKHOUSE_METHOD(partitions)
{
    zend_string *table = NULL;
    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STR(table)
    ZEND_PARSE_PARAMETERS_END();
    std::string tname(ZSTR_VAL(table), ZSTR_LEN(table));
    std::string dbname = currentDatabase(getThis());
    /* Permit one db.table separator; reject residual dots in either component. */
    auto dot = tname.rfind('.');
    if (dot != std::string::npos) {
        dbname = tname.substr(0, dot);
        tname = tname.substr(dot + 1);
    }
    try {
        validateMetadataFilterName(dbname, "database name");
        validateMetadataFilterName(tname, "table name");
        if (dbname.find('.') != std::string::npos || tname.find('.') != std::string::npos) {
            throw std::runtime_error("table argument must be `table` or `db.table` (at most one dot)");
        }
    } catch (const std::exception &e) {
        throwClickHouseError(e);
        return;
    }
    std::string sql =
        "SELECT partition, count() AS parts, sum(rows) AS rows, "
        "sum(bytes_on_disk) AS bytes_on_disk, "
        "min(min_time) AS min_time, max(max_time) AS max_time "
        "FROM system.parts WHERE active AND database = " + sqlStringLiteral(dbname) + " "
        "AND table = " + sqlStringLiteral(tname) + " "
        "GROUP BY partition ORDER BY partition";
    runHelperSelect(return_value, getThis(), sql, 0);
}
/* }}} */

/* {{{ proto array showTables(?string database, ?string like)
 */
CLICKHOUSE_METHOD(showTables)
{
    zend_string *db = NULL, *like = NULL;
    ZEND_PARSE_PARAMETERS_START(0, 2)
        Z_PARAM_OPTIONAL
        Z_PARAM_STR_OR_NULL(db)
        Z_PARAM_STR_OR_NULL(like)
    ZEND_PARSE_PARAMETERS_END();
    std::string dbname = (db && ZSTR_LEN(db) > 0) ? std::string(ZSTR_VAL(db), ZSTR_LEN(db)) : currentDatabase(getThis());
    std::string sql = "SELECT name FROM system.tables WHERE database = " + sqlStringLiteral(dbname);
    if (like && ZSTR_LEN(like) > 0) {
        sql += " AND name LIKE " +
            sqlStringLiteral(std::string(ZSTR_VAL(like), ZSTR_LEN(like)));
    }
    sql += " ORDER BY name";
    runHelperSelect(return_value, getThis(), sql, SC_FETCH_COLUMN);
}
/* }}} */

/* {{{ proto string showCreateTable(string table)
 */
CLICKHOUSE_METHOD(showCreateTable)
{
    zend_string *table = NULL;
    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STR(table)
    ZEND_PARSE_PARAMETERS_END();
    std::string tname(ZSTR_VAL(table), ZSTR_LEN(table));
    try {
        validateIdentifier(tname.c_str(), tname.size(), "table name", true);
    } catch (const std::exception &e) {
        throwClickHouseError(e);
        return;
    }
    std::string sql = "SHOW CREATE TABLE " + tname;
    runHelperSelect(return_value, getThis(), sql, SC_FETCH_ONE | SC_FETCH_COLUMN);
}
/* }}} */

/* {{{ proto int getServerUptime()
 */
CLICKHOUSE_METHOD(getServerUptime)
{
    if (zend_parse_parameters_none() == FAILURE) {
        return;
    }
    runHelperSelect(return_value, getThis(),
        "SELECT uptime() AS uptime",
        SC_FETCH_ONE | SC_FETCH_COLUMN);
}
/* }}} */

/* {{{ proto bool isExists(string database, string table)
 *
 * Returns true when the (database, table) pair exists in
 * system.tables (covers views and dictionaries too). Both arguments
 * are compared as string values, not identifiers.
 */

CLICKHOUSE_METHOD(isExists)
{
    zend_string *db = NULL, *table = NULL;
    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_STR(db)
        Z_PARAM_STR(table)
    ZEND_PARSE_PARAMETERS_END();
    /* Compare names as string values so special-character identifiers remain valid. */
    std::string sql =
        "SELECT count() AS c FROM system.tables WHERE database = " +
        sqlStringLiteral(std::string(ZSTR_VAL(db), ZSTR_LEN(db))) +
        " AND name = " + sqlStringLiteral(std::string(ZSTR_VAL(table), ZSTR_LEN(table)));
    zval row;
    runHelperSelectFirstRow(&row, getThis(), sql);
    if (EG(exception)) return;
    bool exists = false;
    if (Z_TYPE(row) == IS_ARRAY) {
        zval *cnt = zend_hash_str_find(Z_ARRVAL(row), "c", sizeof("c") - 1);
        if (cnt) {
            exists = (zval_get_long(cnt) > 0);
        }
    }
    zval_ptr_dtor(&row);
    RETURN_BOOL(exists);
}
/* }}} */

/* {{{ proto array showDatabases()
 */
CLICKHOUSE_METHOD(showDatabases)
{
    if (zend_parse_parameters_none() == FAILURE) {
        return;
    }
    runHelperSelect(return_value, getThis(),
        "SELECT name FROM system.databases ORDER BY name", SC_FETCH_COLUMN);
}
/* }}} */

/* {{{ proto array showProcesslist()
 *
 * Projects a fixed set of common columns from system.processes
 * instead of `SELECT *`, because the wider table includes
 * Map(LowCardinality(String), ...) columns (ProfileEvents, Settings,
 * used_*) that our Map read path doesn't yet decode.
 */
CLICKHOUSE_METHOD(showProcesslist)
{
    if (zend_parse_parameters_none() == FAILURE) {
        return;
    }
    runHelperSelect(return_value, getThis(),
        "SELECT query_id, user, address, port, initial_user, initial_query_id, "
        "initial_address, interface, os_user, client_hostname, client_name, "
        "client_revision, client_version_major, client_version_minor, "
        "client_version_patch, http_method, http_user_agent, http_referer, "
        "forwarded_for, query, elapsed, read_rows, read_bytes, total_rows_approx, "
        "memory_usage, peak_memory_usage "
        "FROM system.processes", 0);
}
/* }}} */

/* {{{ proto string getServerVersion()
 */
CLICKHOUSE_METHOD(getServerVersion)
{
    if (zend_parse_parameters_none() == FAILURE) {
        return;
    }
    zval row;
    runHelperSelectFirstRow(&row, getThis(), "SELECT version() AS v");
    if (EG(exception)) return;
    if (Z_TYPE(row) == IS_ARRAY) {
        zval *v = zend_hash_str_find(Z_ARRVAL(row), "v", sizeof("v") - 1);
        if (v && Z_TYPE_P(v) == IS_STRING) {
            ZVAL_STR_COPY(return_value, Z_STR_P(v));
            zval_ptr_dtor(&row);
            return;
        }
    }
    zval_ptr_dtor(&row);
    RETURN_EMPTY_STRING();
}
/* }}} */

/* {{{ proto array tableSize(string table)
 *
 * Aggregate row/byte/partition count from system.parts for a single
 * table. Accepts `db.table` form. Returns the assoc row or an empty
 * array when the table has no active parts.
 */
CLICKHOUSE_METHOD(tableSize)
{
    zend_string *table = NULL;
    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STR(table)
    ZEND_PARSE_PARAMETERS_END();
    std::string tname(ZSTR_VAL(table), ZSTR_LEN(table));
    std::string dbname = currentDatabase(getThis());
    /* Same `db.table` split contract as partitions(): last dot wins,
     * residual dots rejected below. */
    auto dot = tname.rfind('.');
    if (dot != std::string::npos) {
        dbname = tname.substr(0, dot);
        tname = tname.substr(dot + 1);
    }
    try {
        validateMetadataFilterName(dbname, "database name");
        validateMetadataFilterName(tname, "table name");
        if (dbname.find('.') != std::string::npos || tname.find('.') != std::string::npos) {
            throw std::runtime_error("table argument must be `table` or `db.table` (at most one dot)");
        }
    } catch (const std::exception &e) {
        throwClickHouseError(e);
        return;
    }
    std::string sql =
        "SELECT sum(rows) AS rows, sum(bytes_on_disk) AS bytes_on_disk, "
        "uniqExact(partition) AS partitions, max(modification_time) AS modification_time "
        "FROM system.parts WHERE active AND database = " + sqlStringLiteral(dbname) +
        " AND table = " + sqlStringLiteral(tname) +
        " GROUP BY database, table";
    runHelperSelectFirstRow(return_value, getThis(), sql);
}
/* }}} */

/* {{{ proto bool truncateTable(string table)
 */
CLICKHOUSE_METHOD(truncateTable)
{
    zend_string *table = NULL;
    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STR(table)
    ZEND_PARSE_PARAMETERS_END();
    try {
        validateIdentifier(ZSTR_VAL(table), ZSTR_LEN(table), "table name", true);
    } catch (const std::exception &e) {
        throwClickHouseError(e);
        return;
    }
    std::string sql = "TRUNCATE TABLE " + std::string(ZSTR_VAL(table), ZSTR_LEN(table));
    RETURN_BOOL(runHelperExec(getThis(), sql));
}
/* }}} */

/* {{{ proto bool dropPartition(string table, string partition)
 *
 * Drop a partition by string value. The partition argument is always
 * single-quote-escaped and emitted as a SQL string literal, so dates
 * ('2024-01-01') and named partitions are safe by default. For
 * integer partitions or partition IDs, fall back to execute() with a
 * hand-built ALTER TABLE statement.
 */
CLICKHOUSE_METHOD(dropPartition)
{
    zend_string *table = NULL, *part = NULL;
    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_STR(table)
        Z_PARAM_STR(part)
    ZEND_PARSE_PARAMETERS_END();
    try {
        validateIdentifier(ZSTR_VAL(table), ZSTR_LEN(table), "table name", true);
    } catch (const std::exception &e) {
        throwClickHouseError(e);
        return;
    }
    for (size_t i = 0; i < ZSTR_LEN(part); ++i) {
        unsigned char c = (unsigned char)ZSTR_VAL(part)[i];
        if (c < 0x20) {
            zend_throw_exception(clickhouse_exception_ce,
                "dropPartition: partition value contains a control character", 0);
            return;
        }
    }
    std::string part_str(ZSTR_VAL(part), ZSTR_LEN(part));
    std::string sql = "ALTER TABLE " + std::string(ZSTR_VAL(table), ZSTR_LEN(table)) +
        " DROP PARTITION " + sqlStringLiteral(part_str);
    RETURN_BOOL(runHelperExec(getThis(), sql));
}
/* }}} */

/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: noet sw=4 ts=4 fdm=marker
 * vim<600: noet sw=4 ts=4
 */
