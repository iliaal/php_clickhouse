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
#include "lib/clickhouse-cpp/clickhouse/exceptions.h"
#include "clickhouse_internal.h"

#include <algorithm>

using namespace clickhouse;
using namespace std;

/* Bound retained memory even when getLogQueries() never drains the log. */
#define CLICKHOUSE_QUERY_LOG_MAX 1024
#define CLICKHOUSE_QUERY_LOG_STRING_MAX_BYTES 8192

std::string queryLogString(const std::string &value)
{
    if (value.size() <= CLICKHOUSE_QUERY_LOG_STRING_MAX_BYTES) {
        return value;
    }
    static const char suffix[] = "... (truncated)";
    constexpr size_t suffix_len = sizeof(suffix) - 1;
    constexpr size_t prefix_len = CLICKHOUSE_QUERY_LOG_STRING_MAX_BYTES - suffix_len;
    std::string out(value.data(), prefix_len);
    out.append(suffix, suffix_len);
    return out;
}

/* Honor doubled quotes and backslash escapes; redact unterminated literals
 * through end of input. Used for logs and traces, never wire SQL. */
static std::string redactSqlLiteralsBounded(const std::string &sql, size_t max_bytes)
{
    std::string out;
    out.reserve(std::min(sql.size(), max_bytes));
    for (size_t i = 0; i < sql.size() && out.size() < max_bytes;) {
        if (sql[i] != '\'') {
            out.push_back(sql[i++]);
            continue;
        }
        out.append("'?'", std::min(size_t(3), max_bytes - out.size()));
        if (out.size() == max_bytes) break;
        ++i;
        while (i < sql.size()) {
            if (sql[i] == '\\' && i + 1 < sql.size()) {
                i += 2;
                continue;
            }
            if (sql[i] == '\'') {
                if (i + 1 < sql.size() && sql[i + 1] == '\'') {
                    i += 2;
                    continue;
                }
                ++i;
                break;
            }
            ++i;
        }
    }
    return out;
}

std::string redactSqlLiterals(const std::string &sql)
{
    return redactSqlLiteralsBounded(sql, std::string::npos);
}

void appendQueryLogCapped(clickhouse_object *obj, QueryLog &&ql)
{
    if (obj->query_log.size() >= CLICKHOUSE_QUERY_LOG_MAX) {
        obj->query_log.pop_front();
    }
    obj->query_log.push_back(std::move(ql));
}

QueryLog buildQueryLog(const clickhouse_object *obj,
                       const std::string &sql, const std::string &qid)
{
    QueryLog ql;
    /* One byte beyond the cap distinguishes an exact fit from truncation.
     * Do not allocate or scan an unbounded redacted suffix just to discard it.
     * The unrestricted helper remains available for verbose callbacks. */
    ql.sql = queryLogString(redactSqlLiteralsBounded(
        sql, CLICKHOUSE_QUERY_LOG_STRING_MAX_BYTES + 1));
    ql.query_id = queryLogString(qid);
    ql.elapsed_ms = obj->stats.elapsed_ms;
    ql.rows_read = obj->stats.rows_read;
    ql.bytes_read = obj->stats.bytes_read;
    return ql;
}

void recordQuerySuccess(clickhouse_object *obj, const std::string &sql, const std::string &qid)
{
    if (!obj->log_enabled) return;
    try {
        appendQueryLogCapped(obj, buildQueryLog(obj, sql, qid));
    } catch (...) { /* swallow; logging is best-effort */ }
}

void recordQueryError(clickhouse_object *obj, const std::string &sql, const std::string &qid, const std::exception &e)
{
    if (!obj->log_enabled) return;
    try {
        QueryLog ql = buildQueryLog(obj, sql, qid);
        if (auto se = dynamic_cast<const clickhouse::ServerException*>(&e)) {
            ql.error_code = se->GetException().code;
        } else {
            ql.error_code = -1;
        }
        ql.error_message = sanitizeError(e.what());
        appendQueryLogCapped(obj, std::move(ql));
    } catch (...) { /* swallow; logging must not throw from inside a catch */ }
}

/* {{{ proto bool enableLogQueries(bool enabled = true)
 *
 * Toggle the query log accumulator. While enabled, each completed
 * select / insert / execute / writeStart appends an entry. Toggling
 * off does NOT clear; getLogQueries() returns and clears.
 */
PHP_METHOD(ClickHouse, enableLogQueries)
{
    zend_bool enabled = 1;
    ZEND_PARSE_PARAMETERS_START(0, 1)
        Z_PARAM_OPTIONAL
        Z_PARAM_BOOL(enabled)
    ZEND_PARSE_PARAMETERS_END();
    clickhouse_object *obj = Z_CLICKHOUSE_P(getThis());
    obj->log_enabled = (enabled != 0);
    RETURN_TRUE;
}
/* }}} */

/* {{{ proto array getLogQueries()
 *
 * Return all accumulated query log entries and clear the buffer. Each
 * entry is an associative array: sql, query_id, elapsed_ms, rows_read,
 * bytes_read, error_code (0 = success, server code on server error,
 * -1 on client/network error), error_message.
 */
PHP_METHOD(ClickHouse, getLogQueries)
{
    if (zend_parse_parameters_none() == FAILURE) return;
    clickhouse_object *obj = Z_CLICKHOUSE_P(getThis());
    array_init(return_value);
    for (const auto &ql : obj->query_log) {
        zval entry;
        array_init(&entry);
        add_assoc_stringl(&entry, "sql", (char*)ql.sql.c_str(), ql.sql.size());
        add_assoc_stringl(&entry, "query_id", (char*)ql.query_id.c_str(), ql.query_id.size());
        add_assoc_double(&entry, "elapsed_ms", ql.elapsed_ms);
        addAssocUInt64(&entry, "rows_read", ql.rows_read);
        addAssocUInt64(&entry, "bytes_read", ql.bytes_read);
        add_assoc_long(&entry, "error_code", (zend_long)ql.error_code);
        add_assoc_stringl(&entry, "error_message",
            (char*)ql.error_message.c_str(), ql.error_message.size());
        add_next_index_zval(return_value, &entry);
    }
    obj->query_log.clear();
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
