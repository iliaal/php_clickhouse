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

/* Include after PHP, clickhouse-cpp, and typesToPhp.hpp headers. */

#ifndef PHP_CLICKHOUSE_INTERNAL_H
#define PHP_CLICKHOUSE_INTERNAL_H

#include <chrono>
#include <deque>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

extern zend_class_entry *clickhouse_ce, *clickhouse_exception_ce, *clickhouse_iter_ce, *clickhouse_statement_ce;

struct ClientStats {
    uint64_t rows_read = 0;
    uint64_t bytes_read = 0;
    uint64_t total_rows = 0;
    uint64_t written_rows = 0;
    uint64_t written_bytes = 0;
    uint64_t blocks = 0;
    uint64_t rows_before_limit = 0;
    bool applied_limit = false;
    double elapsed_ms = 0.0;
    std::string last_query_id;
};
struct QueryLog {
    std::string sql;
    std::string query_id;
    double elapsed_ms = 0.0;
    uint64_t rows_read = 0;
    uint64_t bytes_read = 0;
    int error_code = 0;            // 0 = success; ServerException code on server failure; -1 on client/network failure
    std::string error_message;
};

/* std must be last. create_object/free_obj construct/destruct C++ members
 * explicitly because Zend allocates the object. */
struct clickhouse_object {
    clickhouse::Client *client;
    clickhouse::Block insert_block;
    bool has_insert_block;
    /* Reentrant queries would corrupt the outer call's single-socket packet loop. */
    bool query_active;
    ClientStats stats;
    std::unordered_map<std::string, std::string> settings;
    zval progress_callback;        // IS_UNDEF when unset
    zval profile_callback;         // IS_UNDEF when unset
    zval verbose_callback;         // IS_UNDEF when off or stderr-mode
    bool verbose_to_stderr;
    bool log_enabled;
    std::string insert_sql;
    std::string insert_query_id;
    std::chrono::steady_clock::time_point insert_started_at;
    /* O(1) eviction when the log reaches its cap. */
    std::deque<QueryLog> query_log;
    /* setDatabase() rebuilds from these options so the database survives
     * even internal RetryGuard reconnects that the extension cannot intercept. */
    clickhouse::ClientOptions client_options;
#if PHP_VERSION_ID < 80000
    /* PHP 7.4 has no zend_get_gc_buffer; get_gc must point *table at a
     * buffer that outlives the call. Three slots for the three callbacks. */
    zval gc_buf[3];
#endif
    zend_object std;
};

static inline clickhouse_object *clickhouse_from_obj(zend_object *obj)
{
    return (clickhouse_object *)((char *)obj - offsetof(clickhouse_object, std));
}

#define Z_CLICKHOUSE_P(zv) clickhouse_from_obj(Z_OBJ_P(zv))

/* A params key the query-log redactor cannot tell apart from typed
 * placeholder syntax; the logged template is redacted from its first brace. */
struct PlaceholderKeyError : std::runtime_error {
    using std::runtime_error::runtime_error;
};

std::string queryLogString(const std::string &value);
std::string redactSqlLiterals(const std::string &sql);
void appendQueryLogCapped(clickhouse_object *obj, QueryLog &&ql);
QueryLog buildQueryLog(const clickhouse_object *obj,
                       const std::string &sql, const std::string &qid,
                       bool distrust_braces = false);
void recordQuerySuccess(clickhouse_object *obj, const std::string &sql, const std::string &qid);
void recordQueryError(clickhouse_object *obj, const std::string &sql,
                      const std::string &qid, const std::exception &e);

std::string sanitizeError(const char *what);
void addAssocUInt64(zval *array, const char *key, uint64_t value);

std::string sqlStringLiteral(const std::string &s);
std::string sqlQuotedIdentifier(const std::string &s);
void parseIdentifier(const char *s, size_t len, const char *what,
                     bool allow_dot, std::string *emit_out);
void validateIdentifier(const char *s, size_t len, const char *what, bool allow_dot);
void validateMetadataFilterName(const std::string &s, const char *what);
/* Interned so that reading the property allocates nothing: connection
 * recovery reads it while a fatal error is pending, when any allocation can
 * raise another memory_limit error. */
extern zend_string *clickhouse_database_prop;
std::string currentDatabase(zval *this_obj);
void runHelperSelect(zval *return_value, zval *this_obj,
                     const std::string &sql, zend_long fetch_mode);
bool runHelperExec(zval *this_obj, const std::string &sql);
void runHelperSelectFirstRow(zval *return_value, zval *this_obj, const std::string &sql);
void throwClickHouseError(const std::exception &e, const std::string &query_id = std::string());
void do_select_into(zval *out, zval *this_obj,
                    const char *sql, size_t l_sql,
                    zval *params, zend_long fetch_mode,
                    const std::string &qid, zval *settings,
                    const clickhouse::ExternalTables *external_tables,
                    zval *positional_out);
void do_execute_into(zval *this_obj,
                     const char *sql, size_t l_sql,
                     zval *params, const std::string &qid, zval *settings);

/* Defines a ClickHouse method whose body runs between clickhouseMethodEnter()
 * and clickhouseMethodLeave(); the latter re-raises a bailout that PHP code or
 * a Zend allocation deferred until the body's C++ frames had unwound. */
struct clickhouse_method_state {
    int depth;
    uint64_t invocation;
    uint64_t outer;
};
void clickhouseMethodEnter(clickhouse_method_state *state);
void clickhouseMethodLeave(const clickhouse_method_state *state);
ZEND_NORETURN void clickhouseMethodBailout(const clickhouse_method_state *state);
#define CLICKHOUSE_CLASS_METHOD(cls, name) \
    static void clickhouse_method_##cls##_##name(INTERNAL_FUNCTION_PARAMETERS); \
    PHP_METHOD(cls, name) \
    { \
        clickhouse_method_state clickhouse_state; \
        clickhouseMethodEnter(&clickhouse_state); \
        zend_try { \
            try { \
                clickhouse_method_##cls##_##name(INTERNAL_FUNCTION_PARAM_PASSTHRU); \
            } catch (const PhpBailoutPending &) { \
            } \
        } zend_catch { \
            clickhouseMethodBailout(&clickhouse_state); \
        } zend_end_try(); \
        clickhouseMethodLeave(&clickhouse_state); \
    } \
    static void clickhouse_method_##cls##_##name(INTERNAL_FUNCTION_PARAMETERS)
#define CLICKHOUSE_METHOD(name) CLICKHOUSE_CLASS_METHOD(ClickHouse, name)

/* True when an operation failed: a PHP exception is set, a fatal error is
 * unwinding the enclosing ClickHouse method, or PHP deferred a memory_limit
 * error, which suppresses the exception the operation would throw. Callers
 * must not allocate PHP values in the first two cases. */
bool clickhouseBailoutPending();
bool clickhouseOomPending();
static inline bool clickhouseOperationFailed()
{
    return EG(exception) || clickhouseBailoutPending() || clickhouseOomPending();
}

#endif /* PHP_CLICKHOUSE_INTERNAL_H */

/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: noet sw=4 ts=4 fdm=marker
 * vim<600: noet sw=4 ts=4
 */
