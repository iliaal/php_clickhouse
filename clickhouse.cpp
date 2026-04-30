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
  | Original SeasClick author: SeasX Group <ahhhh.wang@gmail.com>        |
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
#include "Zend/zend_interfaces.h"
#include "Zend/zend_smart_str.h"
#include "ext/json/php_json.h"
#include "main/snprintf.h"  // php_gcvt: locale-independent double formatter
#include "php7_wrapper.h"
}

#include "php_clickhouse.h"

#include "lib/clickhouse-cpp/clickhouse/client.h"
#include "lib/clickhouse-cpp/clickhouse/error_codes.h"
#include "lib/clickhouse-cpp/clickhouse/exceptions.h"
#include "lib/clickhouse-cpp/clickhouse/types/type_parser.h"
#include "typesToPhp.hpp"
#include <chrono>
#include <deque>
#include <optional>
#include <unordered_map>

using namespace clickhouse;
using namespace std;

zend_class_entry *clickhouse_ce, *clickhouse_exception_ce, *clickhouse_iter_ce, *clickhouse_statement_ce;

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

/*
 * Per-Client state lives on the zend_object itself. Replaces the old
 * file-scope std::map<int, ...> bank keyed on Z_OBJ_HANDLE. Two
 * concrete benefits over the old layout: free_obj fires even on
 * bailout (so a fatal error mid-query no longer leaks the Client*),
 * and ZTS works naturally because there is no global state to
 * thread-isolate.
 *
 * The std member must be last; create_object placement-news the
 * non-POD members and free_obj placement-destructs them.
 */
struct clickhouse_object {
    Client *client;
    Block insert_block;
    bool has_insert_block;
    ClientStats stats;
    std::unordered_map<std::string, std::string> settings;
    zval progress_callback;        // IS_UNDEF when unset
    zval profile_callback;         // IS_UNDEF when unset
    zval verbose_callback;         // IS_UNDEF when off or stderr-mode
    bool verbose_to_stderr;        // true when setVerbose(true) was used
    bool log_enabled;
    /* deque, not vector, so the cap-overflow path's pop-front is O(1).
     * vector::erase(begin()) on a 1024-entry log shifted ~16 KB on
     * every query past the cap. */
    std::deque<QueryLog> query_log;
    zend_object std;
};

static inline clickhouse_object *clickhouse_from_obj(zend_object *obj)
{
    return (clickhouse_object *)((char *)obj - XtOffsetOf(clickhouse_object, std));
}

#define Z_CLICKHOUSE_P(zv) clickhouse_from_obj(Z_OBJ_P(zv))

static zend_object_handlers clickhouse_object_handlers;

static zend_object *clickhouse_create_object(zend_class_entry *ce)
{
    clickhouse_object *obj = (clickhouse_object *)zend_object_alloc(sizeof(clickhouse_object), ce);

    obj->client = nullptr;
    obj->has_insert_block = false;
    obj->log_enabled = false;
    obj->verbose_to_stderr = false;
    new (&obj->insert_block) Block();
    new (&obj->stats) ClientStats();
    new (&obj->settings) std::unordered_map<std::string, std::string>();
    new (&obj->query_log) std::deque<QueryLog>();
    ZVAL_UNDEF(&obj->progress_callback);
    ZVAL_UNDEF(&obj->profile_callback);
    ZVAL_UNDEF(&obj->verbose_callback);

    zend_object_std_init(&obj->std, ce);
    object_properties_init(&obj->std, ce);
    obj->std.handlers = &clickhouse_object_handlers;
    return &obj->std;
}

static void clickhouse_free_obj(zend_object *object)
{
    clickhouse_object *obj = clickhouse_from_obj(object);

    /* If a script left writeStart()/write() pending without writeEnd(),
     * close the insert stream first so the server doesn't see a half-open
     * transaction. Swallow errors: free_obj must not throw. */
    if (obj->client && obj->has_insert_block) {
        try { obj->client->EndInsert(); } catch (...) {}
    }

    if (obj->client) {
        delete obj->client;
        obj->client = nullptr;
    }

    if (Z_TYPE(obj->progress_callback) != IS_UNDEF) {
        zval_ptr_dtor(&obj->progress_callback);
        ZVAL_UNDEF(&obj->progress_callback);
    }
    if (Z_TYPE(obj->profile_callback) != IS_UNDEF) {
        zval_ptr_dtor(&obj->profile_callback);
        ZVAL_UNDEF(&obj->profile_callback);
    }
    if (Z_TYPE(obj->verbose_callback) != IS_UNDEF) {
        zval_ptr_dtor(&obj->verbose_callback);
        ZVAL_UNDEF(&obj->verbose_callback);
    }

    obj->insert_block.~Block();
    obj->stats.~ClientStats();
    obj->settings.~unordered_map<std::string, std::string>();
    obj->query_log.~deque<QueryLog>();

    zend_object_std_dtor(&obj->std);
}

/*
 * Streaming row iterator state. blocks accumulate via OnData during
 * selectStream(); the foreach loop walks them lazily without
 * materializing the full result set as a single PHP array.
 */
struct clickhouse_iter_object {
    std::vector<Block> blocks;
    /* Column names cached once on the first OnData callback. Result-set
     * schemas are stable across all blocks in a single query, so caching
     * once in the iterator avoids ~10M std::string heap allocs from
     * GetColumnName(col) on every current() call for million-row scans. */
    std::vector<std::string> column_names;
    size_t block_idx;
    size_t row_idx;
    uint64_t cumulative_row_idx;
    uint64_t total_rows;
    int fetch_mode;
    zend_object std;
};

static inline clickhouse_iter_object *clickhouse_iter_from_obj(zend_object *obj)
{
    return (clickhouse_iter_object *)((char *)obj - XtOffsetOf(clickhouse_iter_object, std));
}

#define Z_CLICKHOUSE_ITER_P(zv) clickhouse_iter_from_obj(Z_OBJ_P(zv))

static zend_object_handlers clickhouse_iter_object_handlers;

static zend_object *clickhouse_iter_create_object(zend_class_entry *ce)
{
    clickhouse_iter_object *iter = (clickhouse_iter_object *)zend_object_alloc(sizeof(clickhouse_iter_object), ce);

    iter->block_idx = 0;
    iter->row_idx = 0;
    iter->cumulative_row_idx = 0;
    iter->total_rows = 0;
    iter->fetch_mode = 0;
    new (&iter->blocks) std::vector<Block>();
    new (&iter->column_names) std::vector<std::string>();

    zend_object_std_init(&iter->std, ce);
    object_properties_init(&iter->std, ce);
    iter->std.handlers = &clickhouse_iter_object_handlers;
    return &iter->std;
}

static void clickhouse_iter_free_obj(zend_object *object)
{
    clickhouse_iter_object *iter = clickhouse_iter_from_obj(object);
    iter->blocks.~vector<Block>();
    iter->column_names.~vector<std::string>();
    zend_object_std_dtor(&iter->std);
}

/*
 * Materialized result wrapper, returned by selectStatement(). The rows
 * zval is a PHP array built once at construction time; iteration uses
 * the HashTable's internal pointer, so a single foreach is the supported
 * mode (nested foreach on the same Statement would fight over one
 * cursor). The statistics zval is a per-call snapshot of obj->stats at
 * the moment selectStatement returned, so callers can stash a Statement
 * across other queries without losing its stats.
 */
struct clickhouse_statement_object {
    zval rows;
    zval statistics;
    zend_object std;
};

static inline clickhouse_statement_object *clickhouse_statement_from_obj(zend_object *obj)
{
    return (clickhouse_statement_object *)((char *)obj - XtOffsetOf(clickhouse_statement_object, std));
}

#define Z_CLICKHOUSE_STATEMENT_P(zv) clickhouse_statement_from_obj(Z_OBJ_P(zv))

static zend_object_handlers clickhouse_statement_object_handlers;

static zend_object *clickhouse_statement_create_object(zend_class_entry *ce)
{
    clickhouse_statement_object *stmt = (clickhouse_statement_object *)zend_object_alloc(sizeof(clickhouse_statement_object), ce);
    ZVAL_UNDEF(&stmt->rows);
    ZVAL_UNDEF(&stmt->statistics);
    zend_object_std_init(&stmt->std, ce);
    object_properties_init(&stmt->std, ce);
    stmt->std.handlers = &clickhouse_statement_object_handlers;
    return &stmt->std;
}

static void clickhouse_statement_free_obj(zend_object *object)
{
    clickhouse_statement_object *stmt = clickhouse_statement_from_obj(object);
    zval_ptr_dtor(&stmt->rows);
    zval_ptr_dtor(&stmt->statistics);
    zend_object_std_dtor(&stmt->std);
}

static std::string sanitizeError(const char *what);
static void throwClickHouseError(const std::exception &e, const std::string &query_id = std::string());

#ifdef COMPILE_DL_CLICKHOUSE
extern "C" {
#ifdef ZTS
    ZEND_TSRMLS_CACHE_DEFINE()
#endif
    ZEND_GET_MODULE(clickhouse)
}
#endif

static PHP_METHOD(ClickHouse, __construct);
static PHP_METHOD(ClickHouse, __destruct);
static PHP_METHOD(ClickHouse, select);
static PHP_METHOD(ClickHouse, insert);
static PHP_METHOD(ClickHouse, insertAssoc);
static PHP_METHOD(ClickHouse, writeStart);
static PHP_METHOD(ClickHouse, write);
static PHP_METHOD(ClickHouse, writeEnd);
static PHP_METHOD(ClickHouse, execute);
static PHP_METHOD(ClickHouse, ping);
static PHP_METHOD(ClickHouse, setSettings);
static PHP_METHOD(ClickHouse, setSetting);
static PHP_METHOD(ClickHouse, setDatabase);
static PHP_METHOD(ClickHouse, setProgressCallback);
static PHP_METHOD(ClickHouse, setProfileCallback);
static PHP_METHOD(ClickHouse, setVerbose);
static PHP_METHOD(ClickHouse, resetConnection);
static PHP_METHOD(ClickHouse, getServerInfo);
static PHP_METHOD(ClickHouse, getCurrentEndpoint);
static PHP_METHOD(ClickHouse, getStatistics);
static PHP_METHOD(ClickHouse, databaseSize);
static PHP_METHOD(ClickHouse, tablesSize);
static PHP_METHOD(ClickHouse, partitions);
static PHP_METHOD(ClickHouse, showTables);
static PHP_METHOD(ClickHouse, showCreateTable);
static PHP_METHOD(ClickHouse, getServerUptime);
static PHP_METHOD(ClickHouse, enableLogQueries);
static PHP_METHOD(ClickHouse, getLogQueries);
static PHP_METHOD(ClickHouse, selectStream);
static PHP_METHOD(ClickHouse, selectStatement);
static PHP_METHOD(ClickHouse, selectStreamCallback);
static PHP_METHOD(ClickHouse, isExists);
static PHP_METHOD(ClickHouse, showDatabases);
static PHP_METHOD(ClickHouse, showProcesslist);
static PHP_METHOD(ClickHouse, getServerVersion);
static PHP_METHOD(ClickHouse, tableSize);
static PHP_METHOD(ClickHouse, truncateTable);
static PHP_METHOD(ClickHouse, dropPartition);

static PHP_METHOD(ClickHouseRowIterator, rewind);
static PHP_METHOD(ClickHouseRowIterator, valid);
static PHP_METHOD(ClickHouseRowIterator, current);
static PHP_METHOD(ClickHouseRowIterator, key);
static PHP_METHOD(ClickHouseRowIterator, next);
static PHP_METHOD(ClickHouseRowIterator, count);

static PHP_METHOD(ClickHouseException, getServerCode);
static PHP_METHOD(ClickHouseException, getServerName);
static PHP_METHOD(ClickHouseException, getQueryId);

static PHP_METHOD(ClickHouseStatement, __construct);
static PHP_METHOD(ClickHouseStatement, count);
static PHP_METHOD(ClickHouseStatement, rewind);
static PHP_METHOD(ClickHouseStatement, valid);
static PHP_METHOD(ClickHouseStatement, current);
static PHP_METHOD(ClickHouseStatement, key);
static PHP_METHOD(ClickHouseStatement, next);
static PHP_METHOD(ClickHouseStatement, offsetExists);
static PHP_METHOD(ClickHouseStatement, offsetGet);
static PHP_METHOD(ClickHouseStatement, offsetSet);
static PHP_METHOD(ClickHouseStatement, offsetUnset);
static PHP_METHOD(ClickHouseStatement, jsonSerialize);
static PHP_METHOD(ClickHouseStatement, toArray);
static PHP_METHOD(ClickHouseStatement, statistics);
static PHP_METHOD(ClickHouseStatement, fetchOne);
static PHP_METHOD(ClickHouseStatement, fetchKeyPair);
static PHP_METHOD(ClickHouseStatement, fetchColumn);

#include "clickhouse_arginfo.h"

/* {{{ clickhouse_functions[] */
const zend_function_entry clickhouse_functions[] =
{
    PHP_FE_END
};
/* }}} */

/* {{{ PHP_MINIT_FUNCTION
 */
PHP_MINIT_FUNCTION(clickhouse)
{
#if defined(COMPILE_DL_CLICKHOUSE) && defined(ZTS)
    ZEND_TSRMLS_CACHE_UPDATE();
#endif

    clickhouse_ce = register_class_ClickHouse();
#if PHP_VERSION_ID >= 80100
    clickhouse_ce->ce_flags |= ZEND_ACC_NOT_SERIALIZABLE;
#endif
    clickhouse_ce->create_object = clickhouse_create_object;
#if PHP_VERSION_ID >= 80400
    clickhouse_ce->default_object_handlers = &clickhouse_object_handlers;
#endif

    memcpy(&clickhouse_object_handlers, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
    clickhouse_object_handlers.offset = XtOffsetOf(clickhouse_object, std);
    clickhouse_object_handlers.free_obj = clickhouse_free_obj;

    clickhouse_exception_ce = register_class_ClickHouseException(zend_ce_exception);

    clickhouse_iter_ce = register_class_ClickHouseRowIterator(zend_ce_iterator, zend_ce_countable);
#if PHP_VERSION_ID >= 80100
    clickhouse_iter_ce->ce_flags |= ZEND_ACC_NOT_SERIALIZABLE;
#endif
    clickhouse_iter_ce->create_object = clickhouse_iter_create_object;
#if PHP_VERSION_ID >= 80400
    clickhouse_iter_ce->default_object_handlers = &clickhouse_iter_object_handlers;
#endif

    memcpy(&clickhouse_iter_object_handlers, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
    clickhouse_iter_object_handlers.offset = XtOffsetOf(clickhouse_iter_object, std);
    clickhouse_iter_object_handlers.free_obj = clickhouse_iter_free_obj;

    clickhouse_statement_ce = register_class_ClickHouseStatement(zend_ce_iterator, zend_ce_countable, zend_ce_arrayaccess, php_json_serializable_ce);
#if PHP_VERSION_ID >= 80100
    clickhouse_statement_ce->ce_flags |= ZEND_ACC_NOT_SERIALIZABLE;
#endif
    clickhouse_statement_ce->create_object = clickhouse_statement_create_object;
#if PHP_VERSION_ID >= 80400
    clickhouse_statement_ce->default_object_handlers = &clickhouse_statement_object_handlers;
#endif

    memcpy(&clickhouse_statement_object_handlers, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
    clickhouse_statement_object_handlers.offset = XtOffsetOf(clickhouse_statement_object, std);
    clickhouse_statement_object_handlers.free_obj = clickhouse_statement_free_obj;

    /* Back-compat aliases for the original SeasClick name. Deprecated;
     * removed in the next major release. */
    zend_register_class_alias(CLICKHOUSE_RES_NAME_LEGACY, clickhouse_ce);
    zend_register_class_alias(CLICKHOUSE_EXCEPTION_NAME_LEGACY, clickhouse_exception_ce);

    return SUCCESS;
}
/* }}} */

/* {{{ PHP_MINFO_FUNCTION
 */
PHP_MINFO_FUNCTION(clickhouse)
{
    php_info_print_table_start();
    php_info_print_table_header(2, "ClickHouse support", "enabled");
    php_info_print_table_row(2, "Version", PHP_CLICKHOUSE_VERSION);
    php_info_print_table_row(2, "Author", "SeasX Group[email: ahhhh.wang@gmail.com], Ilia Alshanetsky");
    php_info_print_table_end();

    DISPLAY_INI_ENTRIES();
}
/* }}} */

/* {{{ clickhouse_module_entry
 */
zend_module_entry clickhouse_module_entry =
{
    STANDARD_MODULE_HEADER,
    CLICKHOUSE_RES_NAME,
    clickhouse_functions,
    PHP_MINIT(clickhouse),
    NULL,
    NULL,
    NULL,
    PHP_MINFO(clickhouse),
    PHP_CLICKHOUSE_VERSION,
    STANDARD_MODULE_PROPERTIES
};
/* }}} */

/* {{{ proto object __construct(array connectParams)
 */
PHP_METHOD(ClickHouse, __construct)
{
    zval *connectParams;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_ARRAY(connectParams)
    ZEND_PARSE_PARAMETERS_END();

    HashTable *_ht = Z_ARRVAL_P(connectParams);
    zval *value;

    zval *this_obj;
    this_obj = getThis();
    if (php_array_get_value(_ht, "host", value))
    {
        ZStrGuard sg(value);
        sc_zend_update_property_stringl(clickhouse_ce, this_obj, "host", sizeof("host") - 1,
                                        sg.val(), sg.len());
    }

    if (php_array_get_value(_ht, "port", value))
    {
        zend_long _p = zval_get_long(value);
        if (_p < 1 || _p > 65535) {
            sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce,
                "port out of 1..65535 range", 0);
            return;
        }
        sc_zend_update_property_long(clickhouse_ce, this_obj, "port", sizeof("port") - 1, _p);
    }

    if (php_array_get_value(_ht, "compression", value))
    {
        long cv = 0;
        if (Z_TYPE_P(value) == IS_STRING) {
            const char *s = Z_STRVAL_P(value);
            if (strcasecmp(s, "lz4") == 0)        cv = 1;
            else if (strcasecmp(s, "zstd") == 0)  cv = 2;
            else if (strcasecmp(s, "none") == 0)  cv = 0;
            else {
                sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce,
                    "Unknown compression name; expected 'lz4', 'zstd', 'none', true, or false", 0);
                return;
            }
        } else {
            cv = zend_is_true(value) ? 1 : 0;
        }
        sc_zend_update_property_long(clickhouse_ce, this_obj, "compression", sizeof("compression") - 1, cv);
    }

    /* The seconds-based timeouts and retry counters all flow into
     * std::chrono::seconds(...) or unsigned setters in clickhouse-cpp.
     * A negative value silently wraps to a giant unsigned through
     * SetSendRetries / SetTcpKeepAlive*; reject up front. */
    /* php_array_get_value is a string-literal-only macro (it uses
     * sizeof(str)-1 for the key length) so it can't be passed a const
     * char* runtime key. The lambda goes through sc_zend_hash_find. */
    auto load_nonneg_long = [&](const char *key, zend_long &out) -> bool {
        zval *v = sc_zend_hash_find(_ht, (char*)key, strlen(key));
        if (!v || ZVAL_IS_NULL(v)) return false;
        zend_long n = zval_get_long(v);
        if (n < 0) {
            std::string msg = std::string(key) + " must be >= 0";
            sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce, msg.c_str(), 0);
            return false; // caller checks EG(exception)
        }
        out = n;
        return true;
    };

    {
        zend_long n;
        if (load_nonneg_long("retry_timeout", n)) {
            sc_zend_update_property_long(clickhouse_ce, this_obj, "retry_timeout", sizeof("retry_timeout") - 1, n);
        } else if (EG(exception)) { return; }
    }
    {
        zend_long n;
        if (load_nonneg_long("retry_count", n)) {
            sc_zend_update_property_long(clickhouse_ce, this_obj, "retry_count", sizeof("retry_count") - 1, n);
        } else if (EG(exception)) { return; }
    }
    {
        zend_long n;
        if (load_nonneg_long("connect_timeout", n)) {
            sc_zend_update_property_long(clickhouse_ce, this_obj, "connect_timeout", sizeof("connect_timeout") - 1, n);
        } else if (EG(exception)) { return; }
    }
    {
        zend_long n;
        if (load_nonneg_long("receive_timeout", n)) {
            sc_zend_update_property_long(clickhouse_ce, this_obj, "receive_timeout", sizeof("receive_timeout") - 1, n);
        } else if (EG(exception)) { return; }
    }

    zval *host = sc_zend_read_property(clickhouse_ce, this_obj, "host", sizeof("host") - 1, 0);
    zval *port = sc_zend_read_property(clickhouse_ce, this_obj, "port", sizeof("port") - 1, 0);
    zval *compression = sc_zend_read_property(clickhouse_ce, this_obj, "compression", sizeof("compression") - 1, 0);
    zval *retry_timeout = sc_zend_read_property(clickhouse_ce, this_obj, "retry_timeout", sizeof("retry_timeout") - 1, 0);
    zval *retry_count = sc_zend_read_property(clickhouse_ce, this_obj, "retry_count", sizeof("retry_count") - 1, 0);
    zval *receive_timeout = sc_zend_read_property(clickhouse_ce, this_obj, "receive_timeout", sizeof("receive_timeout") - 1, 0);
    zval *connect_timeout = sc_zend_read_property(clickhouse_ce, this_obj, "connect_timeout", sizeof("connect_timeout") - 1, 0);

    ClientOptions Options = ClientOptions()
                            .SetHost(std::string(Z_STRVAL_P(host), Z_STRLEN_P(host)))
                            .SetPort((uint16_t)Z_LVAL_P(port))
                            .SetSendRetries(Z_LVAL_P(retry_count))
                            .SetRetryTimeout(std::chrono::seconds(Z_LVAL_P(retry_timeout)))
                            .SetConnectionRecvTimeout(std::chrono::seconds(Z_LVAL_P(receive_timeout)))
                            .SetConnectionConnectTimeout(std::chrono::seconds(Z_LVAL_P(connect_timeout)))
                            .SetPingBeforeQuery(false);
    long cv = Z_LVAL_P(compression);
    if (cv == 1) Options = Options.SetCompressionMethod(CompressionMethod::LZ4);
    else if (cv == 2) Options = Options.SetCompressionMethod(CompressionMethod::ZSTD);

    if (php_array_get_value(_ht, "send_timeout", value)) {
        zend_long n = zval_get_long(value);
        if (n < 0) {
            sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce,
                "send_timeout must be >= 0", 0);
            return;
        }
        Options = Options.SetConnectionSendTimeout(std::chrono::seconds(n));
    }
    /* Millisecond variants override the seconds-based keys. Useful when
     * sub-second precision matters (CI test guards, low-latency hops). */
    auto apply_timeout_ms = [&](const char *key,
                                 ClientOptions& (ClientOptions::*setter)(const std::chrono::milliseconds&)) -> bool {
        zval *v = sc_zend_hash_find(_ht, (char*)key, strlen(key));
        if (!v || ZVAL_IS_NULL(v)) return true;
        zend_long n = zval_get_long(v);
        if (n < 0) {
            std::string msg = std::string(key) + " must be >= 0";
            sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce, msg.c_str(), 0);
            return false;
        }
        Options = (Options.*setter)(std::chrono::milliseconds(n));
        return true;
    };
    if (!apply_timeout_ms("connect_timeout_ms", &ClientOptions::SetConnectionConnectTimeout)) return;
    if (!apply_timeout_ms("receive_timeout_ms", &ClientOptions::SetConnectionRecvTimeout))    return;
    if (!apply_timeout_ms("send_timeout_ms",    &ClientOptions::SetConnectionSendTimeout))   return;
    if (php_array_get_value(_ht, "tcp_nodelay", value)) {
        Options = Options.TcpNoDelay(zend_is_true(value));
    }
    if (php_array_get_value(_ht, "tcp_keepalive", value)) {
        Options = Options.TcpKeepAlive(zend_is_true(value));
    }
    if (php_array_get_value(_ht, "ping_before_query", value)) {
        Options = Options.SetPingBeforeQuery(zend_is_true(value));
    }
    if (php_array_get_value(_ht, "tcp_keepalive_idle", value)) {
        zend_long n = zval_get_long(value);
        if (n < 0) {
            sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce,
                "tcp_keepalive_idle must be >= 0", 0);
            return;
        }
        Options = Options.SetTcpKeepAliveIdle(std::chrono::seconds(n));
    }
    if (php_array_get_value(_ht, "tcp_keepalive_intvl", value)) {
        zend_long n = zval_get_long(value);
        if (n < 0) {
            sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce,
                "tcp_keepalive_intvl must be >= 0", 0);
            return;
        }
        Options = Options.SetTcpKeepAliveInterval(std::chrono::seconds(n));
    }
    if (php_array_get_value(_ht, "tcp_keepalive_cnt", value)) {
        zend_long n = zval_get_long(value);
        if (n < 0 || n > UINT_MAX) {
            sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce,
                "tcp_keepalive_cnt out of range", 0);
            return;
        }
        Options = Options.SetTcpKeepAliveCount((unsigned int)n);
    }
    if (php_array_get_value(_ht, "max_compression_chunk_size", value)) {
        zend_long n = zval_get_long(value);
        if (n < 0 || n > UINT_MAX) {
            sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce,
                "max_compression_chunk_size out of range", 0);
            return;
        }
        Options = Options.SetMaxCompressionChunkSize((unsigned int)n);
    }
#ifdef WITH_OPENSSL
    bool want_ssl = false;
    if (php_array_get_value(_ht, "ssl", value)) {
        want_ssl = zend_is_true(value);
    }
    if (want_ssl) {
        ClientOptions::SSLOptions ssl_opts;
        // Default to TLS 1.2 minimum so a server speaking only 1.0 / 1.1
        // is rejected without the caller having to remember to set this.
        // Caller can override via ssl_min_protocol_version.
        ssl_opts.SetMinProtocolVersion(0x0303);
        if (php_array_get_value(_ht, "ssl_min_protocol_version", value)) {
            static const struct { const char *name; int version; } tls_versions[] = {
                {"tls1.0", 0x0301}, {"tls1.1", 0x0302},
                {"tls1.2", 0x0303}, {"tls1.3", 0x0304},
            };
            int ver = 0;
            {
                ZStrGuard sg(value);
                for (const auto &tv : tls_versions) {
                    if (strcasecmp(sg.val(), tv.name) == 0) {
                        ver = tv.version;
                        break;
                    }
                }
            }
            if (ver == 0) {
                sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce,
                    "ssl_min_protocol_version must be one of tls1.0, tls1.1, tls1.2, tls1.3", 0);
                return;
            }
            ssl_opts.SetMinProtocolVersion(ver);
        }
        if (php_array_get_value(_ht, "ssl_skip_verify", value)) {
            ssl_opts.SetSkipVerification(zend_is_true(value));
        }
        if (php_array_get_value(_ht, "ssl_use_default_ca", value)) {
            ssl_opts.SetUseDefaultCALocations(zend_is_true(value));
        }
        if (php_array_get_value(_ht, "ssl_ca_directory", value)) {
            ZStrGuard sg(value);
            ssl_opts.SetPathToCADirectory(std::string(sg.val(), sg.len()));
        }
        if (php_array_get_value(_ht, "ssl_ca_files", value)) {
            std::vector<std::string> files;
            if (Z_TYPE_P(value) == IS_STRING) {
                files.emplace_back(Z_STRVAL_P(value), Z_STRLEN_P(value));
            } else if (Z_TYPE_P(value) == IS_ARRAY) {
                HashTable *fh = Z_ARRVAL_P(value);
                zval *fv;
                ZEND_HASH_FOREACH_VAL(fh, fv) {
                    ZStrGuard sg(fv);
                    files.emplace_back(sg.val(), sg.len());
                } ZEND_HASH_FOREACH_END();
            }
            ssl_opts.SetPathToCAFiles(files);
        }
        Options = Options.SetSSLOptions(ssl_opts);
    }
#else
    if (php_array_get_value(_ht, "ssl", value)) {
        if (zend_is_true(value)) {
            sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce,
                "php_clickhouse was built without TLS support. Reconfigure with --enable-clickhouse-openssl",
                0);
            return;
        }
    }
#endif

    if (php_array_get_value(_ht, "endpoints", value) && Z_TYPE_P(value) == IS_ARRAY) {
        std::vector<Endpoint> eps;
        HashTable *eps_ht = Z_ARRVAL_P(value);
        zval *ep_zv;
        ZEND_HASH_FOREACH_VAL(eps_ht, ep_zv) {
            if (Z_TYPE_P(ep_zv) != IS_ARRAY) continue;
            HashTable *eh = Z_ARRVAL_P(ep_zv);
            zval *hz = sc_zend_hash_find(eh, (char*)"host", 4);
            zval *pz = sc_zend_hash_find(eh, (char*)"port", 4);
            if (!hz) continue;
            Endpoint e;
            {
                ZStrGuard host_sg(hz);
                e.host = std::string(host_sg.val(), host_sg.len());
            }
            if (pz) {
                zend_long p = zval_get_long(pz);
                if (p < 1 || p > 65535) {
                    sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce,
                        "Endpoint port out of 1..65535 range", 0);
                    return;
                }
                e.port = (uint16_t)p;
            }
            eps.push_back(std::move(e));
        } ZEND_HASH_FOREACH_END();
        if (!eps.empty()) Options = Options.SetEndpoints(eps);
    }

    if (php_array_get_value(_ht, "database", value))
    {
        ZStrGuard sg(value);
        sc_zend_update_property_stringl(clickhouse_ce, this_obj, "database", sizeof("database") - 1,
                                        sg.val(), sg.len());
        Options = Options.SetDefaultDatabase(std::string(sg.val(), sg.len()));
    }

    if (php_array_get_value(_ht, "user", value))
    {
        ZStrGuard sg(value);
        sc_zend_update_property_stringl(clickhouse_ce, this_obj, "user", sizeof("user") - 1,
                                        sg.val(), sg.len());
        Options = Options.SetUser(std::string(sg.val(), sg.len()));
    }

    if (php_array_get_value(_ht, "passwd", value))
    {
        ZStrGuard sg(value);
        Options = Options.SetPassword(std::string(sg.val(), sg.len()));
    }

    try
    {
        clickhouse_object *obj = Z_CLICKHOUSE_P(this_obj);
        if (obj->client) {
            throw std::runtime_error("ClickHouse object is already constructed");
        }
        obj->client = new Client(Options);
    }
    catch (const std::exception& e)
    {
        throwClickHouseError(e, std::string());
        return;
    }

    RETURN_TRUE;
}
/* }}} */

/*
 * Permit identifier chars only: ASCII letter/digit/underscore, plus a
 * single dot for the optional database prefix on a table name. Length
 * must be > 0 and the first character of each segment must be a letter
 * or underscore. Rejects anything else, including the empty string and
 * any quoting characters that could break out of the INSERT statement.
 */
static void validateIdentifier(const char *s, size_t len, const char *what, bool allow_dot)
{
    if (len == 0) {
        throw std::runtime_error(std::string(what) + " must not be empty");
    }
    bool seg_start = true;
    bool dot_seen = false;
    for (size_t i = 0; i < len; ++i) {
        unsigned char c = (unsigned char)s[i];
        if (seg_start) {
            if (!((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_')) {
                throw std::runtime_error(
                    std::string(what) + " must start with a letter or underscore");
            }
            seg_start = false;
            continue;
        }
        if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
            (c >= '0' && c <= '9') || c == '_') {
            continue;
        }
        if (allow_dot && c == '.' && !dot_seen) {
            dot_seen = true;
            seg_start = true;
            continue;
        }
        throw std::runtime_error(
            std::string(what) + " contains an invalid character");
    }
    if (seg_start) {
        throw std::runtime_error(std::string(what) + " has an empty segment");
    }
}

// Convert an optional zend_string * (often the query_id PHP_METHOD
// arg) into a std::string, treating NULL and empty as "no qid".
static inline std::string makeQid(zend_string *s)
{
    return (s && ZSTR_LEN(s) > 0) ? std::string(ZSTR_VAL(s), ZSTR_LEN(s)) : std::string();
}

// Max bytes of an exception message that crosses into userland. Bigger
// than 1024 because real ClickHouse errors with stack hints can run
// long, smaller than the few KB that would let a verbose dump leak.
#define CLICKHOUSE_ERROR_MAX_LEN 4096

/*
 * Strip the embedded SQL fragment from a clickhouse-cpp error message
 * before it crosses into userland. Upstream typically appends the full
 * failing query after a "While executing" / "in query" prefix, which
 * leaks any literal a caller placed in a placeholder (passwords with
 * digits-only values still pass our placeholder validator). Cap length
 * at CLICKHOUSE_ERROR_MAX_LEN as a final defense.
 */
static std::string sanitizeError(const char *what)
{
    std::string msg(what ? what : "");
    static const char *sql_markers[] = {
        "While executing",
        "in query: ",
        "While processing",
    };
    for (const char *marker : sql_markers) {
        std::string::size_type pos = msg.find(marker);
        if (pos != std::string::npos) {
            msg.erase(pos);
            // Drop trailing whitespace/punct left from the cut.
            while (!msg.empty() && (msg.back() == ' ' || msg.back() == ',' ||
                                     msg.back() == ':' || msg.back() == '.')) {
                msg.pop_back();
            }
            break;
        }
    }
    if (msg.size() > CLICKHOUSE_ERROR_MAX_LEN) {
        /* Reserve space for the suffix so the final string respects
         * the documented cap rather than exceeding it by 15 bytes. */
        static const char suffix[] = "... (truncated)";
        constexpr size_t suffix_len = sizeof(suffix) - 1;
        msg.resize(CLICKHOUSE_ERROR_MAX_LEN - suffix_len);
        msg.append(suffix, suffix_len);
    }
    return msg;
}

/*
 * Resolve the Client* on the given object, or throw if __construct
 * never finished installing one (failed connect, or a method called on
 * a half-built object).
 */
static Client* getClient(clickhouse_object *obj)
{
    if (!obj->client) {
        throw std::runtime_error("ClickHouse client is not initialized");
    }
    return obj->client;
}

/*
 * Central thrower. Replaces every catch-block sc_zend_throw call so the
 * server fields land on the exception in one place. ServerException is
 * the only branch that knows the server's error code and name; every
 * other exception (network, validation, ours) leaves the structured
 * fields at their MINIT defaults.
 */
static void throwClickHouseError(const std::exception &e, const std::string &query_id)
{
    /* Preserve a PHP exception that was already raised (e.g. from inside a
     * user-supplied progress/profile/verbose callback that we re-raised as a
     * sentinel C++ throw). Overwriting it would drop the user's stack and
     * leave them with our generic "callback aborted" wrapper. */
    if (EG(exception)) {
        return;
    }
    std::string msg = sanitizeError(e.what());
    zval ex;
    object_init_ex(&ex, clickhouse_exception_ce);

    if (auto se = dynamic_cast<const clickhouse::ServerException*>(&e)) {
        const clickhouse::Exception &exc = se->GetException();
        sc_zend_update_property_long(clickhouse_exception_ce, &ex, "server_code", sizeof("server_code") - 1, (zend_long)exc.code);
        if (!exc.name.empty()) {
            sc_zend_update_property_stringl(clickhouse_exception_ce, &ex, "server_name", sizeof("server_name") - 1, exc.name.c_str(), exc.name.size());
        }
    }
    if (!query_id.empty()) {
        sc_zend_update_property_stringl(clickhouse_exception_ce, &ex, "query_id", sizeof("query_id") - 1, query_id.c_str(), query_id.size());
    }
    sc_zend_update_property_stringl(clickhouse_exception_ce, &ex, "message", sizeof("message") - 1, msg.c_str(), msg.size());
    zend_throw_exception_object(&ex);
}

/*
 * Coerce a PHP zval into the string format ClickHouse expects for
 * server-side parameter values. Matches the textual format the server
 * parses for {name:Type} placeholders. Strings/dates/scalars pass
 * through verbatim (the wire layer adds the surrounding quotes); arrays
 * are formatted as ClickHouse array literals so Array(T) parses cleanly.
 */
static std::string formatParamValue(zval *v, const std::string &type, bool inside_array);

static std::string formatScalarParam(zval *v)
{
    switch (Z_TYPE_P(v)) {
        case IS_NULL:
            return std::string();
        case IS_TRUE:
            return std::string("true");
        case IS_FALSE:
            return std::string("false");
        case IS_LONG: {
            char buf[32];
            int n = snprintf(buf, sizeof(buf), ZEND_LONG_FMT, Z_LVAL_P(v));
            return std::string(buf, (n > 0 && (size_t)n < sizeof(buf)) ? (size_t)n : 0);
        }
        case IS_DOUBLE: {
            /* snprintf("%g") honors LC_NUMERIC, so a PHP user calling
             * setlocale(LC_NUMERIC, 'de_DE') would emit "1,5" on the
             * wire and the ClickHouse server would reject the typed
             * parameter or setting value. php_gcvt takes the decimal
             * separator and exponent char explicitly and is locale-
             * independent, which is what we need at the SQL boundary.
             * 17 significant digits is the IEEE 754 round-trip bound
             * for double, matching the prior %.17g behavior. Buffer
             * needs ~25 bytes worst case; 64 leaves comfortable margin. */
            char buf[64];
            php_gcvt(Z_DVAL_P(v), 17, '.', 'e', buf);
            return std::string(buf);
        }
        default: {
            zend_string *coerced = zval_get_string(v);
            std::string out(ZSTR_VAL(coerced), ZSTR_LEN(coerced));
            zend_string_release(coerced);
            return out;
        }
    }
}

static bool typeNeedsQuoting(const std::string &t)
{
    /* Inner type for an Array(T) typed param. Numeric and bool parse
     * raw; everything else needs single-quotes around each element.
     * Lengths are baked in so we don't strlen each compile-time literal
     * on every dispatch. */
    struct BareType { const char *prefix; size_t len; };
    static constexpr BareType bare[] = {
        {"Int",     3},
        {"UInt",    4},
        {"Float",   5},
        {"Decimal", 7},
        {"Bool",    4},
    };
    for (const auto &b : bare) {
        if (t.compare(0, b.len, b.prefix) == 0) return false;
    }
    return true;
}

static std::string formatParamValue(zval *v, const std::string &type, bool inside_array)
{
    if (Z_TYPE_P(v) == IS_NULL) {
        return std::string();
    }

    if (Z_TYPE_P(v) == IS_ARRAY) {
        std::string inner;
        if (type.compare(0, 6, "Array(") == 0 && type.back() == ')') {
            inner = type.substr(6, type.size() - 7);
        }
        bool quote = inner.empty() ? true : typeNeedsQuoting(inner);
        std::string out = "[";
        bool first = true;
        zval *iv;
        ZEND_HASH_FOREACH_VAL(Z_ARRVAL_P(v), iv) {
            if (!first) out += ",";
            first = false;
            std::string sv = formatScalarParam(iv);
            if (quote) {
                std::string esc;
                esc.reserve(sv.size() + 2);
                esc += "'";
                for (char c : sv) {
                    if (c == '\'' || c == '\\') esc += '\\';
                    esc += c;
                }
                esc += "'";
                out += esc;
            } else {
                out += sv;
            }
        } ZEND_HASH_FOREACH_END();
        out += "]";
        return out;
    }

    /* Scalar inside an Array goes through the same path; the caller is
     * responsible for the surrounding quotes. The wire layer quotes the
     * outer value for non-array typed params. */
    (void)inside_array;
    return formatScalarParam(v);
}

/*
 * Apply the global setSettings map merged with a per-call settings array
 * onto a Query object via Query::SetSetting. Per-call settings override
 * global. Empty per-call array means "use global only".
 */
static void applyMergedSettings(Query &q, clickhouse_object *obj, zval *per_call)
{
    bool have_per_call = (per_call != NULL && Z_TYPE_P(per_call) == IS_ARRAY
                          && zend_hash_num_elements(Z_ARRVAL_P(per_call)) > 0);

    /* Common case: no per-call overrides. Iterate the global map directly
     * instead of paying for a full unordered_map copy on every query. */
    if (!have_per_call) {
        for (const auto &kv : obj->settings) {
            QuerySettingsField f;
            f.value = kv.second;
            f.flags = 0;
            q.SetSetting(kv.first, f);
        }
        return;
    }

    std::unordered_map<std::string, std::string> merged = obj->settings;
    HashTable *ht = Z_ARRVAL_P(per_call);
    zval *vz;
    zend_string *zk;
    zend_ulong nk;
    ZEND_HASH_FOREACH_KEY_VAL(ht, nk, zk, vz) {
        (void)nk;
        if (!zk) continue;
        std::string sval = formatScalarParam(vz);
        merged[std::string(ZSTR_VAL(zk), ZSTR_LEN(zk))] = sval;
    } ZEND_HASH_FOREACH_END();
    for (const auto &kv : merged) {
        QuerySettingsField f;
        f.value = kv.second;
        f.flags = 0;
        q.SetSetting(kv.first, f);
    }
}

/*
 * Wire OnProgress and OnProfile to (a) populate the per-object stats
 * struct and (b) forward to the user's PHP progress callback if one is
 * registered. Stats reset happens at query start in the caller.
 */
static void attachProgressAndProfile(Query &q, clickhouse_object *obj)
{
    q.OnProgress([obj](const Progress &p) {
        ClientStats &st = obj->stats;
        st.rows_read += p.rows;
        st.bytes_read += p.bytes;
        if (p.total_rows > st.total_rows) st.total_rows = p.total_rows;
        st.written_rows += p.written_rows;
        st.written_bytes += p.written_bytes;

        if (Z_TYPE(obj->progress_callback) != IS_UNDEF) {
            zval args[1], retval;
            ZVAL_NULL(&retval);
            array_init(&args[0]);
            add_assoc_long(&args[0], "rows", (zend_long)p.rows);
            add_assoc_long(&args[0], "bytes", (zend_long)p.bytes);
            add_assoc_long(&args[0], "total_rows", (zend_long)p.total_rows);
            add_assoc_long(&args[0], "written_rows", (zend_long)p.written_rows);
            add_assoc_long(&args[0], "written_bytes", (zend_long)p.written_bytes);
            call_user_function(NULL, NULL, &obj->progress_callback, &retval, 1, args);
            zval_ptr_dtor(&args[0]);
            zval_ptr_dtor(&retval);
            /* If the user callback raised, propagate to the packet loop
             * so subsequent OnData/OnProgress callbacks don't run and the
             * outer try-catch in do_select_into surfaces the user's
             * exception (preserved by throwClickHouseError). */
            if (EG(exception)) {
                throw std::runtime_error("progress callback aborted query");
            }
        }
    });
    q.OnProfile([obj](const Profile &pr) {
        ClientStats &st = obj->stats;
        st.blocks = pr.blocks;
        if (pr.calculated_rows_before_limit) {
            st.rows_before_limit = pr.rows_before_limit;
            st.applied_limit = pr.applied_limit;
        }
        /* Profile.bytes is bytes processed server-side; merge as a
         * floor so we never report less than what Progress saw. */
        if (pr.bytes > st.bytes_read) st.bytes_read = pr.bytes;
        if (pr.rows > st.rows_read) st.rows_read = pr.rows;

        if (Z_TYPE(obj->profile_callback) != IS_UNDEF) {
            zval args[1], retval;
            ZVAL_NULL(&retval);
            array_init(&args[0]);
            add_assoc_long(&args[0], "rows", (zend_long)pr.rows);
            add_assoc_long(&args[0], "blocks", (zend_long)pr.blocks);
            add_assoc_long(&args[0], "bytes", (zend_long)pr.bytes);
            add_assoc_long(&args[0], "rows_before_limit", (zend_long)pr.rows_before_limit);
            add_assoc_bool(&args[0], "applied_limit", pr.applied_limit ? 1 : 0);
            add_assoc_bool(&args[0], "calculated_rows_before_limit", pr.calculated_rows_before_limit ? 1 : 0);
            call_user_function(NULL, NULL, &obj->profile_callback, &retval, 1, args);
            zval_ptr_dtor(&args[0]);
            zval_ptr_dtor(&retval);
            if (EG(exception)) {
                throw std::runtime_error("profile callback aborted query");
            }
        }
    });
}

/*
 * Verbose tracing: when enabled (via setVerbose(true|callable)), emit
 * lifecycle events as either JSON lines on stderr or as calls to a
 * user sink. ctx is consumed (zval_ptr_dtor'd) by the helper, so call
 * sites can build a fresh array per event without worrying about
 * cleanup. ctx may be NULL for events with no payload.
 */
static inline bool verbose_active(const clickhouse_object *obj)
{
    return obj->verbose_to_stderr || Z_TYPE(obj->verbose_callback) != IS_UNDEF;
}

static void emitVerbose(clickhouse_object *obj, const char *event, zval *ctx)
{
    if (!verbose_active(obj)) {
        if (ctx) zval_ptr_dtor(ctx);
        return;
    }
    zval payload;
    if (ctx) {
        ZVAL_COPY(&payload, ctx);
    } else {
        array_init(&payload);
    }
    if (obj->verbose_to_stderr) {
        smart_str buf = {0};
        php_json_encode(&buf, &payload, 0);
        smart_str_0(&buf);
        /* php_json_encode can raise (non-UTF8 string in payload, etc.).
         * Don't let that exception bleed onto the next user-facing call;
         * trace output is best-effort. */
        if (EG(exception)) {
            zend_clear_exception();
        }
        const char *body = buf.s ? ZSTR_VAL(buf.s) : "{}";
        size_t body_len = buf.s ? ZSTR_LEN(buf.s) : 2;
        fprintf(stderr, "[clickhouse] %s %.*s\n", event, (int)body_len, body);
        smart_str_free(&buf);
    } else if (Z_TYPE(obj->verbose_callback) != IS_UNDEF) {
        zval args[2], retval;
        ZVAL_NULL(&retval);
        ZVAL_STRING(&args[0], event);
        ZVAL_COPY(&args[1], &payload);
        call_user_function(NULL, NULL, &obj->verbose_callback, &retval, 2, args);
        zval_ptr_dtor(&args[0]);
        zval_ptr_dtor(&args[1]);
        zval_ptr_dtor(&retval);
        /* If the user callback raised, propagate to the packet loop so
         * subsequent OnData / event emissions don't run, mirroring the
         * progress / profile callback re-raise pattern. Without this,
         * the user's PHP exception buffers in EG(exception) and bleeds
         * onto the next unrelated query. Cleanup of payload / ctx
         * happens after this block; do that first then throw. */
        if (EG(exception)) {
            zval_ptr_dtor(&payload);
            if (ctx) zval_ptr_dtor(ctx);
            throw std::runtime_error("verbose callback aborted query");
        }
    }
    zval_ptr_dtor(&payload);
    if (ctx) zval_ptr_dtor(ctx);
}

/*
 * Attach the OnException hook so a server_exception verbose event fires
 * with the server's code/name/message. The data_block events come from
 * the OnData closures in do_select_into / selectStreamCallback so they
 * have access to the per-call block_index counter. Idempotent if
 * verbose is off so the hot path stays cheap.
 */
static void attachVerbose(Query &q, clickhouse_object *obj)
{
    if (!verbose_active(obj)) return;

    q.OnException([obj](const Exception &e) {
        zval ctx;
        array_init(&ctx);
        add_assoc_long(&ctx, "code", (zend_long)e.code);
        add_assoc_string(&ctx, "name", e.name.c_str());
        add_assoc_string(&ctx, "message", e.display_text.c_str());
        emitVerbose(obj, "server_exception", &ctx);
    });
}

static void resetStats(clickhouse_object *obj)
{
    obj->stats = ClientStats();
}

/*
 * Append a completed-query record to the per-client log if logging is
 * enabled. Pulls elapsed_ms / rows_read / bytes_read from the just-
 * populated stats. No-op when logging is off so the hot path stays
 * cheap on production deployments.
 */
/* Cap on retained QueryLog entries before getLogQueries() is called to
 * drain. A long-running PHP-FPM worker with logging on otherwise grows
 * the vector unboundedly (each entry holds two arbitrary-length strings).
 * When the cap is reached we drop the oldest in-place. */
#define CLICKHOUSE_QUERY_LOG_MAX 1024

static void appendQueryLogCapped(clickhouse_object *obj, QueryLog &&ql)
{
    if (obj->query_log.size() >= CLICKHOUSE_QUERY_LOG_MAX) {
        obj->query_log.pop_front();
    }
    obj->query_log.push_back(std::move(ql));
}

/* Build the QueryLog row shared by success and error paths. Caller fills
 * error_code / error_message for the error variant. */
static QueryLog buildQueryLog(const clickhouse_object *obj,
                              const std::string &sql, const std::string &qid)
{
    QueryLog ql;
    ql.sql = sql;
    ql.query_id = qid;
    ql.elapsed_ms = obj->stats.elapsed_ms;
    ql.rows_read = obj->stats.rows_read;
    ql.bytes_read = obj->stats.bytes_read;
    return ql;
}

static void recordQuerySuccess(clickhouse_object *obj, const std::string &sql, const std::string &qid)
{
    if (!obj->log_enabled) return;
    /* Never let a nested allocation failure here escape the wrapper. The caller
     * may already be inside a catch-block (recording the previous error); a
     * second uncaught exception would call std::terminate. */
    try {
        appendQueryLogCapped(obj, buildQueryLog(obj, sql, qid));
    } catch (...) { /* swallow; logging is best-effort */ }
}

static void recordQueryError(clickhouse_object *obj, const std::string &sql, const std::string &qid, const std::exception &e)
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

/*
 * Build the static INSERT INTO ... ( cols ) VALUES prefix from a PHP
 * column-name list. Returns by value (NRVO). Each column name is
 * validated through validateIdentifier; the table name is validated
 * with allow_dot=true so "db.tbl" works.
 *
 * The prior version used std::stringstream and an out-param + raw
 * char* table_name which forced a strlen() and lost the length the
 * caller already had.
 */
static std::string getInsertSql(std::string_view table_name, const zval *columns)
{
    validateIdentifier(table_name.data(), table_name.size(), "table name", true);

    HashTable *columns_ht = Z_ARRVAL_P(const_cast<zval*>(columns));
    size_t count = zend_hash_num_elements(columns_ht);
    if (count == 0) {
        throw std::runtime_error("Column list must not be empty");
    }

    /* Pre-size: "INSERT INTO " + table + " ( " + cols + " ) VALUES",
     * estimating ~16 chars per column on average. */
    std::string out;
    out.reserve(table_name.size() + 16 * count + 32);
    out.append("INSERT INTO ");
    out.append(table_name.data(), table_name.size());
    out.append(" ( ");

    bool first = true;
    zval *pzval;
    ZEND_HASH_FOREACH_VAL(columns_ht, pzval)
    {
        ZStrGuard sg(pzval);
        validateIdentifier(sg.val(), sg.len(), "column name", false);
        if (!first) out.append(",");
        out.append(sg.val(), sg.len());
        first = false;
    }
    ZEND_HASH_FOREACH_END();

    out.append(" ) VALUES");
    return out;
}

/*
 * Substitute placeholders in `sql` with values from `params_ht`.
 *
 * Two syntaxes are supported and routed differently:
 *
 *   {name}        client-side identifier substitution. Value must
 *                 consist of letters, digits, `_`, `.`, `,`, whitespace,
 *                 or `-`. Used for table and column names plus simple
 *                 dotted/comma-joined identifier lists; rejects every
 *                 character a SQL injection needs (and the parens /
 *                 asterisks / plus a function call or subquery would
 *                 use). Callers that need expression fragments should
 *                 pre-validate upstream.
 *
 *   {name:Type}   server-side parameter (ClickHouse native). The SQL
 *                 text is left untouched (the server parses {name:Type}
 *                 itself); the value is collected into `out_params` so
 *                 the caller can pass it to Query::SetParam. The wire
 *                 layer single-quotes and the server parses according
 *                 to Type. PHP arrays format as ClickHouse array
 *                 literals so Array(T) parses cleanly.
 *
 * If a parameter is provided that doesn't appear in the SQL (in either
 * form), the call throws. Multiple occurrences of the same `{name}`
 * placeholder are all replaced.
 *
 * `out_params` collects (name, optional<value>) pairs. nullopt is
 * routed to Query::SetParam as the server-side NULL sentinel.
 */
struct TypedParam {
    std::string name;
    std::optional<std::string> value;  // nullopt → server NULL
};

static void applyPlaceholders(string &sql, HashTable *params_ht, std::vector<TypedParam> &out_params)
{
    zval *pzval;
    zend_string *zk;
    zend_ulong nk;

    ZEND_HASH_FOREACH_KEY_VAL(params_ht, nk, zk, pzval) {
        (void)nk;
        if (!zk) {
            throw std::runtime_error("Placeholder array keys must be strings");
        }
        std::string name(ZSTR_VAL(zk), ZSTR_LEN(zk));

        /* Detect the {name:Type} server-side form. We scan the SQL for
         * the prefix `{name:` and capture the matching Type up to the
         * closing `}`. If found, this parameter is server-side. */
        std::string typed_prefix = "{" + name + ":";
        size_t tpos = sql.find(typed_prefix);
        if (tpos != std::string::npos) {
            size_t close = sql.find('}', tpos + typed_prefix.size());
            if (close == std::string::npos) {
                throw std::runtime_error(
                    "Unterminated typed placeholder for {" + name + "}");
            }
            std::string type = sql.substr(tpos + typed_prefix.size(),
                                          close - (tpos + typed_prefix.size()));
            TypedParam tp;
            tp.name = name;
            if (Z_TYPE_P(pzval) != IS_NULL) {
                tp.value = formatParamValue(pzval, type, false);
            }
            out_params.push_back(std::move(tp));
            continue;
        }

        /* Fall through: client-side {name} identifier substitution.
         * Whitelist is intentionally narrow and aligned with the docs
         * ("use for table and column names"): letters, digits, `_`, `.`,
         * `,`, whitespace, `-`. We deliberately exclude `*`, `(`, `)`
         * because they let a caller smuggle a function call or subquery
         * into positions that look identifier-shaped (e.g. ORDER BY).
         * Callers that need expression fragments should pre-validate
         * upstream rather than rely on this layer. */
        zend_string *coerced = zval_get_string(pzval);
        const char *val = ZSTR_VAL(coerced);
        size_t vlen = ZSTR_LEN(coerced);
        for (size_t i = 0; i < vlen; ++i) {
            unsigned char c = (unsigned char)val[i];
            bool ok =
                (c >= 'A' && c <= 'Z') ||
                (c >= 'a' && c <= 'z') ||
                (c >= '0' && c <= '9') ||
                c == '_' || c == '.' || c == ',' || c == ' ' ||
                c == '\t' || c == '-';
            if (!ok) {
                std::string err = "Placeholder value for {" + name + "} contains an unsafe character";
                zend_string_release(coerced);
                throw std::runtime_error(err);
            }
        }
        std::string needle = "{" + name + "}";
        std::string repl(val, vlen);
        zend_string_release(coerced);
        size_t pos = sql.find(needle);
        if (pos == std::string::npos) {
            throw std::runtime_error(
                "Placeholder {" + name + "} does not appear in the SQL");
        }
        while (pos != std::string::npos) {
            sql.replace(pos, needle.size(), repl);
            pos = sql.find(needle, pos + repl.size());
        }
    } ZEND_HASH_FOREACH_END();
}

static void attachTypedParams(Query &q, const std::vector<TypedParam> &params)
{
    for (const auto &p : params) {
        q.SetParam(p.name, p.value);
    }
}

/* {{{ proto bool ping()
 */
PHP_METHOD(ClickHouse, ping)
{
    try {
        clickhouse_object *obj = Z_CLICKHOUSE_P(getThis());
        Client *client = getClient(obj);
        client->Ping();
    } catch (const std::exception& e) {
        throwClickHouseError(e);
        return;
    }
    RETURN_TRUE;
}

/*
 * Internal: run a SELECT and write rows into `out`, which the caller
 * must have zero-initialized (we either array_init it or, for
 * SC_FETCH_ONE, write a scalar zval directly). On error, throws via
 * throwClickHouseError() and leaves `out` undefined; callers should
 * check EG(exception) on return.
 */
static void do_select_into(zval *out, zval *this_obj,
                           const char *sql, size_t l_sql,
                           zval *params, zend_long fetch_mode,
                           const std::string &qid, zval *settings)
{
    clickhouse_object *obj = Z_CLICKHOUSE_P(this_obj);
    try
    {
        Client *client = getClient(obj);

        if (obj->has_insert_block)
        {
            throw std::runtime_error("The insert operation is now in progress");
        }

        std::string sql_s(sql, l_sql);
        std::vector<TypedParam> typed_params;

        if (params != NULL && Z_TYPE_P(params) == IS_ARRAY)
        {
            applyPlaceholders(sql_s, Z_ARRVAL_P(params), typed_params);
        } else if (params != NULL && Z_TYPE_P(params) != IS_ARRAY) {
            throw std::runtime_error("The second argument to the select function must be an array");
        }

        Query query = qid.empty() ? Query(sql_s) : Query(sql_s, qid);
        attachTypedParams(query, typed_params);
        applyMergedSettings(query, obj, settings);
        resetStats(obj);
        obj->stats.last_query_id = qid;
        attachProgressAndProfile(query, obj);
        attachVerbose(query, obj);

        if (verbose_active(obj)) {
            zval ctx;
            array_init(&ctx);
            add_assoc_stringl(&ctx, "sql", (char*)sql_s.data(), sql_s.size());
            add_assoc_stringl(&ctx, "query_id", (char*)qid.data(), qid.size());
            add_assoc_long(&ctx, "settings_count", (zend_long)obj->settings.size());
            add_assoc_long(&ctx, "fetch_mode", (zend_long)fetch_mode);
            emitVerbose(obj, "select_start", &ctx);
        }

        if (!(fetch_mode & SC_FETCH_ONE)) {
            array_init(out);
        }

        size_t verbose_block_idx = 0;
        bool fetched_one = false;
        query.OnData([out, fetch_mode, &fetched_one, obj, &verbose_block_idx](const Block &block) {
            if (verbose_active(obj)) {
                zval ctx;
                array_init(&ctx);
                add_assoc_long(&ctx, "rows", (zend_long)block.GetRowCount());
                add_assoc_long(&ctx, "columns", (zend_long)block.GetColumnCount());
                add_assoc_long(&ctx, "block_index", (zend_long)verbose_block_idx++);
                emitVerbose(obj, "data_block", &ctx);
            }
            if (fetch_mode & SC_FETCH_ONE) {
                if (!fetched_one && block.GetRowCount() > 0 && block.GetColumnCount() > 0) {
                    convertToZval(out, block[0], 0, "", 0, fetch_mode);
                    fetched_one = true;
                }
                return;
            }

            /* Hoist column names out of the row loop; clickhouse-cpp
             * returns a fresh std::string per call, and the names are
             * stable across all rows of a block. */
            const size_t col_count = block.GetColumnCount();
            std::vector<std::string> col_names;
            col_names.reserve(col_count);
            for (size_t c = 0; c < col_count; ++c) {
                col_names.emplace_back(block.GetColumnName(c));
            }

            for (size_t row = 0; row < block.GetRowCount(); ++row)
            {
                if (fetch_mode & SC_FETCH_KEY_PAIR) {
                    if (col_count < 2) {
                        throw std::runtime_error("Key pair mode requires at least 2 columns to be present");
                    }
                    zval kp_col1, kp_col2;
                    ZVAL_UNDEF(&kp_col1);
                    ZVAL_UNDEF(&kp_col2);
                    try {
                        convertToZval(&kp_col1, block[0], row, "", 0, fetch_mode|SC_FETCH_ONE);
                        convertToZval(&kp_col2, block[1], row, "", 0, fetch_mode|SC_FETCH_ONE);
                    } catch (...) {
                        if (Z_TYPE(kp_col1) != IS_UNDEF) zval_ptr_dtor(&kp_col1);
                        if (Z_TYPE(kp_col2) != IS_UNDEF) zval_ptr_dtor(&kp_col2);
                        throw;
                    }

                    if (Z_TYPE(kp_col1) == IS_LONG) {
                         sc_zend_hash_index_update(Z_ARRVAL_P(out), Z_LVAL(kp_col1), &kp_col2);
                    } else {
                        /* zval_get_string is the modern non-mutating
                         * coercion (matches the post-CR-312 fetchKeyPair
                         * path). The prior convert_to_string mutated
                         * kp_col1 in place; both worked but the new form
                         * keeps the two key-pair paths in lockstep. */
                        zend_string *coerced = zval_get_string(&kp_col1);
                        zend_symtable_update(Z_ARRVAL_P(out), coerced, &kp_col2);
                        zend_string_release(coerced);
                    }
                    zval_ptr_dtor(&kp_col1);
                    continue;
                }

                zval row_tmp;
                ZVAL_UNDEF(&row_tmp);
                if (!(fetch_mode & SC_FETCH_COLUMN)) {
                    array_init(&row_tmp);
                }

                try {
                    for (size_t column = 0; column < col_count; ++column)
                    {
                        if (fetch_mode & SC_FETCH_COLUMN) {
                            convertToZval(&row_tmp, block[0], row, "", 0, fetch_mode|SC_FETCH_ONE);
                            break;
                        } else {
                            convertToZval(&row_tmp, block[column], row, col_names[column], 0, fetch_mode);
                        }
                    }
                } catch (...) {
                    if (Z_TYPE(row_tmp) != IS_UNDEF) zval_ptr_dtor(&row_tmp);
                    throw;
                }
                add_next_index_zval(out, &row_tmp);
            }
        });

        auto t0 = std::chrono::steady_clock::now();
        client->Select(query);
        auto t1 = std::chrono::steady_clock::now();
        obj->stats.elapsed_ms =
            std::chrono::duration<double, std::milli>(t1 - t0).count();
        recordQuerySuccess(obj, sql_s, qid);

        if (verbose_active(obj)) {
            zval ctx;
            array_init(&ctx);
            add_assoc_double(&ctx, "elapsed_ms", obj->stats.elapsed_ms);
            add_assoc_long(&ctx, "rows_read", (zend_long)obj->stats.rows_read);
            add_assoc_long(&ctx, "bytes_read", (zend_long)obj->stats.bytes_read);
            add_assoc_long(&ctx, "blocks", (zend_long)verbose_block_idx);
            emitVerbose(obj, "select_finish", &ctx);
        }
    }
    catch (const std::exception& e)
    {
        recordQueryError(obj, std::string(sql, l_sql), qid, e);
        throwClickHouseError(e, qid);
    }
}

/*
 * Internal: snapshot a ClientStats into a fresh PHP assoc array. Used
 * by both ClickHouse::getStatistics() and ClickHouseStatement (where
 * the snapshot lives on the Statement object so it survives the
 * Client running other queries afterwards).
 */
static void buildStatsArray(zval *out, const ClientStats &st)
{
    array_init(out);
    add_assoc_long(out, "rows_read", (zend_long)st.rows_read);
    add_assoc_long(out, "bytes_read", (zend_long)st.bytes_read);
    add_assoc_long(out, "total_rows", (zend_long)st.total_rows);
    add_assoc_long(out, "written_rows", (zend_long)st.written_rows);
    add_assoc_long(out, "written_bytes", (zend_long)st.written_bytes);
    add_assoc_long(out, "blocks", (zend_long)st.blocks);
    add_assoc_long(out, "rows_before_limit", (zend_long)st.rows_before_limit);
    add_assoc_bool(out, "applied_limit", st.applied_limit ? 1 : 0);
    add_assoc_double(out, "elapsed_ms", st.elapsed_ms);
    add_assoc_stringl(out, "query_id", st.last_query_id.data(), st.last_query_id.size());
}

/* {{{ proto array select(string sql, array params, int mode, string query_id, array settings)
 */
PHP_METHOD(ClickHouse, select)
{
    zend_string *sql = NULL;
    zval* params = NULL;
    zend_long fetch_mode = 0;
    zend_string *query_id = NULL;
    zval *settings = NULL;

    ZEND_PARSE_PARAMETERS_START(1, 5)
        Z_PARAM_STR(sql)
        Z_PARAM_OPTIONAL
        Z_PARAM_ARRAY(params)
        Z_PARAM_LONG(fetch_mode)
        Z_PARAM_STR(query_id)
        Z_PARAM_ARRAY(settings)
    ZEND_PARSE_PARAMETERS_END();
    std::string qid = makeQid(query_id);
    do_select_into(return_value, getThis(), ZSTR_VAL(sql), ZSTR_LEN(sql), params, fetch_mode, qid, settings);
}
/* }}} */

/* {{{ proto ClickHouseStatement selectStatement(string sql, array params, string query_id, array settings)
 *
 * smi2/phpClickHouse-style result wrapper. Runs the SELECT and returns
 * a ClickHouseStatement that implements Iterator + Countable +
 * ArrayAccess + JsonSerializable over the materialized rows, plus
 * fetchOne / fetchKeyPair / fetchColumn / statistics / toArray. The
 * Statement carries a per-call stats snapshot so it survives the
 * Client running other queries afterwards.
 */
PHP_METHOD(ClickHouse, selectStatement)
{
    zend_string *sql = NULL;
    zval *params = NULL;
    zend_string *query_id = NULL;
    zval *settings = NULL;

    ZEND_PARSE_PARAMETERS_START(1, 4)
        Z_PARAM_STR(sql)
        Z_PARAM_OPTIONAL
        Z_PARAM_ARRAY(params)
        Z_PARAM_STR(query_id)
        Z_PARAM_ARRAY(settings)
    ZEND_PARSE_PARAMETERS_END();
    std::string qid = makeQid(query_id);

    object_init_ex(return_value, clickhouse_statement_ce);
    clickhouse_statement_object *stmt = Z_CLICKHOUSE_STATEMENT_P(return_value);

    do_select_into(&stmt->rows, getThis(), ZSTR_VAL(sql), ZSTR_LEN(sql), params, 0, qid, settings);
    if (EG(exception)) {
        zval_ptr_dtor(return_value);
        ZVAL_UNDEF(return_value);
        return;
    }

    clickhouse_object *obj = Z_CLICKHOUSE_P(getThis());
    buildStatsArray(&stmt->statistics, obj->stats);
}
/* }}} */

/*
 * Build a column-major zval matrix [col][row] from a row-major PHP
 * HashTable [row][col]. Used by insert() and write() to transpose the
 * user's row-of-rows input into the per-column shape zvalToBlock wants.
 *
 * column_names != NULL: missing positional entries are looked up by
 * name (insert() accepts {col=>val} rows). NULL: positional only.
 *
 * Sets *out to an IS_ARRAY zval the caller owns. On throw, *out is
 * reset to IS_UNDEF and any partial state is freed.
 */
static void buildColumnMajorRows(HashTable *values_ht, size_t columns_count,
                                 const std::vector<zend_string*> *column_names,
                                 zval *out)
{
    array_init(out);
    try {
        zval inner;
        for (size_t i = 0; i < columns_count; ++i) {
            array_init(&inner);
            zval *pzval, *fzval;
            ZEND_HASH_FOREACH_VAL(values_ht, pzval) {
                if (Z_TYPE_P(pzval) != IS_ARRAY) {
                    zval_ptr_dtor(&inner);
                    throw std::runtime_error(
                        "The insert function needs to pass in a two-dimensional array");
                }
                fzval = sc_zend_hash_index_find(Z_ARRVAL_P(pzval), i);
                if (!fzval && column_names) {
                    zend_string *col = (*column_names)[i];
                    fzval = sc_zend_hash_find(Z_ARRVAL_P(pzval),
                                              ZSTR_VAL(col), ZSTR_LEN(col));
                }
                if (!fzval) {
                    zval_ptr_dtor(&inner);
                    throw std::runtime_error(
                        "The number of parameters inserted per line is inconsistent");
                }
                sc_zval_add_ref(fzval);
                add_next_index_zval(&inner, fzval);
            } ZEND_HASH_FOREACH_END();
            add_next_index_zval(out, &inner);
        }
    } catch (...) {
        zval_ptr_dtor(out);
        ZVAL_UNDEF(out);
        throw;
    }
}

/* {{{ proto array insert(string table, array columns, array values, string query_id, array settings)
 */
/*
 * Internal: run an INSERT against `table` with the column-name list
 * `columns` and the row-major matrix `values`. On error throws a PHP
 * exception via throwClickHouseError; callers should check EG(exception)
 * on return. Used by both ClickHouse::insert and ClickHouse::insertAssoc
 * (insertAssoc transposes the assoc-array input first, then calls here
 * directly instead of going through call_user_function on "insert").
 */
static void do_insert_into(zval *this_obj, zend_string *table,
                           zval *columns, zval *values,
                           const std::string &qid, zval *settings)
{
    string sql;

    // Storage for return_should lives in the function frame so that an
    // exception thrown by BeginInsert / SendInsertBlock / EndInsert can
    // still reach a valid zval header to free its array_init'd HashTable.
    zval transposed;
    ZVAL_UNDEF(&transposed);

    clickhouse_object *obj = Z_CLICKHOUSE_P(this_obj);
    try
    {
        Client *client = getClient(obj);

        if (obj->has_insert_block)
        {
            throw std::runtime_error("The insert operation is now in progress");
        }

        HashTable *columns_ht = Z_ARRVAL_P(columns);
        HashTable *values_ht = Z_ARRVAL_P(values);
        size_t columns_count = zend_hash_num_elements(columns_ht);

        /* Materialize the column-name list once, validating each entry is
         * a string. Indexing columns_ht by 0..N-1 directly assumes a packed
         * list; sparse or associative inputs (e.g. ['a' => 'col1']) used
         * to NULL-deref `key` a few lines down. Iterating in HashTable order
         * is the safe substitute. */
        std::vector<zend_string*> column_names;
        column_names.reserve(columns_count);
        {
            zval *cz;
            ZEND_HASH_FOREACH_VAL(columns_ht, cz) {
                if (Z_TYPE_P(cz) != IS_STRING) {
                    throw std::runtime_error(
                        "The columns array must be a list of column-name strings");
                }
                column_names.push_back(Z_STR_P(cz));
            } ZEND_HASH_FOREACH_END();
        }

        buildColumnMajorRows(values_ht, columns_count, &column_names, &transposed);

        sql = getInsertSql(std::string_view(ZSTR_VAL(table), ZSTR_LEN(table)), columns);

        Query insertQuery = qid.empty() ? Query(sql) : Query(sql, qid);
        applyMergedSettings(insertQuery, obj, settings);
        resetStats(obj);
        obj->stats.last_query_id = qid;
        attachProgressAndProfile(insertQuery, obj);
        attachVerbose(insertQuery, obj);
        Block blockQuery = client->BeginInsert(insertQuery);
        bool insert_open = true;

        try {
            Block blockInsert;
            size_t index = 0;
            zval *pzval;

            ZEND_HASH_FOREACH_VAL(Z_ARRVAL(transposed), pzval)
            {
                zvalToBlock(blockInsert, blockQuery, index, pzval);
                index++;
            }
            ZEND_HASH_FOREACH_END();

            auto t0 = std::chrono::steady_clock::now();
            client->SendInsertBlock(blockInsert);
            client->EndInsert();
            insert_open = false;
            auto t1 = std::chrono::steady_clock::now();
            obj->stats.elapsed_ms =
                std::chrono::duration<double, std::milli>(t1 - t0).count();
        } catch (...) {
            /* BeginInsert opened the wire; if a subsequent step throws,
             * try once to close the stream so the underlying client's
             * inserting_ flag clears. Swallow secondary failures. */
            if (insert_open) {
                try { client->EndInsert(); } catch (...) {}
            }
            throw;
        }
        recordQuerySuccess(obj, sql, qid);
        zval_ptr_dtor(&transposed);
        ZVAL_UNDEF(&transposed);
    }
    catch (const std::exception& e)
    {
        if (Z_TYPE(transposed) != IS_UNDEF) {
            zval_ptr_dtor(&transposed);
            ZVAL_UNDEF(&transposed);
        }
        recordQueryError(obj, sql, qid, e);
        throwClickHouseError(e, qid);
    }
}

PHP_METHOD(ClickHouse, insert)
{
    zend_string *table = NULL;
    zval *columns;
    zval *values;
    zend_string *query_id = NULL;
    zval *settings = NULL;

    ZEND_PARSE_PARAMETERS_START(3, 5)
        Z_PARAM_STR(table)
        Z_PARAM_ARRAY(columns)
        Z_PARAM_ARRAY(values)
        Z_PARAM_OPTIONAL
        Z_PARAM_STR(query_id)
        Z_PARAM_ARRAY(settings)
    ZEND_PARSE_PARAMETERS_END();

    do_insert_into(getThis(), table, columns, values, makeQid(query_id), settings);
    if (EG(exception)) {
        return;
    }
    RETURN_TRUE;
}
/* }}} */

/* {{{ proto bool writeStart(string table, array columns, string query_id, array settings)
 */
PHP_METHOD(ClickHouse, writeStart)
{
    zend_string *table = NULL;
    zval *columns;
    zend_string *query_id = NULL;
    zval *settings = NULL;

    string sql;

    ZEND_PARSE_PARAMETERS_START(2, 4)
        Z_PARAM_STR(table)
        Z_PARAM_ARRAY(columns)
        Z_PARAM_OPTIONAL
        Z_PARAM_STR(query_id)
        Z_PARAM_ARRAY(settings)
    ZEND_PARSE_PARAMETERS_END();
    std::string qid = makeQid(query_id);

    clickhouse_object *obj = Z_CLICKHOUSE_P(getThis());
    try
    {
        Client *client = getClient(obj);

        if (obj->has_insert_block)
        {
            throw std::runtime_error("The insert operation is now in progress");
        }

        sql = getInsertSql(std::string_view(ZSTR_VAL(table), ZSTR_LEN(table)), columns);

        Query insertQuery = qid.empty() ? Query(sql) : Query(sql, qid);
        applyMergedSettings(insertQuery, obj, settings);
        resetStats(obj);
        obj->stats.last_query_id = qid;
        attachProgressAndProfile(insertQuery, obj);
        attachVerbose(insertQuery, obj);
        Block blockQuery = client->BeginInsert(insertQuery);

        obj->insert_block = blockQuery;
        obj->has_insert_block = true;
        recordQuerySuccess(obj, sql, qid);
    }
    catch (const std::exception& e)
    {
        recordQueryError(obj, sql, qid, e);
        throwClickHouseError(e, qid);
    }
    RETURN_TRUE;
}
/* }}} */

/* {{{ proto array insert(string table, array columns, array values)
 */
PHP_METHOD(ClickHouse, write)
{
    zval *values;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_ARRAY(values)
    ZEND_PARSE_PARAMETERS_END();

    zval transposed;
    ZVAL_UNDEF(&transposed);

    try
    {
        zval *first_data = NULL;
        HashTable *values_ht = Z_ARRVAL_P(values);
        sc_zend_hash_get_current_data(values_ht, (void**) &first_data);
        if (NULL == first_data || Z_TYPE_P(first_data) != IS_ARRAY)
        {
            throw std::runtime_error("Empty or non-array first row passed to write()");
        }
        size_t columns_count = zend_hash_num_elements(Z_ARRVAL_P(first_data));

        buildColumnMajorRows(values_ht, columns_count, NULL, &transposed);

        clickhouse_object *obj = Z_CLICKHOUSE_P(getThis());
        Client *client = getClient(obj);

        if (!obj->has_insert_block) {
            throw std::runtime_error("write() called without a matching writeStart()");
        }
        Block &blockQuery = obj->insert_block;

        Block blockInsert;
        size_t index = 0;
        zval *pzval;

        ZEND_HASH_FOREACH_VAL(Z_ARRVAL(transposed), pzval)
        {
            zvalToBlock(blockInsert, blockQuery, index, pzval);
            index++;
        }
        ZEND_HASH_FOREACH_END();

        client->SendInsertBlock(blockInsert);
        zval_ptr_dtor(&transposed);
        ZVAL_UNDEF(&transposed);
    }
    catch (const std::exception& e)
    {
        if (Z_TYPE(transposed) != IS_UNDEF) {
            zval_ptr_dtor(&transposed);
            ZVAL_UNDEF(&transposed);
        }
        /* SendInsertBlock failed mid-stream. The wire is broken; mark the
         * insert as no longer in progress so the user can resetConnection
         * and start over without first calling writeEnd (which would
         * itself fail). */
        clickhouse_object *obj = Z_CLICKHOUSE_P(getThis());
        obj->insert_block = Block();
        obj->has_insert_block = false;
        throwClickHouseError(e);
    }
    RETURN_TRUE;
}
/* }}} */

/* {{{ proto array insert(string table, array columns, array values)
 */
PHP_METHOD(ClickHouse, writeEnd)
{
    clickhouse_object *obj = Z_CLICKHOUSE_P(getThis());
    try
    {
        Client *client = getClient(obj);
        if (!obj->has_insert_block) {
            throw std::runtime_error("writeEnd() called without a matching writeStart()");
        }

        client->EndInsert();
        obj->insert_block = Block();
        obj->has_insert_block = false;
    }
    catch (const std::exception& e)
    {
        /* Clear the local state even if EndInsert failed. The server-side
         * stream is no longer recoverable here; leaving has_insert_block
         * true would wedge every subsequent call until the PHP object
         * is destroyed. */
        obj->insert_block = Block();
        obj->has_insert_block = false;
        throwClickHouseError(e);
        return;
    }
    RETURN_TRUE;
}
/* }}} */

/* {{{ proto bool execute(string sql, array params, string query_id, array settings)
 */
/*
 * Internal: run a non-result-bearing statement (DDL, INSERT...SELECT,
 * SET, etc.). On error throws a PHP exception via throwClickHouseError;
 * callers should check EG(exception) on return. Used by ClickHouse::execute
 * and the SQL-helper one-liners (truncateTable, dropPartition, ...) so
 * those don't have to round-trip through call_user_function on "execute".
 */
static void do_execute_into(zval *this_obj,
                            const char *sql, size_t l_sql,
                            zval *params, const std::string &qid, zval *settings)
{
    clickhouse_object *obj = Z_CLICKHOUSE_P(this_obj);
    try
    {
        Client *client = getClient(obj);

        if (obj->has_insert_block)
        {
            throw std::runtime_error("The insert operation is now in progress");
        }

        string sql_s = std::string(sql, l_sql);
        std::vector<TypedParam> typed_params;

        if (params != NULL && Z_TYPE_P(params) == IS_ARRAY)
        {
            applyPlaceholders(sql_s, Z_ARRVAL_P(params), typed_params);
        } else if (params != NULL && Z_TYPE_P(params) != IS_ARRAY) {
            throw std::runtime_error("The second argument to execute must be an array");
        }

        Query query = qid.empty() ? Query(sql_s) : Query(sql_s, qid);
        attachTypedParams(query, typed_params);
        applyMergedSettings(query, obj, settings);
        resetStats(obj);
        obj->stats.last_query_id = qid;
        attachProgressAndProfile(query, obj);
        attachVerbose(query, obj);

        if (verbose_active(obj)) {
            zval ctx;
            array_init(&ctx);
            add_assoc_stringl(&ctx, "sql", (char*)sql_s.data(), sql_s.size());
            add_assoc_stringl(&ctx, "query_id", (char*)qid.data(), qid.size());
            add_assoc_long(&ctx, "settings_count", (zend_long)obj->settings.size());
            emitVerbose(obj, "execute_start", &ctx);
        }

        auto t0 = std::chrono::steady_clock::now();
        client->Execute(query);
        auto t1 = std::chrono::steady_clock::now();
        obj->stats.elapsed_ms =
            std::chrono::duration<double, std::milli>(t1 - t0).count();
        recordQuerySuccess(obj, sql_s, qid);

        if (verbose_active(obj)) {
            zval ctx;
            array_init(&ctx);
            add_assoc_double(&ctx, "elapsed_ms", obj->stats.elapsed_ms);
            emitVerbose(obj, "execute_finish", &ctx);
        }
    }
    catch (const std::exception& e)
    {
        recordQueryError(obj, std::string(sql, l_sql), qid, e);
        throwClickHouseError(e, qid);
    }
}

PHP_METHOD(ClickHouse, execute)
{
    zend_string *sql = NULL;
    zval* params = NULL;
    zend_string *query_id = NULL;
    zval *settings = NULL;

    ZEND_PARSE_PARAMETERS_START(1, 4)
        Z_PARAM_STR(sql)
        Z_PARAM_OPTIONAL
        Z_PARAM_ARRAY(params)
        Z_PARAM_STR(query_id)
        Z_PARAM_ARRAY(settings)
    ZEND_PARSE_PARAMETERS_END();

    do_execute_into(getThis(), ZSTR_VAL(sql), ZSTR_LEN(sql), params,
                    makeQid(query_id), settings);
    if (EG(exception)) {
        return;
    }
    RETURN_TRUE;
}
/* }}} */

/* {{{ proto static setSettings(array settings)
 *
 * Replace the client-wide settings map. Per-call settings supplied to
 * select/insert/execute/writeStart override these. Pass an empty array
 * to clear. Returns $this so callers can chain.
 */
PHP_METHOD(ClickHouse, setSettings)
{
    zval *arr;
    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_ARRAY(arr)
    ZEND_PARSE_PARAMETERS_END();
    clickhouse_object *obj = Z_CLICKHOUSE_P(getThis());
    /* Build into a temporary first so a malformed key doesn't leave the
     * caller's settings half-replaced. The validator matches setSetting's
     * (numeric keys rejected, empty strings rejected) so a setting array
     * accepted by one entry point is also accepted by the other. */
    std::unordered_map<std::string, std::string> m;
    HashTable *ht = Z_ARRVAL_P(arr);
    zval *vz;
    zend_string *zk;
    zend_ulong nk;
    ZEND_HASH_FOREACH_KEY_VAL(ht, nk, zk, vz) {
        (void)nk;
        if (!zk) {
            sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce,
                "setting keys must be strings", 0);
            return;
        }
        if (ZSTR_LEN(zk) == 0) {
            sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce,
                "setting key must not be empty", 0);
            return;
        }
        m[std::string(ZSTR_VAL(zk), ZSTR_LEN(zk))] = formatScalarParam(vz);
    } ZEND_HASH_FOREACH_END();
    obj->settings = std::move(m);
    RETURN_ZVAL(getThis(), 1, 0);
}
/* }}} */

/* {{{ proto static setSetting(string key, mixed value)
 *
 * Set a single client-wide setting. Equivalent to calling setSettings()
 * with a one-key array merged onto the existing map. Returns $this.
 */
PHP_METHOD(ClickHouse, setSetting)
{
    zend_string *key = NULL;
    zval *value = NULL;
    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_STR(key)
        Z_PARAM_ZVAL(value)
    ZEND_PARSE_PARAMETERS_END();
    if (ZSTR_LEN(key) == 0) {
        sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce,
            "setting key must not be empty", 0);
        return;
    }
    clickhouse_object *obj = Z_CLICKHOUSE_P(getThis());
    obj->settings[std::string(ZSTR_VAL(key), ZSTR_LEN(key))] = formatScalarParam(value);
    RETURN_ZVAL(getThis(), 1, 0);
}
/* }}} */

/* {{{ proto static setDatabase(string database)
 *
 * Switch the active database for subsequent queries. Issues USE on the
 * server, then updates the cached `database` property used by helpers
 * that take a default database (databaseSize, tablesSize, etc.).
 * Returns $this.
 */
PHP_METHOD(ClickHouse, setDatabase)
{
    zend_string *db = NULL;
    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STR(db)
    ZEND_PARSE_PARAMETERS_END();
    try {
        validateIdentifier(ZSTR_VAL(db), ZSTR_LEN(db), "database name", false);
    } catch (const std::exception &e) {
        throwClickHouseError(e);
        return;
    }
    clickhouse_object *obj = Z_CLICKHOUSE_P(getThis());
    if (!obj->client) {
        throwClickHouseError(std::runtime_error("Client is not connected"));
        return;
    }
    try {
        obj->client->Execute(Query("USE " + std::string(ZSTR_VAL(db), ZSTR_LEN(db))));
    } catch (const std::exception &e) {
        throwClickHouseError(e);
        return;
    }
    sc_zend_update_property_stringl(clickhouse_ce, getThis(), "database", sizeof("database") - 1, ZSTR_VAL(db), ZSTR_LEN(db));
    RETURN_ZVAL(getThis(), 1, 0);
}
/* }}} */

/* {{{ proto bool setProgressCallback(?callable callback)
 *
 * Register a callable invoked for each Progress packet the server
 * sends during select/execute. Pass null to remove. Callback receives
 * a single associative array: rows, bytes, total_rows, written_rows,
 * written_bytes.
 */
/*
 * Register-or-clear shared body for the three callback setters. Returns
 * false on the not-callable error path (with EG(exception) set), true
 * on success or null-clear.
 */
static bool setCallbackField(zval *target, zval *cb, const char *err_name)
{
    if (Z_TYPE(*target) != IS_UNDEF) {
        zval_ptr_dtor(target);
        ZVAL_UNDEF(target);
    }
    if (Z_TYPE_P(cb) == IS_NULL) {
        return true;
    }
    if (!zend_is_callable(cb, 0, NULL)) {
        std::string msg = std::string(err_name) + " expects a callable or null";
        sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce, msg.c_str(), 0);
        return false;
    }
    ZVAL_COPY(target, cb);
    return true;
}

PHP_METHOD(ClickHouse, setProgressCallback)
{
    zval *cb;
    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_ZVAL(cb)
    ZEND_PARSE_PARAMETERS_END();
    clickhouse_object *obj = Z_CLICKHOUSE_P(getThis());
    if (!setCallbackField(&obj->progress_callback, cb, "setProgressCallback")) {
        return;
    }
    RETURN_TRUE;
}
/* }}} */

/* {{{ proto bool setProfileCallback(?callable callback)
 *
 * Register a callable invoked for each Profile packet the server sends
 * (typically once at end of select / execute). Pass null to remove.
 * Callback receives a single associative array: rows, blocks, bytes,
 * rows_before_limit, applied_limit, calculated_rows_before_limit.
 */
PHP_METHOD(ClickHouse, setProfileCallback)
{
    zval *cb;
    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_ZVAL(cb)
    ZEND_PARSE_PARAMETERS_END();
    clickhouse_object *obj = Z_CLICKHOUSE_P(getThis());
    if (!setCallbackField(&obj->profile_callback, cb, "setProfileCallback")) {
        return;
    }
    RETURN_TRUE;
}
/* }}} */

/* {{{ proto static setVerbose(bool|callable sink)
 *
 * Enable protocol-level lifecycle tracing. Pass true to log JSON
 * lines on STDERR, false to disable, or a callable invoked with
 * (string $event, array $context) per event. Events: select_start,
 * data_block, select_finish, execute_start, execute_finish,
 * server_exception (plus the existing progress / profile callbacks
 * are unaffected). Returns $this so callers can chain.
 */
PHP_METHOD(ClickHouse, setVerbose)
{
    zval *sink;
    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_ZVAL(sink)
    ZEND_PARSE_PARAMETERS_END();
    clickhouse_object *obj = Z_CLICKHOUSE_P(getThis());

    /* Reset prior state. */
    if (Z_TYPE(obj->verbose_callback) != IS_UNDEF) {
        zval_ptr_dtor(&obj->verbose_callback);
        ZVAL_UNDEF(&obj->verbose_callback);
    }
    obj->verbose_to_stderr = false;

    if (Z_TYPE_P(sink) == IS_TRUE) {
        obj->verbose_to_stderr = true;
    } else if (Z_TYPE_P(sink) == IS_FALSE || Z_TYPE_P(sink) == IS_NULL) {
        /* false / null: already cleared above. Treat null the same as
         * false to match setProgressCallback / setProfileCallback, both
         * of which accept null as "remove the callback". */
    } else if (zend_is_callable(sink, 0, NULL)) {
        ZVAL_COPY(&obj->verbose_callback, sink);
    } else {
        sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce,
            "setVerbose expects bool, null, or callable", 0);
        return;
    }
    RETURN_ZVAL(getThis(), 1, 0);
}
/* }}} */

/* {{{ proto bool resetConnection()
 *
 * Force-close and re-open the underlying TCP connection. Useful for
 * long-lived workers that want a clean socket after an idle period or
 * after a server-side restart.
 */
PHP_METHOD(ClickHouse, resetConnection)
{
    try {
        clickhouse_object *obj = Z_CLICKHOUSE_P(getThis());
        Client *client = getClient(obj);
        client->ResetConnection();
        /* Drop any half-built insert state. After ResetConnection, the
         * server-side stream this Block was tied to no longer exists, so
         * keeping has_insert_block=true would wedge subsequent calls. */
        if (obj->has_insert_block) {
            obj->insert_block = Block();
            obj->has_insert_block = false;
        }
    } catch (const std::exception& e) {
        throwClickHouseError(e);
        return;
    }
    RETURN_TRUE;
}
/* }}} */

/* {{{ proto array getServerInfo()
 *
 * Return the server identification banner from the most recent connect:
 * name, display_name, version_major, version_minor, version_patch,
 * revision, timezone.
 */
PHP_METHOD(ClickHouse, getServerInfo)
{
    try {
        clickhouse_object *obj = Z_CLICKHOUSE_P(getThis());
        Client *client = getClient(obj);
        const ServerInfo &si = client->GetServerInfo();
        array_init(return_value);
        add_assoc_stringl(return_value, "name", (char*)si.name.c_str(), si.name.size());
        add_assoc_stringl(return_value, "display_name", (char*)si.display_name.c_str(), si.display_name.size());
        add_assoc_long(return_value, "version_major", (zend_long)si.version_major);
        add_assoc_long(return_value, "version_minor", (zend_long)si.version_minor);
        add_assoc_long(return_value, "version_patch", (zend_long)si.version_patch);
        add_assoc_long(return_value, "revision", (zend_long)si.revision);
        add_assoc_stringl(return_value, "timezone", (char*)si.timezone.c_str(), si.timezone.size());
    } catch (const std::exception& e) {
        throwClickHouseError(e);
        return;
    }
}
/* }}} */

/* {{{ proto ?array getCurrentEndpoint()
 *
 * Return the active endpoint as ["host" => ..., "port" => ...]. The
 * single host/port config is modeled internally as a 1-item endpoints
 * list, so this is non-null in normal operation. Returns null only if
 * the underlying client has no endpoint resolved yet.
 */
PHP_METHOD(ClickHouse, getCurrentEndpoint)
{
    try {
        clickhouse_object *obj = Z_CLICKHOUSE_P(getThis());
        Client *client = getClient(obj);
        const std::optional<Endpoint> &ep = client->GetCurrentEndpoint();
        if (!ep.has_value()) {
            RETURN_NULL();
        }
        array_init(return_value);
        add_assoc_stringl(return_value, "host", (char*)ep->host.c_str(), ep->host.size());
        add_assoc_long(return_value, "port", (zend_long)ep->port);
    } catch (const std::exception& e) {
        throwClickHouseError(e);
        return;
    }
}
/* }}} */

/* {{{ proto array getStatistics()
 *
 * Return the rows / bytes / time recorded for the last completed
 * select / execute / insert. Reset on every query.
 */
PHP_METHOD(ClickHouse, getStatistics)
{
    clickhouse_object *obj = Z_CLICKHOUSE_P(getThis());
    buildStatsArray(return_value, obj->stats);
}
/* }}} */

/* {{{ proto bool insertAssoc(string table, array rows, string query_id, array settings)
 *
 * Convenience wrapper over insert(): derives the column list from the
 * keys of the first row, then forwards to insert(). All rows must share
 * the same key set (positional alignment is by first-row key order).
 */
PHP_METHOD(ClickHouse, insertAssoc)
{
    zend_string *table = NULL;
    zval *rows;
    zend_string *query_id = NULL;
    zval *settings = NULL;

    ZEND_PARSE_PARAMETERS_START(2, 4)
        Z_PARAM_STR(table)
        Z_PARAM_ARRAY(rows)
        Z_PARAM_OPTIONAL
        Z_PARAM_STR(query_id)
        Z_PARAM_ARRAY(settings)
    ZEND_PARSE_PARAMETERS_END();
    std::string qid = makeQid(query_id);

    try {
        HashTable *rows_ht = Z_ARRVAL_P(rows);
        if (zend_hash_num_elements(rows_ht) == 0) {
            throw std::runtime_error("insertAssoc: rows is empty");
        }
        zval *first = NULL;
        {
            zval *fz;
            ZEND_HASH_FOREACH_VAL(rows_ht, fz) {
                first = fz;
                break;
            } ZEND_HASH_FOREACH_END();
        }
        if (!first || Z_TYPE_P(first) != IS_ARRAY) {
            throw std::runtime_error("insertAssoc: each row must be an associative array");
        }

        zval columns_zv, values_zv;
        array_init(&columns_zv);
        array_init(&values_zv);

        std::vector<std::string> col_order;
        zval *fv;
        zend_string *fk;
        zend_ulong fnk;
        ZEND_HASH_FOREACH_KEY_VAL(Z_ARRVAL_P(first), fnk, fk, fv) {
            (void)fv;
            (void)fnk;
            if (!fk) {
                zval_ptr_dtor(&columns_zv);
                zval_ptr_dtor(&values_zv);
                throw std::runtime_error("insertAssoc: each row must have string keys (column names)");
            }
            col_order.emplace_back(ZSTR_VAL(fk), ZSTR_LEN(fk));
            add_next_index_stringl(&columns_zv, ZSTR_VAL(fk), ZSTR_LEN(fk));
        } ZEND_HASH_FOREACH_END();

        zval *row_zv;
        ZEND_HASH_FOREACH_VAL(rows_ht, row_zv) {
            if (Z_TYPE_P(row_zv) != IS_ARRAY) {
                zval_ptr_dtor(&columns_zv);
                zval_ptr_dtor(&values_zv);
                throw std::runtime_error("insertAssoc: each row must be an associative array");
            }
            zval row_out;
            array_init(&row_out);
            for (const std::string &col : col_order) {
                zval *cell = sc_zend_hash_find(Z_ARRVAL_P(row_zv), (char*)col.c_str(), col.size());
                if (!cell) {
                    zval_ptr_dtor(&row_out);
                    zval_ptr_dtor(&columns_zv);
                    zval_ptr_dtor(&values_zv);
                    throw std::runtime_error(
                        "insertAssoc: row is missing key '" + col + "'");
                }
                Z_TRY_ADDREF_P(cell);
                add_next_index_zval(&row_out, cell);
            }
            add_next_index_zval(&values_zv, &row_out);
        } ZEND_HASH_FOREACH_END();

        /* Dispatch directly into the shared insert helper. The previous
         * version went through call_user_function("insert", ...) which
         * added a full PHP method-dispatch frame on every assoc insert
         * and exposed the helper to user-defined subclass overrides
         * of insert(). */
        do_insert_into(getThis(), table, &columns_zv, &values_zv, qid, settings);
        zval_ptr_dtor(&columns_zv);
        zval_ptr_dtor(&values_zv);
        if (EG(exception)) {
            return;
        }
    } catch (const std::exception &e) {
        throwClickHouseError(e, qid);
        return;
    }
    RETURN_TRUE;
}
/* }}} */

/*
 * SQL-helper one-liners. Each builds a small SELECT and reuses the
 * select() machinery directly through do_select_into / do_execute_into
 * so settings, progress, stats, and the verbose trace surface apply
 * exactly the same as on the user-visible select() / execute(). The
 * old call_user_function indirection added a full PHP method-dispatch
 * frame per helper call (and exposed the helpers to user-defined
 * subclass overrides of select/execute, which wasn't intended).
 */
static void runHelperSelect(zval *return_value, zval *this_obj, const std::string &sql, zend_long fetch_mode)
{
    do_select_into(return_value, this_obj, sql.c_str(), sql.size(),
                   /*params=*/NULL, fetch_mode, /*qid=*/std::string(),
                   /*settings=*/NULL);
}

static bool runHelperExec(zval *this_obj, const std::string &sql)
{
    do_execute_into(this_obj, sql.c_str(), sql.size(),
                    /*params=*/NULL, /*qid=*/std::string(), /*settings=*/NULL);
    return !EG(exception);
}

static std::string currentDatabase(zval *this_obj)
{
    zval *db = sc_zend_read_property(clickhouse_ce, this_obj, "database", sizeof("database") - 1, 0);
    if (db && Z_TYPE_P(db) == IS_STRING) {
        return std::string(Z_STRVAL_P(db), Z_STRLEN_P(db));
    }
    return std::string("default");
}

/*
 * Return the first row of a helper result as an assoc array, or
 * an empty array if there were no rows.
 */
static void runHelperSelectFirstRow(zval *return_value, zval *this_obj, const std::string &sql)
{
    zval rows;
    runHelperSelect(&rows, this_obj, sql, 0);
    if (EG(exception)) {
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
PHP_METHOD(ClickHouse, databaseSize)
{
    zend_string *db = NULL;
    ZEND_PARSE_PARAMETERS_START(0, 1)
        Z_PARAM_OPTIONAL
        Z_PARAM_STR_OR_NULL(db)
    ZEND_PARSE_PARAMETERS_END();
    std::string dbname = (db && ZSTR_LEN(db) > 0) ? std::string(ZSTR_VAL(db), ZSTR_LEN(db)) : currentDatabase(getThis());
    try {
        validateIdentifier(dbname.c_str(), dbname.size(), "database name", false);
    } catch (const std::exception &e) {
        throwClickHouseError(e);
        return;
    }
    std::string sql =
        "SELECT sum(bytes_on_disk) AS bytes_on_disk, sum(rows) AS rows "
        "FROM system.parts WHERE active AND database = '" + dbname + "'";
    runHelperSelectFirstRow(return_value, getThis(), sql);
}
/* }}} */

/* {{{ proto array tablesSize(?string database)
 */
PHP_METHOD(ClickHouse, tablesSize)
{
    zend_string *db = NULL;
    ZEND_PARSE_PARAMETERS_START(0, 1)
        Z_PARAM_OPTIONAL
        Z_PARAM_STR_OR_NULL(db)
    ZEND_PARSE_PARAMETERS_END();
    std::string dbname = (db && ZSTR_LEN(db) > 0) ? std::string(ZSTR_VAL(db), ZSTR_LEN(db)) : currentDatabase(getThis());
    try {
        validateIdentifier(dbname.c_str(), dbname.size(), "database name", false);
    } catch (const std::exception &e) {
        throwClickHouseError(e);
        return;
    }
    std::string sql =
        "SELECT table, sum(bytes_on_disk) AS bytes_on_disk, sum(rows) AS rows, "
        "max(modification_time) AS modification_time "
        "FROM system.parts WHERE active AND database = '" + dbname + "' "
        "GROUP BY table ORDER BY table";
    runHelperSelect(return_value, getThis(), sql, 0);
}
/* }}} */

/* {{{ proto array partitions(string table)
 */
PHP_METHOD(ClickHouse, partitions)
{
    zend_string *table = NULL;
    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STR(table)
    ZEND_PARSE_PARAMETERS_END();
    std::string tname(ZSTR_VAL(table), ZSTR_LEN(table));
    std::string dbname = currentDatabase(getThis());
    /* Allow `db.table` in the argument; split on the dot. */
    auto dot = tname.find('.');
    if (dot != std::string::npos) {
        dbname = tname.substr(0, dot);
        tname = tname.substr(dot + 1);
    }
    try {
        validateIdentifier(dbname.c_str(), dbname.size(), "database name", false);
        validateIdentifier(tname.c_str(), tname.size(), "table name", false);
    } catch (const std::exception &e) {
        throwClickHouseError(e);
        return;
    }
    std::string sql =
        "SELECT partition, count() AS parts, sum(rows) AS rows, "
        "sum(bytes_on_disk) AS bytes_on_disk, "
        "min(min_time) AS min_time, max(max_time) AS max_time "
        "FROM system.parts WHERE active AND database = '" + dbname + "' "
        "AND table = '" + tname + "' "
        "GROUP BY partition ORDER BY partition";
    runHelperSelect(return_value, getThis(), sql, 0);
}
/* }}} */

/* {{{ proto array showTables(?string database, ?string like)
 */
PHP_METHOD(ClickHouse, showTables)
{
    zend_string *db = NULL, *like = NULL;
    ZEND_PARSE_PARAMETERS_START(0, 2)
        Z_PARAM_OPTIONAL
        Z_PARAM_STR_OR_NULL(db)
        Z_PARAM_STR_OR_NULL(like)
    ZEND_PARSE_PARAMETERS_END();
    std::string dbname = (db && ZSTR_LEN(db) > 0) ? std::string(ZSTR_VAL(db), ZSTR_LEN(db)) : currentDatabase(getThis());
    try {
        validateIdentifier(dbname.c_str(), dbname.size(), "database name", false);
    } catch (const std::exception &e) {
        throwClickHouseError(e);
        return;
    }
    std::string sql = "SELECT name FROM system.tables WHERE database = '" + dbname + "'";
    if (like && ZSTR_LEN(like) > 0) {
        /* Validate the LIKE pattern against the same character set as
         * placeholder values: identifier chars plus % and _ would be the
         * minimum, but we already reject quotes/backslashes there so
         * the only addition is %. */
        for (size_t i = 0; i < ZSTR_LEN(like); ++i) {
            unsigned char c = (unsigned char)ZSTR_VAL(like)[i];
            bool ok =
                (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
                (c >= '0' && c <= '9') ||
                c == '_' || c == '.' || c == '%';
            if (!ok) {
                sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce,
                    "showTables: LIKE pattern contains an unsafe character", 0);
                return;
            }
        }
        sql += " AND name LIKE '" + std::string(ZSTR_VAL(like), ZSTR_LEN(like)) + "'";
    }
    sql += " ORDER BY name";
    runHelperSelect(return_value, getThis(), sql, SC_FETCH_COLUMN);
}
/* }}} */

/* {{{ proto string showCreateTable(string table)
 */
PHP_METHOD(ClickHouse, showCreateTable)
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
PHP_METHOD(ClickHouse, getServerUptime)
{
    runHelperSelect(return_value, getThis(),
        "SELECT uptime() AS uptime",
        SC_FETCH_ONE | SC_FETCH_COLUMN);
}
/* }}} */

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
    clickhouse_object *obj = Z_CLICKHOUSE_P(getThis());
    array_init(return_value);
    for (const auto &ql : obj->query_log) {
        zval entry;
        array_init(&entry);
        add_assoc_stringl(&entry, "sql", (char*)ql.sql.c_str(), ql.sql.size());
        add_assoc_stringl(&entry, "query_id", (char*)ql.query_id.c_str(), ql.query_id.size());
        add_assoc_double(&entry, "elapsed_ms", ql.elapsed_ms);
        add_assoc_long(&entry, "rows_read", (zend_long)ql.rows_read);
        add_assoc_long(&entry, "bytes_read", (zend_long)ql.bytes_read);
        add_assoc_long(&entry, "error_code", (zend_long)ql.error_code);
        add_assoc_stringl(&entry, "error_message",
            (char*)ql.error_message.c_str(), ql.error_message.size());
        add_next_index_zval(return_value, &entry);
    }
    obj->query_log.clear();
}
/* }}} */

/* {{{ proto ClickHouseRowIterator selectStream(string sql, array params, string query_id, array settings)
 *
 * Run a SELECT and return a ClickHouseRowIterator over the rows
 * without materializing the full result as a single PHP array. The
 * implementation buffers all blocks before returning (so the network
 * round-trip is finished by the time the iterator is handed back),
 * then walks them lazily during iteration. Use this when the row
 * shape is fine but the row count is large enough that a full PHP
 * array would balloon zval overhead.
 *
 * For true unbounded streaming where rows must be consumed as they
 * arrive, use selectStreamCallback() instead.
 */
PHP_METHOD(ClickHouse, selectStream)
{
    zend_string *sql = NULL;
    zval *params = NULL;
    zend_string *query_id = NULL;
    zval *settings = NULL;

    ZEND_PARSE_PARAMETERS_START(1, 4)
        Z_PARAM_STR(sql)
        Z_PARAM_OPTIONAL
        Z_PARAM_ARRAY(params)
        Z_PARAM_STR(query_id)
        Z_PARAM_ARRAY(settings)
    ZEND_PARSE_PARAMETERS_END();
    std::string qid = makeQid(query_id);
    clickhouse_object *obj = Z_CLICKHOUSE_P(getThis());

    object_init_ex(return_value, clickhouse_iter_ce);
    clickhouse_iter_object *iter = Z_CLICKHOUSE_ITER_P(return_value);

    try {
        Client *client = getClient(obj);

        if (obj->has_insert_block) {
            throw std::runtime_error("The insert operation is now in progress");
        }

        std::string sql_s = std::string(ZSTR_VAL(sql), ZSTR_LEN(sql));
        std::vector<TypedParam> typed_params;
        if (params != NULL && Z_TYPE_P(params) == IS_ARRAY) {
            applyPlaceholders(sql_s, Z_ARRVAL_P(params), typed_params);
        }

        Query query = qid.empty() ? Query(sql_s) : Query(sql_s, qid);
        attachTypedParams(query, typed_params);
        applyMergedSettings(query, obj, settings);
        resetStats(obj);
        obj->stats.last_query_id = qid;
        attachProgressAndProfile(query, obj);
        attachVerbose(query, obj);

        query.OnData([iter](const Block &block) {
            if (block.GetRowCount() == 0 || block.GetColumnCount() == 0) return;
            iter->total_rows += block.GetRowCount();
            /* Cache column names on the first non-empty block. The schema
             * is identical across all blocks in a single result, so we
             * pay one std::string copy per column, once, instead of one
             * per (row, column) on every current() call. */
            if (iter->column_names.empty()) {
                const size_t nc = block.GetColumnCount();
                iter->column_names.reserve(nc);
                for (size_t c = 0; c < nc; ++c) {
                    iter->column_names.emplace_back(block.GetColumnName(c));
                }
            }
            iter->blocks.push_back(block);
        });

        auto t0 = std::chrono::steady_clock::now();
        client->Select(query);
        auto t1 = std::chrono::steady_clock::now();
        obj->stats.elapsed_ms =
            std::chrono::duration<double, std::milli>(t1 - t0).count();
        recordQuerySuccess(obj, sql_s, qid);
    }
    catch (const std::exception &e) {
        recordQueryError(obj, std::string(ZSTR_VAL(sql), ZSTR_LEN(sql)), qid, e);
        zval_ptr_dtor(return_value);
        ZVAL_NULL(return_value);
        throwClickHouseError(e, qid);
    }
}
/* }}} */

/* {{{ proto bool selectStreamCallback(string sql, callable cb, array params, string query_id, array settings)
 *
 * True per-row streaming: invoke the user callback once per row as
 * blocks arrive from the server, never accumulating the full result
 * in memory. The callback receives a single argument, the row as an
 * associative array keyed by column name. Returns true on success.
 */
PHP_METHOD(ClickHouse, selectStreamCallback)
{
    zend_string *sql = NULL;
    zval *cb = NULL;
    zval *params = NULL;
    zend_string *query_id = NULL;
    zval *settings = NULL;

    ZEND_PARSE_PARAMETERS_START(2, 5)
        Z_PARAM_STR(sql)
        Z_PARAM_ZVAL(cb)
        Z_PARAM_OPTIONAL
        Z_PARAM_ARRAY(params)
        Z_PARAM_STR(query_id)
        Z_PARAM_ARRAY(settings)
    ZEND_PARSE_PARAMETERS_END();

    if (!zend_is_callable(cb, 0, NULL)) {
        sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce,
            "Argument 2 passed to selectStreamCallback must be callable", 0);
        return;
    }

    std::string qid = makeQid(query_id);
    clickhouse_object *obj = Z_CLICKHOUSE_P(getThis());

    try {
        Client *client = getClient(obj);

        if (obj->has_insert_block) {
            throw std::runtime_error("The insert operation is now in progress");
        }

        std::string sql_s = std::string(ZSTR_VAL(sql), ZSTR_LEN(sql));
        std::vector<TypedParam> typed_params;
        if (params != NULL && Z_TYPE_P(params) == IS_ARRAY) {
            applyPlaceholders(sql_s, Z_ARRVAL_P(params), typed_params);
        }

        Query query = qid.empty() ? Query(sql_s) : Query(sql_s, qid);
        attachTypedParams(query, typed_params);
        applyMergedSettings(query, obj, settings);
        resetStats(obj);
        obj->stats.last_query_id = qid;
        attachProgressAndProfile(query, obj);
        attachVerbose(query, obj);

        query.OnData([cb](const Block &block) {
            if (block.GetRowCount() == 0 || block.GetColumnCount() == 0) return;
            /* Hoist column names out of the row loop. clickhouse-cpp returns
             * a fresh std::string per GetColumnName call but the names are
             * stable across all rows of a block. */
            const size_t col_count = block.GetColumnCount();
            std::vector<std::string> col_names;
            col_names.reserve(col_count);
            for (size_t c = 0; c < col_count; ++c) {
                col_names.emplace_back(block.GetColumnName(c));
            }
            for (size_t row = 0; row < block.GetRowCount(); ++row) {
                zval row_zv;
                array_init(&row_zv);
                /* Mirrors do_select_into's exception-safety pattern: a
                 * convertToZval throw mid-row would otherwise leak the
                 * partially-built row_zv HashTable. */
                try {
                    for (size_t col = 0; col < col_count; ++col) {
                        convertToZval(&row_zv, block[col], row, col_names[col], 0, 0);
                    }
                } catch (...) {
                    zval_ptr_dtor(&row_zv);
                    throw;
                }
                zval args[1], retval;
                ZVAL_NULL(&retval);
                ZVAL_COPY_VALUE(&args[0], &row_zv);
                call_user_function(NULL, NULL, cb, &retval, 1, args);
                zval_ptr_dtor(&args[0]);
                zval_ptr_dtor(&retval);
                /* Match the progress / profile / verbose pattern: a throwing
                 * row callback aborts the packet loop instead of silently
                 * over-processing remaining rows and recording the query
                 * as successful. The user's PHP exception is preserved in
                 * EG(exception); the surrounding catch translates the C++
                 * throw to recordQueryError + throwClickHouseError. */
                if (EG(exception)) {
                    throw std::runtime_error("row callback aborted query");
                }
            }
        });

        auto t0 = std::chrono::steady_clock::now();
        client->Select(query);
        auto t1 = std::chrono::steady_clock::now();
        obj->stats.elapsed_ms =
            std::chrono::duration<double, std::milli>(t1 - t0).count();
        recordQuerySuccess(obj, sql_s, qid);
    }
    catch (const std::exception &e) {
        recordQueryError(obj, std::string(ZSTR_VAL(sql), ZSTR_LEN(sql)), qid, e);
        throwClickHouseError(e, qid);
    }
    RETURN_TRUE;
}
/* }}} */

/* ClickHouseRowIterator Iterator interface ------------------------- */

PHP_METHOD(ClickHouseRowIterator, rewind)
{
    clickhouse_iter_object *iter = Z_CLICKHOUSE_ITER_P(getThis());
    iter->block_idx = 0;
    iter->row_idx = 0;
    iter->cumulative_row_idx = 0;
}

PHP_METHOD(ClickHouseRowIterator, valid)
{
    clickhouse_iter_object *iter = Z_CLICKHOUSE_ITER_P(getThis());
    if (iter->block_idx >= iter->blocks.size()) {
        RETURN_FALSE;
    }
    if (iter->row_idx >= iter->blocks[iter->block_idx].GetRowCount()) {
        RETURN_FALSE;
    }
    RETURN_TRUE;
}

PHP_METHOD(ClickHouseRowIterator, current)
{
    clickhouse_iter_object *iter = Z_CLICKHOUSE_ITER_P(getThis());
    if (iter->block_idx >= iter->blocks.size()) {
        RETURN_NULL();
    }
    const Block &block = iter->blocks[iter->block_idx];
    if (iter->row_idx >= block.GetRowCount()) {
        RETURN_NULL();
    }
    array_init(return_value);
    /* convertToZval throws on unsupported / malformed server-side types.
     * The Zend dispatcher is C; let the exception cross it would be UB. */
    static const std::string empty_name;
    try {
        const size_t col_count = block.GetColumnCount();
        for (size_t col = 0; col < col_count; ++col) {
            /* Use the iter-cached names rather than block.GetColumnName(col)
             * which would heap-alloc a fresh std::string per cell. */
            const std::string &name = (col < iter->column_names.size())
                ? iter->column_names[col]
                : empty_name;
            convertToZval(return_value, block[col], iter->row_idx, name, 0, 0);
        }
    } catch (const std::exception &e) {
        zval_ptr_dtor(return_value);
        sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce, e.what(), 0);
        RETURN_NULL();
    }
}

PHP_METHOD(ClickHouseRowIterator, key)
{
    clickhouse_iter_object *iter = Z_CLICKHOUSE_ITER_P(getThis());
    RETURN_LONG((zend_long)iter->cumulative_row_idx);
}

PHP_METHOD(ClickHouseRowIterator, next)
{
    clickhouse_iter_object *iter = Z_CLICKHOUSE_ITER_P(getThis());
    if (iter->block_idx >= iter->blocks.size()) return;
    iter->row_idx++;
    iter->cumulative_row_idx++;
    while (iter->block_idx < iter->blocks.size() &&
           iter->row_idx >= iter->blocks[iter->block_idx].GetRowCount()) {
        iter->block_idx++;
        iter->row_idx = 0;
    }
}

PHP_METHOD(ClickHouseRowIterator, count)
{
    clickhouse_iter_object *iter = Z_CLICKHOUSE_ITER_P(getThis());
    RETURN_LONG((zend_long)iter->total_rows);
}

/* {{{ proto bool isExists(string database, string table)
 *
 * Returns true when the (database, table) pair exists in
 * system.tables (which also covers views and dictionaries on modern
 * ClickHouse). Both arguments are validated as identifiers.
 */
PHP_METHOD(ClickHouse, isExists)
{
    zend_string *db = NULL, *table = NULL;
    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_STR(db)
        Z_PARAM_STR(table)
    ZEND_PARSE_PARAMETERS_END();
    try {
        validateIdentifier(ZSTR_VAL(db), ZSTR_LEN(db), "database name", false);
        validateIdentifier(ZSTR_VAL(table), ZSTR_LEN(table), "table name", false);
    } catch (const std::exception &e) {
        throwClickHouseError(e);
        return;
    }
    std::string sql =
        "SELECT count() AS c FROM system.tables WHERE database = '" +
        std::string(ZSTR_VAL(db), ZSTR_LEN(db)) + "' AND name = '" + std::string(ZSTR_VAL(table), ZSTR_LEN(table)) + "'";
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
PHP_METHOD(ClickHouse, showDatabases)
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
PHP_METHOD(ClickHouse, showProcesslist)
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
PHP_METHOD(ClickHouse, getServerVersion)
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
PHP_METHOD(ClickHouse, tableSize)
{
    zend_string *table = NULL;
    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STR(table)
    ZEND_PARSE_PARAMETERS_END();
    std::string tname(ZSTR_VAL(table), ZSTR_LEN(table));
    std::string dbname = currentDatabase(getThis());
    auto dot = tname.find('.');
    if (dot != std::string::npos) {
        dbname = tname.substr(0, dot);
        tname = tname.substr(dot + 1);
    }
    try {
        validateIdentifier(dbname.c_str(), dbname.size(), "database name", false);
        validateIdentifier(tname.c_str(), tname.size(), "table name", false);
    } catch (const std::exception &e) {
        throwClickHouseError(e);
        return;
    }
    std::string sql =
        "SELECT sum(rows) AS rows, sum(bytes_on_disk) AS bytes_on_disk, "
        "uniqExact(partition) AS partitions, max(modification_time) AS modification_time "
        "FROM system.parts WHERE active AND database = '" + dbname +
        "' AND table = '" + tname + "'";
    runHelperSelectFirstRow(return_value, getThis(), sql);
}
/* }}} */

/* {{{ proto bool truncateTable(string table)
 */
PHP_METHOD(ClickHouse, truncateTable)
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
PHP_METHOD(ClickHouse, dropPartition)
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
    /* Guard against control characters that could break the literal. */
    for (size_t i = 0; i < ZSTR_LEN(part); ++i) {
        unsigned char c = (unsigned char)ZSTR_VAL(part)[i];
        if (c < 0x20) {
            sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce,
                "dropPartition: partition value contains a control character", 0);
            return;
        }
    }
    /* ClickHouse accepts both SQL-standard '' and C-style \' as escapes inside
     * single-quoted literals; backslash must be escaped too or '\'' would be
     * parsed as an escaped quote followed by a closing quote. */
    std::string escaped;
    escaped.reserve(ZSTR_LEN(part) + 8);
    for (size_t i = 0; i < ZSTR_LEN(part); ++i) {
        char c = ZSTR_VAL(part)[i];
        if (c == '\'')      escaped += "\\'";
        else if (c == '\\') escaped += "\\\\";
        else                escaped += c;
    }
    std::string sql = "ALTER TABLE " + std::string(ZSTR_VAL(table), ZSTR_LEN(table)) +
        " DROP PARTITION '" + escaped + "'";
    RETURN_BOOL(runHelperExec(getThis(), sql));
}
/* }}} */

/* {{{ proto void __destruct()
 *
 * No-op. All cleanup (Client teardown, in-progress insert close,
 * progress callback release, settings/log destruction) lives in the
 * free_obj handler so it fires even on bailout.
 */
PHP_METHOD(ClickHouse, __destruct)
{
}
/* }}} */

/* {{{ proto int ClickHouseException::getServerCode()
 * proto ?string ClickHouseException::getServerName()
 * proto ?string ClickHouseException::getQueryId()
 *
 * smi2/phpClickHouse-style getter aliases for the public
 * `server_code`/`server_name`/`query_id` properties. Same data, easier
 * to call from code that was written against the smi2 client.
 */
PHP_METHOD(ClickHouseException, getServerCode)
{
    if (zend_parse_parameters_none() == FAILURE) {
        return;
    }
    zval *p = sc_zend_read_property(clickhouse_exception_ce, getThis(), "server_code", sizeof("server_code") - 1, 0);
    if (p && Z_TYPE_P(p) == IS_LONG) {
        RETURN_LONG(Z_LVAL_P(p));
    }
    RETURN_LONG(0);
}

PHP_METHOD(ClickHouseException, getServerName)
{
    if (zend_parse_parameters_none() == FAILURE) {
        return;
    }
    zval *p = sc_zend_read_property(clickhouse_exception_ce, getThis(), "server_name", sizeof("server_name") - 1, 0);
    if (p && Z_TYPE_P(p) == IS_STRING) {
        RETURN_STRINGL(Z_STRVAL_P(p), Z_STRLEN_P(p));
    }
    RETURN_NULL();
}

PHP_METHOD(ClickHouseException, getQueryId)
{
    if (zend_parse_parameters_none() == FAILURE) {
        return;
    }
    zval *p = sc_zend_read_property(clickhouse_exception_ce, getThis(), "query_id", sizeof("query_id") - 1, 0);
    if (p && Z_TYPE_P(p) == IS_STRING) {
        RETURN_STRINGL(Z_STRVAL_P(p), Z_STRLEN_P(p));
    }
    RETURN_NULL();
}
/* }}} */

/* {{{ ClickHouseStatement methods
 *
 * Materialized result wrapper. Constructed only by
 * ClickHouse::selectStatement(); the public constructor throws.
 */
PHP_METHOD(ClickHouseStatement, __construct)
{
    sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce,
        "ClickHouseStatement is constructed by ClickHouse::selectStatement(); the constructor is private",
        0);
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
        zend_hash_internal_pointer_reset(Z_ARRVAL(stmt->rows));
    }
}

PHP_METHOD(ClickHouseStatement, valid)
{
    if (zend_parse_parameters_none() == FAILURE) return;
    clickhouse_statement_object *stmt = Z_CLICKHOUSE_STATEMENT_P(getThis());
    if (Z_TYPE(stmt->rows) != IS_ARRAY) {
        RETURN_FALSE;
    }
    RETURN_BOOL(zend_hash_get_current_data(Z_ARRVAL(stmt->rows)) != NULL);
}

PHP_METHOD(ClickHouseStatement, current)
{
    if (zend_parse_parameters_none() == FAILURE) return;
    clickhouse_statement_object *stmt = Z_CLICKHOUSE_STATEMENT_P(getThis());
    if (Z_TYPE(stmt->rows) != IS_ARRAY) {
        RETURN_NULL();
    }
    zval *cur = zend_hash_get_current_data(Z_ARRVAL(stmt->rows));
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
    int t = zend_hash_get_current_key(Z_ARRVAL(stmt->rows), &str_key, &num_key);
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
        zend_hash_move_forward(Z_ARRVAL(stmt->rows));
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
        RETURN_BOOL(zend_hash_exists(ht, Z_STR_P(offset)));
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
        v = zend_hash_find(ht, Z_STR_P(offset));
    }
    if (!v) {
        RETURN_NULL();
    }
    ZVAL_COPY(return_value, v);
}

PHP_METHOD(ClickHouseStatement, offsetSet)
{
    sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce,
        "ClickHouseStatement is read-only; offsetSet is not supported", 0);
}

PHP_METHOD(ClickHouseStatement, offsetUnset)
{
    sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce,
        "ClickHouseStatement is read-only; offsetUnset is not supported", 0);
}

/* jsonSerialize and toArray return the same value: a copy of the
 * stored rows array, or an empty array when the statement has none.
 * Shared body keeps the two PHP-visible methods in lockstep. */
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
    /* If the row is itself an assoc array with a single column, return
     * the scalar value (smi2 fetchOne semantics). Otherwise return the
     * full row. */
    if (Z_TYPE_P(first) == IS_ARRAY && zend_hash_num_elements(Z_ARRVAL_P(first)) == 1) {
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
    zval *row;
    ZEND_HASH_FOREACH_VAL(Z_ARRVAL(stmt->rows), row) {
        if (Z_TYPE_P(row) != IS_ARRAY || zend_hash_num_elements(Z_ARRVAL_P(row)) < 2) {
            sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce,
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

        zval val_copy;
        ZVAL_COPY(&val_copy, vv);
        if (Z_TYPE_P(kv) == IS_LONG) {
            zend_hash_index_update(Z_ARRVAL_P(return_value), Z_LVAL_P(kv), &val_copy);
        } else {
            /* zval_get_string is non-mutating and handles object __toString
             * without leaking the pre-bumped val_copy if conversion raises.
             * The prior pattern (ZVAL_COPY + convert_to_string) buffered
             * EG(exception) and continued the loop, accumulating refcount
             * leaks across rows. */
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
    zval *row;
    ZEND_HASH_FOREACH_VAL(Z_ARRVAL(stmt->rows), row) {
        if (Z_TYPE_P(row) != IS_ARRAY) {
            /* Already a flat list of scalars (e.g. FETCH_COLUMN result fed in). */
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
/* }}} */

/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: noet sw=4 ts=4 fdm=marker
 * vim<600: noet sw=4 ts=4
 */
