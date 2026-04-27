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
#include "php7_wrapper.h"
}

#include "php_clickhouse.h"

#include "lib/clickhouse-cpp/clickhouse/client.h"
#include "lib/clickhouse-cpp/clickhouse/error_codes.h"
#include "lib/clickhouse-cpp/clickhouse/exceptions.h"
#include "lib/clickhouse-cpp/clickhouse/types/type_parser.h"
#include "typesToPhp.hpp"
#include <chrono>
#include <map>
#include <sstream>
#include <unordered_map>

using namespace clickhouse;
using namespace std;

zend_class_entry *clickhouse_ce, *clickhouse_exception_ce, *clickhouse_iter_ce;

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
    bool log_enabled;
    std::vector<QueryLog> query_log;
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
    new (&obj->insert_block) Block();
    new (&obj->stats) ClientStats();
    new (&obj->settings) std::unordered_map<std::string, std::string>();
    new (&obj->query_log) std::vector<QueryLog>();
    ZVAL_UNDEF(&obj->progress_callback);
    ZVAL_UNDEF(&obj->profile_callback);

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

    obj->insert_block.~Block();
    obj->stats.~ClientStats();
    obj->settings.~unordered_map<std::string, std::string>();
    obj->query_log.~vector<QueryLog>();

    zend_object_std_dtor(&obj->std);
}

/*
 * Streaming row iterator state. blocks accumulate via OnData during
 * selectStream(); the foreach loop walks them lazily without
 * materializing the full result set as a single PHP array.
 */
struct clickhouse_iter_object {
    std::vector<Block> blocks;
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

    zend_object_std_init(&iter->std, ce);
    object_properties_init(&iter->std, ce);
    iter->std.handlers = &clickhouse_iter_object_handlers;
    return &iter->std;
}

static void clickhouse_iter_free_obj(zend_object *object)
{
    clickhouse_iter_object *iter = clickhouse_iter_from_obj(object);
    iter->blocks.~vector<Block>();
    zend_object_std_dtor(&iter->std);
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
static PHP_METHOD(ClickHouse, setProgressCallback);
static PHP_METHOD(ClickHouse, setProfileCallback);
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
    clickhouse_ce->create_object = clickhouse_create_object;
#if PHP_VERSION_ID >= 80400
    clickhouse_ce->default_object_handlers = &clickhouse_object_handlers;
#endif

    memcpy(&clickhouse_object_handlers, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
    clickhouse_object_handlers.offset = XtOffsetOf(clickhouse_object, std);
    clickhouse_object_handlers.free_obj = clickhouse_free_obj;

    clickhouse_exception_ce = register_class_ClickHouseException(zend_ce_exception);

    clickhouse_iter_ce = register_class_ClickHouseRowIterator(zend_ce_iterator, zend_ce_countable);
    clickhouse_iter_ce->create_object = clickhouse_iter_create_object;
#if PHP_VERSION_ID >= 80400
    clickhouse_iter_ce->default_object_handlers = &clickhouse_iter_object_handlers;
#endif

    memcpy(&clickhouse_iter_object_handlers, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
    clickhouse_iter_object_handlers.offset = XtOffsetOf(clickhouse_iter_object, std);
    clickhouse_iter_object_handlers.free_obj = clickhouse_iter_free_obj;

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

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "z", &connectParams) == FAILURE)
    {
        return;
    }

    HashTable *_ht = Z_ARRVAL_P(connectParams);
    zval *value;

    zval *this_obj;
    this_obj = getThis();
    if (php_array_get_value(_ht, "host", value))
    {
        convert_to_string(value);
        sc_zend_update_property_string(clickhouse_ce, this_obj, "host", sizeof("host") - 1, Z_STRVAL_P(value));
    }

    if (php_array_get_value(_ht, "port", value))
    {
        convert_to_long(value);
        sc_zend_update_property_long(clickhouse_ce, this_obj, "port", sizeof("port") - 1, Z_LVAL_P(value));
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
            convert_to_boolean(value);
            cv = Z_LVAL_P(value);
        }
        sc_zend_update_property_long(clickhouse_ce, this_obj, "compression", sizeof("compression") - 1, cv);
    }

    if (php_array_get_value(_ht, "retry_timeout", value))
    {
        convert_to_long(value);
        sc_zend_update_property_long(clickhouse_ce, this_obj, "retry_timeout", sizeof("retry_timeout") - 1, Z_LVAL_P(value));
    }

    if (php_array_get_value(_ht, "retry_count", value))
    {
        convert_to_long(value);
        sc_zend_update_property_long(clickhouse_ce, this_obj, "retry_count", sizeof("retry_count") - 1, Z_LVAL_P(value));
    }

    if (php_array_get_value(_ht, "connect_timeout", value))
    {
        convert_to_long(value);
        sc_zend_update_property_long(clickhouse_ce, this_obj, "connect_timeout", sizeof("connect_timeout") - 1, Z_LVAL_P(value));
    }

    if (php_array_get_value(_ht, "receive_timeout", value))
    {
        convert_to_long(value);
        sc_zend_update_property_long(clickhouse_ce, this_obj, "receive_timeout", sizeof("receive_timeout") - 1, Z_LVAL_P(value));
    }

    zval *host = sc_zend_read_property(clickhouse_ce, this_obj, "host", sizeof("host") - 1, 0);
    zval *port = sc_zend_read_property(clickhouse_ce, this_obj, "port", sizeof("port") - 1, 0);
    zval *compression = sc_zend_read_property(clickhouse_ce, this_obj, "compression", sizeof("compression") - 1, 0);
    zval *retry_timeout = sc_zend_read_property(clickhouse_ce, this_obj, "retry_timeout", sizeof("retry_timeout") - 1, 0);
    zval *retry_count = sc_zend_read_property(clickhouse_ce, this_obj, "retry_count", sizeof("retry_count") - 1, 0);
    zval *receive_timeout = sc_zend_read_property(clickhouse_ce, this_obj, "receive_timeout", sizeof("receive_timeout") - 1, 0);
    zval *connect_timeout = sc_zend_read_property(clickhouse_ce, this_obj, "connect_timeout", sizeof("connect_timeout") - 1, 0);

    ClientOptions Options = ClientOptions()
                            .SetHost(Z_STRVAL_P(host))
                            .SetPort(Z_LVAL_P(port))
                            .SetSendRetries(Z_LVAL_P(retry_count))
                            .SetRetryTimeout(std::chrono::seconds(Z_LVAL_P(retry_timeout)))
                            .SetConnectionRecvTimeout(std::chrono::seconds(Z_LVAL_P(receive_timeout)))
                            .SetConnectionConnectTimeout(std::chrono::seconds(Z_LVAL_P(connect_timeout)))
                            .SetPingBeforeQuery(false);
    long cv = Z_LVAL_P(compression);
    if (cv == 1) Options = Options.SetCompressionMethod(CompressionMethod::LZ4);
    else if (cv == 2) Options = Options.SetCompressionMethod(CompressionMethod::ZSTD);

    if (php_array_get_value(_ht, "send_timeout", value)) {
        convert_to_long(value);
        Options = Options.SetConnectionSendTimeout(std::chrono::seconds(Z_LVAL_P(value)));
    }
    /* Millisecond variants override the seconds-based keys. Useful when
     * sub-second precision matters (CI test guards, low-latency hops). */
    if (php_array_get_value(_ht, "connect_timeout_ms", value)) {
        convert_to_long(value);
        if (Z_LVAL_P(value) < 0) {
            sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce,
                "connect_timeout_ms must be >= 0", 0);
            return;
        }
        Options = Options.SetConnectionConnectTimeout(std::chrono::milliseconds(Z_LVAL_P(value)));
    }
    if (php_array_get_value(_ht, "receive_timeout_ms", value)) {
        convert_to_long(value);
        if (Z_LVAL_P(value) < 0) {
            sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce,
                "receive_timeout_ms must be >= 0", 0);
            return;
        }
        Options = Options.SetConnectionRecvTimeout(std::chrono::milliseconds(Z_LVAL_P(value)));
    }
    if (php_array_get_value(_ht, "send_timeout_ms", value)) {
        convert_to_long(value);
        if (Z_LVAL_P(value) < 0) {
            sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce,
                "send_timeout_ms must be >= 0", 0);
            return;
        }
        Options = Options.SetConnectionSendTimeout(std::chrono::milliseconds(Z_LVAL_P(value)));
    }
    if (php_array_get_value(_ht, "tcp_nodelay", value)) {
        convert_to_boolean(value);
        Options = Options.TcpNoDelay(Z_LVAL_P(value) != 0);
    }
    if (php_array_get_value(_ht, "tcp_keepalive", value)) {
        convert_to_boolean(value);
        Options = Options.TcpKeepAlive(Z_LVAL_P(value) != 0);
    }
    if (php_array_get_value(_ht, "ping_before_query", value)) {
        convert_to_boolean(value);
        Options = Options.SetPingBeforeQuery(Z_LVAL_P(value) != 0);
    }
    if (php_array_get_value(_ht, "tcp_keepalive_idle", value)) {
        convert_to_long(value);
        Options = Options.SetTcpKeepAliveIdle(std::chrono::seconds(Z_LVAL_P(value)));
    }
    if (php_array_get_value(_ht, "tcp_keepalive_intvl", value)) {
        convert_to_long(value);
        Options = Options.SetTcpKeepAliveInterval(std::chrono::seconds(Z_LVAL_P(value)));
    }
    if (php_array_get_value(_ht, "tcp_keepalive_cnt", value)) {
        convert_to_long(value);
        zend_long n = Z_LVAL_P(value);
        if (n < 0 || n > UINT_MAX) {
            sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce,
                "tcp_keepalive_cnt out of range", 0);
            return;
        }
        Options = Options.SetTcpKeepAliveCount((unsigned int)n);
    }
    if (php_array_get_value(_ht, "max_compression_chunk_size", value)) {
        convert_to_long(value);
        zend_long n = Z_LVAL_P(value);
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
        convert_to_boolean(value);
        want_ssl = (Z_LVAL_P(value) != 0);
    }
    if (want_ssl) {
        ClientOptions::SSLOptions ssl_opts;
        // Default to TLS 1.2 minimum so a server speaking only 1.0 / 1.1
        // is rejected without the caller having to remember to set this.
        // Caller can override via ssl_min_protocol_version.
        ssl_opts.SetMinProtocolVersion(0x0303);
        if (php_array_get_value(_ht, "ssl_min_protocol_version", value)) {
            convert_to_string(value);
            const char *s = Z_STRVAL_P(value);
            int ver = 0;
            if (strcasecmp(s, "tls1.0") == 0)      ver = 0x0301;
            else if (strcasecmp(s, "tls1.1") == 0) ver = 0x0302;
            else if (strcasecmp(s, "tls1.2") == 0) ver = 0x0303;
            else if (strcasecmp(s, "tls1.3") == 0) ver = 0x0304;
            else {
                sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce,
                    "ssl_min_protocol_version must be one of tls1.0, tls1.1, tls1.2, tls1.3", 0);
                return;
            }
            ssl_opts.SetMinProtocolVersion(ver);
        }
        if (php_array_get_value(_ht, "ssl_skip_verify", value)) {
            convert_to_boolean(value);
            ssl_opts.SetSkipVerification(Z_LVAL_P(value) != 0);
        }
        if (php_array_get_value(_ht, "ssl_use_default_ca", value)) {
            convert_to_boolean(value);
            ssl_opts.SetUseDefaultCALocations(Z_LVAL_P(value) != 0);
        }
        if (php_array_get_value(_ht, "ssl_ca_directory", value)) {
            convert_to_string(value);
            ssl_opts.SetPathToCADirectory(std::string(Z_STRVAL_P(value), Z_STRLEN_P(value)));
        }
        if (php_array_get_value(_ht, "ssl_ca_files", value)) {
            std::vector<std::string> files;
            if (Z_TYPE_P(value) == IS_STRING) {
                files.emplace_back(Z_STRVAL_P(value), Z_STRLEN_P(value));
            } else if (Z_TYPE_P(value) == IS_ARRAY) {
                HashTable *fh = Z_ARRVAL_P(value);
                zval *fv;
                ZEND_HASH_FOREACH_VAL(fh, fv) {
                    convert_to_string(fv);
                    files.emplace_back(Z_STRVAL_P(fv), Z_STRLEN_P(fv));
                } ZEND_HASH_FOREACH_END();
            }
            ssl_opts.SetPathToCAFiles(files);
        }
        Options = Options.SetSSLOptions(ssl_opts);
    }
#else
    if (php_array_get_value(_ht, "ssl", value)) {
        convert_to_boolean(value);
        if (Z_LVAL_P(value) != 0) {
            sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce,
                "php_clickhouse was built without TLS support. Reconfigure with --enable-clickhouse-openssl",
                0);
            RETURN_FALSE;
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
            convert_to_string(hz);
            Endpoint e;
            e.host = std::string(Z_STRVAL_P(hz), Z_STRLEN_P(hz));
            if (pz) {
                convert_to_long(pz);
                zend_long p = Z_LVAL_P(pz);
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
        convert_to_string(value);
        sc_zend_update_property_string(clickhouse_ce, this_obj, "database", sizeof("database") - 1, Z_STRVAL_P(value));
        Options = Options.SetDefaultDatabase(Z_STRVAL_P(value));
    }

    if (php_array_get_value(_ht, "user", value))
    {
        convert_to_string(value);
        sc_zend_update_property_string(clickhouse_ce, this_obj, "user", sizeof("user") - 1, Z_STRVAL_P(value));
        Options = Options.SetUser(Z_STRVAL_P(value));
    }

    if (php_array_get_value(_ht, "passwd", value))
    {
        convert_to_string(value);
        Options = Options.SetPassword(Z_STRVAL_P(value));
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
        msg.resize(CLICKHOUSE_ERROR_MAX_LEN);
        msg += "... (truncated)";
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
            char buf[64];
            int n = snprintf(buf, sizeof(buf), "%.17g", Z_DVAL_P(v));
            return std::string(buf, (n > 0 && (size_t)n < sizeof(buf)) ? (size_t)n : 0);
        }
        default:
            convert_to_string(v);
            return std::string(Z_STRVAL_P(v), Z_STRLEN_P(v));
    }
}

static bool typeNeedsQuoting(const std::string &t)
{
    /* Inner type for an Array(T) typed param. Numeric and bool parse
     * raw; everything else needs single-quotes around each element. */
    static const char *bare[] = {"Int", "UInt", "Float", "Decimal", "Bool"};
    for (const char *p : bare) {
        if (t.compare(0, strlen(p), p) == 0) return false;
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
    std::unordered_map<std::string, std::string> merged = obj->settings;
    if (per_call != NULL && Z_TYPE_P(per_call) == IS_ARRAY) {
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
    }
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
        }
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
static void recordQuerySuccess(clickhouse_object *obj, const std::string &sql, const std::string &qid)
{
    if (!obj->log_enabled) return;
    QueryLog ql;
    ql.sql = sql;
    ql.query_id = qid;
    ql.elapsed_ms = obj->stats.elapsed_ms;
    ql.rows_read = obj->stats.rows_read;
    ql.bytes_read = obj->stats.bytes_read;
    obj->query_log.push_back(std::move(ql));
}

static void recordQueryError(clickhouse_object *obj, const std::string &sql, const std::string &qid, const std::exception &e)
{
    if (!obj->log_enabled) return;
    QueryLog ql;
    ql.sql = sql;
    ql.query_id = qid;
    ql.elapsed_ms = obj->stats.elapsed_ms;
    ql.rows_read = obj->stats.rows_read;
    ql.bytes_read = obj->stats.bytes_read;
    if (auto se = dynamic_cast<const clickhouse::ServerException*>(&e)) {
        ql.error_code = se->GetException().code;
    } else {
        ql.error_code = -1;
    }
    ql.error_message = sanitizeError(e.what());
    obj->query_log.push_back(std::move(ql));
}

void getInsertSql(string *sql, char *table_name, zval *columns)
{
    zval *pzval;
    std::stringstream fields_section;

    validateIdentifier(table_name, strlen(table_name), "table name", true);

    HashTable *columns_ht = Z_ARRVAL_P(columns);
    size_t count = zend_hash_num_elements(columns_ht);
    if (count == 0) {
        throw std::runtime_error("Column list must not be empty");
    }
    size_t index = 0;

    ZEND_HASH_FOREACH_VAL(columns_ht, pzval)
    {
        convert_to_string(pzval);
        validateIdentifier(Z_STRVAL_P(pzval), Z_STRLEN_P(pzval), "column name", false);
        if (index >= (count - 1))
        {
            fields_section << (string)Z_STRVAL_P(pzval);
        }
        else
        {
            fields_section << (string)Z_STRVAL_P(pzval) << ",";
        }
        index++;
    }
    ZEND_HASH_FOREACH_END();
    *sql = "INSERT INTO " + (string)table_name + " ( " + fields_section.str() + " ) VALUES";
}

/*
 * Substitute placeholders in `sql` with values from `params_ht`.
 *
 * Two syntaxes are supported and routed differently:
 *
 *   {name}        client-side identifier substitution. Value must
 *                 consist of identifier characters / digits / dots /
 *                 commas / parentheses / asterisks / plus-minus /
 *                 whitespace. Used for table and column names plus
 *                 simple numeric expressions; rejects every character
 *                 a SQL injection needs.
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
 * `out_params` contains tuples (name, value, is_null). The caller is
 * expected to call SetParam(name, optional<string>) on the final Query.
 */
struct TypedParam {
    std::string name;
    std::string value;
    bool is_null;
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
            tp.is_null = (Z_TYPE_P(pzval) == IS_NULL);
            if (!tp.is_null) {
                tp.value = formatParamValue(pzval, type, false);
            }
            out_params.push_back(std::move(tp));
            continue;
        }

        /* Fall through: client-side {name} identifier substitution. */
        convert_to_string(pzval);
        const char *val = Z_STRVAL_P(pzval);
        size_t vlen = Z_STRLEN_P(pzval);
        for (size_t i = 0; i < vlen; ++i) {
            unsigned char c = (unsigned char)val[i];
            bool ok =
                (c >= 'A' && c <= 'Z') ||
                (c >= 'a' && c <= 'z') ||
                (c >= '0' && c <= '9') ||
                c == '_' || c == '.' || c == ',' || c == ' ' ||
                c == '\t' || c == '*' || c == '(' || c == ')' ||
                c == '+' || c == '-';
            if (!ok) {
                throw std::runtime_error(
                    "Placeholder value for {" + name + "} contains an unsafe character");
            }
        }
        std::string needle = "{" + name + "}";
        size_t pos = sql.find(needle);
        if (pos == std::string::npos) {
            throw std::runtime_error(
                "Placeholder {" + name + "} does not appear in the SQL");
        }
        std::string repl(val, vlen);
        while (pos != std::string::npos) {
            sql.replace(pos, needle.size(), repl);
            pos = sql.find(needle, pos + repl.size());
        }
    } ZEND_HASH_FOREACH_END();
}

static void attachTypedParams(Query &q, const std::vector<TypedParam> &params)
{
    for (const auto &p : params) {
        if (p.is_null) {
            q.SetParam(p.name, std::nullopt);
        } else {
            q.SetParam(p.name, p.value);
        }
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

/* {{{ proto array select(string sql, array params, int mode, string query_id, array settings)
 */
PHP_METHOD(ClickHouse, select)
{
    char *sql = NULL;
    size_t l_sql = 0;
    zval* params = NULL;
    zend_long fetch_mode = 0;
    char *query_id = NULL;
    size_t l_query_id = 0;
    zval *settings = NULL;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "s|zlsa", &sql, &l_sql, &params, &fetch_mode, &query_id, &l_query_id, &settings) == FAILURE)
    {
        return;
    }
    std::string qid = (query_id && l_query_id > 0) ? std::string(query_id, l_query_id) : std::string();
    clickhouse_object *obj = Z_CLICKHOUSE_P(getThis());
    try
    {
        Client *client = getClient(obj);

        if (obj->has_insert_block)
        {
            throw std::runtime_error("The insert operation is now in progress");
        }

        string sql_s = (string)sql;
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

        if (!(fetch_mode & SC_FETCH_ONE)) {
            array_init(return_value);
        }

        // Track whether FETCH_ONE has already emitted a row, so a multi-
        // block result returns the very first row and not the first row
        // of the last block.
        bool fetched_one = false;
        query.OnData([return_value, fetch_mode, &fetched_one](const Block &block) {
            if (fetch_mode & SC_FETCH_ONE) {
                if (!fetched_one && block.GetRowCount() > 0 && block.GetColumnCount() > 0) {
                    convertToZval(return_value, block[0], 0, "", 0, fetch_mode);
                    fetched_one = true;
                }
                return;
            }

            zval *return_tmp;
            for (size_t row = 0; row < block.GetRowCount(); ++row)
            {
                if (fetch_mode & SC_FETCH_KEY_PAIR) {
                    if (block.GetColumnCount() < 2) {
                        throw std::runtime_error("Key pair mode requires at least 2 columns to be present");
                    }
                    zval *col1, *col2;
                    SC_MAKE_STD_ZVAL(col1);
                    SC_MAKE_STD_ZVAL(col2);

                    convertToZval(col1, block[0], row, "", 0, fetch_mode|SC_FETCH_ONE);
                    convertToZval(col2, block[1], row, "", 0, fetch_mode|SC_FETCH_ONE);

                    if (Z_TYPE_P(col1) == IS_LONG) {
                         sc_zend_hash_index_update(Z_ARRVAL_P(return_value), Z_LVAL_P(col1), col2);
                    } else {
                        convert_to_string(col1);
                        zend_symtable_update(Z_ARRVAL_P(return_value), Z_STR_P(col1), col2);
                    }
                    zval_ptr_dtor(col1);
                    continue;
                }

                SC_MAKE_STD_ZVAL(return_tmp);
                if (!(fetch_mode & SC_FETCH_COLUMN)) {
                    array_init(return_tmp);
                }

                for (size_t column = 0; column < block.GetColumnCount(); ++column)
                {
                    string column_name = block.GetColumnName(column);
                    if (fetch_mode & SC_FETCH_COLUMN) {
                        convertToZval(return_tmp, block[0], row, "", 0, fetch_mode|SC_FETCH_ONE);
                        break;
                    } else {
                        convertToZval(return_tmp, block[column], row, column_name, 0, fetch_mode);
                    }
                }
                add_next_index_zval(return_value, return_tmp);
            }
        });

        auto t0 = std::chrono::steady_clock::now();
        client->Select(query);
        auto t1 = std::chrono::steady_clock::now();
        obj->stats.elapsed_ms =
            std::chrono::duration<double, std::milli>(t1 - t0).count();
        recordQuerySuccess(obj, sql_s, qid);
    }
    catch (const std::exception& e)
    {
        recordQueryError(obj, std::string(sql, l_sql), qid, e);
        throwClickHouseError(e, qid);
    }
}
/* }}} */

/* {{{ proto array insert(string table, array columns, array values, string query_id, array settings)
 */
PHP_METHOD(ClickHouse, insert)
{
    char *table = NULL;
    size_t l_table = 0;
    zval *columns;
    zval *values;
    char *query_id = NULL;
    size_t l_query_id = 0;
    zval *settings = NULL;

    string sql;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "szz|sa", &table, &l_table, &columns, &values, &query_id, &l_query_id, &settings) == FAILURE)
    {
        return;
    }
    std::string qid = (query_id && l_query_id > 0) ? std::string(query_id, l_query_id) : std::string();

    // Storage for return_should lives in the function frame so that an
    // exception thrown by BeginInsert / SendInsertBlock / EndInsert can
    // still reach a valid zval header to free its array_init'd HashTable.
    zval _return_should_storage;
    zval *return_should = NULL;
    zval _return_tmp_storage;
    zval *return_tmp_pending = NULL;

    clickhouse_object *obj = Z_CLICKHOUSE_P(getThis());
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

        return_should = &_return_should_storage;
        ZVAL_UNDEF(return_should);
        array_init(return_should);

        zval *fzval;
        zval *pzval;

        for(size_t i = 0; i < columns_count; i++)
        {
            zval *key = sc_zend_hash_index_find(columns_ht, i);
            return_tmp_pending = &_return_tmp_storage;
            ZVAL_UNDEF(return_tmp_pending);
            array_init(return_tmp_pending);

            ZEND_HASH_FOREACH_VAL(values_ht, pzval)
            {
                if (Z_TYPE_P(pzval) != IS_ARRAY)
                {
                    throw std::runtime_error("The insert function needs to pass in a two-dimensional array");
                }
                fzval = sc_zend_hash_index_find(Z_ARRVAL_P(pzval), i);
                if (NULL == fzval && Z_TYPE_P(key) == IS_STRING)
                {
                    fzval = sc_zend_hash_find(Z_ARRVAL_P(pzval), Z_STRVAL_P(key), Z_STRLEN_P(key));
                }
                if (NULL == fzval)
                {
                    throw std::runtime_error("The number of parameters inserted per line is inconsistent");
                }
                sc_zval_add_ref(fzval);
                add_next_index_zval(return_tmp_pending, fzval);
            }
            ZEND_HASH_FOREACH_END();

            add_next_index_zval(return_should, return_tmp_pending);
            return_tmp_pending = NULL;
        }

        getInsertSql(&sql, table, columns);

        Query insertQuery = qid.empty() ? Query(sql) : Query(sql, qid);
        applyMergedSettings(insertQuery, obj, settings);
        resetStats(obj);
        obj->stats.last_query_id = qid;
        attachProgressAndProfile(insertQuery, obj);
        Block blockQuery = client->BeginInsert(insertQuery);

        Block blockInsert;
        size_t index = 0;

        ZEND_HASH_FOREACH_VAL(Z_ARRVAL_P(return_should), pzval)
        {
            zvalToBlock(blockInsert, blockQuery, index, pzval);
            index++;
        }
        ZEND_HASH_FOREACH_END();

        auto t0 = std::chrono::steady_clock::now();
        client->SendInsertBlock(blockInsert);
        client->EndInsert();
        auto t1 = std::chrono::steady_clock::now();
        obj->stats.elapsed_ms =
            std::chrono::duration<double, std::milli>(t1 - t0).count();
        recordQuerySuccess(obj, sql, qid);
        sc_zval_ptr_dtor(&return_should);
        return_should = NULL;
    }
    catch (const std::exception& e)
    {
        if (return_tmp_pending) {
            sc_zval_ptr_dtor(&return_tmp_pending);
        }
        if (return_should) {
            sc_zval_ptr_dtor(&return_should);
        }
        recordQueryError(obj, sql, qid, e);
        throwClickHouseError(e, qid);
    }
    RETURN_TRUE;
}
/* }}} */

/* {{{ proto bool writeStart(string table, array columns, string query_id, array settings)
 */
PHP_METHOD(ClickHouse, writeStart)
{
    char *table = NULL;
    size_t l_table = 0;
    zval *columns;
    char *query_id = NULL;
    size_t l_query_id = 0;
    zval *settings = NULL;

    string sql;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "sz|sa", &table, &l_table, &columns, &query_id, &l_query_id, &settings) == FAILURE)
    {
        return;
    }
    std::string qid = (query_id && l_query_id > 0) ? std::string(query_id, l_query_id) : std::string();

    clickhouse_object *obj = Z_CLICKHOUSE_P(getThis());
    try
    {
        Client *client = getClient(obj);

        if (obj->has_insert_block)
        {
            throw std::runtime_error("The insert operation is now in progress");
        }

        getInsertSql(&sql, table, columns);

        Query insertQuery = qid.empty() ? Query(sql) : Query(sql, qid);
        applyMergedSettings(insertQuery, obj, settings);
        resetStats(obj);
        obj->stats.last_query_id = qid;
        attachProgressAndProfile(insertQuery, obj);
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

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "z", &values) == FAILURE)
    {
        return;
    }

    zval _return_should_storage;
    zval *return_should = NULL;
    zval _return_tmp_storage;
    zval *return_tmp_pending = NULL;

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
        return_should = &_return_should_storage;
        ZVAL_UNDEF(return_should);
        array_init(return_should);

        zval *fzval;
        zval *pzval;

        for(size_t i = 0; i < columns_count; i++)
        {
            return_tmp_pending = &_return_tmp_storage;
            ZVAL_UNDEF(return_tmp_pending);
            array_init(return_tmp_pending);

            ZEND_HASH_FOREACH_VAL(values_ht, pzval)
            {
                if (Z_TYPE_P(pzval) != IS_ARRAY)
                {
                    throw std::runtime_error("The insert function needs to pass in a two-dimensional array");
                }
                fzval = sc_zend_hash_index_find(Z_ARRVAL_P(pzval), i);
                if (NULL == fzval)
                {
                    throw std::runtime_error("The number of parameters inserted per line is inconsistent");
                }
                sc_zval_add_ref(fzval);
                add_next_index_zval(return_tmp_pending, fzval);
            }
            ZEND_HASH_FOREACH_END();

            add_next_index_zval(return_should, return_tmp_pending);
            return_tmp_pending = NULL;
        }


        clickhouse_object *obj = Z_CLICKHOUSE_P(getThis());
        Client *client = getClient(obj);

        if (!obj->has_insert_block) {
            throw std::runtime_error("write() called without a matching writeStart()");
        }
        Block &blockQuery = obj->insert_block;

        Block blockInsert;
        size_t index = 0;

        ZEND_HASH_FOREACH_VAL(Z_ARRVAL_P(return_should), pzval)
        {
            zvalToBlock(blockInsert, blockQuery, index, pzval);
            index++;
        }
        ZEND_HASH_FOREACH_END();

        client->SendInsertBlock(blockInsert);
        sc_zval_ptr_dtor(&return_should);
        return_should = NULL;
    }
    catch (const std::exception& e)
    {
        if (return_tmp_pending) {
            sc_zval_ptr_dtor(&return_tmp_pending);
        }
        if (return_should) {
            sc_zval_ptr_dtor(&return_should);
        }
        throwClickHouseError(e);
    }
    RETURN_TRUE;
}
/* }}} */

/* {{{ proto array insert(string table, array columns, array values)
 */
PHP_METHOD(ClickHouse, writeEnd)
{
    try
    {
        clickhouse_object *obj = Z_CLICKHOUSE_P(getThis());
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
        throwClickHouseError(e);
        return;
    }
    RETURN_TRUE;
}
/* }}} */

/* {{{ proto bool execute(string sql, array params, string query_id, array settings)
 */
PHP_METHOD(ClickHouse, execute)
{
    char *sql = NULL;
    size_t l_sql = 0;
    zval* params = NULL;
    char *query_id = NULL;
    size_t l_query_id = 0;
    zval *settings = NULL;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "s|zsa", &sql, &l_sql, &params, &query_id, &l_query_id, &settings) == FAILURE)
    {
        return;
    }
    std::string qid = (query_id && l_query_id > 0) ? std::string(query_id, l_query_id) : std::string();

    clickhouse_object *obj = Z_CLICKHOUSE_P(getThis());
    try
    {
        Client *client = getClient(obj);

        if (obj->has_insert_block)
        {
            throw std::runtime_error("The insert operation is now in progress");
        }

        string sql_s = (string)sql;
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

        auto t0 = std::chrono::steady_clock::now();
        client->Execute(query);
        auto t1 = std::chrono::steady_clock::now();
        obj->stats.elapsed_ms =
            std::chrono::duration<double, std::milli>(t1 - t0).count();
        recordQuerySuccess(obj, sql_s, qid);
    }
    catch (const std::exception& e)
    {
        recordQueryError(obj, std::string(sql, l_sql), qid, e);
        throwClickHouseError(e, qid);
    }
    RETURN_TRUE;
}
/* }}} */

/* {{{ proto bool setSettings(array settings)
 *
 * Replace the client-wide settings map. Per-call settings supplied to
 * select/insert/execute/writeStart override these. Pass an empty array
 * to clear.
 */
PHP_METHOD(ClickHouse, setSettings)
{
    zval *arr;
    if (zend_parse_parameters(ZEND_NUM_ARGS(), "a", &arr) == FAILURE) {
        return;
    }
    clickhouse_object *obj = Z_CLICKHOUSE_P(getThis());
    std::unordered_map<std::string, std::string> m;
    HashTable *ht = Z_ARRVAL_P(arr);
    zval *vz;
    zend_string *zk;
    zend_ulong nk;
    ZEND_HASH_FOREACH_KEY_VAL(ht, nk, zk, vz) {
        (void)nk;
        if (!zk) continue;
        m[std::string(ZSTR_VAL(zk), ZSTR_LEN(zk))] = formatScalarParam(vz);
    } ZEND_HASH_FOREACH_END();
    obj->settings = std::move(m);
    RETURN_TRUE;
}
/* }}} */

/* {{{ proto bool setProgressCallback(?callable callback)
 *
 * Register a callable invoked for each Progress packet the server
 * sends during select/execute. Pass null to remove. Callback receives
 * a single associative array: rows, bytes, total_rows, written_rows,
 * written_bytes.
 */
PHP_METHOD(ClickHouse, setProgressCallback)
{
    zval *cb;
    if (zend_parse_parameters(ZEND_NUM_ARGS(), "z", &cb) == FAILURE) {
        return;
    }
    clickhouse_object *obj = Z_CLICKHOUSE_P(getThis());

    if (Z_TYPE(obj->progress_callback) != IS_UNDEF) {
        zval_ptr_dtor(&obj->progress_callback);
        ZVAL_UNDEF(&obj->progress_callback);
    }

    if (Z_TYPE_P(cb) == IS_NULL) {
        RETURN_TRUE;
    }
    if (!zend_is_callable(cb, 0, NULL)) {
        zend_throw_exception(clickhouse_exception_ce,
            "setProgressCallback expects a callable or null", 0);
        return;
    }
    ZVAL_COPY(&obj->progress_callback, cb);
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
    if (zend_parse_parameters(ZEND_NUM_ARGS(), "z", &cb) == FAILURE) {
        return;
    }
    clickhouse_object *obj = Z_CLICKHOUSE_P(getThis());

    if (Z_TYPE(obj->profile_callback) != IS_UNDEF) {
        zval_ptr_dtor(&obj->profile_callback);
        ZVAL_UNDEF(&obj->profile_callback);
    }
    if (Z_TYPE_P(cb) == IS_NULL) {
        RETURN_TRUE;
    }
    if (!zend_is_callable(cb, 0, NULL)) {
        zend_throw_exception(clickhouse_exception_ce,
            "setProfileCallback expects a callable or null", 0);
        return;
    }
    ZVAL_COPY(&obj->profile_callback, cb);
    RETURN_TRUE;
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
    ClientStats &st = obj->stats;
    array_init(return_value);
    add_assoc_long(return_value, "rows_read", (zend_long)st.rows_read);
    add_assoc_long(return_value, "bytes_read", (zend_long)st.bytes_read);
    add_assoc_long(return_value, "total_rows", (zend_long)st.total_rows);
    add_assoc_long(return_value, "written_rows", (zend_long)st.written_rows);
    add_assoc_long(return_value, "written_bytes", (zend_long)st.written_bytes);
    add_assoc_long(return_value, "blocks", (zend_long)st.blocks);
    add_assoc_long(return_value, "rows_before_limit", (zend_long)st.rows_before_limit);
    add_assoc_bool(return_value, "applied_limit", st.applied_limit ? 1 : 0);
    add_assoc_double(return_value, "elapsed_ms", st.elapsed_ms);
    add_assoc_stringl(return_value, "query_id", st.last_query_id.data(), st.last_query_id.size());
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
    char *table = NULL;
    size_t l_table = 0;
    zval *rows;
    char *query_id = NULL;
    size_t l_query_id = 0;
    zval *settings = NULL;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "sz|sa", &table, &l_table, &rows, &query_id, &l_query_id, &settings) == FAILURE) {
        return;
    }
    std::string qid = (query_id && l_query_id > 0) ? std::string(query_id, l_query_id) : std::string();

    try {
        if (Z_TYPE_P(rows) != IS_ARRAY) {
            throw std::runtime_error("insertAssoc: rows must be an array");
        }
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

        /* Forward to insert() by call_user_function so we get exactly
         * the same code path (validation, settings merge, stats, etc.)
         * and keep this wrapper a thin layer. */
        zval method, args[5], retval;
        ZVAL_STRING(&method, "insert");
        ZVAL_STRINGL(&args[0], table, l_table);
        ZVAL_COPY_VALUE(&args[1], &columns_zv);
        ZVAL_COPY_VALUE(&args[2], &values_zv);
        if (!qid.empty()) {
            ZVAL_STRINGL(&args[3], qid.c_str(), qid.size());
        } else {
            ZVAL_EMPTY_STRING(&args[3]);
        }
        if (settings) {
            ZVAL_COPY_VALUE(&args[4], settings);
            Z_TRY_ADDREF_P(settings);
        } else {
            array_init(&args[4]);
        }
        ZVAL_NULL(&retval);
        int n = settings ? 5 : 5;
        call_user_function(NULL, getThis(), &method, &retval, n, args);
        zval_ptr_dtor(&method);
        zval_ptr_dtor(&args[0]);
        zval_ptr_dtor(&args[1]);
        zval_ptr_dtor(&args[2]);
        zval_ptr_dtor(&args[3]);
        zval_ptr_dtor(&args[4]);
        if (EG(exception)) {
            zval_ptr_dtor(&retval);
            return;
        }
        zval_ptr_dtor(&retval);
    } catch (const std::exception &e) {
        throwClickHouseError(e, qid);
        return;
    }
    RETURN_TRUE;
}
/* }}} */

/*
 * SQL-helper one-liners. Each builds a small SELECT and reuses the
 * select() machinery via call_user_function so settings, progress,
 * and stats also apply to these.
 */
static void runHelperSelect(zval *return_value, zval *this_obj, const std::string &sql, zend_long fetch_mode)
{
    zval method, args[3], retval;
    ZVAL_STRING(&method, "select");
    ZVAL_STRINGL(&args[0], sql.c_str(), sql.size());
    array_init(&args[1]);
    ZVAL_LONG(&args[2], fetch_mode);
    ZVAL_NULL(&retval);
    call_user_function(NULL, this_obj, &method, &retval, 3, args);
    zval_ptr_dtor(&method);
    zval_ptr_dtor(&args[0]);
    zval_ptr_dtor(&args[1]);
    if (EG(exception)) {
        zval_ptr_dtor(&retval);
        return;
    }
    ZVAL_COPY_VALUE(return_value, &retval);
}

static bool runHelperExec(zval *this_obj, const std::string &sql)
{
    zval method, args[1], retval;
    ZVAL_STRING(&method, "execute");
    ZVAL_STRINGL(&args[0], sql.c_str(), sql.size());
    ZVAL_NULL(&retval);
    call_user_function(NULL, this_obj, &method, &retval, 1, args);
    zval_ptr_dtor(&method);
    zval_ptr_dtor(&args[0]);
    bool ok = !EG(exception) && Z_TYPE(retval) == IS_TRUE;
    zval_ptr_dtor(&retval);
    return ok;
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
    char *db = NULL;
    size_t l_db = 0;
    if (zend_parse_parameters(ZEND_NUM_ARGS(), "|s!", &db, &l_db) == FAILURE) {
        return;
    }
    std::string dbname = (db && l_db > 0) ? std::string(db, l_db) : currentDatabase(getThis());
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
    char *db = NULL;
    size_t l_db = 0;
    if (zend_parse_parameters(ZEND_NUM_ARGS(), "|s!", &db, &l_db) == FAILURE) {
        return;
    }
    std::string dbname = (db && l_db > 0) ? std::string(db, l_db) : currentDatabase(getThis());
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
    char *table = NULL;
    size_t l_table = 0;
    if (zend_parse_parameters(ZEND_NUM_ARGS(), "s", &table, &l_table) == FAILURE) {
        return;
    }
    std::string tname(table, l_table);
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
    char *db = NULL, *like = NULL;
    size_t l_db = 0, l_like = 0;
    if (zend_parse_parameters(ZEND_NUM_ARGS(), "|s!s!", &db, &l_db, &like, &l_like) == FAILURE) {
        return;
    }
    std::string dbname = (db && l_db > 0) ? std::string(db, l_db) : currentDatabase(getThis());
    try {
        validateIdentifier(dbname.c_str(), dbname.size(), "database name", false);
    } catch (const std::exception &e) {
        throwClickHouseError(e);
        return;
    }
    std::string sql = "SELECT name FROM system.tables WHERE database = '" + dbname + "'";
    if (like && l_like > 0) {
        /* Validate the LIKE pattern against the same character set as
         * placeholder values: identifier chars plus % and _ would be the
         * minimum, but we already reject quotes/backslashes there so
         * the only addition is %. */
        for (size_t i = 0; i < l_like; ++i) {
            unsigned char c = (unsigned char)like[i];
            bool ok =
                (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
                (c >= '0' && c <= '9') ||
                c == '_' || c == '.' || c == '%';
            if (!ok) {
                zend_throw_exception(clickhouse_exception_ce,
                    "showTables: LIKE pattern contains an unsafe character", 0);
                return;
            }
        }
        sql += " AND name LIKE '" + std::string(like, l_like) + "'";
    }
    sql += " ORDER BY name";
    runHelperSelect(return_value, getThis(), sql, SC_FETCH_COLUMN);
}
/* }}} */

/* {{{ proto string showCreateTable(string table)
 */
PHP_METHOD(ClickHouse, showCreateTable)
{
    char *table = NULL;
    size_t l_table = 0;
    if (zend_parse_parameters(ZEND_NUM_ARGS(), "s", &table, &l_table) == FAILURE) {
        return;
    }
    std::string tname(table, l_table);
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
    if (zend_parse_parameters(ZEND_NUM_ARGS(), "|b", &enabled) == FAILURE) {
        return;
    }
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
    char *sql = NULL;
    size_t l_sql = 0;
    zval *params = NULL;
    char *query_id = NULL;
    size_t l_query_id = 0;
    zval *settings = NULL;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "s|asa", &sql, &l_sql, &params, &query_id, &l_query_id, &settings) == FAILURE) {
        return;
    }
    std::string qid = (query_id && l_query_id > 0) ? std::string(query_id, l_query_id) : std::string();
    clickhouse_object *obj = Z_CLICKHOUSE_P(getThis());

    object_init_ex(return_value, clickhouse_iter_ce);
    clickhouse_iter_object *iter = Z_CLICKHOUSE_ITER_P(return_value);

    try {
        Client *client = getClient(obj);

        if (obj->has_insert_block) {
            throw std::runtime_error("The insert operation is now in progress");
        }

        std::string sql_s = (std::string)sql;
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

        query.OnData([iter](const Block &block) {
            if (block.GetRowCount() == 0 || block.GetColumnCount() == 0) return;
            iter->total_rows += block.GetRowCount();
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
        recordQueryError(obj, std::string(sql, l_sql), qid, e);
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
    char *sql = NULL;
    size_t l_sql = 0;
    zval *cb = NULL;
    zval *params = NULL;
    char *query_id = NULL;
    size_t l_query_id = 0;
    zval *settings = NULL;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "sz|asa", &sql, &l_sql, &cb, &params, &query_id, &l_query_id, &settings) == FAILURE) {
        return;
    }

    if (!zend_is_callable(cb, 0, NULL)) {
        sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce,
            "Argument 2 passed to selectStreamCallback must be callable", 0);
        return;
    }

    std::string qid = (query_id && l_query_id > 0) ? std::string(query_id, l_query_id) : std::string();
    clickhouse_object *obj = Z_CLICKHOUSE_P(getThis());

    try {
        Client *client = getClient(obj);

        if (obj->has_insert_block) {
            throw std::runtime_error("The insert operation is now in progress");
        }

        std::string sql_s = (std::string)sql;
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

        query.OnData([cb](const Block &block) {
            if (block.GetRowCount() == 0 || block.GetColumnCount() == 0) return;
            for (size_t row = 0; row < block.GetRowCount(); ++row) {
                zval row_zv;
                array_init(&row_zv);
                for (size_t col = 0; col < block.GetColumnCount(); ++col) {
                    convertToZval(&row_zv, block[col], row, block.GetColumnName(col), 0, 0);
                }
                zval args[1], retval;
                ZVAL_NULL(&retval);
                ZVAL_COPY_VALUE(&args[0], &row_zv);
                call_user_function(NULL, NULL, cb, &retval, 1, args);
                zval_ptr_dtor(&args[0]);
                zval_ptr_dtor(&retval);
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
        recordQueryError(obj, std::string(sql, l_sql), qid, e);
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
    for (size_t col = 0; col < block.GetColumnCount(); ++col) {
        convertToZval(return_value, block[col], iter->row_idx, block.GetColumnName(col), 0, 0);
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
    char *db = NULL, *table = NULL;
    size_t l_db = 0, l_table = 0;
    if (zend_parse_parameters(ZEND_NUM_ARGS(), "ss", &db, &l_db, &table, &l_table) == FAILURE) {
        return;
    }
    try {
        validateIdentifier(db, l_db, "database name", false);
        validateIdentifier(table, l_table, "table name", false);
    } catch (const std::exception &e) {
        throwClickHouseError(e);
        return;
    }
    std::string sql =
        "SELECT count() AS c FROM system.tables WHERE database = '" +
        std::string(db, l_db) + "' AND name = '" + std::string(table, l_table) + "'";
    zval row;
    runHelperSelectFirstRow(&row, getThis(), sql);
    if (EG(exception)) return;
    bool exists = false;
    if (Z_TYPE(row) == IS_ARRAY) {
        zval *cnt = zend_hash_str_find(Z_ARRVAL(row), "c", sizeof("c") - 1);
        if (cnt) {
            convert_to_long(cnt);
            exists = (Z_LVAL_P(cnt) > 0);
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
    char *table = NULL;
    size_t l_table = 0;
    if (zend_parse_parameters(ZEND_NUM_ARGS(), "s", &table, &l_table) == FAILURE) {
        return;
    }
    std::string tname(table, l_table);
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
    char *table = NULL;
    size_t l_table = 0;
    if (zend_parse_parameters(ZEND_NUM_ARGS(), "s", &table, &l_table) == FAILURE) {
        return;
    }
    try {
        validateIdentifier(table, l_table, "table name", true);
    } catch (const std::exception &e) {
        throwClickHouseError(e);
        return;
    }
    std::string sql = "TRUNCATE TABLE " + std::string(table, l_table);
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
    char *table = NULL, *part = NULL;
    size_t l_table = 0, l_part = 0;
    if (zend_parse_parameters(ZEND_NUM_ARGS(), "ss", &table, &l_table, &part, &l_part) == FAILURE) {
        return;
    }
    try {
        validateIdentifier(table, l_table, "table name", true);
    } catch (const std::exception &e) {
        throwClickHouseError(e);
        return;
    }
    /* Guard against control characters that could break the literal. */
    for (size_t i = 0; i < l_part; ++i) {
        unsigned char c = (unsigned char)part[i];
        if (c < 0x20) {
            sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce,
                "dropPartition: partition value contains a control character", 0);
            return;
        }
    }
    /* SQL-standard single-quote doubling. */
    std::string escaped;
    escaped.reserve(l_part + 4);
    for (size_t i = 0; i < l_part; ++i) {
        if (part[i] == '\'') escaped += "''";
        else escaped += part[i];
    }
    std::string sql = "ALTER TABLE " + std::string(table, l_table) +
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

/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: noet sw=4 ts=4 fdm=marker
 * vim<600: noet sw=4 ts=4
 */
