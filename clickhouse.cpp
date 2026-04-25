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
#include "php7_wrapper.h"
}

#include "php_clickhouse.h"

#include "lib/clickhouse-cpp/clickhouse/client.h"
#include "lib/clickhouse-cpp/clickhouse/error_codes.h"
#include "lib/clickhouse-cpp/clickhouse/types/type_parser.h"
#include "typesToPhp.hpp"
#include <iostream>
#include <map>
#include <sstream>

using namespace clickhouse;
using namespace std;

zend_class_entry *clickhouse_ce, *clickhouse_exception_ce;
map<int, Client*> clientMap;
map<int, Block> clientInsertBlack;

static std::string sanitizeError(const char *what);

#ifdef COMPILE_DL_CLICKHOUSE
extern "C" {
    ZEND_GET_MODULE(clickhouse)
}
#endif

// PHP_FUNCTION(clickhouse_version)
// {
//     SC_RETURN_STRINGL(PHP_CLICKHOUSE_VERSION, strlen(PHP_CLICKHOUSE_VERSION));
// }

static PHP_METHOD(CLICKHOUSE_RES_NAME, __construct);
static PHP_METHOD(CLICKHOUSE_RES_NAME, __destruct);
static PHP_METHOD(CLICKHOUSE_RES_NAME, select);
static PHP_METHOD(CLICKHOUSE_RES_NAME, insert);
static PHP_METHOD(CLICKHOUSE_RES_NAME, writeStart);
static PHP_METHOD(CLICKHOUSE_RES_NAME, write);
static PHP_METHOD(CLICKHOUSE_RES_NAME, writeEnd);
static PHP_METHOD(CLICKHOUSE_RES_NAME, execute);
static PHP_METHOD(CLICKHOUSE_RES_NAME, ping);

ZEND_BEGIN_ARG_INFO_EX(clickhouse_construct, 0, 0, 1)
ZEND_ARG_INFO(0, connectParams)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(clickhouse_destruct, 0, 0, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(clickhouse_select, 0, 0, 1)
ZEND_ARG_INFO(0, sql)
ZEND_ARG_INFO(0, params)
ZEND_ARG_INFO(0, fetch_mode)
ZEND_ARG_INFO(0, query_id)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(clickhouse_insert, 0, 0, 3)
ZEND_ARG_INFO(0, table)
ZEND_ARG_INFO(0, columns)
ZEND_ARG_INFO(0, values)
ZEND_ARG_INFO(0, query_id)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(clickhouse_writeStart, 0, 0, 2)
ZEND_ARG_INFO(0, table)
ZEND_ARG_INFO(0, columns)
ZEND_ARG_INFO(0, query_id)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(clickhouse_write, 0, 0, 1)
ZEND_ARG_INFO(0, values)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(clickhouse_writeEnd, 0, 0, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(clickhouse_execute, 0, 0, 1)
ZEND_ARG_INFO(0, sql)
ZEND_ARG_INFO(0, params)
ZEND_ARG_INFO(0, query_id)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(clickhouse_ping, 0, 0, 0)
ZEND_END_ARG_INFO()

/* {{{ clickhouse_functions[] */
const zend_function_entry clickhouse_functions[] =
{
    //PHP_FE(clickhouse_version,	NULL)
    PHP_FE_END
};
/* }}} */

const zend_function_entry clickhouse_methods[] =
{
    PHP_ME(CLICKHOUSE_RES_NAME, __construct,   clickhouse_construct, ZEND_ACC_PUBLIC | ZEND_ACC_CTOR)
    PHP_ME(CLICKHOUSE_RES_NAME, __destruct,    clickhouse_destruct, ZEND_ACC_PUBLIC)
    PHP_ME(CLICKHOUSE_RES_NAME, select,        clickhouse_select, ZEND_ACC_PUBLIC)
    PHP_ME(CLICKHOUSE_RES_NAME, insert,        clickhouse_insert, ZEND_ACC_PUBLIC)
    PHP_ME(CLICKHOUSE_RES_NAME, writeStart,    clickhouse_writeStart, ZEND_ACC_PUBLIC)
    PHP_ME(CLICKHOUSE_RES_NAME, write,         clickhouse_write, ZEND_ACC_PUBLIC)
    PHP_ME(CLICKHOUSE_RES_NAME, writeEnd,      clickhouse_writeEnd, ZEND_ACC_PUBLIC)
    PHP_ME(CLICKHOUSE_RES_NAME, execute,       clickhouse_execute, ZEND_ACC_PUBLIC)
    PHP_ME(CLICKHOUSE_RES_NAME, ping,          clickhouse_ping, ZEND_ACC_PUBLIC)
    PHP_FE_END
};

#define REGISTER_CLICKHOUSE_CONST_LONG(const_name, value) \
	zend_declare_class_constant_long(clickhouse_ce, const_name, sizeof(const_name)-1, (zend_long)value);

/* {{{ PHP_MINIT_FUNCTION
 */
PHP_MINIT_FUNCTION(clickhouse)
{
#ifdef ZTS
    // The wrapper keeps Client and in-progress Block state in process-
    // global maps keyed on Z_OBJ_HANDLE. Under ZTS the same handle space
    // is shared across worker threads, so two requests can race or
    // cross-wire each other's clients. Refusing to load is safer than
    // shipping a quiet thread-safety bug; if you genuinely need ZTS, the
    // state needs to move into per-thread storage first.
    php_error(E_CORE_ERROR,
        "php_clickhouse: ZTS PHP builds are not supported. "
        "Rebuild PHP with --disable-zts or use the NTS variant.");
    return FAILURE;
#endif

    zend_class_entry ce_main, ce_exception;
    INIT_CLASS_ENTRY(ce_main, CLICKHOUSE_RES_NAME, clickhouse_methods);
    INIT_CLASS_ENTRY(ce_exception, CLICKHOUSE_EXCEPTION_NAME, NULL);

    clickhouse_ce = zend_register_internal_class_ex(&ce_main, NULL);
    clickhouse_exception_ce = zend_register_internal_class_ex(&ce_exception, zend_ce_exception);

    /* Back-compat aliases for the original SeasClick name. Deprecated;
     * removed in the next major release. */
    zend_register_class_alias(CLICKHOUSE_RES_NAME_LEGACY, clickhouse_ce);
    zend_register_class_alias(CLICKHOUSE_EXCEPTION_NAME_LEGACY, clickhouse_exception_ce);

    zend_declare_property_stringl(clickhouse_ce, "host", strlen("host"), "127.0.0.1", sizeof("127.0.0.1") - 1, ZEND_ACC_PROTECTED);
    zend_declare_property_long(clickhouse_ce, "port", strlen("port"), 9000, ZEND_ACC_PROTECTED);
    zend_declare_property_stringl(clickhouse_ce, "database", strlen("database"), "default", sizeof("default") - 1, ZEND_ACC_PROTECTED);
    zend_declare_property_null(clickhouse_ce, "user", strlen("user"), ZEND_ACC_PROTECTED);
    /* No "passwd" property: keeping the secret out of get_object_vars,
     * var_dump, serialize, and reflection by simply not storing it. */
    zend_declare_property_bool(clickhouse_ce, "compression", strlen("compression"), false, ZEND_ACC_PROTECTED);
    zend_declare_property_long(clickhouse_ce, "retry_timeout", strlen("retry_timeout"), 5, ZEND_ACC_PROTECTED);
    zend_declare_property_long(clickhouse_ce, "retry_count", strlen("retry_count"), 1, ZEND_ACC_PROTECTED);
    zend_declare_property_long(clickhouse_ce, "receive_timeout", strlen("receive_timeout"), 0, ZEND_ACC_PROTECTED);
    zend_declare_property_long(clickhouse_ce, "connect_timeout", strlen("connect_timeout"), 5, ZEND_ACC_PROTECTED);

    REGISTER_CLICKHOUSE_CONST_LONG("FETCH_ONE", (zend_long)SC_FETCH_ONE);
    REGISTER_CLICKHOUSE_CONST_LONG("FETCH_KEY_PAIR", (zend_long)SC_FETCH_KEY_PAIR);
    REGISTER_CLICKHOUSE_CONST_LONG("DATE_AS_STRINGS", (zend_long)SC_FETCH_DATE_AS_STRINGS);
    REGISTER_CLICKHOUSE_CONST_LONG("FETCH_COLUMN", (zend_long)SC_FETCH_COLUMN);

    clickhouse_ce->ce_flags |= ZEND_ACC_FINAL;
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
PHP_METHOD(CLICKHOUSE_RES_NAME, __construct)
{
    zval *connectParams;

#ifndef FAST_ZPP
    if (zend_parse_parameters(ZEND_NUM_ARGS(), "z", &connectParams) == FAILURE)
    {
        return;
    }
#else
#undef IS_UNDEF
#define IS_UNDEF Z_EXPECTED_LONG
    ZEND_PARSE_PARAMETERS_START(1, 1)
    Z_PARAM_ARRAY(connectParams)
    ZEND_PARSE_PARAMETERS_END();
#undef IS_UNDEF
#define IS_UNDEF 0
#endif

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
    if (php_array_get_value(_ht, "tcp_nodelay", value)) {
        convert_to_boolean(value);
        Options = Options.TcpNoDelay(Z_LVAL_P(value) != 0);
    }
    if (php_array_get_value(_ht, "tcp_keepalive", value)) {
        convert_to_boolean(value);
        Options = Options.TcpKeepAlive(Z_LVAL_P(value) != 0);
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
        int key = (int)Z_OBJ_HANDLE(*this_obj);
        if (clientMap.count(key)) {
            throw std::runtime_error("ClickHouse object is already constructed");
        }
        Client *client = new Client(Options);
        try {
            clientMap.insert(std::pair<int, Client*>(key, client));
        } catch (...) {
            delete client;
            throw;
        }
    }
    catch (const std::exception& e)
    {
        sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce, sanitizeError(e.what()).c_str(), 0);
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
 * Resolve the Client* for the given object handle, or throw if no entry
 * exists. The miss case happens when __construct threw before inserting,
 * or when a method runs after __destruct removed the entry.
 */
static Client* getClient(int key)
{
    auto it = clientMap.find(key);
    if (it == clientMap.end()) {
        throw std::runtime_error("ClickHouse client is not initialized");
    }
    return it->second;
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
 * Substitute {name} placeholders in `sql` with values from `params_ht`.
 *
 * Placeholder values must consist only of identifier characters, digits,
 * decimal points, commas, parentheses, asterisks, plus/minus signs, or
 * whitespace. That set covers identifiers, comma-separated column lists,
 * numeric literals, and `count(*)`-style expressions but rejects every
 * character a SQL injection would need: quotes, semicolons, backslashes,
 * angle brackets, etc. A non-conforming value throws ClickHouseException.
 *
 * If a placeholder name isn't present in the SQL the call also throws
 * rather than silently passing through. Multiple occurrences of the same
 * placeholder are all replaced.
 */
static void applyPlaceholders(string &sql, HashTable *params_ht)
{
    zval *pzval;
    zend_string *zk;
    zend_ulong nk;

    ZEND_HASH_FOREACH_KEY_VAL(params_ht, nk, zk, pzval) {
        (void)nk;
        if (!zk) {
            throw std::runtime_error("Placeholder array keys must be strings");
        }
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
                    "Placeholder value for {" + std::string(ZSTR_VAL(zk), ZSTR_LEN(zk)) +
                    "} contains an unsafe character");
            }
        }
        std::string needle = "{" + std::string(ZSTR_VAL(zk), ZSTR_LEN(zk)) + "}";
        size_t pos = sql.find(needle);
        if (pos == std::string::npos) {
            throw std::runtime_error(
                "Placeholder {" + std::string(ZSTR_VAL(zk), ZSTR_LEN(zk)) +
                "} does not appear in the SQL");
        }
        std::string repl(val, vlen);
        while (pos != std::string::npos) {
            sql.replace(pos, needle.size(), repl);
            pos = sql.find(needle, pos + repl.size());
        }
    } ZEND_HASH_FOREACH_END();
}

/* {{{ proto bool ping()
 */
PHP_METHOD(CLICKHOUSE_RES_NAME, ping)
{
    int key = Z_OBJ_HANDLE(*getThis());
    try {
        Client *client = getClient(key);
        client->Ping();
    } catch (const std::exception& e) {
        sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce, sanitizeError(e.what()).c_str(), 0);
        return;
    }
    RETURN_TRUE;
}

/* {{{ proto array select(string sql, array params, int mode)
 */
PHP_METHOD(CLICKHOUSE_RES_NAME, select)
{
    char *sql = NULL;
    size_t l_sql = 0;
    zval* params = NULL;
    zend_long fetch_mode = 0;
    char *query_id = NULL;
    size_t l_query_id = 0;

#ifndef FAST_ZPP
    if (zend_parse_parameters(ZEND_NUM_ARGS(), "s|zls", &sql, &l_sql, &params, &fetch_mode, &query_id, &l_query_id) == FAILURE)
    {
        return;
    }
#else
#undef IS_UNDEF
#define IS_UNDEF Z_EXPECTED_LONG
    ZEND_PARSE_PARAMETERS_START(1, 4)
    Z_PARAM_STRING(sql, l_sql)
    Z_PARAM_OPTIONAL
    Z_PARAM_ARRAY(params)
    Z_PARAM_LONG(fetch_mode)
    Z_PARAM_STRING(query_id, l_query_id)
    ZEND_PARSE_PARAMETERS_END();
#undef IS_UNDEF
#define IS_UNDEF 0
#endif
    try
    {
        int key = Z_OBJ_HANDLE(*getThis());
        Client *client = getClient(key);

        if (clientInsertBlack.count(key))
        {
            throw std::runtime_error("The insert operation is now in progress");
        }

        string sql_s = (string)sql;
        if (ZEND_NUM_ARGS() > 1 && params != NULL)
        {
            if (Z_TYPE_P(params) != IS_ARRAY)
            {
                throw std::runtime_error("The second argument to the select function must be an array");
            }

            applyPlaceholders(sql_s, Z_ARRVAL_P(params));
        }

        if (!(fetch_mode & SC_FETCH_ONE)) {
            array_init(return_value);
        }

        std::string qid = (query_id && l_query_id > 0) ? std::string(query_id, l_query_id) : std::string();
        // Track whether FETCH_ONE has already emitted a row, so a multi-
        // block result returns the very first row and not the first row
        // of the last block.
        bool fetched_one = false;
        auto select_cb = [return_value, fetch_mode, &fetched_one](const Block &block) {
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
        };
        if (!qid.empty()) {
            client->Select(sql_s, qid, select_cb);
        } else {
            client->Select(sql_s, select_cb);
        }
    }
    catch (const std::exception& e)
    {
        sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce, sanitizeError(e.what()).c_str(), 0);
    }
}
/* }}} */

/* {{{ proto array insert(string table, array columns, array values)
 */
PHP_METHOD(CLICKHOUSE_RES_NAME, insert)
{
    char *table = NULL;
    size_t l_table = 0;
    zval *columns;
    zval *values;
    char *query_id = NULL;
    size_t l_query_id = 0;

    string sql;

#ifndef FAST_ZPP
    if (zend_parse_parameters(ZEND_NUM_ARGS(), "szz|s", &table, &l_table, &columns, &values, &query_id, &l_query_id) == FAILURE)
    {
        return;
    }
#else
#undef IS_UNDEF
#define IS_UNDEF Z_EXPECTED_LONG
    ZEND_PARSE_PARAMETERS_START(3, 4)
    Z_PARAM_STRING(table, l_table)
    Z_PARAM_ARRAY(columns)
    Z_PARAM_ARRAY(values)
    Z_PARAM_OPTIONAL
    Z_PARAM_STRING(query_id, l_query_id)
    ZEND_PARSE_PARAMETERS_END();
#undef IS_UNDEF
#define IS_UNDEF 0
#endif

    // Storage for return_should lives in the function frame so that an
    // exception thrown by BeginInsert / SendInsertBlock / EndInsert can
    // still reach a valid zval header to free its array_init'd HashTable.
    zval _return_should_storage;
    zval *return_should = NULL;
    zval _return_tmp_storage;
    zval *return_tmp_pending = NULL;

    try
    {
        int key = Z_OBJ_HANDLE(*getThis());
        Client *client = getClient(key);

        if (clientInsertBlack.count(key))
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

        Block blockQuery = (query_id && l_query_id > 0)
            ? client->BeginInsert(sql, std::string(query_id, l_query_id))
            : client->BeginInsert(sql);

        Block blockInsert;
        size_t index = 0;

        ZEND_HASH_FOREACH_VAL(Z_ARRVAL_P(return_should), pzval)
        {
            zvalToBlock(blockInsert, blockQuery, index, pzval);
            index++;
        }
        ZEND_HASH_FOREACH_END();

        client->SendInsertBlock(blockInsert);
        client->EndInsert();
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
        sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce, sanitizeError(e.what()).c_str(), 0);
    }
    RETURN_TRUE;
}
/* }}} */

/* {{{ proto array insert(string table, array columns, array values)
 */
PHP_METHOD(CLICKHOUSE_RES_NAME, writeStart)
{
    char *table = NULL;
    size_t l_table = 0;
    zval *columns;
    char *query_id = NULL;
    size_t l_query_id = 0;

    string sql;

#ifndef FAST_ZPP
    if (zend_parse_parameters(ZEND_NUM_ARGS(), "sz|s", &table, &l_table, &columns, &query_id, &l_query_id) == FAILURE)
    {
        return;
    }
#else
#undef IS_UNDEF
#define IS_UNDEF Z_EXPECTED_LONG
    ZEND_PARSE_PARAMETERS_START(2, 3)
    Z_PARAM_STRING(table, l_table)
    Z_PARAM_ARRAY(columns)
    Z_PARAM_OPTIONAL
    Z_PARAM_STRING(query_id, l_query_id)
    ZEND_PARSE_PARAMETERS_END();
#undef IS_UNDEF
#define IS_UNDEF 0
#endif

    try
    {
        int key = Z_OBJ_HANDLE(*getThis());
        Client *client = getClient(key);

        if (clientInsertBlack.count(key))
        {
            throw std::runtime_error("The insert operation is now in progress");
        }

        getInsertSql(&sql, table, columns);

        Block blockQuery = (query_id && l_query_id > 0)
            ? client->BeginInsert(sql, std::string(query_id, l_query_id))
            : client->BeginInsert(sql);

        clientInsertBlack.insert(std::pair<int, Block>(key, blockQuery));
    }
    catch (const std::exception& e)
    {
        sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce, sanitizeError(e.what()).c_str(), 0);
    }
    RETURN_TRUE;
}
/* }}} */

/* {{{ proto array insert(string table, array columns, array values)
 */
PHP_METHOD(CLICKHOUSE_RES_NAME, write)
{
    zval *values;

#ifndef FAST_ZPP
    if (zend_parse_parameters(ZEND_NUM_ARGS(), "z", &values) == FAILURE)
    {
        return;
    }
#else
#undef IS_UNDEF
#define IS_UNDEF Z_EXPECTED_LONG
    ZEND_PARSE_PARAMETERS_START(1, 1)
    Z_PARAM_ARRAY(values)
    ZEND_PARSE_PARAMETERS_END();
#undef IS_UNDEF
#define IS_UNDEF 0
#endif

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


        int key = Z_OBJ_HANDLE(*getThis());
        Client *client = getClient(key);

        auto blockIt = clientInsertBlack.find(key);
        if (blockIt == clientInsertBlack.end()) {
            throw std::runtime_error("write() called without a matching writeStart()");
        }
        Block &blockQuery = blockIt->second;

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
        sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce, sanitizeError(e.what()).c_str(), 0);
    }
    RETURN_TRUE;
}
/* }}} */

/* {{{ proto array insert(string table, array columns, array values)
 */
PHP_METHOD(CLICKHOUSE_RES_NAME, writeEnd)
{
    try
    {
        int key = Z_OBJ_HANDLE(*getThis());
        Client *client = getClient(key);
        if (!clientInsertBlack.count(key)) {
            throw std::runtime_error("writeEnd() called without a matching writeStart()");
        }

        client->EndInsert();
        clientInsertBlack.erase(key);
    }
    catch (const std::exception& e)
    {
        sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce, sanitizeError(e.what()).c_str(), 0);
        return;
    }
    RETURN_TRUE;
}
/* }}} */

/* {{{ proto bool execute(string sql, array params)
 */
PHP_METHOD(CLICKHOUSE_RES_NAME, execute)
{
    char *sql = NULL;
    size_t l_sql = 0;
    zval* params = NULL;
    char *query_id = NULL;
    size_t l_query_id = 0;

#ifndef FAST_ZPP
    if (zend_parse_parameters(ZEND_NUM_ARGS(), "s|zs", &sql, &l_sql, &params, &query_id, &l_query_id) == FAILURE)
    {
        return;
    }
#else
#undef IS_UNDEF
#define IS_UNDEF Z_EXPECTED_LONG
    ZEND_PARSE_PARAMETERS_START(1, 3)
    Z_PARAM_STRING(sql, l_sql)
    Z_PARAM_OPTIONAL
    Z_PARAM_ARRAY(params)
    Z_PARAM_STRING(query_id, l_query_id)
    ZEND_PARSE_PARAMETERS_END();
#undef IS_UNDEF
#define IS_UNDEF 0
#endif

    try
    {
        int key = Z_OBJ_HANDLE(*getThis());
        Client *client = getClient(key);

        if (clientInsertBlack.count(key))
        {
            throw std::runtime_error("The insert operation is now in progress");
        }

        string sql_s = (string)sql;
        if (ZEND_NUM_ARGS() > 1 && params != NULL)
        {
            if (Z_TYPE_P(params) != IS_ARRAY)
            {
                throw std::runtime_error("The second argument to the select function must be an array");
            }

            applyPlaceholders(sql_s, Z_ARRVAL_P(params));
        }

        if (query_id && l_query_id > 0) {
            client->Execute(Query(sql_s, std::string(query_id, l_query_id)));
        } else {
            client->Execute(sql_s);
        }

    }
    catch (const std::exception& e)
    {
        sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce, sanitizeError(e.what()).c_str(), 0);
    }
    RETURN_TRUE;
}
/* }}} */

/* {{{ proto array __destruct()
 */
PHP_METHOD(CLICKHOUSE_RES_NAME, __destruct)
{
    int key = Z_OBJ_HANDLE(*getThis());
    auto it = clientMap.find(key);
    if (it == clientMap.end()) {
        // __construct never inserted (failed connect, etc). Nothing to clean.
        RETURN_TRUE;
    }
    Client *client = it->second;

    // If a script left writeStart()/write() pending without writeEnd(),
    // close the insert stream first so the server doesn't see a half-open
    // transaction. Swallow errors — destructors must not throw.
    if (clientInsertBlack.count(key)) {
        try { client->EndInsert(); } catch (...) {}
        clientInsertBlack.erase(key);
    }

    delete client;
    clientMap.erase(key);
    RETURN_TRUE;
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
