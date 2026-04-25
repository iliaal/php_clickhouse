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
    zend_declare_property_null(clickhouse_ce, "passwd", strlen("passwd"), ZEND_ACC_PROTECTED);
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
            if (strcasecmp(s, "lz4") == 0) cv = 1;
            else if (strcasecmp(s, "zstd") == 0) cv = 2;
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
        Options = Options.SetTcpKeepAliveCount((unsigned int)Z_LVAL_P(value));
    }
    if (php_array_get_value(_ht, "max_compression_chunk_size", value)) {
        convert_to_long(value);
        Options = Options.SetMaxCompressionChunkSize((unsigned int)Z_LVAL_P(value));
    }
#ifdef WITH_OPENSSL
    bool want_ssl = false;
    if (php_array_get_value(_ht, "ssl", value)) {
        convert_to_boolean(value);
        want_ssl = (Z_LVAL_P(value) != 0);
    }
    if (want_ssl) {
        ClientOptions::SSLOptions ssl_opts;
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
            if (pz) { convert_to_long(pz); e.port = (uint16_t)Z_LVAL_P(pz); }
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
        sc_zend_update_property_string(clickhouse_ce, this_obj, "passwd", sizeof("passwd") - 1, Z_STRVAL_P(value));
        Options = Options.SetPassword(Z_STRVAL_P(value));
    }

    try
    {
        Client *client = new Client(Options);
        int key = Z_OBJ_HANDLE(*this_obj);

        clientMap.insert(std::pair<int, Client*>(key, client));

    }
    catch (const std::exception& e)
    {
        sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce, e.what(), 0);
    }

    RETURN_TRUE;
}
/* }}} */

void getInsertSql(string *sql, char *table_name, zval *columns)
{
    zval *pzval;
    std::stringstream fields_section;

    HashTable *columns_ht = Z_ARRVAL_P(columns);
    size_t count = zend_hash_num_elements(columns_ht);
    size_t index = 0;

    ZEND_HASH_FOREACH_VAL(columns_ht, pzval)
    {
        convert_to_string(pzval);
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

/* {{{ proto bool ping()
 */
PHP_METHOD(CLICKHOUSE_RES_NAME, ping)
{
        int key = Z_OBJ_HANDLE(*getThis());
        Client *client = clientMap.at(key);

        try {
            client->Ping();
        } catch (const std::exception& e) {
            sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce, e.what(), 0);
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
        Client *client = clientMap.at(key);

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

            HashTable *params_ht = Z_ARRVAL_P(params);
            zval *pzval;
            char *str_key;
            uint32_t str_keylen;
            int keytype;
            (void)keytype;

            SC_HASHTABLE_FOREACH_START2(params_ht, str_key, str_keylen, keytype, pzval)
            {
                convert_to_string(pzval);
                sql_s.replace(sql_s.find("{" + (string)str_key + "}"), str_keylen + 2, (string)Z_STRVAL_P(pzval));
            }
            SC_HASHTABLE_FOREACH_END();
        }

        if (!(fetch_mode & SC_FETCH_ONE)) {
            array_init(return_value);
        }

        std::string qid = (query_id && l_query_id > 0) ? std::string(query_id, l_query_id) : std::string();
        auto select_cb = [return_value, fetch_mode](const Block &block) {
            if (fetch_mode & SC_FETCH_ONE) {
                if (block.GetRowCount() > 0 && block.GetColumnCount() > 0) {
                    convertToZval(return_value, block[0], 0, "", 0, fetch_mode);
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
        sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce, e.what(), 0);
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
        Client *client = clientMap.at(key);

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
        sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce, e.what(), 0);
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
        Client *client = clientMap.at(key);

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
        sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce, e.what(), 0);
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
        Client *client = clientMap.at(key);

        Block blockQuery = clientInsertBlack.at(key);

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
        sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce, e.what(), 0);
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
        Client *client = clientMap.at(key);
        clientInsertBlack.erase(key);

        client->EndInsert();
    }
    catch (const std::exception& e)
    {
        sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce, e.what(), 0);
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
        Client *client = clientMap.at(key);

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

            HashTable *params_ht = Z_ARRVAL_P(params);
            zval *pzval;
            char *str_key;
            uint32_t str_keylen;
            int keytype;
            (void)keytype;

            SC_HASHTABLE_FOREACH_START2(params_ht, str_key, str_keylen, keytype, pzval)
            {
                convert_to_string(pzval);
                sql_s.replace(sql_s.find("{" + (string)str_key + "}"), str_keylen + 2, (string)Z_STRVAL_P(pzval));
            }
            SC_HASHTABLE_FOREACH_END();
        }

        if (query_id && l_query_id > 0) {
            client->Execute(Query(sql_s, std::string(query_id, l_query_id)));
        } else {
            client->Execute(sql_s);
        }

    }
    catch (const std::exception& e)
    {
        sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce, e.what(), 0);
    }
    RETURN_TRUE;
}
/* }}} */

/* {{{ proto array __destruct()
 */
PHP_METHOD(CLICKHOUSE_RES_NAME, __destruct)
{
    try
    {
        int key = Z_OBJ_HANDLE(*getThis());
        Client *client = clientMap.at(key);
        delete client;
        clientMap.erase(key);
        clientInsertBlack.erase(key);

    }
    catch (const std::exception& e)
    {
        sc_zend_throw_exception_tsrmls_cc(clickhouse_exception_ce, e.what(), 0);
    }
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
