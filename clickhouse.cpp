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
#include "lib/clickhouse-cpp/clickhouse/base/output.h"
#include "lib/clickhouse-cpp/clickhouse/types/type_parser.h"
#include "lib/clickhouse-cpp/clickhouse/columns/factory.h"
#include "lib/clickhouse-cpp/clickhouse/columns/json.h"
#include "lib/clickhouse-cpp/clickhouse/columns/string.h"
#include "lib/clickhouse-cpp/clickhouse/columns/numeric.h"
#include "lib/clickhouse-cpp/clickhouse/columns/bool.h"
#include "lib/clickhouse-cpp/clickhouse/columns/nullable.h"
#include "typesToPhp.hpp"
#include "clickhouse_internal.h"
#include <algorithm>
#include <chrono>
#include <cinttypes>
#include <cmath>
#include <cstring>
#include <deque>
#include <limits>
#include <optional>
#include <unordered_map>

using namespace clickhouse;
using namespace std;

 zend_class_entry *clickhouse_ce, *clickhouse_exception_ce, *clickhouse_iter_ce, *clickhouse_statement_ce;


static zend_object_handlers clickhouse_object_handlers;

static zend_object *clickhouse_create_object(zend_class_entry *ce)
{
    clickhouse_object *obj = (clickhouse_object *)zend_object_alloc(sizeof(clickhouse_object), ce);

    obj->client = nullptr;
    obj->has_insert_block = false;
    obj->query_active = false;
    obj->log_enabled = false;
    obj->verbose_to_stderr = false;
    new (&obj->insert_block) Block();
    new (&obj->stats) ClientStats();
    new (&obj->settings) std::unordered_map<std::string, std::string>();
    new (&obj->insert_sql) std::string();
    new (&obj->insert_query_id) std::string();
    new (&obj->insert_started_at) std::chrono::steady_clock::time_point();
    new (&obj->query_log) std::deque<QueryLog>();
    new (&obj->client_options) ClientOptions();
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

    /* ~Client finalizes orphaned inserts; reconnecting cannot roll back blocks
     * already persisted by ClickHouse. The vendored teardown caps send/receive
     * timeouts at 5s (including unlimited timeouts); see LOCAL_PATCHES.md.
     * Cleanup belongs in free_obj so bailout also releases native state. */
    if (obj->client) {
        if (obj->has_insert_block) {
            obj->insert_block = Block();
            obj->has_insert_block = false;
            obj->insert_sql.clear();
            obj->insert_query_id.clear();
            obj->insert_started_at = std::chrono::steady_clock::time_point();
        }
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
    obj->insert_sql.~basic_string();
    obj->insert_query_id.~basic_string();
    obj->insert_started_at.~time_point();
    obj->query_log.~deque<QueryLog>();
    obj->client_options.~ClientOptions();

    zend_object_std_dtor(&obj->std);
}

/* Expose callback zvals outside the property table so GC can collect
 * client -> closure -> client cycles. */
#if PHP_VERSION_ID >= 80000
static HashTable *clickhouse_get_gc(zend_object *object, zval **table, int *n)
{
    clickhouse_object *obj = clickhouse_from_obj(object);
    zend_get_gc_buffer *buf = zend_get_gc_buffer_create();
    if (Z_TYPE(obj->progress_callback) != IS_UNDEF) {
        zend_get_gc_buffer_add_zval(buf, &obj->progress_callback);
    }
    if (Z_TYPE(obj->profile_callback) != IS_UNDEF) {
        zend_get_gc_buffer_add_zval(buf, &obj->profile_callback);
    }
    if (Z_TYPE(obj->verbose_callback) != IS_UNDEF) {
        zend_get_gc_buffer_add_zval(buf, &obj->verbose_callback);
    }
    zend_get_gc_buffer_use(buf, table, n);
    return zend_std_get_properties(object);
}
#else
/* PHP 7.4: object passed as zval*, no zend_get_gc_buffer. Fill the
 * per-object gc_buf (which outlives the call) and point *table at it.
 * ZVAL_COPY_VALUE without an addref matches the 8.x buffer semantics:
 * the collector only traverses the pointers, it doesn't own them. */
static HashTable *clickhouse_get_gc(zval *object, zval **table, int *n)
{
    clickhouse_object *obj = Z_CLICKHOUSE_P(object);
    int i = 0;
    if (Z_TYPE(obj->progress_callback) != IS_UNDEF) {
        ZVAL_COPY_VALUE(&obj->gc_buf[i++], &obj->progress_callback);
    }
    if (Z_TYPE(obj->profile_callback) != IS_UNDEF) {
        ZVAL_COPY_VALUE(&obj->gc_buf[i++], &obj->profile_callback);
    }
    if (Z_TYPE(obj->verbose_callback) != IS_UNDEF) {
        ZVAL_COPY_VALUE(&obj->gc_buf[i++], &obj->verbose_callback);
    }
    *table = obj->gc_buf;
    *n = i;
    return zend_std_get_properties(object);
}
#endif

/*
 * Streaming row iterator state. blocks accumulate via OnData during
 * selectStream(); the foreach loop walks them lazily without
 * materializing the full result set as a single PHP array.
 */
struct clickhouse_iter_object {
    std::vector<Block> blocks;
    /* Cache stable result-schema names to avoid GetColumnName allocations per cell. */
    std::vector<std::string> column_names;
    size_t block_idx;
    size_t row_idx;
    uint64_t cumulative_row_idx;
    uint64_t total_rows;
    /* Conservative C++-heap charge for blocks retained by selectStream
     * (not visible to PHP memory_limit). Used only as a soft tripwire. */
    size_t buffered_estimate;
    int fetch_mode;
    zend_object std;
};

static inline clickhouse_iter_object *clickhouse_iter_from_obj(zend_object *obj)
{
    return (clickhouse_iter_object *)((char *)obj - offsetof(clickhouse_iter_object, std));
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
    iter->buffered_estimate = 0;
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

/* Use a private cursor: toArray/jsonSerialize share the rows HashTable,
 * whose internal pointer must not be mutated. Nested iteration shares pos.
 * statistics is a snapshot that survives later queries on the client. */
struct clickhouse_statement_object {
    zval rows;
    zval positional_rows;
    zval statistics;
    HashPosition pos;
#if PHP_VERSION_ID < 80000
    /* PHP 7.4: get_gc must point *table at storage that outlives the call. */
    zval gc_buf[3];
#endif
    zend_object std;
};

static inline clickhouse_statement_object *clickhouse_statement_from_obj(zend_object *obj)
{
    return (clickhouse_statement_object *)((char *)obj - offsetof(clickhouse_statement_object, std));
}

#define Z_CLICKHOUSE_STATEMENT_P(zv) clickhouse_statement_from_obj(Z_OBJ_P(zv))

static zend_object_handlers clickhouse_statement_object_handlers;

static zend_object *clickhouse_statement_create_object(zend_class_entry *ce)
{
    clickhouse_statement_object *stmt = (clickhouse_statement_object *)zend_object_alloc(sizeof(clickhouse_statement_object), ce);
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

/* Object-valued cells can form cycles through these non-property zvals. */
#if PHP_VERSION_ID >= 80000
static HashTable *clickhouse_statement_get_gc(zend_object *object, zval **table, int *n)
{
    clickhouse_statement_object *stmt = clickhouse_statement_from_obj(object);
    zend_get_gc_buffer *buf = zend_get_gc_buffer_create();
    if (Z_TYPE(stmt->rows) != IS_UNDEF) {
        zend_get_gc_buffer_add_zval(buf, &stmt->rows);
    }
    if (Z_TYPE(stmt->positional_rows) != IS_UNDEF) {
        zend_get_gc_buffer_add_zval(buf, &stmt->positional_rows);
    }
    if (Z_TYPE(stmt->statistics) != IS_UNDEF) {
        zend_get_gc_buffer_add_zval(buf, &stmt->statistics);
    }
    zend_get_gc_buffer_use(buf, table, n);
    return zend_std_get_properties(object);
}
#else
static HashTable *clickhouse_statement_get_gc(zval *object, zval **table, int *n)
{
    clickhouse_statement_object *stmt = Z_CLICKHOUSE_STATEMENT_P(object);
    int i = 0;
    if (Z_TYPE(stmt->rows) != IS_UNDEF) {
        ZVAL_COPY_VALUE(&stmt->gc_buf[i++], &stmt->rows);
    }
    if (Z_TYPE(stmt->positional_rows) != IS_UNDEF) {
        ZVAL_COPY_VALUE(&stmt->gc_buf[i++], &stmt->positional_rows);
    }
    if (Z_TYPE(stmt->statistics) != IS_UNDEF) {
        ZVAL_COPY_VALUE(&stmt->gc_buf[i++], &stmt->statistics);
    }
    *table = stmt->gc_buf;
    *n = i;
    return zend_std_get_properties(object);
}
#endif

struct DerefZvalHold {
    zval value;

    explicit DerefZvalHold(zval *source) {
        if (source) {
            ZVAL_COPY_DEREF(&value, source);
        } else {
            ZVAL_UNDEF(&value);
        }
    }
    ~DerefZvalHold() {
        if (Z_TYPE(value) != IS_UNDEF) {
            zval_ptr_dtor(&value);
        }
    }
    zval *get() {
        return Z_TYPE(value) == IS_UNDEF ? nullptr : &value;
    }
    DerefZvalHold(const DerefZvalHold&) = delete;
    DerefZvalHold& operator=(const DerefZvalHold&) = delete;
};

struct RowsSnapshot {
    zval value;

    explicit RowsSnapshot(HashTable *rows) {
        array_init_size(&value, zend_hash_num_elements(rows));
        zval *row;
        ZEND_HASH_FOREACH_VAL(rows, row) {
            zval copy;
            ZVAL_COPY_DEREF(&copy, row);
            add_next_index_zval(&value, &copy);
        } ZEND_HASH_FOREACH_END();
    }
    ~RowsSnapshot() {
        zval_ptr_dtor(&value);
    }
    HashTable *get() {
        return Z_ARRVAL(value);
    }
    RowsSnapshot(const RowsSnapshot&) = delete;
    RowsSnapshot& operator=(const RowsSnapshot&) = delete;
};

static size_t saturatingAddSize(size_t left, size_t right)
{
    return right > std::numeric_limits<size_t>::max() - left
        ? std::numeric_limits<size_t>::max()
        : left + right;
}

static size_t saturatingMulSize(size_t left, size_t right)
{
    if (left == 0 || right == 0) {
        return 0;
    }
    return left > std::numeric_limits<size_t>::max() / right
        ? std::numeric_limits<size_t>::max()
        : left * right;
}

class CountingOutput final : public OutputStream {
public:
    size_t size = 0;

protected:
    size_t DoWrite(const void *, size_t len) override {
        size = saturatingAddSize(size, len);
        return len;
    }
};

static size_t retainedColumnPayloadBytes(const ColumnRef &column)
{
    switch (column->GetType().GetCode()) {
        case Type::String: {
            auto strings = column->As<ColumnString>();
            if (!strings) {
                throw std::runtime_error("String column has unexpected representation");
            }
            size_t bytes = saturatingMulSize(
                strings->Size(), sizeof(std::string_view));
            for (size_t row = 0; row < strings->Size(); ++row) {
                bytes = saturatingAddSize(bytes, strings->At(row).size());
            }
            return bytes;
        }
        case Type::FixedString: {
            auto strings = column->As<ColumnFixedString>();
            if (!strings) {
                throw std::runtime_error("FixedString column has unexpected representation");
            }
            return saturatingMulSize(strings->Size(), strings->FixedSize());
        }
        case Type::JSON: {
            auto json = column->As<ColumnJSON>();
            if (!json) {
                throw std::runtime_error("JSON column has unexpected representation");
            }
            size_t bytes = saturatingMulSize(
                json->Size(), sizeof(std::string_view));
            for (size_t row = 0; row < json->Size(); ++row) {
                bytes = saturatingAddSize(bytes, json->At(row).size());
            }
            return bytes;
        }
        case Type::Int8:
        case Type::Int16:
        case Type::Int32:
        case Type::Int64:
        case Type::UInt8:
        case Type::UInt16:
        case Type::UInt32:
        case Type::UInt64:
        case Type::Float32:
        case Type::Float64:
        case Type::DateTime:
        case Type::Date:
        case Type::Enum8:
        case Type::Enum16:
        case Type::UUID:
        case Type::IPv4:
        case Type::IPv6:
        case Type::Int128:
        case Type::UInt128:
        case Type::Decimal:
        case Type::Decimal32:
        case Type::Decimal64:
        case Type::Decimal128:
        case Type::DateTime64:
        case Type::Date32:
        case Type::Time:
        case Type::Time64:
        case Type::Bool:
        /* Nullable(Nothing): a bare NULL literal or a NULL-padded UNION arm.
         * Carries no payload, and ColumnNothing::SaveBody throws, so it must
         * never reach the Save() fallback. */
        case Type::Void:
            return 0;
        default: {
            /* Composite and future column types are charged by serialized
             * size. Save() is best-effort: a column type the vendored library
             * declines to serialize still has to be streamable, so fall back
             * to the structural floor rather than failing the query. */
            CountingOutput output;
            try {
                column->Save(&output);
            } catch (const std::exception &) {
                return 0;
            }
            return output.size;
        }
    }
}

static size_t estimateRetainedBlockBytes(const Block &block)
{
    size_t payload = 0;
    for (size_t column = 0; column < block.GetColumnCount(); ++column) {
        payload = saturatingAddSize(
            payload, retainedColumnPayloadBytes(block[column]));
    }
    size_t cells = saturatingMulSize(
        block.GetRowCount(), block.GetColumnCount());
    size_t structural_floor = saturatingMulSize(cells, 32);
    return std::max(payload, structural_floor);
}

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
static PHP_METHOD(ClickHouse, selectWithExternalData);
static PHP_METHOD(ClickHouse, selectToStream);
static PHP_METHOD(ClickHouse, insert);
static PHP_METHOD(ClickHouse, insertAssoc);
static PHP_METHOD(ClickHouse, insertFromStream);
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
PHP_METHOD(ClickHouse, databaseSize);
PHP_METHOD(ClickHouse, tablesSize);
PHP_METHOD(ClickHouse, partitions);
PHP_METHOD(ClickHouse, showTables);
PHP_METHOD(ClickHouse, showCreateTable);
PHP_METHOD(ClickHouse, getServerUptime);
PHP_METHOD(ClickHouse, enableLogQueries);
PHP_METHOD(ClickHouse, getLogQueries);
static PHP_METHOD(ClickHouse, selectStream);
static PHP_METHOD(ClickHouse, selectStatement);
static PHP_METHOD(ClickHouse, selectStreamCallback);
PHP_METHOD(ClickHouse, isExists);
PHP_METHOD(ClickHouse, showDatabases);
PHP_METHOD(ClickHouse, showProcesslist);
PHP_METHOD(ClickHouse, getServerVersion);
PHP_METHOD(ClickHouse, tableSize);
PHP_METHOD(ClickHouse, truncateTable);
PHP_METHOD(ClickHouse, dropPartition);

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

#if PHP_VERSION_ID < 80100
static int clickhouse_serialize_deny(zval *object, unsigned char **buffer, size_t *buf_len, zend_serialize_data *data)
{
    (void)buffer;
    (void)buf_len;
    (void)data;
    zend_throw_exception_ex(NULL, 0, "Serialization of '%s' is not allowed", ZSTR_VAL(Z_OBJCE_P(object)->name));
    return FAILURE;
}

static int clickhouse_unserialize_deny(zval *object, zend_class_entry *ce, const unsigned char *buf, size_t buf_len, zend_unserialize_data *data)
{
    (void)object;
    (void)buf;
    (void)buf_len;
    (void)data;
    zend_throw_exception_ex(NULL, 0, "Unserialization of '%s' is not allowed", ZSTR_VAL(ce->name));
    return FAILURE;
}
#endif

static void clickhouse_mark_not_serializable(zend_class_entry *ce)
{
#if PHP_VERSION_ID >= 80100
    ce->ce_flags |= ZEND_ACC_NOT_SERIALIZABLE;
#else
    ce->serialize = clickhouse_serialize_deny;
    ce->unserialize = clickhouse_unserialize_deny;
#endif
}

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
    clickhouse_mark_not_serializable(clickhouse_ce);
    clickhouse_ce->create_object = clickhouse_create_object;
#if PHP_VERSION_ID >= 80400
    clickhouse_ce->default_object_handlers = &clickhouse_object_handlers;
#endif

    memcpy(&clickhouse_object_handlers, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
    clickhouse_object_handlers.offset = offsetof(clickhouse_object, std);
    clickhouse_object_handlers.free_obj = clickhouse_free_obj;
    clickhouse_object_handlers.get_gc = clickhouse_get_gc;
    /* The std clone handler allocates a bare zend_object with no room
     * for the C++ prefix; free_obj on that clone then frees a pointer
     * offset bytes before the allocation. Refuse cloning instead. */
    clickhouse_object_handlers.clone_obj = NULL;

    clickhouse_exception_ce = register_class_ClickHouseException(zend_ce_exception);

    clickhouse_iter_ce = register_class_ClickHouseRowIterator(zend_ce_iterator, zend_ce_countable);
    clickhouse_mark_not_serializable(clickhouse_iter_ce);
    clickhouse_iter_ce->create_object = clickhouse_iter_create_object;
#if PHP_VERSION_ID >= 80400
    clickhouse_iter_ce->default_object_handlers = &clickhouse_iter_object_handlers;
#endif

    memcpy(&clickhouse_iter_object_handlers, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
    clickhouse_iter_object_handlers.offset = offsetof(clickhouse_iter_object, std);
    clickhouse_iter_object_handlers.free_obj = clickhouse_iter_free_obj;
    clickhouse_iter_object_handlers.clone_obj = NULL;

    zend_class_entry *json_serializable_ce = static_cast<zend_class_entry *>(
        zend_hash_str_find_ptr(
            CG(class_table), "jsonserializable", sizeof("JsonSerializable") - 1));
    if (!json_serializable_ce) {
        return FAILURE;
    }
    clickhouse_statement_ce = register_class_ClickHouseStatement(
        zend_ce_iterator, zend_ce_countable, zend_ce_arrayaccess,
        json_serializable_ce);
    clickhouse_mark_not_serializable(clickhouse_statement_ce);
    clickhouse_statement_ce->create_object = clickhouse_statement_create_object;
#if PHP_VERSION_ID >= 80400
    clickhouse_statement_ce->default_object_handlers = &clickhouse_statement_object_handlers;
#endif

    memcpy(&clickhouse_statement_object_handlers, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
    clickhouse_statement_object_handlers.offset = offsetof(clickhouse_statement_object, std);
    clickhouse_statement_object_handlers.free_obj = clickhouse_statement_free_obj;
    clickhouse_statement_object_handlers.get_gc = clickhouse_statement_get_gc;
    clickhouse_statement_object_handlers.clone_obj = NULL;

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
static const zend_module_dep clickhouse_deps[] = {
    ZEND_MOD_REQUIRED("json")
    ZEND_MOD_END
};

zend_module_entry clickhouse_module_entry =
{
    STANDARD_MODULE_HEADER_EX, NULL,
    clickhouse_deps,
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

/* The handshake takes a literal database name, not a db.table identifier. */
static void validateDatabaseName(const char *s, size_t len)
{
    if (len == 0) {
        throw std::runtime_error("database name must not be empty");
    }
    if (memchr(s, '.', len) != nullptr) {
        throw std::runtime_error("database name contains an invalid character");
    }
}

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

    /* PHP allows an explicit second __construct() call. Reject it before any
     * property is written, or the config changes while the client stays stale. */
    if (Z_CLICKHOUSE_P(this_obj)->client) {
        zend_throw_exception(clickhouse_exception_ce,
            "ClickHouse object is already constructed", 0);
        return;
    }

    bool host_configured = false;
    if (php_array_get_value(_ht, "host", value))
    {
        host_configured = true;
        /* ZStrGuard turns a throwing __toString() into a C++ throw. This runs
         * before the main try block, so an uncaught throw would escape the Zend
         * dispatcher and abort the process. */
        try {
            ZStrGuard sg(value);
            sc_zend_update_property_stringl(clickhouse_ce, this_obj, "host", sizeof("host") - 1,
                                            sg.val(), sg.len());
        } catch (const std::exception &e) {
            throwClickHouseError(e);
            return;
        }
    }

    if (php_array_get_value(_ht, "port", value))
    {
        zend_long _p = zval_get_long(value);
        if (_p < 1 || _p > 65535) {
            zend_throw_exception(clickhouse_exception_ce,
                "port out of 1..65535 range", 0);
            return;
        }
        sc_zend_update_property_long(clickhouse_ce, this_obj, "port", sizeof("port") - 1, _p);
    }

    if (php_array_get_value(_ht, "compression", value))
    {
        /* 0=none, 1=lz4, 2=zstd. Accept the integer codes, string names, and
         * bool (true→lz4, false→none). Do not use zend_is_true for non-bool
         * numbers: that maps 2 to true→1 and silently downgrades zstd. */
        long cv = 0;
        ZVAL_DEREF(value);
        if (Z_TYPE_P(value) == IS_STRING) {
            const char *s = Z_STRVAL_P(value);
            if (strcasecmp(s, "lz4") == 0)        cv = 1;
            else if (strcasecmp(s, "zstd") == 0)  cv = 2;
            else if (strcasecmp(s, "none") == 0)  cv = 0;
            else {
                zend_throw_exception(clickhouse_exception_ce,
                    "Unknown compression name; expected 'lz4', 'zstd', 'none', 0, 1, 2, true, or false", 0);
                return;
            }
        } else if (Z_TYPE_P(value) == IS_TRUE) {
            cv = 1;
        } else if (Z_TYPE_P(value) == IS_FALSE) {
            cv = 0;
        } else if (Z_TYPE_P(value) == IS_LONG) {
            zend_long n = Z_LVAL_P(value);
            if (n < 0 || n > 2) {
                zend_throw_exception(clickhouse_exception_ce,
                    "compression out of range; expected 0 (none), 1 (lz4), or 2 (zstd)", 0);
                return;
            }
            cv = (long)n;
        } else if (Z_TYPE_P(value) == IS_DOUBLE) {
            double d = Z_DVAL_P(value);
            if (d != 0.0 && d != 1.0 && d != 2.0) {
                zend_throw_exception(clickhouse_exception_ce,
                    "compression out of range; expected 0 (none), 1 (lz4), or 2 (zstd)", 0);
                return;
            }
            cv = (long)d;
        } else {
            zend_throw_exception(clickhouse_exception_ce,
                "compression must be 'lz4', 'zstd', 'none', 0, 1, 2, true, or false", 0);
            return;
        }
        sc_zend_update_property_long(clickhouse_ce, this_obj, "compression", sizeof("compression") - 1, cv);
    }

    /* Negative timeouts/retry counts would wrap in unsigned native setters. */
    /* php_array_get_value uses sizeof(key), so runtime keys need zend_hash_str_find. */
    auto unsigned_max_as_zend_long = [](uint64_t max) -> zend_long {
        return max > (uint64_t)ZEND_LONG_MAX ? ZEND_LONG_MAX : (zend_long)max;
    };
    auto load_bounded_nonneg_long = [&](const char *key, zend_long max, zend_long &out) -> bool {
        zval *v = zend_hash_str_find(_ht, (char*)key, strlen(key));
        if (!v || ZVAL_IS_NULL(v)) return false;
        zend_long n = zval_get_long(v);
        if (n < 0 || n > max) {
            std::string msg = std::string(key) + " out of range";
            zend_throw_exception(clickhouse_exception_ce, msg.c_str(), 0);
            return false; // caller checks EG(exception)
        }
        out = n;
        return true;
    };

    {
        zend_long n;
        if (load_bounded_nonneg_long("retry_timeout", ZEND_LONG_MAX, n)) {
            sc_zend_update_property_long(clickhouse_ce, this_obj, "retry_timeout", sizeof("retry_timeout") - 1, n);
        } else if (EG(exception)) { return; }
    }
    {
        zend_long n;
        if (load_bounded_nonneg_long(
                "retry_count", unsigned_max_as_zend_long(UINT_MAX), n)) {
            sc_zend_update_property_long(clickhouse_ce, this_obj, "retry_count", sizeof("retry_count") - 1, n);
        } else if (EG(exception)) { return; }
    }
    {
        zend_long n;
        if (load_bounded_nonneg_long("connect_timeout", (zend_long)(INT_MAX / 1000), n)) {
            sc_zend_update_property_long(clickhouse_ce, this_obj, "connect_timeout", sizeof("connect_timeout") - 1, n);
        } else if (EG(exception)) { return; }
    }
    {
        zend_long n;
        if (load_bounded_nonneg_long("receive_timeout", (zend_long)(UINT_MAX / 1000U), n)) {
            sc_zend_update_property_long(clickhouse_ce, this_obj, "receive_timeout", sizeof("receive_timeout") - 1, n);
        } else if (EG(exception)) { return; }
    }

    /* Declared, freshly written properties return their slots, leaving shared rv unused. */
    zval _rv;
    zval *host = sc_zend_read_property(clickhouse_ce, this_obj, "host", sizeof("host") - 1, 0, &_rv);
    zval *port = sc_zend_read_property(clickhouse_ce, this_obj, "port", sizeof("port") - 1, 0, &_rv);
    zval *compression = sc_zend_read_property(clickhouse_ce, this_obj, "compression", sizeof("compression") - 1, 0, &_rv);
    zval *retry_timeout = sc_zend_read_property(clickhouse_ce, this_obj, "retry_timeout", sizeof("retry_timeout") - 1, 0, &_rv);
    zval *retry_count = sc_zend_read_property(clickhouse_ce, this_obj, "retry_count", sizeof("retry_count") - 1, 0, &_rv);
    zval *receive_timeout = sc_zend_read_property(clickhouse_ce, this_obj, "receive_timeout", sizeof("receive_timeout") - 1, 0, &_rv);
    zval *connect_timeout = sc_zend_read_property(clickhouse_ce, this_obj, "connect_timeout", sizeof("connect_timeout") - 1, 0, &_rv);

    /* C++ allocation failures must not escape the Zend dispatcher. */
    try
    {
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
            if (n < 0 || n > (zend_long)(UINT_MAX / 1000U)) {
                zend_throw_exception(clickhouse_exception_ce,
                    "send_timeout out of range", 0);
                return;
            }
            Options = Options.SetConnectionSendTimeout(std::chrono::seconds(n));
        }
        /* Millisecond keys override seconds-based keys. */
        auto apply_timeout_ms = [&](const char *key,
                                     int64_t max,
                                     ClientOptions& (ClientOptions::*setter)(const std::chrono::milliseconds&)) -> bool {
            zval *v = zend_hash_str_find(_ht, (char*)key, strlen(key));
            if (!v || ZVAL_IS_NULL(v)) return true;
            auto fail = [&]() -> bool {
                std::string msg = std::string(key) + " out of range";
                zend_throw_exception(clickhouse_exception_ce, msg.c_str(), 0);
                return false;
            };
            /* On 32-bit PHP, valid values above LONG_MAX arrive as doubles;
             * zval_get_long would wrap them. */
            zval *dv = v;
            ZVAL_DEREF(dv);
            int64_t n;
            if (Z_TYPE_P(dv) == IS_DOUBLE) {
                double d = Z_DVAL_P(dv);
                if (!std::isfinite(d) || d < 0.0 || d != std::trunc(d) ||
                    d > (double)max) {
                    return fail();
                }
                n = (int64_t)d;
            } else {
                zend_long l = zval_get_long(v);
                if (l < 0 || l > max) {
                    return fail();
                }
                n = (int64_t)l;
            }
            Options = (Options.*setter)(std::chrono::milliseconds(n));
            return true;
        };
        if (!apply_timeout_ms("connect_timeout_ms", (int64_t)INT_MAX, &ClientOptions::SetConnectionConnectTimeout)) return;
        if (!apply_timeout_ms(
                "receive_timeout_ms", (int64_t)UINT32_MAX,
                &ClientOptions::SetConnectionRecvTimeout)) return;
        if (!apply_timeout_ms(
                "send_timeout_ms", (int64_t)UINT32_MAX,
                &ClientOptions::SetConnectionSendTimeout)) return;
        if (php_array_get_value(_ht, "tcp_nodelay", value)) {
            Options = Options.TcpNoDelay(zend_is_true(value));
        }
        if (php_array_get_value(_ht, "tcp_keepalive", value)) {
            Options = Options.TcpKeepAlive(zend_is_true(value));
        }
        if (php_array_get_value(_ht, "ping_before_query", value)) {
            Options = Options.SetPingBeforeQuery(zend_is_true(value));
        }
        auto apply_nonneg_int_max = [&](const char *key, auto apply) -> bool {
            zval *v = zend_hash_str_find(_ht, (char*)key, strlen(key));
            if (!v) return true;
            if (Z_ISREF_P(v)) v = Z_REFVAL_P(v);
            if (ZVAL_IS_NULL(v)) return true;
            zend_long n = zval_get_long(v);
            if (n < 0 || n > INT_MAX) {
                std::string msg = std::string(key) + " out of range";
                zend_throw_exception(clickhouse_exception_ce, msg.c_str(), 0);
                return false;
            }
            apply(n);
            return true;
        };
        if (!apply_nonneg_int_max("tcp_keepalive_idle", [&](zend_long n) {
                Options = Options.SetTcpKeepAliveIdle(std::chrono::seconds(n));
            })) return;
        if (!apply_nonneg_int_max("tcp_keepalive_intvl", [&](zend_long n) {
                Options = Options.SetTcpKeepAliveInterval(std::chrono::seconds(n));
            })) return;
        if (!apply_nonneg_int_max("tcp_keepalive_cnt", [&](zend_long n) {
                Options = Options.SetTcpKeepAliveCount((unsigned int)n);
            })) return;
        if (!apply_nonneg_int_max("max_compression_chunk_size", [&](zend_long n) {
                Options = Options.SetMaxCompressionChunkSize((unsigned int)n);
            })) return;
    #ifdef WITH_OPENSSL
        bool want_ssl = false;
        if (php_array_get_value(_ht, "ssl", value)) {
            want_ssl = zend_is_true(value);
        }
        /* Validate TLS options even when disabled so invalid configuration cannot hide. */
        {
        ClientOptions::SSLOptions ssl_opts;
        // TLS 1.2 minimum, overridable below.
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
                zend_throw_exception(clickhouse_exception_ce,
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
            ZVAL_DEREF(value);
            if (Z_TYPE_P(value) == IS_STRING) {
                files.emplace_back(Z_STRVAL_P(value), Z_STRLEN_P(value));
            } else if (Z_TYPE_P(value) == IS_ARRAY) {
                DerefZvalHold files_hold(value);
                HashTable *fh = Z_ARRVAL_P(files_hold.get());
                zval *fv;
                ZEND_HASH_FOREACH_VAL(fh, fv) {
                    zval *ev = fv;
                    if (Z_ISREF_P(ev)) ev = Z_REFVAL_P(ev);
                    if (Z_TYPE_P(ev) != IS_STRING && Z_TYPE_P(ev) != IS_OBJECT) {
                        zend_throw_exception(clickhouse_exception_ce,
                            "ssl_ca_files must be a string or an array of strings", 0);
                        return;
                    }
                    ZStrGuard sg(ev);
                    files.emplace_back(sg.val(), sg.len());
                } ZEND_HASH_FOREACH_END();
            } else {
                zend_throw_exception(clickhouse_exception_ce,
                    "ssl_ca_files must be a string or an array of strings", 0);
                return;
            }
            ssl_opts.SetPathToCAFiles(files);
        }
        if (want_ssl) {
            Options = Options.SetSSLOptions(ssl_opts);
        }
        }
        /* Reject unknown TLS keys even when TLS is disabled. */
        {
        static const char *known_ssl_keys[] = {
            "ssl", "ssl_min_protocol_version", "ssl_skip_verify",
            "ssl_use_default_ca", "ssl_ca_directory", "ssl_ca_files",
        };
        std::string unknown;
        zend_string *sk;
        zend_ulong snk;
        ZEND_HASH_FOREACH_KEY(_ht, snk, sk) {
            (void)snk;
            if (!sk || ZSTR_LEN(sk) < 3 || memcmp(ZSTR_VAL(sk), "ssl", 3) != 0) continue;
            bool ok = false;
            for (const char *k : known_ssl_keys) {
                if (ZSTR_LEN(sk) == strlen(k) && memcmp(ZSTR_VAL(sk), k, ZSTR_LEN(sk)) == 0) {
                    ok = true;
                    break;
                }
            }
            if (!ok) unknown.assign(ZSTR_VAL(sk), ZSTR_LEN(sk));
        } ZEND_HASH_FOREACH_END();
        if (!unknown.empty()) {
            std::string msg = "unknown ssl_* option '" + unknown + "'";
            zend_throw_exception(clickhouse_exception_ce, msg.c_str(), 0);
            return;
        }
        }
    #else
        if (php_array_get_value(_ht, "ssl", value)) {
            if (zend_is_true(value)) {
                zend_throw_exception(clickhouse_exception_ce,
                    "php_clickhouse was built without TLS support. Reconfigure with --enable-clickhouse-openssl",
                    0);
                return;
            }
        }
        /* Any ssl_* material needs a TLS-enabled build. Explicit false
         * booleans (ssl_skip_verify=false) are no-ops and stay allowed; any
         * other value or unknown ssl_* key is rejected. */
        {
        static const char *bool_ssl_keys[] = {"ssl_skip_verify", "ssl_use_default_ca"};
        std::string offending;
        zend_string *sk;
        zend_ulong snk;
        zval *sv;
        ZEND_HASH_FOREACH_KEY_VAL(_ht, snk, sk, sv) {
            (void)snk;
            if (!sk || ZSTR_LEN(sk) < 4 || memcmp(ZSTR_VAL(sk), "ssl_", 4) != 0) continue;
            zval *v = sv;
            if (Z_ISREF_P(v)) v = Z_REFVAL_P(v);
            if (ZVAL_IS_NULL(v)) continue;
            bool is_bool_key = false;
            for (const char *k : bool_ssl_keys) {
                if (ZSTR_LEN(sk) == strlen(k) && memcmp(ZSTR_VAL(sk), k, ZSTR_LEN(sk)) == 0) {
                    is_bool_key = true;
                    break;
                }
            }
            if (is_bool_key && !zend_is_true(v)) continue;
            offending.assign(ZSTR_VAL(sk), ZSTR_LEN(sk));
        } ZEND_HASH_FOREACH_END();
        if (!offending.empty()) {
            std::string msg = "ssl_* option '" + offending +
                "' requires php_clickhouse built with TLS support (--enable-clickhouse-openssl)";
            zend_throw_exception(clickhouse_exception_ce, msg.c_str(), 0);
            return;
        }
        }
    #endif

        if (php_array_get_value(_ht, "endpoints", value)) {
            /* Invalid endpoints must not silently fall back to localhost. */
            if (Z_TYPE_P(value) != IS_ARRAY) {
                zend_throw_exception(clickhouse_exception_ce,
                    "endpoints must be a list of [host, port] arrays", 0);
                return;
            }
            std::vector<Endpoint> eps;
            DerefZvalHold endpoints_hold(value);
            HashTable *eps_ht = Z_ARRVAL_P(endpoints_hold.get());
            zval *ep_zv;
            ZEND_HASH_FOREACH_VAL(eps_ht, ep_zv) {
                ZVAL_DEREF(ep_zv);
                if (Z_TYPE_P(ep_zv) != IS_ARRAY) {
                    zend_throw_exception(clickhouse_exception_ce,
                        "each endpoints entry must be an array with a 'host' key", 0);
                    return;
                }
                HashTable *eh = Z_ARRVAL_P(ep_zv);
                zval *hz = zend_hash_str_find(eh, (char*)"host", 4);
                zval *pz = zend_hash_str_find(eh, (char*)"port", 4);
                if (hz) ZVAL_DEREF(hz);
                if (pz) ZVAL_DEREF(pz);
                if (!hz || Z_TYPE_P(hz) == IS_NULL) {
                    zend_throw_exception(clickhouse_exception_ce,
                        "each endpoints entry requires a non-null 'host'", 0);
                    return;
                }
                DerefZvalHold host_hold(hz);
                DerefZvalHold port_hold(pz);
                Endpoint e;
                {
                    ZStrGuard host_sg(host_hold.get());
                    e.host = std::string(host_sg.val(), host_sg.len());
                }
                if (port_hold.get()) {
                    zend_long p = zval_get_long(port_hold.get());
                    if (p < 1 || p > 65535) {
                        zend_throw_exception(clickhouse_exception_ce,
                            "Endpoint port out of 1..65535 range", 0);
                        return;
                    }
                    e.port = (uint16_t)p;
                }
                eps.push_back(std::move(e));
            } ZEND_HASH_FOREACH_END();
            if (eps.empty()) {
                zend_throw_exception(clickhouse_exception_ce,
                    "endpoints was provided but contained no usable entries", 0);
                return;
            }
            /* clickhouse-cpp prepends a nonempty host to endpoints; suppress the
             * implicit localhost unless the caller explicitly configured it. */
            if (!host_configured) {
                Options = Options.SetHost(std::string());
            }
            Options = Options.SetEndpoints(eps);
        }

        if (php_array_get_value(_ht, "database", value))
        {
            ZStrGuard sg(value);
            validateDatabaseName(sg.val(), sg.len());
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

        /* `password` is accepted as an alias for `passwd`; when both are
         * present `passwd` wins. */
        bool have_passwd = php_array_get_value(_ht, "passwd", value);
        if (!have_passwd) {
            have_passwd = php_array_get_value(_ht, "password", value);
        }
        if (have_passwd)
        {
            ZStrGuard sg(value);
            Options = Options.SetPassword(std::string(sg.val(), sg.len()));
        }

        clickhouse_object *obj = Z_CLICKHOUSE_P(this_obj);
        if (obj->client) {
            throw std::runtime_error("ClickHouse object is already constructed");
        }
        obj->client = new Client(Options);
        obj->client_options = Options;
    }
    catch (const std::exception& e)
    {
        throwClickHouseError(e, std::string());
        return;
    }

    RETURN_TRUE;
}
/* }}} */

static inline std::string makeQid(zend_string *s)
{
    return (s && ZSTR_LEN(s) > 0) ? std::string(ZSTR_VAL(s), ZSTR_LEN(s)) : std::string();
}

#define CLICKHOUSE_ERROR_MAX_LEN 4096

/* Server errors can embed SQL and bound secrets after execution markers. */
std::string sanitizeError(const char *what)
{
    std::string msg(what ? what : "");
    auto lower_of = [](const std::string &s) {
        std::string l(s);
        for (char &c : l) { if (c >= 'A' && c <= 'Z') c = (char)(c + 32); }
        return l;
    };

    /* Parameter parse errors echo values before any SQL marker. */
    {
        std::string lower = lower_of(msg);
        std::string::size_type vp = lower.find("value ");
        std::string::size_type cp = lower.find(" cannot be parsed");
        if (vp != std::string::npos && cp != std::string::npos && vp + 6 <= cp) {
            msg.replace(vp + 6, cp - (vp + 6), "<redacted>");
        }
    }

    /* ClickHouse also emits lowercase execution markers. */
    std::string lower = lower_of(msg);
    static const char *sql_markers[] = {
        "while executing",
        "in query: ",
        "while processing",
    };
    /* Strip at the earliest marker in the message, regardless of array order. */
    std::string::size_type cut = std::string::npos;
    for (const char *marker : sql_markers) {
        std::string::size_type pos = lower.find(marker);
        if (pos != std::string::npos && (cut == std::string::npos || pos < cut)) {
            cut = pos;
        }
    }
    if (cut != std::string::npos) {
        msg.erase(cut);
        while (!msg.empty() && (msg.back() == ' ' || msg.back() == ',' ||
                                 msg.back() == ':' || msg.back() == '.')) {
            msg.pop_back();
        }
    }
    if (msg.size() > CLICKHOUSE_ERROR_MAX_LEN) {
        static const char suffix[] = "... (truncated)";
        constexpr size_t suffix_len = sizeof(suffix) - 1;
        msg.resize(CLICKHOUSE_ERROR_MAX_LEN - suffix_len);
        msg.append(suffix, suffix_len);
    }
    return msg;
}

static Client* getClient(clickhouse_object *obj)
{
    if (!obj->client) {
        throw std::runtime_error("ClickHouse client is not initialized");
    }
    return obj->client;
}

static zend_string *zvalGetStringOrThrow(zval *v)
{
    zend_string *s = zval_get_string(v);
    if (!s || EG(exception)) {
        if (s) {
            zend_string_release(s);
        }
        throw std::runtime_error("PHP string conversion failed");
    }
    return s;
}

static void setElapsedSince(clickhouse_object *obj,
                            std::chrono::steady_clock::time_point started_at)
{
    auto now = std::chrono::steady_clock::now();
    obj->stats.elapsed_ms =
        std::chrono::duration<double, std::milli>(now - started_at).count();
}

static void clearStreamingInsertState(clickhouse_object *obj)
{
    obj->insert_block = Block();
    obj->has_insert_block = false;
    obj->insert_sql.clear();
    obj->insert_query_id.clear();
    obj->insert_started_at = std::chrono::steady_clock::time_point();
}

static void resetConnectionReapplyDatabase(zval *this_obj, clickhouse_object *obj,
                                           bool clear_insert_state)
{
    /* Mirrors upstream patch 0007: clear PHP streaming state before the
     * reconnect. If ResetConnection() throws (max_connections, port
     * exhaustion, DNS failure), a still-set has_insert_block would make
     * teardown send the terminating empty block down the dirty wire and
     * commit a partial insert. */
    if (clear_insert_state && obj->has_insert_block) {
        clearStreamingInsertState(obj);
    }
    Client *client = getClient(obj);
    try {
        client->ResetConnection();
    } catch (const Error &) {
        /* Both OpenSSLError and ProtocolError must rotate endpoints. */
        client->ResetConnectionEndpoint();
    } catch (const std::system_error &) {
        client->ResetConnectionEndpoint();
    }
    std::string dbname = currentDatabase(this_obj);
    if (!dbname.empty() && dbname != "default") {
        client->Execute(Query("USE " + sqlQuotedIdentifier(dbname)));
    }
}

/* Best-effort reset: a failed ResetConnection() or USE-reapply is recorded
 * in the query log. Returns true when the connection is usable again;
 * callers clear PHP streaming state only on true so an unrecovered wire
 * is not hidden behind cleared flags. */
static bool tryResetConnectionReapplyDatabase(zval *this_obj, clickhouse_object *obj,
                                              bool clear_insert_state)
{
    try {
        resetConnectionReapplyDatabase(this_obj, obj, clear_insert_state);
        return true;
    } catch (const std::exception &e) {
        try {
            recordQueryError(obj, obj->insert_sql, obj->insert_query_id, e);
        } catch (...) {}
        return false;
    } catch (...) {
        return false;
    }
}


/* Only ServerException supplies structured server code/name fields. */
void throwClickHouseError(const std::exception &e, const std::string &query_id)
{
    /* Preserve the original PHP exception behind a sentinel C++ throw. */
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

static std::string formatParamValue(zval *v, const std::string &type,
                                    bool inside_array, unsigned array_depth = 0);

static std::string formatScalarParam(zval *v)
{
    ZVAL_DEREF(v);
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
            /* Locale-independent decimal point; 17 digits round-trip an IEEE 754 double. */
            char buf[64];
            php_gcvt(Z_DVAL_P(v), 17, '.', 'e', buf);
            return std::string(buf);
        }
        default: {
            if (Z_TYPE_P(v) == IS_ARRAY || Z_TYPE_P(v) == IS_RESOURCE) {
                throw std::runtime_error(
                    "setting/parameter value must be a scalar (string, int, float, bool, or null), not an array or resource");
            }
            zend_string *coerced = zvalGetStringOrThrow(v);
            std::string out(ZSTR_VAL(coerced), ZSTR_LEN(coerced));
            zend_string_release(coerced);
            return out;
        }
    }
}

static bool wrappedTypeInner(const std::string &t, const char *prefix, size_t prefix_len, std::string &inner)
{
    if (t.size() > prefix_len + 1 &&
        t.compare(0, prefix_len, prefix) == 0 &&
        t.back() == ')') {
        inner = t.substr(prefix_len, t.size() - prefix_len - 1);
        return true;
    }
    return false;
}

static bool arrayInnerType(const std::string &type, std::string &inner)
{
    std::string t = type;
    for (;;) {
        if (wrappedTypeInner(t, "LowCardinality(", 15, inner) ||
            wrappedTypeInner(t, "Nullable(", 9, inner)) {
            t = inner;
            continue;
        }
        return wrappedTypeInner(t, "Array(", 6, inner);
    }
}

static bool typeNeedsQuoting(const std::string &t)
{
    if (t.compare(0, 9, "Nullable(") == 0 && t.back() == ')') {
        return typeNeedsQuoting(t.substr(9, t.size() - 10));
    }
    if (t.compare(0, 15, "LowCardinality(") == 0 && t.back() == ')') {
        return typeNeedsQuoting(t.substr(15, t.size() - 16));
    }

    /* Numeric/bool array elements are bare; other types need quotes. */
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

/* Unquoted array elements must not inject separators or SQL structure.
 * PHP numeric/bool values are formatted locally; coerced strings need validation. */
static bool isBareNumericLiteral(const std::string &s)
{
    if (s.empty()) return false;

    /* ClickHouse accepts signed inf/nan for floats, but its bound-array
     * parser rejects hexadecimal integer literals. */
    size_t i = 0;
    if (s[i] == '+' || s[i] == '-') ++i;
    std::string rest = s.substr(i);
    auto ieq = [](const std::string &a, const char *b) {
        size_t n = strlen(b);
        if (a.size() != n) return false;
        for (size_t k = 0; k < n; ++k) {
            if (tolower((unsigned char)a[k]) != b[k]) return false;
        }
        return true;
    };
    if (ieq(rest, "inf") || ieq(rest, "nan")) return true;

    bool any_digit = false;
    for (char c : s) {
        if (c >= '0' && c <= '9') { any_digit = true; continue; }
        if (c == '+' || c == '-' || c == '.' || c == 'e' || c == 'E') continue;
        return false;
    }
    return any_digit;
}

static bool typeAllowsNull(const std::string &t)
{
    if (t.compare(0, 9, "Nullable(") == 0 && t.back() == ')') {
        return true;
    }
    if (t.compare(0, 15, "LowCardinality(") == 0 && t.back() == ')') {
        return typeAllowsNull(t.substr(15, t.size() - 16));
    }
    return false;
}

static std::string formatParamValue(zval *v, const std::string &type,
                                    bool inside_array, unsigned array_depth)
{
    if (Z_TYPE_P(v) == IS_NULL) {
        return std::string();
    }

    if (Z_TYPE_P(v) == IS_ARRAY) {
        if (array_depth >= 32) {
            throw std::runtime_error(
                "Array typed parameter nesting depth exceeds limit of 32");
        }
        std::string inner;
        /* Only Array(T) (optionally under LowCardinality/Nullable wrappers)
         * may be formatted as a ClickHouse array literal. Map/Tuple/etc.
         * would drop keys via ZEND_HASH_FOREACH_VAL and produce garbage. */
        if (!arrayInnerType(type, inner)) {
            throw std::runtime_error(
                "PHP array value requires an Array(...) typed parameter "
                "(Map/Tuple/other composites are not supported as bound "
                "parameters; use a string representation or external data)");
        }
        bool quote = typeNeedsQuoting(inner);
        bool allow_null = typeAllowsNull(inner);
        std::string nested_inner;
        bool nested_array = arrayInnerType(inner, nested_inner);
        std::string out = "[";
        bool first = true;
        DerefZvalHold values_hold(v);
        zval *iv;
        ZEND_HASH_FOREACH_VAL(Z_ARRVAL(values_hold.value), iv) {
            ZVAL_DEREF(iv);
            if (!first) out += ",";
            first = false;
            if (Z_TYPE_P(iv) == IS_NULL) {
                if (!allow_null) {
                    throw std::runtime_error(
                        "NULL element in non-Nullable Array typed parameter");
                }
                out += "NULL";
                continue;
            }
            if (nested_array) {
                if (Z_TYPE_P(iv) != IS_ARRAY) {
                    throw std::runtime_error(
                        "Array typed parameter element must be an array");
                }
                out += formatParamValue(iv, inner, true, array_depth + 1);
                continue;
            }
            std::string sv = formatScalarParam(iv);
            if (quote) {
                /* Bound-array parsing requires doubled quotes and literal backslashes,
                 * unlike SQL-source string escaping. */
                std::string esc;
                esc.reserve(sv.size() + 2);
                esc += "'";
                for (char c : sv) {
                    if (c == '\'') esc += '\'';
                    esc += c;
                }
                esc += "'";
                out += esc;
            } else {
                /* Coerced strings/objects need validation before unquoted insertion. */
                switch (Z_TYPE_P(iv)) {
                    case IS_LONG:
                    case IS_DOUBLE:
                    case IS_TRUE:
                    case IS_FALSE:
                        break;
                    default:
                        if (!isBareNumericLiteral(sv)) {
                            throw std::runtime_error(
                                "non-numeric element in a numeric Array typed "
                                "parameter (each string/object element must be "
                                "a single numeric literal)");
                        }
                }
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
        /* An empty setting key terminates the native-protocol settings section. */
        if (!zk) {
            throw std::runtime_error("setting keys must be strings");
        }
        if (ZSTR_LEN(zk) == 0) {
            throw std::runtime_error("setting key must not be empty");
        }
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

void addAssocUInt64(zval *array, const char *key, uint64_t value)
{
    if (value <= (uint64_t)ZEND_LONG_MAX) {
        add_assoc_long(array, key, (zend_long)value);
        return;
    }
    std::string text = std::to_string(value);
    add_assoc_stringl(array, key, text.data(), text.size());
}

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
            addAssocUInt64(&args[0], "rows", p.rows);
            addAssocUInt64(&args[0], "bytes", p.bytes);
            addAssocUInt64(&args[0], "total_rows", p.total_rows);
            addAssocUInt64(&args[0], "written_rows", p.written_rows);
            addAssocUInt64(&args[0], "written_bytes", p.written_bytes);
            /* Pin the callable and bound object if the callback unregisters itself. */
            zval cb_copy;
            ZVAL_COPY(&cb_copy, &obj->progress_callback);
            call_user_function(NULL, NULL, &cb_copy, &retval, 1, args);
            zval_ptr_dtor(&cb_copy);
            zval_ptr_dtor(&args[0]);
            zval_ptr_dtor(&retval);
            /* Stop the packet loop; throwClickHouseError preserves the PHP exception. */
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
            addAssocUInt64(&args[0], "rows", pr.rows);
            addAssocUInt64(&args[0], "blocks", pr.blocks);
            addAssocUInt64(&args[0], "bytes", pr.bytes);
            addAssocUInt64(&args[0], "rows_before_limit", pr.rows_before_limit);
            add_assoc_bool(&args[0], "applied_limit", pr.applied_limit ? 1 : 0);
            add_assoc_bool(&args[0], "calculated_rows_before_limit", pr.calculated_rows_before_limit ? 1 : 0);
            /* Pin the callable across the call; see the OnProgress note. */
            zval cb_copy;
            ZVAL_COPY(&cb_copy, &obj->profile_callback);
            call_user_function(NULL, NULL, &cb_copy, &retval, 1, args);
            zval_ptr_dtor(&cb_copy);
            zval_ptr_dtor(&args[0]);
            zval_ptr_dtor(&retval);
            if (EG(exception)) {
                throw std::runtime_error("profile callback aborted query");
            }
        }
    });
}

static inline bool verbose_active(const clickhouse_object *obj)
{
    return obj->verbose_to_stderr || Z_TYPE(obj->verbose_callback) != IS_UNDEF;
}

/* Consumes ctx, which may be NULL for an event without payload. */
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
        zend_object *pre_exc = EG(exception);
        php_json_encode(&buf, &payload, 0);
        smart_str_0(&buf);
        /* Trace encoding is best-effort; preserve any previously pending exception. */
        if (EG(exception) && EG(exception) != pre_exc) {
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
        /* A sink may unregister itself; pin its callable and bound object. */
        zval cb_copy;
        ZVAL_COPY(&cb_copy, &obj->verbose_callback);
        call_user_function(NULL, NULL, &cb_copy, &retval, 2, args);
        zval_ptr_dtor(&cb_copy);
        zval_ptr_dtor(&args[0]);
        zval_ptr_dtor(&args[1]);
        zval_ptr_dtor(&retval);
        /* Clean up before aborting the packet loop on a PHP callback exception. */
        if (EG(exception)) {
            zval_ptr_dtor(&payload);
            if (ctx) zval_ptr_dtor(ctx);
            throw std::runtime_error("verbose callback aborted query");
        }
    }
    zval_ptr_dtor(&payload);
    if (ctx) zval_ptr_dtor(ctx);
}

static void attachVerbose(Query &q, clickhouse_object *obj)
{
    if (!verbose_active(obj)) return;

    q.OnException([obj](const Exception &e) {
        zval ctx;
        array_init(&ctx);
        add_assoc_long(&ctx, "code", (zend_long)e.code);
        add_assoc_string(&ctx, "name", e.name.c_str());
        /* Server error text can expose embedded SQL and bound secrets. */
        std::string vmsg = sanitizeError(e.display_text.c_str());
        add_assoc_stringl(&ctx, "message", (char*)vmsg.data(), vmsg.size());
        emitVerbose(obj, "server_exception", &ctx);
    });
}

static void resetStats(clickhouse_object *obj)
{
    obj->stats = ClientStats();
}

/* Clear callbacks before their captured stack locals expire. */
static void detachQueryCallbacks(Query &q)
{
    q.OnData(SelectCallback{});
    q.OnDataCancelable(SelectCancelableCallback{});
    q.OnException(ExceptionCallback{});
    q.OnProgress(ProgressCallback{});
    q.OnServerLog(SelectServerLogCallback{});
    q.OnProfileEvents(ProfileEventsCallback{});
    q.OnProfile(ProfileCallback{});
}

struct QueryActiveGuard {
    clickhouse_object *obj;
    bool armed;
    explicit QueryActiveGuard(clickhouse_object *o) : obj(o), armed(false) {
        if (obj->query_active) {
            throw std::runtime_error(
                "Reentrant operation: another query is already in progress "
                "on this ClickHouse instance. Use a separate ClickHouse "
                "instance from inside row / progress / profile / verbose "
                "callbacks.");
        }
        obj->query_active = true;
        armed = true;
    }
    ~QueryActiveGuard() { if (armed) obj->query_active = false; }
    QueryActiveGuard(const QueryActiveGuard&) = delete;
    QueryActiveGuard& operator=(const QueryActiveGuard&) = delete;
};


static std::string getInsertSql(std::string_view table_name, const zval *columns)
{
    HashTable *columns_ht = Z_ARRVAL_P(const_cast<zval*>(columns));
    size_t count = zend_hash_num_elements(columns_ht);

    std::string out;
    out.reserve(table_name.size() + 16 * count + 32);
    out.append("INSERT INTO ");
    parseIdentifier(table_name.data(), table_name.size(), "table name", true, &out);
    if (count == 0) {
        throw std::runtime_error("Column list must not be empty");
    }
    out.append(" ( ");

    bool first = true;
    zval *pzval;
    ZEND_HASH_FOREACH_VAL(columns_ht, pzval)
    {
        ZStrGuard sg(pzval);
        if (!first) out.append(",");
        parseIdentifier(sg.val(), sg.len(), "column name", false, &out);
        first = false;
    }
    ZEND_HASH_FOREACH_END();

    out.append(" ) VALUES");
    return out;
}

/*
 * Substitute placeholders in `sql` with values from `params_ht`.
 *
 *   {name}        client-side identifier substitution. A scalar must be
 *                 one token: an identifier (`[A-Za-z_][A-Za-z0-9_]*`,
 *                 optionally db-qualified by one dot) or a numeric
 *                 literal. Whitespace, commas, and other punctuation are
 *                 rejected so `{tbl}` = "a, b" cannot turn `FROM {tbl}`
 *                 into a cross join. An array joins validated tokens
 *                 with ", " for column lists.
 *
 *   {name:Type}   server-side parameter. The SQL text is left untouched;
 *                 the value goes into `out_params` for Query::SetParam,
 *                 and nullopt means server-side NULL. PHP arrays format
 *                 as ClickHouse array literals.
 *
 * A parameter that appears in neither form throws. Every occurrence of
 * a `{name}` placeholder is replaced.
 */
struct TypedParam {
    std::string name;
    std::optional<std::string> value;  // nullopt → server NULL
};

/* One identifier (optionally db-qualified) or numeric literal per token;
 * only array-valued placeholders may introduce comma-separated lists. */
static std::string validatePlaceholderToken(const char *val, size_t vlen,
                                            const std::string &name)
{
    auto throwInvalid = [&](const char *why) {
        throw std::runtime_error(
            "Placeholder value for {" + name + "} is invalid: " + why);
    };
    if (vlen == 0) throwInvalid("empty");

    auto isAlpha = [](unsigned char c) {
        return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_';
    };
    auto isDigit = [](unsigned char c) { return c >= '0' && c <= '9'; };

    size_t i = 0;
    unsigned char c0 = (unsigned char)val[0];
    if (isDigit(c0) || c0 == '+' || c0 == '-') {
        if (c0 == '+' || c0 == '-') {
            i++;
            if (i >= vlen || !isDigit((unsigned char)val[i])) {
                throwInvalid("sign without digits");
            }
        }
        bool seen_digit = false;
        while (i < vlen && isDigit((unsigned char)val[i])) { i++; seen_digit = true; }
        if (i < vlen && val[i] == '.') {
            i++;
            while (i < vlen && isDigit((unsigned char)val[i])) { i++; seen_digit = true; }
        }
        if (i < vlen && (val[i] == 'e' || val[i] == 'E')) {
            i++;
            if (i < vlen && (val[i] == '+' || val[i] == '-')) i++;
            if (i >= vlen || !isDigit((unsigned char)val[i])) {
                throwInvalid("malformed exponent");
            }
            while (i < vlen && isDigit((unsigned char)val[i])) i++;
        }
        if (!seen_digit) throwInvalid("numeric token without digits");
    } else if (isAlpha(c0)) {
        i++;
        bool dot_seen = false;
        while (i < vlen) {
            unsigned char c = (unsigned char)val[i];
            if (isAlpha(c) || isDigit(c)) { i++; continue; }
            if (c == '.' && !dot_seen) {
                dot_seen = true; i++;
                if (i >= vlen) throwInvalid("trailing dot");
                if (!isAlpha((unsigned char)val[i])) {
                    throwInvalid("segment after dot must start with a letter or underscore");
                }
                i++;
                continue;
            }
            break;
        }
    } else {
        throwInvalid("must start with a digit, sign, letter, or underscore");
    }
    if (i != vlen) {
        throwInvalid("only one identifier or numeric literal per token; "
                     "use array-valued placeholders for column lists");
    }
    return std::string(val, vlen);
}

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
        if (ZSTR_LEN(zk) == 0) {
            throw std::runtime_error("Placeholder array keys must be non-empty");
        }
        ZVAL_DEREF(pzval);
        std::string name(ZSTR_VAL(zk), ZSTR_LEN(zk));

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

        std::string repl;
        if (Z_TYPE_P(pzval) == IS_ARRAY) {
            DerefZvalHold values_hold(pzval);
            HashTable *aht = Z_ARRVAL_P(values_hold.get());
            if (zend_hash_num_elements(aht) == 0) {
                throw std::runtime_error(
                    "Placeholder value for {" + name + "} is invalid: empty array");
            }
            zval *iv;
            bool first = true;
            ZEND_HASH_FOREACH_VAL(aht, iv) {
                ZVAL_DEREF(iv);
                /* Nested arrays stringify to a bogus but syntactically valid "Array" identifier. */
                if (Z_TYPE_P(iv) == IS_ARRAY) {
                    throw std::runtime_error(
                        "Placeholder value for {" + name +
                        "} is invalid: a list element must not be an array");
                }
                zend_string *coerced = zvalGetStringOrThrow(iv);
                std::string tok;
                try {
                    tok = validatePlaceholderToken(
                        ZSTR_VAL(coerced), ZSTR_LEN(coerced), name);
                } catch (...) {
                    zend_string_release(coerced);
                    throw;
                }
                zend_string_release(coerced);
                if (!first) repl += ", ";
                first = false;
                repl += tok;
            } ZEND_HASH_FOREACH_END();
        } else {
            zend_string *coerced = zvalGetStringOrThrow(pzval);
            try {
                repl = validatePlaceholderToken(
                    ZSTR_VAL(coerced), ZSTR_LEN(coerced), name);
            } catch (...) {
                zend_string_release(coerced);
                throw;
            }
            zend_string_release(coerced);
        }
        std::string needle = "{" + name + "}";
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

/*
 * Shared prelude for select / execute / stream paths after getClient +
 * QueryActiveGuard: reset stats, reject mid-insert, apply placeholders,
 * attach settings/progress/profile/verbose callbacks. sql_s is the SQL
 * after placeholder rewrite; log_sql remains the caller's original text
 * for logging and verbose events.
 * params_err_msg: if non-null, a non-array params zval throws that text;
 * if null, non-array params are ignored (optional-arg paths).
 */
static void prepareQuery(clickhouse_object *obj,
                         std::string &log_sql,
                         const std::string &qid,
                         zval *params,
                         zval *settings,
                         Query &query,
                         const char *params_err_msg)
{
    resetStats(obj);
    obj->stats.last_query_id = qid;

    if (obj->has_insert_block) {
        throw std::runtime_error("The insert operation is now in progress");
    }

    std::string sql_s = log_sql;
    std::vector<TypedParam> typed_params;
    if (params != NULL && Z_TYPE_P(params) == IS_ARRAY) {
        applyPlaceholders(sql_s, Z_ARRVAL_P(params), typed_params);
        /* Log the template so substituted identifiers/numeric secrets stay private. */
    } else if (params != NULL && Z_TYPE_P(params) != IS_ARRAY) {
        if (params_err_msg) {
            throw std::runtime_error(params_err_msg);
        }
    }

    query = qid.empty() ? Query(sql_s) : Query(sql_s, qid);
    attachTypedParams(query, typed_params);
    applyMergedSettings(query, obj, settings);
    attachProgressAndProfile(query, obj);
    attachVerbose(query, obj);
}

/*
 * Select recovery: ServerException leaves the wire clean (no reset);
 * any other throw mid-stream resets the connection. Optional detach clears
 * stack-capturing callbacks before rethrow / after success. on_error runs
 * before rethrow (e.g. free a stream buffer). Sets elapsed_ms in all paths.
 */
static void runSelectWithRecovery(Client *client,
                                  Query &query,
                                  zval *this_obj,
                                  clickhouse_object *obj,
                                  const ExternalTables *external_tables,
                                  bool detach_callbacks,
                                  std::function<void()> on_error = nullptr)
{
    auto t0 = std::chrono::steady_clock::now();
    try {
        if (external_tables && !external_tables->empty()) {
            client->SelectWithExternalData(query, *external_tables);
        } else {
            client->Select(query);
        }
    } catch (const ServerException &) {
        setElapsedSince(obj, t0);
        if (on_error) {
            on_error();
        }
        if (detach_callbacks) {
            detachQueryCallbacks(query);
        }
        throw;
    } catch (...) {
        setElapsedSince(obj, t0);
        if (on_error) {
            on_error();
        }
        tryResetConnectionReapplyDatabase(this_obj, obj, false);
        if (detach_callbacks) {
            detachQueryCallbacks(query);
        }
        throw;
    }
    if (detach_callbacks) {
        detachQueryCallbacks(query);
    }
    setElapsedSince(obj, t0);
}

/*
 * Execute path: same recovery split as Select, without detach (Execute has
 * no OnData stack captures that outlive the call).
 */
static void runExecuteWithRecovery(Client *client,
                                   Query &query,
                                   zval *this_obj,
                                   clickhouse_object *obj)
{
    auto t0 = std::chrono::steady_clock::now();
    try {
        client->Execute(query);
    } catch (const ServerException &) {
        setElapsedSince(obj, t0);
        throw;
    } catch (...) {
        setElapsedSince(obj, t0);
        tryResetConnectionReapplyDatabase(this_obj, obj, false);
        throw;
    }
    setElapsedSince(obj, t0);
}

/* {{{ proto bool ping()
 */
PHP_METHOD(ClickHouse, ping)
{
    if (zend_parse_parameters_none() == FAILURE) return;
    try {
        clickhouse_object *obj = Z_CLICKHOUSE_P(getThis());
        Client *client = getClient(obj);
        QueryActiveGuard guard(obj);
        /* Between writeStart/writeEnd the wire stays in insert mode,
         * even when query_active is false between calls. */
        if (obj->has_insert_block) {
            throw std::runtime_error("The insert operation is now in progress");
        }
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
void do_select_into(zval *out, zval *this_obj,
                           const char *sql, size_t l_sql,
                           zval *params, zend_long fetch_mode,
                           const std::string &qid, zval *settings,
                           const ExternalTables *external_tables,
                           zval *positional_out = NULL)
{
    clickhouse_object *obj = Z_CLICKHOUSE_P(this_obj);
    std::string log_sql(sql, l_sql);
    ConvertDepthScopeGuard depth_scope;
    try
    {
        Client *client = getClient(obj);
        QueryActiveGuard guard(obj);

        Query query;
        prepareQuery(obj, log_sql, qid, params, settings, query,
                     "The second argument to the select function must be an array");

        if (verbose_active(obj)) {
            zval ctx;
            array_init(&ctx);
            std::string redacted_sql = redactSqlLiterals(log_sql);
            add_assoc_stringl(&ctx, "sql", (char*)redacted_sql.data(), redacted_sql.size());
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
        /* Duplicate names collapse in assoc rows. Retain positional rows only
         * for that case; otherwise Statement can use the assoc rows directly. */
        bool pos_decided = false;
        bool pos_active = false;
        query.OnData([out, positional_out, fetch_mode, &fetched_one, obj, &verbose_block_idx,
                      &pos_decided, &pos_active](const Block &block) {
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

            /* GetColumnName allocates a string; names are stable across the block. */
            const size_t col_count = block.GetColumnCount();
            std::vector<std::string> col_names;
            col_names.reserve(col_count);
            for (size_t c = 0; c < col_count; ++c) {
                col_names.emplace_back(block.GetColumnName(c));
            }

            if (positional_out && !pos_decided) {
                pos_decided = true;
                for (size_t a = 0; a < col_count && !pos_active; ++a) {
                    for (size_t b = a + 1; b < col_count; ++b) {
                        if (col_names[a] == col_names[b]) { pos_active = true; break; }
                    }
                }
                if (pos_active) {
                    array_init(positional_out);
                }
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

                    if (Z_TYPE(kp_col1) == IS_ARRAY || Z_TYPE(kp_col1) == IS_OBJECT) {
                        /* Composite key column would stringify to "Array";
                         * reject so the result isn't silently collapsed. */
                        zval_ptr_dtor(&kp_col1);
                        zval_ptr_dtor(&kp_col2);
                        throw std::runtime_error("Key pair mode requires a scalar key column");
                    }
                    if (Z_TYPE(kp_col1) == IS_LONG) {
                         zend_hash_index_update(Z_ARRVAL_P(out), Z_LVAL(kp_col1), &kp_col2);
                    } else {
                        zend_string *coerced = NULL;
                        try {
                            coerced = zvalGetStringOrThrow(&kp_col1);
                            zend_symtable_update(Z_ARRVAL_P(out), coerced, &kp_col2);
                            zend_string_release(coerced);
                        } catch (...) {
                            if (coerced) zend_string_release(coerced);
                            zval_ptr_dtor(&kp_col1);
                            zval_ptr_dtor(&kp_col2);
                            throw;
                        }
                    }
                    zval_ptr_dtor(&kp_col1);
                    continue;
                }

                zval row_tmp;
                ZVAL_UNDEF(&row_tmp);
                if (!(fetch_mode & SC_FETCH_COLUMN)) {
                    array_init_size(&row_tmp, (uint32_t)col_count);
                }

                try {
                    if (pos_active && !(fetch_mode & SC_FETCH_COLUMN)) {
                        zval pos_tmp;
                        array_init_size(&pos_tmp, (uint32_t)col_count);
                        try {
                            for (size_t column = 0; column < col_count; ++column)
                            {
                                zval cell;
                                ZVAL_UNDEF(&cell);
                                convertToZval(&cell, block[column], row, "", 0, fetch_mode|SC_FETCH_ONE);

                                zval assoc_cell;
                                ZVAL_COPY(&assoc_cell, &cell);
                                add_assoc_zval_ex(&row_tmp,
                                    col_names[column].c_str(),
                                    col_names[column].length(),
                                    &assoc_cell);
                                add_next_index_zval(&pos_tmp, &cell);
                            }
                            add_next_index_zval(positional_out, &pos_tmp);
                        } catch (...) {
                            zval_ptr_dtor(&pos_tmp);
                            throw;
                        }
                    } else {
                        for (size_t column = 0; column < col_count; ++column)
                        {
                            if (fetch_mode & SC_FETCH_COLUMN) {
                                convertToZval(&row_tmp, block[0], row, "", 0, fetch_mode|SC_FETCH_ONE);
                                break;
                            }
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

        runSelectWithRecovery(client, query, this_obj, obj, external_tables,
                              /*detach_callbacks=*/true);
        if (verbose_active(obj)) {
            zval ctx;
            array_init(&ctx);
            add_assoc_double(&ctx, "elapsed_ms", obj->stats.elapsed_ms);
            addAssocUInt64(&ctx, "rows_read", obj->stats.rows_read);
            addAssocUInt64(&ctx, "bytes_read", obj->stats.bytes_read);
            addAssocUInt64(&ctx, "blocks", verbose_block_idx);
            emitVerbose(obj, "select_finish", &ctx);
        }
        recordQuerySuccess(obj, log_sql, qid);
    }
    catch (const std::exception& e)
    {
        recordQueryError(obj, log_sql, qid, e);
        throwClickHouseError(e, qid);
    }
}

static void buildStatsArray(zval *out, const ClientStats &st)
{
    array_init(out);
    addAssocUInt64(out, "rows_read", st.rows_read);
    addAssocUInt64(out, "bytes_read", st.bytes_read);
    addAssocUInt64(out, "total_rows", st.total_rows);
    addAssocUInt64(out, "written_rows", st.written_rows);
    addAssocUInt64(out, "written_bytes", st.written_bytes);
    addAssocUInt64(out, "blocks", st.blocks);
    addAssocUInt64(out, "rows_before_limit", st.rows_before_limit);
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
    do_select_into(return_value, getThis(), ZSTR_VAL(sql), ZSTR_LEN(sql), params, fetch_mode, qid, settings, NULL);
}
/* }}} */

static void validateRowShapes(HashTable *values_ht, size_t columns_count);
static void buildSingleColumnZval(HashTable *values_ht, size_t column_index,
                                  const std::vector<zend_string*> *column_names,
                                  zval *out);
static ColumnRef buildColumnFromRows(HashTable *rows_ht, size_t col_index,
                                     const std::vector<zend_string*> *col_names,
                                     TypeRef type);

/* Entry shape: ['name' => ..., 'columns' => ['col' => 'Type', ...], 'rows' => [...]]. */
static Block buildExternalTableBlock(zval *entry, std::string &name_out)
{
    ZVAL_DEREF(entry);
    if (Z_TYPE_P(entry) != IS_ARRAY) {
        throw std::runtime_error("externals must be a list of arrays");
    }
    HashTable *ht = Z_ARRVAL_P(entry);

    zval *name_zv    = zend_hash_str_find(ht, "name", sizeof("name") - 1);
    zval *columns_zv = zend_hash_str_find(ht, "columns", sizeof("columns") - 1);
    zval *rows_zv    = zend_hash_str_find(ht, "rows", sizeof("rows") - 1);
    if (name_zv)    ZVAL_DEREF(name_zv);
    if (columns_zv) ZVAL_DEREF(columns_zv);
    if (rows_zv)    ZVAL_DEREF(rows_zv);

    if (!name_zv || Z_TYPE_P(name_zv) != IS_STRING) {
        throw std::runtime_error("external table requires string 'name'");
    }
    if (!columns_zv || Z_TYPE_P(columns_zv) != IS_ARRAY) {
        throw std::runtime_error("external table requires array 'columns'");
    }
    if (!rows_zv || Z_TYPE_P(rows_zv) != IS_ARRAY) {
        throw std::runtime_error("external table requires array 'rows'");
    }

    validateIdentifier(Z_STRVAL_P(name_zv), Z_STRLEN_P(name_zv),
                       "external table name", /*allow_dot=*/false);
    name_out.assign(Z_STRVAL_P(name_zv), Z_STRLEN_P(name_zv));

    /* Cell coercion can replace referenced descriptor members; keep arrays alive. */
    DerefZvalHold columns_hold(columns_zv);
    RowsSnapshot rows_hold(Z_ARRVAL_P(rows_zv));

    HashTable *columns_ht = Z_ARRVAL_P(columns_hold.get());
    HashTable *rows_ht    = rows_hold.get();
    size_t columns_count  = zend_hash_num_elements(columns_ht);
    if (columns_count == 0) {
        throw std::runtime_error("external table '" + name_out + "' has no columns");
    }

    std::vector<std::string> col_names;
    std::vector<TypeRef>     col_types;
    std::vector<zend_string*> col_names_zs;
    col_names.reserve(columns_count);
    col_types.reserve(columns_count);
    col_names_zs.reserve(columns_count);
    {
        zend_string *k;
        zend_ulong   nk;
        zval        *type_zv;
        ZEND_HASH_FOREACH_KEY_VAL(columns_ht, nk, k, type_zv) {
            (void)nk;
            if (!k) {
                throw std::runtime_error("external table '" + name_out +
                    "' columns must be an associative array of name => type");
            }
            ZVAL_DEREF(type_zv);
            if (Z_TYPE_P(type_zv) != IS_STRING) {
                throw std::runtime_error("external table '" + name_out +
                    "' column '" + std::string(ZSTR_VAL(k), ZSTR_LEN(k)) +
                    "' type must be a string");
            }
            validateIdentifier(ZSTR_VAL(k), ZSTR_LEN(k),
                               "external column name", /*allow_dot=*/false);
            std::string type_name(Z_STRVAL_P(type_zv), Z_STRLEN_P(type_zv));
            ColumnRef templ = CreateColumnByType(type_name);
            if (!templ) {
                throw std::runtime_error("external table '" + name_out +
                    "' column '" + std::string(ZSTR_VAL(k), ZSTR_LEN(k)) +
                    "' has unsupported type '" + type_name + "'");
            }
            col_names.emplace_back(ZSTR_VAL(k), ZSTR_LEN(k));
            col_types.push_back(templ->Type());
            col_names_zs.push_back(k);
        } ZEND_HASH_FOREACH_END();
    }

    validateRowShapes(rows_ht, columns_count);

    /* Zero-row blocks terminate external data; empty named tables never
     * reach the server and must be rejected here. */
    if (zend_hash_num_elements(rows_ht) == 0) {
        throw std::runtime_error(
            "external table '" + name_out + "' has no rows; the native protocol "
            "cannot carry an empty named external table — guard the call in "
            "userland and skip the query when the filter set is empty");
    }

    Block block;
    for (size_t c = 0; c < columns_count; ++c) {
        block.AppendColumn(col_names[c],
            buildColumnFromRows(rows_ht, c, &col_names_zs, col_types[c]));
    }
    return block;
}

/* {{{ proto mixed selectWithExternalData(string sql, array externals, array params, int mode, string query_id, array settings)
 *
 * SELECT with one or more named in-memory tables sent alongside the
 * query (ClickHouse "external data"). Keeps the SQL small when filtering
 * by a big list, e.g. `SELECT id, name FROM users WHERE id IN ext_ids`
 * with `ext_ids` supplied as 50k rows.
 *
 * Each entry of `externals`:
 *   ['name'    => 'ext_ids',
 *    'columns' => ['id' => 'UInt64'],
 *    'rows'    => [[1], [2], ...]]
 *
 * Multiple externals per call supported; names must appear literally
 * in the query body. Empty `externals` is rejected; use select()
 * when no external data is needed.
 */
PHP_METHOD(ClickHouse, selectWithExternalData)
{
    zend_string *sql = NULL;
    zval *externals = NULL;
    zval *params = NULL;
    zend_long fetch_mode = 0;
    zend_string *query_id = NULL;
    zval *settings = NULL;

    ZEND_PARSE_PARAMETERS_START(2, 6)
        Z_PARAM_STR(sql)
        Z_PARAM_ARRAY(externals)
        Z_PARAM_OPTIONAL
        Z_PARAM_ARRAY(params)
        Z_PARAM_LONG(fetch_mode)
        Z_PARAM_STR(query_id)
        Z_PARAM_ARRAY(settings)
    ZEND_PARSE_PARAMETERS_END();

    HashTable *externals_ht = Z_ARRVAL_P(externals);
    size_t externals_count = zend_hash_num_elements(externals_ht);
    if (externals_count == 0) {
        zend_throw_exception(clickhouse_exception_ce,
            "selectWithExternalData requires at least one external table; "
            "use select() when no external data is needed", 0);
        return;
    }

    /* ExternalTable borrows names and Blocks; reserve stable storage through Select. */
    std::vector<std::string> ext_names;
    std::vector<Block>       ext_blocks;
    ExternalTables           ext_tables;
    ext_names.reserve(externals_count);
    ext_blocks.reserve(externals_count);
    ext_tables.reserve(externals_count);

    try {
        zval *entry;
        ZEND_HASH_FOREACH_VAL(externals_ht, entry) {
            std::string nm;
            Block blk = buildExternalTableBlock(entry, nm);
            ext_names.push_back(std::move(nm));
            ext_blocks.push_back(std::move(blk));
            ext_tables.push_back(ExternalTable{
                std::string_view(ext_names.back()),
                ext_blocks.back()
            });
        } ZEND_HASH_FOREACH_END();
    } catch (const std::exception &e) {
        throwClickHouseError(e);
        return;
    }

    std::string qid = makeQid(query_id);
    do_select_into(return_value, getThis(), ZSTR_VAL(sql), ZSTR_LEN(sql),
                   params, fetch_mode, qid, settings, &ext_tables);
}
/* }}} */

/* Wire format shared by selectToStream() (output) and insertFromStream()
 * (input). The same four formats apply to both directions; aliases
 * TSV / TSVWithNames match ClickHouse's own short names. */
enum class StreamFormat { TSV, TSVWithNames, CSV, CSVWithNames };

static bool parseStreamFormat(const char *s, size_t l, StreamFormat &out)
{
    auto eq = [&](const char *lit) {
        size_t n = strlen(lit);
        return l == n && memcmp(s, lit, n) == 0;
    };
    if (eq("TabSeparated") || eq("TSV"))                    { out = StreamFormat::TSV;          return true; }
    if (eq("TabSeparatedWithNames") || eq("TSVWithNames"))  { out = StreamFormat::TSVWithNames; return true; }
    if (eq("CSV"))                                          { out = StreamFormat::CSV;          return true; }
    if (eq("CSVWithNames"))                                 { out = StreamFormat::CSVWithNames; return true; }
    return false;
}

static inline bool streamFormatIsCSV(StreamFormat f)
{
    return f == StreamFormat::CSV || f == StreamFormat::CSVWithNames;
}

static inline bool streamFormatHasHeader(StreamFormat f)
{
    return f == StreamFormat::TSVWithNames || f == StreamFormat::CSVWithNames;
}

/* Text formats cannot unambiguously serialize composite columns. */
static bool isStreamableColumnType(const TypeRef &t, std::string &reason_out)
{
    switch (t->GetCode()) {
        case Type::Code::Nullable:
            return isStreamableColumnType(t->As<NullableType>()->GetNestedType(), reason_out);
        case Type::Code::LowCardinality:
            return isStreamableColumnType(t->As<LowCardinalityType>()->GetNestedType(), reason_out);
        case Type::Code::Array:
        case Type::Code::Tuple:
        case Type::Code::Map:
        case Type::Code::Point:
        case Type::Code::Ring:
        case Type::Code::Polygon:
        case Type::Code::MultiPolygon:
            reason_out = t->GetName();
            return false;
        default:
            return true;
    }
}

static void tsvAppendEscaped(smart_str *buf, const char *s, size_t len)
{
    for (size_t i = 0; i < len; ++i) {
        char c = s[i];
        switch (c) {
            case '\\': smart_str_appendl(buf, "\\\\", 2); break;
            case '\t': smart_str_appendl(buf, "\\t", 2);  break;
            case '\n': smart_str_appendl(buf, "\\n", 2);  break;
            case '\r': smart_str_appendl(buf, "\\r", 2);  break;
            case '\0': smart_str_appendl(buf, "\\0", 2);  break;
            default:   smart_str_appendc(buf, c);
        }
    }
}

/* Quote literal "\N" so import distinguishes it from NULL. */
static void csvAppendEscaped(smart_str *buf, const char *s, size_t len)
{
    bool needs_quoting = (len == 2 && s[0] == '\\' && s[1] == 'N');
    for (size_t i = 0; i < len; ++i) {
        char c = s[i];
        if (c == '"' || c == ',' || c == '\n' || c == '\r') {
            needs_quoting = true;
            break;
        }
    }
    if (!needs_quoting) {
        smart_str_appendl(buf, s, len);
        return;
    }
    smart_str_appendc(buf, '"');
    for (size_t i = 0; i < len; ++i) {
        char c = s[i];
        if (c == '"') {
            smart_str_appendl(buf, "\"\"", 2);
        } else {
            smart_str_appendc(buf, c);
        }
    }
    smart_str_appendc(buf, '"');
}

/* Bypass zvals while matching their scalar string format and precision.
 * Scalar spellings need no TSV/CSV escaping; unsupported types fall back. */
static bool tryAppendStreamCell(smart_str *buf, const ColumnRef &col, size_t row)
{
    const Column *c = col.get();
    ColumnRef nested;
    Type::Code code = c->Type()->GetCode();
    if (code == Type::Code::Nullable) {
        auto n = col->As<ColumnNullable>();
        if (!n) {
            return false;
        }
        if (n->IsNull(row)) {
            smart_str_appendl(buf, "\\N", 2);
            return true;
        }
        nested = n->Nested();
        c = nested.get();
        code = c->Type()->GetCode();
        if (code == Type::Code::Nullable) {
            return false;
        }
    }
    char tmp[64];
    int l = -1;
    switch (code) {
        case Type::Code::Int8:
            l = snprintf(tmp, sizeof(tmp), ZEND_LONG_FMT,
                         (zend_long)static_cast<const ColumnInt8 *>(c)->At(row));
            break;
        case Type::Code::Int16:
            l = snprintf(tmp, sizeof(tmp), ZEND_LONG_FMT,
                         (zend_long)static_cast<const ColumnInt16 *>(c)->At(row));
            break;
        case Type::Code::Int32:
            l = snprintf(tmp, sizeof(tmp), ZEND_LONG_FMT,
                         (zend_long)static_cast<const ColumnInt32 *>(c)->At(row));
            break;
        case Type::Code::Int64: {
            int64_t v = static_cast<const ColumnInt64 *>(c)->At(row);
            if (v >= (int64_t)ZEND_LONG_MIN && v <= (int64_t)ZEND_LONG_MAX) {
                l = snprintf(tmp, sizeof(tmp), ZEND_LONG_FMT, (zend_long)v);
            } else {
                l = snprintf(tmp, sizeof(tmp), "%" PRId64, v);
            }
            break;
        }
        case Type::Code::UInt8:
            l = snprintf(tmp, sizeof(tmp), ZEND_LONG_FMT,
                         (zend_long)static_cast<const ColumnUInt8 *>(c)->At(row));
            break;
        case Type::Code::UInt16:
            l = snprintf(tmp, sizeof(tmp), ZEND_LONG_FMT,
                         (zend_long)static_cast<const ColumnUInt16 *>(c)->At(row));
            break;
        case Type::Code::UInt32:
            l = snprintf(tmp, sizeof(tmp), ZEND_LONG_FMT,
                         (zend_long)static_cast<const ColumnUInt32 *>(c)->At(row));
            break;
        case Type::Code::UInt64: {
            uint64_t v = static_cast<const ColumnUInt64 *>(c)->At(row);
            if (v <= (uint64_t)ZEND_LONG_MAX) {
                l = snprintf(tmp, sizeof(tmp), ZEND_LONG_FMT, (zend_long)v);
            } else {
                l = snprintf(tmp, sizeof(tmp), "%" PRIu64, v);
            }
            break;
        }
        case Type::Code::Float32:
            /* 'E' matches zval_get_string's %H formatting. */
            php_gcvt((double)static_cast<const ColumnFloat32 *>(c)->At(row),
                     (int)EG(precision), '.', 'E', tmp);
            l = (int)strlen(tmp);
            break;
        case Type::Code::Float64:
            php_gcvt(static_cast<const ColumnFloat64 *>(c)->At(row),
                     (int)EG(precision), '.', 'E', tmp);
            l = (int)strlen(tmp);
            break;
        case Type::Code::Bool:
            if (static_cast<const ColumnBool *>(c)->At(row)) {
                smart_str_appendl(buf, "1", 1);
            }
            return true;
        default:
            return false;
    }
    if (l < 0 || (size_t)l >= sizeof(tmp)) {
        return false;
    }
    smart_str_appendl(buf, tmp, (size_t)l);
    return true;
}

static void appendCellForStream(smart_str *buf, zval *cell, StreamFormat fmt)
{
    if (Z_TYPE_P(cell) == IS_NULL) {
        smart_str_appendl(buf, "\\N", 2);
        return;
    }
    const char *p;
    size_t l;
    zend_string *zs = NULL;
    if (Z_TYPE_P(cell) == IS_STRING) {
        p = Z_STRVAL_P(cell);
        l = Z_STRLEN_P(cell);
    } else {
        zs = zval_get_string(cell);
        p = ZSTR_VAL(zs);
        l = ZSTR_LEN(zs);
    }
    if (streamFormatIsCSV(fmt)) {
        csvAppendEscaped(buf, p, l);
    } else {
        tsvAppendEscaped(buf, p, l);
    }
    if (zs) zend_string_release(zs);
}

static void flushStreamBuf(smart_str *buf, zval *stream_zv)
{
    if (!buf->s || ZSTR_LEN(buf->s) == 0) return;
    /* A callback may fclose() the resource; never cache its php_stream pointer. */
    /* Silent lookup lets a closed stream surface as ClickHouseException. */
    php_stream *stream = (php_stream*)zend_fetch_resource2_ex(
        stream_zv, NULL, php_file_le_stream(), php_file_le_pstream());
    if (!stream) {
        throw std::runtime_error("selectToStream: stream was closed during the query");
    }
    /* Stream wrappers may make partial writes; fail only on no progress. */
    const char *data = ZSTR_VAL(buf->s);
    size_t n = ZSTR_LEN(buf->s);
    size_t off = 0;
    while (off < n) {
        ssize_t w = php_stream_write(stream, data + off, n - off);
        if (w <= 0) {
            throw std::runtime_error("selectToStream: write to PHP stream failed");
        }
        off += (size_t)w;
    }
    smart_str_free(buf);
}

/* Also release the buffer if the post-Select flush throws. */
struct SmartStrGuard {
    smart_str *buf;
    explicit SmartStrGuard(smart_str *b) : buf(b) {}
    ~SmartStrGuard() { smart_str_free(buf); }
    SmartStrGuard(const SmartStrGuard&) = delete;
    SmartStrGuard& operator=(const SmartStrGuard&) = delete;
};

static zend_long do_select_to_stream(zval *this_obj,
                                     const char *sql, size_t l_sql,
                                     zval *params, zval *stream_zv,
                                     StreamFormat fmt,
                                     const std::string &qid, zval *settings)
{
    clickhouse_object *obj = Z_CLICKHOUSE_P(this_obj);
    zend_long total_rows = 0;
    std::string log_sql(sql, l_sql);
    ConvertDepthScopeGuard depth_scope;
    try
    {
        Client *client = getClient(obj);
        QueryActiveGuard guard(obj);

        Query query;
        prepareQuery(obj, log_sql, qid, params, settings, query,
                     "The second argument to selectToStream must be an array");

        if (verbose_active(obj)) {
            zval ctx;
            array_init(&ctx);
            std::string redacted_sql = redactSqlLiterals(log_sql);
            add_assoc_stringl(&ctx, "sql", (char*)redacted_sql.data(), redacted_sql.size());
            add_assoc_stringl(&ctx, "query_id", (char*)qid.data(), qid.size());
            add_assoc_long(&ctx, "settings_count", (zend_long)obj->settings.size());
            emitVerbose(obj, "select_start", &ctx);
        }

        bool header_written = false;
        smart_str buf = {0};
        SmartStrGuard buf_guard(&buf);
        size_t verbose_block_idx = 0;
        const long fetch_mode = SC_FETCH_DATE_AS_STRINGS | SC_FETCH_ONE;
        const char *row_term  = streamFormatIsCSV(fmt) ? "\r\n" : "\n";
        const size_t row_term_len = streamFormatIsCSV(fmt) ? 2 : 1;
        const char  cell_sep  = streamFormatIsCSV(fmt) ? ','  : '\t';

        query.OnData([&](const Block &block) {
            const size_t col_count = block.GetColumnCount();
            const size_t row_count = block.GetRowCount();
            if (col_count == 0) return;
            if (verbose_active(obj)) {
                zval ctx;
                array_init(&ctx);
                add_assoc_long(&ctx, "rows", (zend_long)row_count);
                add_assoc_long(&ctx, "columns", (zend_long)col_count);
                add_assoc_long(&ctx, "block_index", (zend_long)verbose_block_idx++);
                emitVerbose(obj, "data_block", &ctx);
            }

            if (!header_written) {
                /* Schema is stable across all blocks of a query. */
                for (size_t c = 0; c < col_count; ++c) {
                    std::string reason;
                    if (!isStreamableColumnType(block[c]->Type(), reason)) {
                        throw std::runtime_error(
                            "selectToStream: column '" + std::string(block.GetColumnName(c)) +
                            "' has unsupported type '" + reason +
                            "'; TSV/CSV cannot represent it");
                    }
                }
                if (streamFormatHasHeader(fmt)) {
                    for (size_t c = 0; c < col_count; ++c) {
                        if (c > 0) smart_str_appendc(&buf, cell_sep);
                        std::string nm = block.GetColumnName(c);
                        if (streamFormatIsCSV(fmt)) {
                            csvAppendEscaped(&buf, nm.data(), nm.size());
                        } else {
                            tsvAppendEscaped(&buf, nm.data(), nm.size());
                        }
                    }
                    smart_str_appendl(&buf, row_term, row_term_len);
                }
                header_written = true;
            }

            for (size_t r = 0; r < row_count; ++r) {
                for (size_t c = 0; c < col_count; ++c) {
                    if (c > 0) smart_str_appendc(&buf, cell_sep);
                    if (tryAppendStreamCell(&buf, block[c], r)) {
                        continue;
                    }
                    zval cell;
                    ZVAL_UNDEF(&cell);
                    try {
                        convertToZval(&cell, block[c], r, "", 0, fetch_mode);
                    } catch (...) {
                        if (Z_TYPE(cell) != IS_UNDEF) zval_ptr_dtor(&cell);
                        throw;
                    }
                    appendCellForStream(&buf, &cell, fmt);
                    zval_ptr_dtor(&cell);
                }
                smart_str_appendl(&buf, row_term, row_term_len);
                ++total_rows;
            }

            flushStreamBuf(&buf, stream_zv);
        });

        runSelectWithRecovery(client, query, this_obj, obj, nullptr,
                              /*detach_callbacks=*/false);
        flushStreamBuf(&buf, stream_zv);
        if (verbose_active(obj)) {
            zval ctx;
            array_init(&ctx);
            add_assoc_double(&ctx, "elapsed_ms", obj->stats.elapsed_ms);
            addAssocUInt64(&ctx, "rows_read", obj->stats.rows_read);
            addAssocUInt64(&ctx, "bytes_read", obj->stats.bytes_read);
            addAssocUInt64(&ctx, "blocks", verbose_block_idx);
            emitVerbose(obj, "select_finish", &ctx);
        }
        recordQuerySuccess(obj, log_sql, qid);
    }
    catch (const std::exception &e)
    {
        recordQueryError(obj, log_sql, qid, e);
        throwClickHouseError(e, qid);
        return 0;
    }
    return total_rows;
}

/* {{{ proto int selectToStream(string sql, array params, mixed stream, string format = "TabSeparated", string query_id = "", array settings = [])
 *
 * Run a SELECT and write rows directly to a PHP stream resource in
 * TSV / CSV format (with optional column-name header). Returns the
 * number of rows written. Cells are formatted from native column data
 * and flushed block by block, with no per-row PHP arrays.
 *
 * Supported formats: TabSeparated (alias TSV), TabSeparatedWithNames
 * (alias TSVWithNames), CSV, CSVWithNames. Other values are rejected.
 *
 * Dates always emit as YYYY-MM-DD / YYYY-MM-DD HH:MM:SS[.fff] strings;
 * Decimal / Int128 / UInt128 as decimal strings. Array / Tuple / Map /
 * geometry columns are rejected because text formats can't serialize
 * them unambiguously. Nullable and LowCardinality wrappers around
 * supported scalars are fine.
 *
 * FixedString cells have trailing NUL padding trimmed, as in regular
 * reads. FIXEDSTRING_BINARY is not settable here, so binary payloads
 * ending in NUL lose the pad on export.
 */
PHP_METHOD(ClickHouse, selectToStream)
{
    zend_string *sql = NULL;
    zval *params = NULL;
    zval *stream_zv = NULL;
    zend_string *format_s = NULL;
    zend_string *query_id = NULL;
    zval *settings = NULL;

    ZEND_PARSE_PARAMETERS_START(3, 6)
        Z_PARAM_STR(sql)
        Z_PARAM_ARRAY(params)
        Z_PARAM_ZVAL(stream_zv)
        Z_PARAM_OPTIONAL
        Z_PARAM_STR(format_s)
        Z_PARAM_STR(query_id)
        Z_PARAM_ARRAY(settings)
    ZEND_PARSE_PARAMETERS_END();

    StreamFormat fmt = StreamFormat::TSV;
    if (format_s) {
        if (!parseStreamFormat(ZSTR_VAL(format_s), ZSTR_LEN(format_s), fmt)) {
            zend_throw_exception(clickhouse_exception_ce,
                "selectToStream: unknown format; expected TabSeparated, "
                "TabSeparatedWithNames, CSV, or CSVWithNames", 0);
            return;
        }
    }

    php_stream *stream = NULL;
    php_stream_from_zval_no_verify(stream, stream_zv);
    if (!stream) {
        zend_throw_exception(clickhouse_exception_ce,
            "selectToStream: argument 3 must be an open stream resource", 0);
        return;
    }

    std::string qid = makeQid(query_id);
    zend_long n = do_select_to_stream(getThis(), ZSTR_VAL(sql), ZSTR_LEN(sql),
                                      params, stream_zv, fmt, qid, settings);
    if (EG(exception)) return;
    RETURN_LONG(n);
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
    zend_long fetch_mode = 0;

    ZEND_PARSE_PARAMETERS_START(1, 5)
        Z_PARAM_STR(sql)
        Z_PARAM_OPTIONAL
        Z_PARAM_ARRAY(params)
        Z_PARAM_STR(query_id)
        Z_PARAM_ARRAY(settings)
        Z_PARAM_LONG(fetch_mode)
    ZEND_PARSE_PARAMETERS_END();
    std::string qid = makeQid(query_id);

    object_init_ex(return_value, clickhouse_statement_ce);
    clickhouse_statement_object *stmt = Z_CLICKHOUSE_STATEMENT_P(return_value);

    /* Statement iteration requires full rows; ignore result-shape flags. */
    do_select_into(&stmt->rows, getThis(), ZSTR_VAL(sql), ZSTR_LEN(sql), params,
                   fetch_mode & SC_FETCH_VALUE_FLAGS, qid, settings, NULL, &stmt->positional_rows);
    if (EG(exception)) {
        zval_ptr_dtor(return_value);
        ZVAL_UNDEF(return_value);
        return;
    }

    clickhouse_object *obj = Z_CLICKHOUSE_P(getThis());
    buildStatsArray(&stmt->statistics, obj->stats);
}
/* }}} */

/* Reject extra cells before the per-column gather can silently drop them.
 * Missing cells are checked during lookup. */
static void validateRowShapes(HashTable *values_ht, size_t columns_count)
{
    zval *pzval;
    ZEND_HASH_FOREACH_VAL(values_ht, pzval) {
        ZVAL_DEREF(pzval);
        if (Z_TYPE_P(pzval) != IS_ARRAY) {
            throw std::runtime_error(
                "The insert function needs to pass in a two-dimensional array");
        }
        size_t row_count = zend_hash_num_elements(Z_ARRVAL_P(pzval));
        if (row_count > columns_count) {
            throw std::runtime_error(
                "row has " + std::to_string(row_count) +
                " cells but only " + std::to_string(columns_count) +
                " columns were declared; extra cells would be silently dropped");
        }
    } ZEND_HASH_FOREACH_END();
}

/* Build one column at a time to bound intermediate PHP memory.
 * column_names enables name lookup after positional lookup fails.
 * Caller owns out; failure frees partial state and leaves it UNDEF. */
static void buildSingleColumnZval(HashTable *values_ht, size_t column_index,
                                  const std::vector<zend_string*> *column_names,
                                  zval *out)
{
    array_init_size(out, zend_hash_num_elements(values_ht));
    try {
        zval *pzval;
        ZEND_HASH_FOREACH_VAL(values_ht, pzval) {
            zval *fzval = extractRowCell(pzval, column_index, column_names);
            Z_TRY_ADDREF_P(fzval);
            add_next_index_zval(out, fzval);
        } ZEND_HASH_FOREACH_END();
    } catch (...) {
        zval_ptr_dtor(out);
        ZVAL_UNDEF(out);
        throw;
    }
}

static ColumnRef buildColumnFromRows(HashTable *rows_ht, size_t col_index,
                                     const std::vector<zend_string*> *col_names,
                                     TypeRef type)
{
    ColumnRef fused = tryBuildScalarColumnFromRows(rows_ht, col_index, col_names, type);
    if (fused) {
        return fused;
    }
    zval inner;
    buildSingleColumnZval(rows_ht, col_index, col_names, &inner);
    try {
        ColumnRef column = insertColumn(type, &inner);
        zval_ptr_dtor(&inner);
        return column;
    } catch (...) {
        zval_ptr_dtor(&inner);
        throw;
    }
}

/* {{{ proto array insert(string table, array columns, array values, string query_id, array settings)
 */
/* On error sets a PHP exception; callers must check EG(exception). */
static void do_insert_into(zval *this_obj, zend_string *table,
                           zval *columns, zval *values,
                           const std::string &qid, zval *settings)
{
    string sql;

    clickhouse_object *obj = Z_CLICKHOUSE_P(this_obj);
    try
    {
        Client *client = getClient(obj);
        QueryActiveGuard guard(obj);

        resetStats(obj);
        obj->stats.last_query_id = qid;

        if (obj->has_insert_block)
        {
            throw std::runtime_error("The insert operation is now in progress");
        }

        HashTable *columns_ht = Z_ARRVAL_P(columns);
        RowsSnapshot values_hold(Z_ARRVAL_P(values));
        HashTable *values_ht = values_hold.get();
        size_t columns_count = zend_hash_num_elements(columns_ht);

        /* Sparse/associative column lists require HashTable order, not numeric lookup. */
        std::vector<zend_string*> column_names;
        column_names.reserve(columns_count);
        /* Pin names across callbacks/coercion that may replace referenced columns. */
        struct ColNameGuard {
            std::vector<zend_string*> &v;
            ~ColNameGuard() { for (zend_string *s : v) zend_string_release(s); }
        } col_name_guard{column_names};
        {
            zval *cz;
            ZEND_HASH_FOREACH_VAL(columns_ht, cz) {
                ZVAL_DEREF(cz);
                if (Z_TYPE_P(cz) != IS_STRING) {
                    throw std::runtime_error(
                        "The columns array must be a list of column-name strings");
                }
                zend_string_addref(Z_STR_P(cz));
                column_names.push_back(Z_STR_P(cz));
            } ZEND_HASH_FOREACH_END();
        }

        validateRowShapes(values_ht, columns_count);

        sql = getInsertSql(std::string_view(ZSTR_VAL(table), ZSTR_LEN(table)), columns);

        Query insertQuery = qid.empty() ? Query(sql) : Query(sql, qid);
        applyMergedSettings(insertQuery, obj, settings);
        attachProgressAndProfile(insertQuery, obj);
        attachVerbose(insertQuery, obj);
        /* BeginInsert can throw after setting native inserting_; recover before reuse. */
        Block blockQuery;
        auto t0 = std::chrono::steady_clock::now();
        try {
            blockQuery = client->BeginInsert(insertQuery);
        } catch (...) {
            setElapsedSince(obj, t0);
            tryResetConnectionReapplyDatabase(this_obj, obj, false);
            throw;
        }
        bool insert_open = true;
        bool block_sent = false;

        /* Reentrant inserts on another client must not inherit relaxed NULL conversion. */
        InsertConversionScopeGuard conversion_scope;

        try {
            Block blockInsert;

            for (size_t index = 0; index < columns_count; ++index) {
                blockInsert.AppendColumn(
                    blockQuery.GetColumnName(index),
                    buildColumnFromRows(values_ht, index, &column_names,
                                        blockQuery[index]->Type()));
            }

            /* A partial send dirties the wire; recovery must reset rather than finalize. */
            block_sent = true;
            client->SendInsertBlock(blockInsert);
            client->EndInsert();
            insert_open = false;
            setElapsedSince(obj, t0);
        } catch (...) {
            setElapsedSince(obj, t0);
            /* Before any send, EndInsert closes an empty insert on a healthy wire.
             * A send/end failure needs reset. Reset recovers the handle, not the
             * data: ClickHouse may already have persisted transmitted blocks. */
            if (insert_open) {
                if (block_sent) {
                    tryResetConnectionReapplyDatabase(this_obj, obj, false);
                } else {
                    /* A failed EndInsert leaves native inserting_ set; reset before reuse. */
                    try {
                        client->EndInsert();
                    } catch (...) {
                        tryResetConnectionReapplyDatabase(this_obj, obj, false);
                    }
                }
            }
            throw;
        }
        recordQuerySuccess(obj, sql, qid);
    }
    catch (const std::exception& e)
    {
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

static bool acceptsNullCell(const TypeRef &t)
{
    switch (t->GetCode()) {
        case Type::Code::Nullable:
            return true;
        case Type::Code::LowCardinality:
            return acceptsNullCell(t->As<LowCardinalityType>()->GetNestedType());
        default:
            return false;
    }
}

static void enforceStreamCellMemoryLimit(size_t native_capacity,
                                         size_t zend_copy_size)
{
    if (native_capacity < 64 * 1024) {
        return;
    }
    zend_long configured = PG(memory_limit);
    if (configured < 0) {
        return;
    }
    size_t limit = (size_t)configured;
    size_t used = zend_memory_usage(true);
    if (used >= limit || native_capacity > limit - used ||
        zend_copy_size > limit - used - native_capacity) {
        throw std::runtime_error(
            "insertFromStream: cell exceeds the available PHP memory_limit");
    }
}

/* Push the just-parsed cell into the current row and clear the buffer.
 * TSV uses cell_is_null (set when `\N` starts a cell), so `\\N` stays
 * the two-character string. CSV has no escapes, so an unquoted literal
 * `\N` cell is matched by bytes. */
static void pushCell(std::string &cell_buf, bool cell_is_quoted,
                     bool cell_is_null, StreamFormat fmt,
                     std::vector<zval> &row_cells)
{
    zval z;
    if (cell_is_null) {
        ZVAL_NULL(&z);
    } else if (streamFormatIsCSV(fmt) && !cell_is_quoted &&
               cell_buf.size() == 2 && cell_buf[0] == '\\' &&
               cell_buf[1] == 'N') {
        ZVAL_NULL(&z);
    } else {
        enforceStreamCellMemoryLimit(cell_buf.capacity(), cell_buf.size());
        ZVAL_STRINGL(&z, cell_buf.data(), cell_buf.size());
    }
    row_cells.push_back(z);
    cell_buf.clear();
}

struct InsertStreamParser {
    StreamFormat fmt;
    size_t expected_cols;
    /* Set by insertFromStream() for *WithNames formats: the declared
     * $columns, compared case-sensitively against the stream header. */
    const std::vector<std::string> *expected_columns = nullptr;

    enum class State { CellStart, InCell, InQuoted, QuotePending } state = State::CellStart;
    std::string cell_buf;
    bool cell_is_quoted = false;     // CSV: did this cell start with `"`?
    std::vector<zval> row_cells;
    bool first_row_skipped = false;
    bool prev_was_cr = false;
    size_t pending_empty_rows = 0;
    /* TSV-only: a `\` at the very end of one feed() chunk has to wait
     * for the next chunk to know what it escapes. Same role prev_was_cr
     * plays for CRLF straddling a chunk boundary. */
    bool pending_backslash = false;
    /* TSV NULL consumes the whole cell; any bytes after \N are invalid. */
    bool cell_is_null = false;

    void appendCellByte(char value) {
        if (cell_buf.size() == cell_buf.max_size()) {
            throw std::runtime_error(
                "insertFromStream: cell exceeds the available PHP memory_limit");
        }
        size_t required = cell_buf.size() + 1;
        if (required > cell_buf.capacity()) {
            size_t target = cell_buf.capacity() > cell_buf.max_size() / 2
                ? required
                : cell_buf.capacity() * 2;
            if (target < required) {
                target = required;
            }
            enforceStreamCellMemoryLimit(target, required);
            cell_buf.reserve(target);
            enforceStreamCellMemoryLimit(cell_buf.capacity(), required);
        }
        cell_buf.push_back(value);
    }

    /* Owned by the parser between calls; transferred to the caller via
     * finishRow() and consumed there. */

    void finishCell() {
        pushCell(cell_buf, cell_is_quoted, cell_is_null, fmt, row_cells);
        cell_is_quoted = false;
        cell_is_null = false;
    }

    /* The handler takes zval ownership; the destructor cleans any remainder on throw. */
    template<typename RowHandler>
    void finishRow(RowHandler &on_row) {
        if (streamFormatHasHeader(fmt) && !first_row_skipped) {
            /* Header names and order must match positional destination columns. */
            if (expected_columns) {
                if (row_cells.size() != expected_columns->size()) {
                    std::string got = std::to_string(row_cells.size()) + " cells";
                    for (zval &z : row_cells) zval_ptr_dtor(&z);
                    row_cells.clear();
                    throw std::runtime_error(
                        "insertFromStream: header has " + got +
                        " but " + std::to_string(expected_columns->size()) +
                        " columns were declared");
                }
                for (size_t c = 0; c < row_cells.size(); ++c) {
                    zval *cell = &row_cells[c];
                    const std::string &want = (*expected_columns)[c];
                    if (Z_TYPE_P(cell) == IS_STRING &&
                        Z_STRLEN_P(cell) == want.size() &&
                        std::string(Z_STRVAL_P(cell), Z_STRLEN_P(cell)) == want) {
                        continue;
                    }
                    std::string got(Z_TYPE_P(cell) == IS_STRING
                        ? std::string(Z_STRVAL_P(cell), Z_STRLEN_P(cell)) : "\\N");
                    for (zval &z : row_cells) zval_ptr_dtor(&z);
                    row_cells.clear();
                    throw std::runtime_error(
                        "insertFromStream: header column " + std::to_string(c + 1) +
                        " is '" + got + "' but '" + want + "' was declared");
                }
            }
            for (zval &z : row_cells) zval_ptr_dtor(&z);
            row_cells.clear();
            first_row_skipped = true;
            return;
        }
        if (row_cells.size() != expected_cols) {
            size_t got = row_cells.size();
            for (zval &z : row_cells) zval_ptr_dtor(&z);
            row_cells.clear();
            throw std::runtime_error(
                "insertFromStream: row has " + std::to_string(got) +
                " cells but " + std::to_string(expected_cols) +
                " columns were declared");
        }
        on_row(row_cells);
        row_cells.clear();
    }

    ~InsertStreamParser() {
        for (zval &z : row_cells) zval_ptr_dtor(&z);
    }

    template<typename RowHandler>
    void feed(const char *data, size_t len, RowHandler &on_row) {
        const bool csv = streamFormatIsCSV(fmt);
        const char cell_sep = csv ? ',' : '\t';

        /* Return true when the escape consumed the byte; false requests reprocessing.
         * Shared by in-chunk and cross-chunk escapes. */
        auto decode_escape_byte = [&](char n) -> bool {
            switch (n) {
                case '\\': appendCellByte('\\'); return true;
                case 't':  appendCellByte('\t'); return true;
                case 'n':  appendCellByte('\n'); return true;
                case 'r':  appendCellByte('\r'); return true;
                case '0':  appendCellByte('\0'); return true;
                case 'b':  appendCellByte('\b'); return true;
                case 'f':  appendCellByte('\f'); return true;
                case 'a':  appendCellByte('\a'); return true;
                case 'v':  appendCellByte('\v'); return true;
                case 'N':
                    if (cell_buf.empty()) {
                        appendCellByte('\\');
                        appendCellByte('N');
                        cell_is_null = true;
                        return true;
                    }
                    appendCellByte('\\');
                    return false;
                default:
                    /* ClickHouse drops the backslash on unknown TSV escapes. */
                    appendCellByte(n);
                    return true;
            }
        };

        /* Skip blank lines consistently for all column counts, including before headers. */
        auto flush_pending_empty_rows = [&]() {
            pending_empty_rows = 0;
        };

        for (size_t i = 0; i < len; ++i) {
            char c = data[i];

            if (pending_backslash) {
                pending_backslash = false;
                if (state == State::CellStart) state = State::InCell;
                if (decode_escape_byte(c)) continue;
            }

            /* Drop deferred blanks before a leading separator can append a cell. */
            if (pending_empty_rows > 0 && !(prev_was_cr && c == '\n')) {
                flush_pending_empty_rows();
            }

            /* CSV-only quoted-cell state machine. TSV ignores '"' entirely. */
            if (csv && state == State::InQuoted) {
                if (c == '"') {
                    state = State::QuotePending;
                } else {
                    if (UNEXPECTED(cell_buf.capacity() >= 64 * 1024)) {
                        appendCellByte(c);
                    } else {
                        cell_buf.push_back(c);
                    }
                }
                continue;
            }
            if (csv && state == State::QuotePending) {
                if (c == '"') {
                    appendCellByte('"');
                    state = State::InQuoted;
                    continue;
                }
                /* RFC 4180 allows only a separator, row terminator, or EOF after closing quotes. */
                if (c != cell_sep && c != '\n' && c != '\r') {
                    throw std::runtime_error(
                        "insertFromStream: malformed CSV - byte after closing "
                        "quote must be ',', newline, or end of input");
                }
                state = State::InCell;
            }

            if (c == cell_sep) {
                finishCell();
                state = State::CellStart;
                continue;
            }
            if (c == '\n' || c == '\r') {
                if (c == '\r') {
                    prev_was_cr = true;
                } else {
                    if (prev_was_cr) { prev_was_cr = false; continue; }
                }
                if (state == State::CellStart && row_cells.empty() && cell_buf.empty()) {
                    ++pending_empty_rows;
                    state = State::CellStart;
                    continue;
                }
                finishCell();
                finishRow(on_row);
                state = State::CellStart;
                continue;
            }
            prev_was_cr = false;

            if (cell_is_null) {
                throw std::runtime_error(
                    "insertFromStream: TSV `\\N` is the whole-cell NULL "
                    "marker and cannot be followed by other data");
            }

            if (state == State::CellStart) {
                if (csv && c == '"') {
                    state = State::InQuoted;
                    cell_is_quoted = true;
                    continue;
                }
                state = State::InCell;
                /* fallthrough */
            }

            /* TSV escapes inside an unquoted cell. CSV unquoted cells
             * have no escape syntax. */
            if (!csv && c == '\\') {
                if (i + 1 >= len) {
                    /* Defer escapes split across feed() chunks. */
                    pending_backslash = true;
                    break;
                }
                if (decode_escape_byte(data[i + 1])) {
                    ++i;
                    continue;
                }
                continue;
            }

            if (UNEXPECTED(cell_buf.capacity() >= 64 * 1024)) {
                appendCellByte(c);
            } else {
                cell_buf.push_back(c);
            }
        }
    }

    template<typename RowHandler>
    void finish(RowHandler &on_row) {
        if (pending_backslash) {
            /* A dangling backslash at EOF is literal TSV content. */
            appendCellByte('\\');
            pending_backslash = false;
            if (state == State::CellStart) state = State::InCell;
        }
        if (state == State::QuotePending) {
            state = State::InCell;
        }
        if (state == State::InQuoted) {
            throw std::runtime_error("insertFromStream: unterminated quoted CSV cell at EOF");
        }
        pending_empty_rows = 0;
        /* Quoted empty cells still form a row, even without a trailing newline. */
        if (!cell_buf.empty() || !row_cells.empty() || cell_is_quoted) {
            finishCell();
            finishRow(on_row);
        }
    }
};

/* {{{ proto int insertFromStream(string table, array columns, mixed stream, string format = "TabSeparated", int batch_rows = 10000, string query_id = "", array settings = [])
 *
 * Stream-parse a TSV / CSV file (or any PHP stream resource) and INSERT
 * the rows into `table` in batches of `batch_rows`. Bytes are parsed in
 * C++; only `batch_rows` worth of per-column zvals exist at a time, so
 * the function works on inputs larger than memory.
 *
 * Formats: TabSeparated (alias TSV), TabSeparatedWithNames (alias
 * TSVWithNames), CSV, CSVWithNames. For `*WithNames`, the first row of
 * the input is compared case-sensitively against `$columns` (order
 * matters; a mismatch throws) and then discarded. Blank lines are
 * skipped wherever they appear.
 *
 * NULL: a literal cell `\N` (in either format) becomes a PHP null and
 * is rejected unless the target column is Nullable. Empty CSV cells
 * become empty strings, not NULL.
 *
 * Cells reach insertColumn() as IS_STRING and get the same coercion as
 * insert(). `Time` columns reject string input, so TSV / CSV imports
 * into Time columns are unsupported.
 *
 * Returns the total number of rows inserted.
 */
PHP_METHOD(ClickHouse, insertFromStream)
{
    zend_string *table = NULL;
    zval *columns = NULL;
    zval *stream_zv = NULL;
    zend_string *format_s = NULL;
    zend_long batch_rows = 10000;
    zend_string *query_id = NULL;
    zval *settings = NULL;

    ZEND_PARSE_PARAMETERS_START(3, 7)
        Z_PARAM_STR(table)
        Z_PARAM_ARRAY(columns)
        Z_PARAM_ZVAL(stream_zv)
        Z_PARAM_OPTIONAL
        Z_PARAM_STR(format_s)
        Z_PARAM_LONG(batch_rows)
        Z_PARAM_STR(query_id)
        Z_PARAM_ARRAY(settings)
    ZEND_PARSE_PARAMETERS_END();

    StreamFormat fmt = StreamFormat::TSV;
    if (format_s) {
        if (!parseStreamFormat(ZSTR_VAL(format_s), ZSTR_LEN(format_s), fmt)) {
            zend_throw_exception(clickhouse_exception_ce,
                "insertFromStream: unknown format; expected TabSeparated, "
                "TabSeparatedWithNames, CSV, or CSVWithNames", 0);
            return;
        }
    }

    if (batch_rows < 1) {
        zend_throw_exception(clickhouse_exception_ce,
            "insertFromStream: batch_rows must be >= 1", 0);
        return;
    }
    /* Bound buffered rows even when the caller requests a huge batch. */
    const zend_long MAX_BATCH_ROWS = 10000000; /* 10M rows */
    if (batch_rows > MAX_BATCH_ROWS) {
        zend_throw_exception(clickhouse_exception_ce,
            "insertFromStream: batch_rows exceeds the maximum of 10000000 "
            "(a larger value would buffer the whole stream in memory, "
            "defeating batching)", 0);
        return;
    }

    php_stream *stream = NULL;
    php_stream_from_zval_no_verify(stream, stream_zv);
    if (!stream) {
        zend_throw_exception(clickhouse_exception_ce,
            "insertFromStream: argument 3 must be an open stream resource", 0);
        return;
    }

    HashTable *columns_ht = Z_ARRVAL_P(columns);
    size_t columns_count = zend_hash_num_elements(columns_ht);
    if (columns_count == 0) {
        zend_throw_exception(clickhouse_exception_ce,
            "insertFromStream: columns list cannot be empty", 0);
        return;
    }

    std::string qid = makeQid(query_id);
    clickhouse_object *obj = Z_CLICKHOUSE_P(getThis());
    std::string sql;
    zend_long total_rows = 0;

    try {
        Client *client = getClient(obj);
        QueryActiveGuard guard(obj);

        resetStats(obj);
        obj->stats.last_query_id = qid;

        if (obj->has_insert_block) {
            throw std::runtime_error("The insert operation is now in progress");
        }

        /* Copy header names before userland coercion can alter referenced columns. */
        std::vector<std::string> header_names;
        {
            header_names.reserve(columns_count);
            zval *cz;
            ZEND_HASH_FOREACH_VAL(columns_ht, cz) {
                ZVAL_DEREF(cz);
                if (Z_TYPE_P(cz) != IS_STRING) {
                    throw std::runtime_error(
                        "insertFromStream: columns must be a list of column-name strings");
                }
                header_names.emplace_back(Z_STRVAL_P(cz), Z_STRLEN_P(cz));
            } ZEND_HASH_FOREACH_END();
        }

        sql = getInsertSql(std::string_view(ZSTR_VAL(table), ZSTR_LEN(table)), columns);

        Query insertQuery = qid.empty() ? Query(sql) : Query(sql, qid);
        applyMergedSettings(insertQuery, obj, settings);
        attachProgressAndProfile(insertQuery, obj);
        attachVerbose(insertQuery, obj);

        Block blockQuery;
        auto t0 = std::chrono::steady_clock::now();
        try {
            blockQuery = client->BeginInsert(insertQuery);
        } catch (...) {
            setElapsedSince(obj, t0);
            tryResetConnectionReapplyDatabase(getThis(), obj, false);
            throw;
        }

        bool insert_open  = true;
        bool block_dirty  = false;
        /* Latch across batches: a pre-send validation failure should EndInsert
         * without reconnecting, preserving session tables/settings. */
        bool sent_any_block = false;

        /* Isolate conversion state from reentrant inserts on other clients. */
        InsertConversionScopeGuard conversion_scope;

        std::vector<zval> col_zvals(columns_count);
        for (size_t c = 0; c < columns_count; ++c) array_init(&col_zvals[c]);

        auto cleanup_col_zvals = [&]() {
            for (size_t c = 0; c < columns_count; ++c) {
                if (Z_TYPE(col_zvals[c]) != IS_UNDEF) zval_ptr_dtor(&col_zvals[c]);
            }
        };

        size_t pending_rows = 0;

        auto flush_batch = [&]() {
            if (pending_rows == 0) return;
            Block blockInsert;
            for (size_t c = 0; c < columns_count; ++c) {
                zvalToBlock(blockInsert, blockQuery, c, &col_zvals[c]);
                zval_ptr_dtor(&col_zvals[c]);
                array_init(&col_zvals[c]);
            }
            block_dirty = true;
            client->SendInsertBlock(blockInsert);
            block_dirty = false;
            sent_any_block = true;
            pending_rows = 0;
        };

        auto on_row = [&](std::vector<zval> &row) {
            /* row.size() == columns_count is guaranteed by InsertStreamParser. */
            for (size_t c = 0; c < columns_count; ++c) {
                /* Reject NULL for non-Nullable columns before coercion can turn it into 0 or "". */
                if (Z_TYPE(row[c]) == IS_NULL &&
                    !acceptsNullCell(blockQuery[c]->Type())) {
                    throw std::runtime_error(
                        "insertFromStream: NULL (`\\N`) value for column '" +
                        std::string(blockQuery.GetColumnName(c)) +
                        "' which is not Nullable");
                }
                add_next_index_zval(&col_zvals[c], &row[c]);
                ZVAL_UNDEF(&row[c]);
            }
            ++pending_rows;
            ++total_rows;
            if (pending_rows >= (size_t)batch_rows) {
                flush_batch();
            }
        };

        try {
            stream = (php_stream *)zend_fetch_resource2_ex(
                stream_zv, NULL, php_file_le_stream(), php_file_le_pstream());
            if (!stream) {
                throw std::runtime_error(
                    "insertFromStream: argument 3 must remain an open stream resource");
            }

            InsertStreamParser parser;
            parser.fmt = fmt;
            parser.expected_cols = columns_count;
            parser.expected_columns = &header_names;
            parser.row_cells.reserve(columns_count);
            parser.cell_buf.reserve(256);

            constexpr size_t CHUNK = 64 * 1024;
            char buf[CHUNK];
            while (true) {
                ssize_t n = php_stream_read(stream, buf, CHUNK);
                if (n < 0) {
                    /* Read errors must trigger recovery, not finalize partially parsed input as EOF. */
                    throw std::runtime_error(
                        "insertFromStream: stream read returned an error");
                }
                if (n == 0) {
                    if (php_stream_eof(stream)) break;
                    /* Some wrappers return 0 without setting the EOF
                     * flag (transient unavailability, badly-written
                     * userland wrapper). Don't conflate with EOF. */
                    throw std::runtime_error(
                        "insertFromStream: stream returned 0 bytes without reaching EOF");
                }
                parser.feed(buf, (size_t)n, on_row);
            }
            parser.finish(on_row);

            flush_batch();
            client->EndInsert();
            insert_open = false;
            setElapsedSince(obj, t0);
        } catch (...) {
            setElapsedSince(obj, t0);
            cleanup_col_zvals();
            if (insert_open) {
                if (block_dirty || sent_any_block) {
                    /* A block was (partly) transmitted, so the open insert on
                     * the wire is dirty; only a reset can recover it. */
                    tryResetConnectionReapplyDatabase(getThis(), obj, false);
                } else {
                    /* No data was sent: close the empty insert to preserve session state.
                     * Reset only if EndInsert itself fails. */
                    try {
                        client->EndInsert();
                    } catch (...) {
                        tryResetConnectionReapplyDatabase(getThis(), obj, false);
                    }
                }
            }
            throw;
        }

        cleanup_col_zvals();
        recordQuerySuccess(obj, sql, qid);
    }
    catch (const std::exception &e) {
        recordQueryError(obj, sql, qid, e);
        throwClickHouseError(e, qid);
        return;
    }

    RETURN_LONG(total_rows);
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
        QueryActiveGuard guard(obj);

        resetStats(obj);
        obj->stats.last_query_id = qid;

        if (obj->has_insert_block)
        {
            throw std::runtime_error("The insert operation is now in progress");
        }

        sql = getInsertSql(std::string_view(ZSTR_VAL(table), ZSTR_LEN(table)), columns);

        Query insertQuery = qid.empty() ? Query(sql) : Query(sql, qid);
        applyMergedSettings(insertQuery, obj, settings);
        attachProgressAndProfile(insertQuery, obj);
        attachVerbose(insertQuery, obj);
        /* Recover both a partial BeginInsert and failure installing PHP insert state. */
        Block blockQuery;
        auto t0 = std::chrono::steady_clock::now();
        try {
            blockQuery = client->BeginInsert(insertQuery);
            obj->insert_block = blockQuery;
            obj->has_insert_block = true;
            obj->insert_sql = sql;
            obj->insert_query_id = qid;
            obj->insert_started_at = t0;
        } catch (...) {
            setElapsedSince(obj, t0);
            /* Pass false to retain PHP insert state if reconnect fails;
             * preserve the original BeginInsert error. */
            bool recovery_ok = tryResetConnectionReapplyDatabase(getThis(), obj, false);
            if (recovery_ok) {
                clearStreamingInsertState(obj);
            }
            throw;
        }
    }
    catch (const std::exception& e)
    {
        recordQueryError(obj, sql, qid, e);
        throwClickHouseError(e, qid);
        return;
    }
    RETURN_TRUE;
}
/* }}} */

/* {{{ proto bool write(array values)
 */
PHP_METHOD(ClickHouse, write)
{
    zval *values;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_ARRAY(values)
    ZEND_PARSE_PARAMETERS_END();

    bool session_owned = false;
    bool block_send_started = false;
    try
    {
        clickhouse_object *obj = Z_CLICKHOUSE_P(getThis());
        Client *client = getClient(obj);
        QueryActiveGuard guard(obj);
        session_owned = true;

        if (!obj->has_insert_block) {
            throw std::runtime_error("write() called without a matching writeStart()");
        }

        RowsSnapshot values_hold(Z_ARRVAL_P(values));
        HashTable *values_ht = values_hold.get();
        if (zend_hash_num_elements(values_ht) == 0) {
            RETURN_TRUE;
        }

        /* Use the declared width; deriving it from a short row silently drops columns. */
        size_t columns_count = obj->insert_block.GetColumnCount();
        if (columns_count == 0) {
            throw std::runtime_error(
                "writeStart() block has no columns; cannot stream rows");
        }

        validateRowShapes(values_ht, columns_count);
        Block &blockQuery = obj->insert_block;

        /* Isolate conversion state from reentrant inserts on other clients. */
        InsertConversionScopeGuard conversion_scope;

        Block blockInsert;
        for (size_t index = 0; index < columns_count; ++index) {
            /* Positional only: streaming write() has no column-name fallback. */
            blockInsert.AppendColumn(
                blockQuery.GetColumnName(index),
                buildColumnFromRows(values_ht, index, NULL,
                                    blockQuery[index]->Type()));
        }

        /* A send failure needs reset; conversion failure leaves the wire healthy. */
        block_send_started = true;
        client->SendInsertBlock(blockInsert);
    }
    catch (const std::exception& e)
    {
        /* Recover only if this call acquired the wire; a rejected reentrant call
         * must not disturb the outer operation. Healthy-wire failures finalize
         * prior writes; partial sends require reset. Neither path rolls back
         * blocks ClickHouse already persisted. Preserve the original error. */
        clickhouse_object *obj = Z_CLICKHOUSE_P(getThis());
        if (!session_owned) {
            throwClickHouseError(e, std::string());
            return;
        }
        std::string sql = obj->insert_sql;
        std::string qid = obj->insert_query_id;
        /* Clear PHP insert state only after recovery succeeds; otherwise retain
         * the dirty-wire marker. Reset failures are logged separately. */
        bool recovery_ok = true;
        if (obj->has_insert_block) {
            setElapsedSince(obj, obj->insert_started_at);
        }
        if (obj->client && obj->has_insert_block) {
            if (block_send_started) {
                recovery_ok = tryResetConnectionReapplyDatabase(getThis(), obj, false);
            } else {
                try {
                    obj->client->EndInsert();
                } catch (...) {
                    recovery_ok = tryResetConnectionReapplyDatabase(getThis(), obj, false);
                }
            }
        }
        if (!sql.empty() || !qid.empty()) {
            recordQueryError(obj, sql, qid, e);
        }
        if (recovery_ok) {
            clearStreamingInsertState(obj);
        }
        /* Conversion failures finalize earlier batches (non-transactional
         * ClickHouse). Callers that catch and retry must open a new stream
         * with writeStart(); reusing this stream is impossible. */
        throwClickHouseError(e, qid);
        return;
    }
    RETURN_TRUE;
}
/* }}} */

/* {{{ proto bool writeEnd()
 */
PHP_METHOD(ClickHouse, writeEnd)
{
    if (zend_parse_parameters_none() == FAILURE) return;
    clickhouse_object *obj = Z_CLICKHOUSE_P(getThis());
    std::string sql;
    std::string qid;
    bool end_insert_started = false;
    try
    {
        Client *client = getClient(obj);
        QueryActiveGuard guard(obj);
        if (!obj->has_insert_block) {
            throw std::runtime_error("writeEnd() called without a matching writeStart()");
        }

        sql = obj->insert_sql;
        qid = obj->insert_query_id;
        auto started_at = obj->insert_started_at;
        end_insert_started = true;
        client->EndInsert();
        setElapsedSince(obj, started_at);
        recordQuerySuccess(obj, sql, qid);
        clearStreamingInsertState(obj);
    }
    catch (const std::exception& e)
    {
        /* EndInsert failure leaves native inserting_ set. Reset for reuse
         * while preserving the original error; reset failures are logged. */
        bool recovery_ok = true;
        if (obj->has_insert_block) {
            setElapsedSince(obj, obj->insert_started_at);
        }
        if (end_insert_started && obj->client) {
            recovery_ok = tryResetConnectionReapplyDatabase(getThis(), obj, true);
        }
        if (!sql.empty() || !qid.empty()) {
            recordQueryError(obj, sql, qid, e);
        }
        if (recovery_ok && end_insert_started && obj->has_insert_block) {
            clearStreamingInsertState(obj);
        }
        throwClickHouseError(e, qid);
        return;
    }
    RETURN_TRUE;
}
/* }}} */

/* {{{ proto bool execute(string sql, array params, string query_id, array settings)
 */
/* On error sets a PHP exception; callers must check EG(exception). */
void do_execute_into(zval *this_obj,
                            const char *sql, size_t l_sql,
                            zval *params, const std::string &qid, zval *settings)
{
    clickhouse_object *obj = Z_CLICKHOUSE_P(this_obj);
    std::string log_sql(sql, l_sql);
    try
    {
        Client *client = getClient(obj);
        QueryActiveGuard guard(obj);

        Query query;
        prepareQuery(obj, log_sql, qid, params, settings, query,
                     "The second argument to execute must be an array");

        if (verbose_active(obj)) {
            zval ctx;
            array_init(&ctx);
            std::string redacted_sql = redactSqlLiterals(log_sql);
            add_assoc_stringl(&ctx, "sql", (char*)redacted_sql.data(), redacted_sql.size());
            add_assoc_stringl(&ctx, "query_id", (char*)qid.data(), qid.size());
            add_assoc_long(&ctx, "settings_count", (zend_long)obj->settings.size());
            emitVerbose(obj, "execute_start", &ctx);
        }

        runExecuteWithRecovery(client, query, this_obj, obj);
        if (verbose_active(obj)) {
            zval ctx;
            array_init(&ctx);
            add_assoc_double(&ctx, "elapsed_ms", obj->stats.elapsed_ms);
            emitVerbose(obj, "execute_finish", &ctx);
        }
        recordQuerySuccess(obj, log_sql, qid);
    }
    catch (const std::exception& e)
    {
        recordQueryError(obj, log_sql, qid, e);
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
    /* Validate into a temporary so malformed settings leave the old map intact. */
    std::unordered_map<std::string, std::string> m;
    HashTable *ht = Z_ARRVAL_P(arr);
    zval *vz;
    zend_string *zk;
    zend_ulong nk;
    try {
        ZEND_HASH_FOREACH_KEY_VAL(ht, nk, zk, vz) {
            (void)nk;
            if (!zk) {
                zend_throw_exception(clickhouse_exception_ce,
                    "setting keys must be strings", 0);
                return;
            }
            if (ZSTR_LEN(zk) == 0) {
                zend_throw_exception(clickhouse_exception_ce,
                    "setting key must not be empty", 0);
                return;
            }
            m[std::string(ZSTR_VAL(zk), ZSTR_LEN(zk))] = formatScalarParam(vz);
        } ZEND_HASH_FOREACH_END();
    } catch (const std::exception &e) {
        throwClickHouseError(e);
        return;
    }
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
        zend_throw_exception(clickhouse_exception_ce,
            "setting key must not be empty", 0);
        return;
    }
    clickhouse_object *obj = Z_CLICKHOUSE_P(getThis());
    try {
        obj->settings[std::string(ZSTR_VAL(key), ZSTR_LEN(key))] = formatScalarParam(value);
    } catch (const std::exception &e) {
        throwClickHouseError(e);
        return;
    }
    RETURN_ZVAL(getThis(), 1, 0);
}
/* }}} */

/* {{{ proto static setDatabase(string database)
 *
 * Switch the connection and cached default database for subsequent queries.
 * Returns $this.
 */
PHP_METHOD(ClickHouse, setDatabase)
{
    zend_string *db = NULL;
    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STR(db)
    ZEND_PARSE_PARAMETERS_END();
    if (ZSTR_LEN(db) == 0) {
        throwClickHouseError(std::runtime_error("database name must not be empty"));
        return;
    }
    clickhouse_object *obj = Z_CLICKHOUSE_P(getThis());
    if (!obj->client) {
        throwClickHouseError(std::runtime_error("Client is not connected"));
        return;
    }
    try {
        QueryActiveGuard guard(obj);
        if (obj->has_insert_block) {
            throw std::runtime_error("The insert operation is now in progress");
        }
        /* Persist the database in connection options for internal reconnects.
         * Connect before swapping so a failed switch leaves the old client usable. */
        std::string new_db(ZSTR_VAL(db), ZSTR_LEN(db));
        validateDatabaseName(new_db.c_str(), new_db.size());
        /* Setters mutate in place; copy options before a potentially failing connect. */
        ClientOptions new_opts = obj->client_options;
        new_opts.SetDefaultDatabase(new_db);
        Client *new_client = new Client(new_opts);
        delete obj->client;
        obj->client = new_client;
        obj->client_options = new_opts;
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
static bool setCallbackField(zval *target, zval *cb, const char *err_name)
{
    bool clear_only = Z_TYPE_P(cb) == IS_NULL;
    if (!clear_only && !zend_is_callable(cb, 0, NULL)) {
        std::string msg = std::string(err_name) + " expects a callable or null";
        zend_throw_exception(clickhouse_exception_ce, msg.c_str(), 0);
        return false;
    }
    if (Z_TYPE(*target) != IS_UNDEF) {
        zval_ptr_dtor(target);
        ZVAL_UNDEF(target);
    }
    if (clear_only) {
        return true;
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

    bool new_verbose_to_stderr = false;
    bool new_callback = false;

    if (Z_TYPE_P(sink) == IS_TRUE) {
        new_verbose_to_stderr = true;
    } else if (Z_TYPE_P(sink) == IS_FALSE || Z_TYPE_P(sink) == IS_NULL) {
    } else if (zend_is_callable(sink, 0, NULL)) {
        new_callback = true;
    } else {
        zend_throw_exception(clickhouse_exception_ce,
            "setVerbose expects bool, null, or callable", 0);
        return;
    }

    if (Z_TYPE(obj->verbose_callback) != IS_UNDEF) {
        zval_ptr_dtor(&obj->verbose_callback);
        ZVAL_UNDEF(&obj->verbose_callback);
    }
    obj->verbose_to_stderr = new_verbose_to_stderr;

    if (new_callback) {
        ZVAL_COPY(&obj->verbose_callback, sink);
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
    if (zend_parse_parameters_none() == FAILURE) return;
    try {
        clickhouse_object *obj = Z_CLICKHOUSE_P(getThis());
        QueryActiveGuard guard(obj);
        resetConnectionReapplyDatabase(getThis(), obj, true);
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
    if (zend_parse_parameters_none() == FAILURE) return;
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
    if (zend_parse_parameters_none() == FAILURE) return;
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
    if (zend_parse_parameters_none() == FAILURE) return;
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
        if (first) {
            ZVAL_DEREF(first);
        }
        if (!first || Z_TYPE_P(first) != IS_ARRAY) {
            throw std::runtime_error("insertAssoc: each row must be an associative array");
        }

        zval columns_zv;
        array_init(&columns_zv);

        /* Reuse the first row HashTable as the expected key set without allocating names. */
        HashTable *first_ht = Z_ARRVAL_P(first);
        size_t expected_count = zend_hash_num_elements(first_ht);
        {
            zval *fv;
            zend_string *fk;
            zend_ulong fnk;
            ZEND_HASH_FOREACH_KEY_VAL(first_ht, fnk, fk, fv) {
                (void)fv;
                (void)fnk;
                if (!fk) {
                    zval_ptr_dtor(&columns_zv);
                    throw std::runtime_error("insertAssoc: each row must have string keys (column names)");
                }
                add_next_index_stringl(&columns_zv, ZSTR_VAL(fk), ZSTR_LEN(fk));
            } ZEND_HASH_FOREACH_END();
        }

        /* The gatherer tries numeric indexes first; reject integer keys and
         * mismatched key sets before they can select the wrong values. */
        zval *row_zv;
        ZEND_HASH_FOREACH_VAL(rows_ht, row_zv) {
            ZVAL_DEREF(row_zv);
            if (Z_TYPE_P(row_zv) != IS_ARRAY) {
                zval_ptr_dtor(&columns_zv);
                throw std::runtime_error("insertAssoc: each row must be an associative array");
            }
            HashTable *row_ht = Z_ARRVAL_P(row_zv);
            if (zend_hash_num_elements(row_ht) != expected_count) {
                zval_ptr_dtor(&columns_zv);
                throw std::runtime_error(
                    "insertAssoc: row key count differs from first row");
            }
            zend_string *rk;
            zend_ulong rnk;
            zval *rv;
            ZEND_HASH_FOREACH_KEY_VAL(row_ht, rnk, rk, rv) {
                (void)rv;
                (void)rnk;
                if (!rk) {
                    zval_ptr_dtor(&columns_zv);
                    throw std::runtime_error(
                        "insertAssoc: each row must have string keys (column names)");
                }
                if (!zend_hash_exists(first_ht, rk)) {
                    zval_ptr_dtor(&columns_zv);
                    throw std::runtime_error(
                        std::string("insertAssoc: unexpected key '") +
                        std::string(ZSTR_VAL(rk), ZSTR_LEN(rk)) +
                        "' (not in first row's column set)");
                }
            } ZEND_HASH_FOREACH_END();
        } ZEND_HASH_FOREACH_END();

        do_insert_into(getThis(), table, &columns_zv, rows, qid, settings);
        zval_ptr_dtor(&columns_zv);
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




/* {{{ proto ClickHouseRowIterator selectStream(string sql, array params, string query_id, array settings)
 *
 * Run a SELECT and return a ClickHouseRowIterator. All native blocks
 * are buffered before returning; rows become PHP arrays lazily during
 * iteration. For unbounded streaming, use selectStreamCallback().
 */
PHP_METHOD(ClickHouse, selectStream)
{
    zend_string *sql = NULL;
    zval *params = NULL;
    zend_string *query_id = NULL;
    zval *settings = NULL;
    zend_long fetch_mode = 0;

    ZEND_PARSE_PARAMETERS_START(1, 5)
        Z_PARAM_STR(sql)
        Z_PARAM_OPTIONAL
        Z_PARAM_ARRAY(params)
        Z_PARAM_STR(query_id)
        Z_PARAM_ARRAY(settings)
        Z_PARAM_LONG(fetch_mode)
    ZEND_PARSE_PARAMETERS_END();
    std::string qid = makeQid(query_id);
    clickhouse_object *obj = Z_CLICKHOUSE_P(getThis());

    object_init_ex(return_value, clickhouse_iter_ce);
    clickhouse_iter_object *iter = Z_CLICKHOUSE_ITER_P(return_value);
    iter->fetch_mode = fetch_mode;
    std::string log_sql(ZSTR_VAL(sql), ZSTR_LEN(sql));
    ConvertDepthScopeGuard depth_scope;

    try {
        Client *client = getClient(obj);
        QueryActiveGuard guard(obj);

        Query query;
        prepareQuery(obj, log_sql, qid, params, settings, query,
                     /*params_err_msg=*/nullptr);

        if (verbose_active(obj)) {
            zval ctx;
            array_init(&ctx);
            std::string redacted_sql = redactSqlLiterals(log_sql);
            add_assoc_stringl(&ctx, "sql", (char*)redacted_sql.data(), redacted_sql.size());
            add_assoc_stringl(&ctx, "query_id", (char*)qid.data(), qid.size());
            add_assoc_long(&ctx, "settings_count", (zend_long)obj->settings.size());
            add_assoc_long(&ctx, "fetch_mode", (zend_long)fetch_mode);
            emitVerbose(obj, "select_start", &ctx);
        }

        size_t verbose_block_idx = 0;
        query.OnData([iter, obj, &verbose_block_idx](const Block &block) {
            if (block.GetRowCount() == 0 || block.GetColumnCount() == 0) return;
            if (verbose_active(obj)) {
                zval ctx;
                array_init(&ctx);
                add_assoc_long(&ctx, "rows", (zend_long)block.GetRowCount());
                add_assoc_long(&ctx, "columns", (zend_long)block.GetColumnCount());
                add_assoc_long(&ctx, "block_index", (zend_long)verbose_block_idx++);
                emitVerbose(obj, "data_block", &ctx);
            }
            if (iter->column_names.empty()) {
                const size_t nc = block.GetColumnCount();
                iter->column_names.reserve(nc);
                for (size_t c = 0; c < nc; ++c) {
                    iter->column_names.emplace_back(block.GetColumnName(c));
                }
            }
            zend_long configured = PG(memory_limit);
            size_t incoming = 0;
            if (configured > 0) {
                incoming = estimateRetainedBlockBytes(block);
                size_t limit = (size_t)configured;
                size_t php_used = zend_memory_usage(1);
                if (php_used >= limit ||
                    iter->buffered_estimate > limit - php_used ||
                    incoming > limit - php_used - iter->buffered_estimate) {
                    throw std::runtime_error(
                        "selectStream: buffered result exceeds PHP memory_limit; "
                        "use selectStreamCallback() for unbounded streaming or "
                        "raise memory_limit");
                }
            }
            iter->buffered_estimate =
                saturatingAddSize(iter->buffered_estimate, incoming);
            iter->total_rows += block.GetRowCount();
            iter->blocks.push_back(block);
        });

        runSelectWithRecovery(client, query, getThis(), obj, nullptr,
                              /*detach_callbacks=*/true);
        if (verbose_active(obj)) {
            zval ctx;
            array_init(&ctx);
            add_assoc_double(&ctx, "elapsed_ms", obj->stats.elapsed_ms);
            addAssocUInt64(&ctx, "rows_read", obj->stats.rows_read);
            addAssocUInt64(&ctx, "bytes_read", obj->stats.bytes_read);
            addAssocUInt64(&ctx, "blocks", verbose_block_idx);
            emitVerbose(obj, "select_finish", &ctx);
        }
        recordQuerySuccess(obj, log_sql, qid);
    }
    catch (const std::exception &e) {
        recordQueryError(obj, log_sql, qid, e);
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
    zend_long fetch_mode = 0;

    ZEND_PARSE_PARAMETERS_START(2, 6)
        Z_PARAM_STR(sql)
        Z_PARAM_ZVAL(cb)
        Z_PARAM_OPTIONAL
        Z_PARAM_ARRAY(params)
        Z_PARAM_STR(query_id)
        Z_PARAM_ARRAY(settings)
        Z_PARAM_LONG(fetch_mode)
    ZEND_PARSE_PARAMETERS_END();

    if (!zend_is_callable(cb, 0, NULL)) {
        zend_throw_exception(clickhouse_exception_ce,
            "Argument 2 passed to selectStreamCallback must be callable", 0);
        return;
    }

    std::string qid = makeQid(query_id);
    clickhouse_object *obj = Z_CLICKHOUSE_P(getThis());
    std::string log_sql(ZSTR_VAL(sql), ZSTR_LEN(sql));
    ConvertDepthScopeGuard depth_scope;

    try {
        Client *client = getClient(obj);
        QueryActiveGuard guard(obj);

        Query query;
        prepareQuery(obj, log_sql, qid, params, settings, query,
                     /*params_err_msg=*/nullptr);

        if (verbose_active(obj)) {
            zval ctx;
            array_init(&ctx);
            std::string redacted_sql = redactSqlLiterals(log_sql);
            add_assoc_stringl(&ctx, "sql", (char*)redacted_sql.data(), redacted_sql.size());
            add_assoc_stringl(&ctx, "query_id", (char*)qid.data(), qid.size());
            add_assoc_long(&ctx, "settings_count", (zend_long)obj->settings.size());
            emitVerbose(obj, "select_start", &ctx);
        }

        size_t verbose_block_idx = 0;
        /* Callback false requests cancellation and counts as success. */
        bool stop_requested = false;
        query.OnData([cb, obj, &verbose_block_idx, &stop_requested, fetch_mode](const Block &block) {
            /* In-flight blocks the server sent before processing the
             * Cancel must not invoke user code again. */
            if (stop_requested) return;
            if (block.GetRowCount() == 0 || block.GetColumnCount() == 0) return;
            if (verbose_active(obj)) {
                zval ctx;
                array_init(&ctx);
                add_assoc_long(&ctx, "rows", (zend_long)block.GetRowCount());
                add_assoc_long(&ctx, "columns", (zend_long)block.GetColumnCount());
                add_assoc_long(&ctx, "block_index", (zend_long)verbose_block_idx++);
                emitVerbose(obj, "data_block", &ctx);
            }
            /* GetColumnName allocates; cache names outside the row loop. */
            const size_t col_count = block.GetColumnCount();
            std::vector<std::string> col_names;
            col_names.reserve(col_count);
            for (size_t c = 0; c < col_count; ++c) {
                col_names.emplace_back(block.GetColumnName(c));
            }
            for (size_t row = 0; row < block.GetRowCount(); ++row) {
                zval row_zv;
                array_init(&row_zv);
                try {
                    for (size_t col = 0; col < col_count; ++col) {
                        convertToZval(&row_zv, block[col], row, col_names[col], 0,
                                      fetch_mode & SC_FETCH_VALUE_FLAGS);
                    }
                } catch (...) {
                    zval_ptr_dtor(&row_zv);
                    throw;
                }
                zval args[1], retval;
                ZVAL_NULL(&retval);
                ZVAL_COPY_VALUE(&args[0], &row_zv);
                call_user_function(NULL, NULL, cb, &retval, 1, args);
                bool row_stop = (Z_TYPE(retval) == IS_FALSE);
                zval_ptr_dtor(&args[0]);
                zval_ptr_dtor(&retval);
                /* Abort the packet loop while preserving the PHP callback exception. */
                if (EG(exception)) {
                    throw std::runtime_error("row callback aborted query");
                }
                if (row_stop) {
                    stop_requested = true;
                    break;
                }
            }
        });
        query.OnDataCancelable([&stop_requested](const Block &) {
            return !stop_requested;
        });

        runSelectWithRecovery(client, query, getThis(), obj, nullptr,
                              /*detach_callbacks=*/true);
        if (verbose_active(obj)) {
            zval ctx;
            array_init(&ctx);
            add_assoc_double(&ctx, "elapsed_ms", obj->stats.elapsed_ms);
            addAssocUInt64(&ctx, "rows_read", obj->stats.rows_read);
            addAssocUInt64(&ctx, "bytes_read", obj->stats.bytes_read);
            addAssocUInt64(&ctx, "blocks", verbose_block_idx);
            emitVerbose(obj, "select_finish", &ctx);
        }
        recordQuerySuccess(obj, log_sql, qid);
    }
    catch (const std::exception &e) {
        recordQueryError(obj, log_sql, qid, e);
        throwClickHouseError(e, qid);
    }
    RETURN_TRUE;
}
/* }}} */

PHP_METHOD(ClickHouseRowIterator, rewind)
{
    if (zend_parse_parameters_none() == FAILURE) return;
    clickhouse_iter_object *iter = Z_CLICKHOUSE_ITER_P(getThis());
    iter->block_idx = 0;
    iter->row_idx = 0;
    iter->cumulative_row_idx = 0;
}

PHP_METHOD(ClickHouseRowIterator, valid)
{
    if (zend_parse_parameters_none() == FAILURE) RETURN_FALSE;
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
    if (zend_parse_parameters_none() == FAILURE) return;
    clickhouse_iter_object *iter = Z_CLICKHOUSE_ITER_P(getThis());
    if (iter->block_idx >= iter->blocks.size()) {
        RETURN_NULL();
    }
    const Block &block = iter->blocks[iter->block_idx];
    if (iter->row_idx >= block.GetRowCount()) {
        RETURN_NULL();
    }
    array_init(return_value);
    /* convertToZval throws on unsupported or malformed server-side types;
     * letting the exception cross the C Zend dispatcher is UB. */
    static const std::string empty_name;
    try {
        const size_t col_count = block.GetColumnCount();
        for (size_t col = 0; col < col_count; ++col) {
            const std::string &name = (col < iter->column_names.size())
                ? iter->column_names[col]
                : empty_name;
            convertToZval(return_value, block[col], iter->row_idx, name, 0,
                          iter->fetch_mode & SC_FETCH_VALUE_FLAGS);
        }
    } catch (const std::exception &e) {
        zval_ptr_dtor(return_value);
        throwClickHouseError(e);
        RETURN_NULL();
    }
}

PHP_METHOD(ClickHouseRowIterator, key)
{
    if (zend_parse_parameters_none() == FAILURE) RETURN_LONG(0);
    clickhouse_iter_object *iter = Z_CLICKHOUSE_ITER_P(getThis());
    RETURN_LONG((zend_long)iter->cumulative_row_idx);
}

PHP_METHOD(ClickHouseRowIterator, next)
{
    if (zend_parse_parameters_none() == FAILURE) return;
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
    if (zend_parse_parameters_none() == FAILURE) RETURN_LONG(0);
    clickhouse_iter_object *iter = Z_CLICKHOUSE_ITER_P(getThis());
    RETURN_LONG((zend_long)iter->total_rows);
}


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
    zval rv;
    zval *p = sc_zend_read_property(clickhouse_exception_ce, getThis(), "server_code", sizeof("server_code") - 1, 0, &rv);
    if (p) ZVAL_DEREF(p);
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
    zval rv;
    zval *p = sc_zend_read_property(clickhouse_exception_ce, getThis(), "server_name", sizeof("server_name") - 1, 0, &rv);
    if (p) ZVAL_DEREF(p);
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
    zval rv;
    zval *p = sc_zend_read_property(clickhouse_exception_ce, getThis(), "query_id", sizeof("query_id") - 1, 0, &rv);
    if (p) ZVAL_DEREF(p);
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
/* }}} */

/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: noet sw=4 ts=4 fdm=marker
 * vim<600: noet sw=4 ts=4
 */
