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
#include <string>
#include <stdexcept>

/* Own zval_get_string output across C++ exceptions. A throwing __toString
 * returns an interned empty string; raise a sentinel C++ exception while
 * preserving EG(exception) for the PHP boundary. */
struct ZStrGuard {
    zend_string *s;
    explicit ZStrGuard(zval *zv) : s(zval_get_string(zv)) {
        if (UNEXPECTED(EG(exception))) {
            throw std::runtime_error("exception while converting a value to string");
        }
    }
    ~ZStrGuard() { if (s) zend_string_release(s); }
    ZStrGuard(const ZStrGuard&) = delete;
    ZStrGuard& operator=(const ZStrGuard&) = delete;
    const char *val() const { return ZSTR_VAL(s); }
    size_t      len() const { return ZSTR_LEN(s); }
};

/* Isolate top-level inserts from another client's reentrant NULL/depth state;
 * restore outer state on scope exit. */
struct InsertConversionScopeGuard {
    int saved_null;
    int saved_depth;
    InsertConversionScopeGuard();
    ~InsertConversionScopeGuard();
    InsertConversionScopeGuard(const InsertConversionScopeGuard&) = delete;
    InsertConversionScopeGuard& operator=(const InsertConversionScopeGuard&) = delete;
};

/* Isolate convert_depth at top-level select entrypoints (same reentry
 * concern as InsertConversionScopeGuard, without the null-strictness reset). */
struct ConvertDepthScopeGuard {
    int saved_depth;
    ConvertDepthScopeGuard();
    ~ConvertDepthScopeGuard();
    ConvertDepthScopeGuard(const ConvertDepthScopeGuard&) = delete;
    ConvertDepthScopeGuard& operator=(const ConvertDepthScopeGuard&) = delete;
};

clickhouse::ColumnRef createColumn(clickhouse::TypeRef type);

/* Reject caller-supplied type names whose length or nesting would exhaust the
 * library's recursive type factory. */
void validateCallerTypeName(const std::string &type_name, const std::string &context);

clickhouse::ColumnRef insertColumn(clickhouse::TypeRef type, zval *value_zval);

void convertToZval(zval *arr, const clickhouse::ColumnRef& columnRef, int row,
                   const std::string& column_name, int8_t is_array, long fetch_mode);

void zvalToBlock(clickhouse::Block& blockDes, clickhouse::Block& blockSrc,
                 zend_ulong num_key, zval *value_zval);

/* Positional lookup, then name fallback; validates row shape and dereferences cells. */
zval *extractRowCell(zval *row_pz, size_t col_index,
                     const std::vector<zend_string*> *col_names);

/* Build without transposing; nullptr requests the caller's transpose fallback. */
clickhouse::ColumnRef tryBuildScalarColumnFromRows(
    HashTable *rows_ht, size_t col_index,
    const std::vector<zend_string*> *col_names, clickhouse::TypeRef type);

/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: noet sw=4 ts=4 fdm=marker
 * vim<600: noet sw=4 ts=4
 */
