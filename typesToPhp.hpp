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

/*
 * RAII wrapper for the zend_string returned by zval_get_string. Used at
 * PHP-to-C boundaries where the surrounding code can throw without
 * forcing every site to write try { ... } catch { release; throw; }.
 */
struct ZStrGuard {
    zend_string *s;
    explicit ZStrGuard(zval *zv) : s(zval_get_string(zv)) {}
    ~ZStrGuard() { if (s) zend_string_release(s); }
    ZStrGuard(const ZStrGuard&) = delete;
    ZStrGuard& operator=(const ZStrGuard&) = delete;
    const char *val() const { return ZSTR_VAL(s); }
    size_t      len() const { return ZSTR_LEN(s); }
};

clickhouse::ColumnRef createColumn(clickhouse::TypeRef type);

clickhouse::ColumnRef insertColumn(clickhouse::TypeRef type, zval *value_zval);

void convertToZval(zval *arr, const clickhouse::ColumnRef& columnRef, int row,
                   const std::string& column_name, int8_t is_array, long fetch_mode);

void zvalToBlock(clickhouse::Block& blockDes, clickhouse::Block& blockSrc,
                 zend_ulong num_key, zval *value_zval);

/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: noet sw=4 ts=4 fdm=marker
 * vim<600: noet sw=4 ts=4
 */
