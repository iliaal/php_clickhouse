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

#define sc_zend_throw_exception_tsrmls_cc(a, b, c) zend_throw_exception(a, b, c)

#define sc_zend_hash_index_update   zend_hash_index_update
#define sc_zend_hash_find   zend_hash_str_find
#define sc_zend_hash_index_find   zend_hash_index_find
#define SC_MAKE_STD_ZVAL(p)             zval _stack_zval_##p; p = &(_stack_zval_##p)
#define sc_zval_ptr_dtor(p)  zval_ptr_dtor(*p)
#define sc_zval_add_ref(p)   Z_TRY_ADDREF_P(p)
#define sc_add_assoc_long_ex                  add_assoc_long_ex
#define sc_add_assoc_double_ex                add_assoc_double_ex
#define sc_add_assoc_zval_ex                  add_assoc_zval_ex
#define sc_add_assoc_stringl_ex(a, b, c, d, e, f)               add_assoc_stringl_ex(a, b, c, d, e)
#define sc_add_assoc_null_ex(a, b, c)               add_assoc_null_ex(a, b, c)

static inline zval* sc_zend_read_property(zend_class_entry *class_ptr, zval *obj, const char *s, int len, int silent)
{
    zval rv;
#if PHP_VERSION_ID < 80000
    return zend_read_property(class_ptr, obj, s, len, silent, &rv);
#else
    return zend_read_property(class_ptr, Z_OBJ_P(obj), s, len, silent, &rv);
#endif
}

static inline void sc_zend_update_property_string(zend_class_entry *scope, zval *object, const char *name, size_t name_length, const char *value)
{
#if PHP_VERSION_ID < 80000
    zend_update_property_string(scope, object, name, name_length, value);
#else
    zend_update_property_string(scope, Z_OBJ_P(object), name, name_length, value);
#endif
}

static inline void sc_zend_update_property_long(zend_class_entry *scope, zval *object, const char *name, size_t name_length, zend_long value)
{
#if PHP_VERSION_ID < 80000
    zend_update_property_long(scope, object, name, name_length, value);
#else
    zend_update_property_long(scope, Z_OBJ_P(object), name, name_length, value);
#endif
}

static inline void sc_zend_update_property_stringl(zend_class_entry *scope, zval *object, const char *name, size_t name_length, const char *value, size_t value_length)
{
#if PHP_VERSION_ID < 80000
    zend_update_property_stringl(scope, object, name, name_length, value, value_length);
#else
    zend_update_property_stringl(scope, Z_OBJ_P(object), name, name_length, value, value_length);
#endif
}

#define sc_add_next_index_stringl(arr, str, len, dup)    add_next_index_stringl(arr, str, len)

static inline int sc_zend_hash_get_current_data(HashTable *ht, void **v)
{
    zval *value = zend_hash_get_current_data(ht);
    if (value == NULL)
    {
        return FAILURE;
    }
    *v = (void *) value;
    return SUCCESS;
}

#define php_array_get_value(ht, str, v) ((v = sc_zend_hash_find(ht, (char *)str, sizeof(str)-1)) && !ZVAL_IS_NULL(v))

/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: noet sw=4 ts=4 fdm=marker
 * vim<600: noet sw=4 ts=4
 */
