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

#define SC_MAKE_STD_ZVAL(p)             zval _stack_zval_##p; p = &(_stack_zval_##p)

/* __get may return a pointer into rv; the caller must keep rv alive. */
static inline zval* sc_zend_read_property(zend_class_entry *class_ptr, zval *obj, const char *s, int len, int silent, zval *rv)
{
#if PHP_VERSION_ID < 80000
    return zend_read_property(class_ptr, obj, s, len, silent, rv);
#else
    return zend_read_property(class_ptr, Z_OBJ_P(obj), s, len, silent, rv);
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

/* Referenced config values must expose their underlying type to callers. */
#define php_array_get_value(ht, str, v) \
    (((v = zend_hash_str_find(ht, (char *)str, sizeof(str)-1)) != NULL) \
     && ((v = (Z_ISREF_P(v) ? Z_REFVAL_P(v) : v)), !Z_ISNULL_P(v)))

/* FAST_ZPP _OR_NULL convenience macros are PHP 8.0+. Shim for 7.4 with
 * the older Z_PARAM_*_EX(dest, check_null, separate) form. */
#if PHP_VERSION_ID < 80000
#  ifndef Z_PARAM_STR_OR_NULL
#    define Z_PARAM_STR_OR_NULL(dest)   Z_PARAM_STR_EX(dest, 1, 0)
#  endif
#endif

/* Keep generated arginfo usable on older PHP, dropping only unsupported types. */

#if PHP_VERSION_ID < 80400
static zend_always_inline zend_class_entry *zend_register_internal_class_with_flags(
    zend_class_entry *class_entry,
    zend_class_entry *parent_ce,
    uint32_t ce_flags)
{
    zend_class_entry *ce = zend_register_internal_class_ex(class_entry, parent_ce);
    if (ce && ce_flags) {
        ce->ce_flags |= ce_flags;
    }
    return ce;
}
#endif

/* Generated callers ignore the return value; pre-8.3 constants are untyped. */
#if PHP_VERSION_ID < 80300
static zend_always_inline void zend_declare_typed_class_constant(
    zend_class_entry *ce,
    zend_string *name,
    zval *value,
    int access_type,
    zend_string *doc_comment,
    zend_type type)
{
    (void) type;
    zend_declare_class_constant_ex(ce, name, value, access_type, doc_comment);
}
#endif

#if PHP_VERSION_ID < 80000
# define ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(pass_by_ref, name, type, allow_null, default_value) \
    ZEND_ARG_TYPE_INFO(pass_by_ref, name, type, allow_null)
/* PHP 7.4 cannot express unions; unused mask tokens must not expand. */
# define ZEND_ARG_TYPE_MASK(pass_by_ref, name, type_mask, default_value) \
    ZEND_ARG_TYPE_INFO(pass_by_ref, name, 0, 0)
/* Only mixed/static become untyped; native 7.4 macros preserve other types. */
# define IS_MIXED 0
# define IS_STATIC 0
/* PHP 7.4 encodes zend_type as an integer with a nullable low bit. */
# define ZEND_TYPE_INIT_MASK(mask) (mask)
# define MAY_BE_LONG ZEND_TYPE_ENCODE(IS_LONG, 0)
# define MAY_BE_STRING ZEND_TYPE_ENCODE(IS_STRING, 0)
# define MAY_BE_NULL ((zend_type) 1)
# define MAY_BE_BOOL ZEND_TYPE_ENCODE(_IS_BOOL, 0)
# define MAY_BE_DOUBLE ZEND_TYPE_ENCODE(IS_DOUBLE, 0)
# define MAY_BE_ARRAY ZEND_TYPE_ENCODE(IS_ARRAY, 0)
#endif

/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: noet sw=4 ts=4 fdm=marker
 * vim<600: noet sw=4 ts=4
 */
