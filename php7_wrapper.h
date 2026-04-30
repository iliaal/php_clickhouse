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

/* FAST_ZPP _OR_NULL convenience macros are PHP 8.0+. Shim for 7.4 with
 * the older Z_PARAM_*_EX(dest, check_null, separate) form. */
#if PHP_VERSION_ID < 80000
#  ifndef Z_PARAM_STR_OR_NULL
#    define Z_PARAM_STR_OR_NULL(dest)   Z_PARAM_STR_EX(dest, 1, 0)
#  endif
#  ifndef Z_PARAM_ARRAY_OR_NULL
#    define Z_PARAM_ARRAY_OR_NULL(dest) Z_PARAM_ARRAY_EX(dest, 1, 0)
#  endif
#  ifndef Z_PARAM_ZVAL_OR_NULL
#    define Z_PARAM_ZVAL_OR_NULL(dest)  Z_PARAM_ZVAL_EX(dest, 1, 0)
#  endif
#endif

/*
 * gen_stub.php on PHP master emits typed-parameter, typed-return-value, and
 * typed-class-constant macros that don't exist on older PHP. Shim them to
 * pre-typed equivalents so the generated arginfo header compiles unchanged
 * across the entire build matrix (PHP 7.4 through 8.5).
 *
 * The shims drop type information rather than emulate it. On builds older
 * than the threshold for each shim, reflection signatures revert to untyped
 * (parameter and return types disappear; typed class constants become
 * untyped). Runtime behavior is unchanged; only the introspection surface
 * is reduced.
 *
 * If gen_stub.php starts emitting more 8.x-only macros, extend this block
 * rather than narrowing the build matrix.
 */

/* PHP 8.4: zend_register_internal_class_with_flags rolls class registration
 * and flag setting into one call. Pre-8.4 splits them. */
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

/* PHP 8.3: typed class constants. Pre-8.3 uses the untyped variant; the
 * type argument is discarded. The shim returns void rather than
 * zend_class_constant* because the generated code never reads the return. */
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

/* PHP 8.0:
 *
 * - The IS_MIXED type tag and default-value-aware argument macros are
 *   8.0+ only. Pre-8.0 has neither. The native ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX
 *   exists on 7.4 but its expansion references the `type` argument, so
 *   passing IS_MIXED through it fails at compile time even though the
 *   outer macro is recognized.
 * - Override the macro on pre-8.0 to drop the type argument entirely
 *   (returning untyped reflection). Then IS_MIXED never reaches the
 *   compiler and the rest of the arginfo header compiles cleanly.
 */
#if PHP_VERSION_ID < 80000
# undef ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX
# define ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(name, return_reference, required_num_args, type, allow_null) \
    ZEND_BEGIN_ARG_INFO_EX(name, 0, return_reference, required_num_args)
/* ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE is wholly new in 8.0; pre-8.0
 * keeps the type info but drops the default-value annotation. */
# define ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(pass_by_ref, name, type, allow_null, default_value) \
    ZEND_ARG_TYPE_INFO(pass_by_ref, name, type, allow_null)
/* IS_MIXED is a PHP 8.0+ type tag (value 0x09). On 7.4 it isn't
 * declared. Return-type uses are stripped above by the
 * ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX shim, but parameter types
 * reach ZEND_ARG_TYPE_INFO directly with the IS_MIXED token still in
 * place. Define it to 0 (= IS_UNDEF, "no type constraint") so the
 * argument compiles untyped on 7.4, matching pre-8.0 reflection. */
# define IS_MIXED 0
/* gen_stub builds typed properties / typed class constants via
 * (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_<TYPE>[|MAY_BE_NULL]). 7.4
 * has typed properties but uses a different zend_type construction
 * pathway and lacks both the MAY_BE_* mask constants in this form
 * and the ZEND_TYPE_INIT_MASK macro. Shim them all to compile-time
 * zeros: the property/constant gets registered with no type info,
 * matching pre-7.4 behavior even where the runtime would have
 * supported the typed declaration on 7.4. */
# define ZEND_TYPE_INIT_MASK(mask) ((zend_type) 0)
# define MAY_BE_LONG 0
# define MAY_BE_STRING 0
# define MAY_BE_NULL 0
# define MAY_BE_BOOL 0
# define MAY_BE_DOUBLE 0
# define MAY_BE_ARRAY 0
#endif

/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: noet sw=4 ts=4 fdm=marker
 * vim<600: noet sw=4 ts=4
 */
