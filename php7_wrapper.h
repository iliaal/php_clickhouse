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

/*
 * rv must be a caller-owned zval that outlives the use of the returned
 * pointer. zend_read_property returns a pointer INTO rv whenever the read
 * is satisfied by a magic __get rather than a declared property slot, so a
 * function-local rv (the previous design) left the caller dereferencing an
 * expired stack frame for any subclass that defines __get.
 */
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

/* Deref the found value so callers branching on Z_TYPE_P(v) see the real
 * type rather than IS_REFERENCE (a by-ref config value, e.g.
 * ['compression' => &$x], would otherwise fall through type checks). The
 * conditional-assignment keeps it a single expression usable in `if`. */
#define php_array_get_value(ht, str, v) \
    (((v = zend_hash_str_find(ht, (char *)str, sizeof(str)-1)) != NULL) \
     && ((v = (Z_ISREF_P(v) ? Z_REFVAL_P(v) : v)), !ZVAL_IS_NULL(v)))

/* FAST_ZPP _OR_NULL convenience macros are PHP 8.0+. Shim for 7.4 with
 * the older Z_PARAM_*_EX(dest, check_null, separate) form. */
#if PHP_VERSION_ID < 80000
#  ifndef Z_PARAM_STR_OR_NULL
#    define Z_PARAM_STR_OR_NULL(dest)   Z_PARAM_STR_EX(dest, 1, 0)
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
 * - PHP 7.4 can represent the scalar/array/object return types generated
 *   for this extension. Define only the PHP 8-only mixed/static tags as
 *   untyped so the native 7.4 macro preserves every representable type.
 */
#if PHP_VERSION_ID < 80000
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
# define IS_STATIC 0
/* gen_stub builds typed properties / typed class constants via
 * (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_<TYPE>[|MAY_BE_NULL]). 7.4
 * has typed properties but represents zend_type as an encoded integer
 * rather than PHP 8's mask structure. Map the generated single-type
 * masks to that encoding; nullable properties add the existing low bit. */
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
