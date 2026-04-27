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
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

extern "C" {
#include "php.h"
#include "php_ini.h"
#include "ext/standard/info.h"
#include "php7_wrapper.h"
};

#include "php_clickhouse.h"

#include "lib/clickhouse-cpp/clickhouse/client.h"
#include "lib/clickhouse-cpp/clickhouse/error_codes.h"
#include "lib/clickhouse-cpp/clickhouse/types/type_parser.h"
#include "lib/clickhouse-cpp/clickhouse/columns/factory.h"
#include "lib/clickhouse-cpp/clickhouse/columns/geo.h"
#include "lib/clickhouse-cpp/clickhouse/columns/ip4.h"
#include "lib/clickhouse-cpp/clickhouse/columns/ip6.h"
#include "lib/clickhouse-cpp/clickhouse/columns/lowcardinality.h"
#include "lib/clickhouse-cpp/clickhouse/columns/map.h"
#include <map>
#include <sstream>
#include <iomanip>
#include <algorithm>

#include "typesToPhp.hpp"

using namespace clickhouse;
using namespace std;

/*
 * Parse "YYYY-MM-DD" or "YYYY-MM-DD HH:MM:SS" into a Unix epoch.
 *
 * Use timegm, not mktime: the read paths in convertToZval all format
 * via gmtime, so the round-trip needs to be UTC symmetric. Using
 * mktime would reinterpret the parsed components as local time and
 * shift the stored value by the runner's TZ offset.
 */
static std::time_t to_time_t(const std::string& str, bool is_date = true)
{
    std::tm t = {0};
    std::istringstream ss(str);
    ss >> std::get_time(&t, is_date ? "%Y-%m-%d" : "%Y-%m-%d %H:%M:%S");
#ifdef _WIN32
    /* MSVC has no timegm(); _mkgmtime() is the documented equivalent. */
    return _mkgmtime(&t);
#else
    return timegm(&t);
#endif
}

ColumnRef createColumn(TypeRef type)
{
    switch (type->GetCode())
    {
    case Type::Code::UInt64:
    {
        return std::make_shared<ColumnUInt64>();
    }
    case Type::Code::UInt8:
    {
        return std::make_shared<ColumnUInt8>();
    }
    case Type::Code::UInt16:
    {
        return std::make_shared<ColumnUInt16>();
    }
    case Type::Code::UInt32:
    {
        return std::make_shared<ColumnUInt32>();
    }

    case Type::Code::Int8:
    {
        return std::make_shared<ColumnInt8>();
    }
    case Type::Code::Int16:
    {
        return std::make_shared<ColumnInt16>();
    }
    case Type::Code::Int32:
    {
        return std::make_shared<ColumnInt32>();
    }
    case Type::Code::Int64:
    {
        return std::make_shared<ColumnInt64>();
    }

    case Type::Code::UUID:
    {
        return std::make_shared<ColumnUUID>();
    }

    case Type::Code::Float32:
    {
        return std::make_shared<ColumnFloat32>();
    }
    case Type::Code::Float64:
    {
        return std::make_shared<ColumnFloat64>();
    }

    case Type::Code::String:
    {
        return std::make_shared<ColumnString>();
    }
    case Type::Code::FixedString:
    {
        string typeName = type->GetName();
        typeName.erase(typeName.find("FixedString("), 12);
        typeName.erase(typeName.find(")"), 1);
        return std::make_shared<ColumnFixedString>(std::stoi(typeName));
    }

    case Type::Code::DateTime:
    {
        return std::make_shared<ColumnDateTime>();
    }
    case Type::Code::DateTime64:
    {
        return std::make_shared<ColumnDateTime64>(type->As<DateTime64Type>()->GetPrecision());
    }
    case Type::Code::Date:
    {
        return std::make_shared<ColumnDate>();
    }
    case Type::Code::Date32:
    {
        return std::make_shared<ColumnDate32>();
    }
    case Type::Code::Time:
    {
        return std::make_shared<ColumnTime>();
    }
    case Type::Code::Time64:
    {
        return std::make_shared<ColumnTime64>(type->As<Time64Type>()->GetPrecision());
    }
    case Type::Code::Int128:
    {
        return std::make_shared<ColumnInt128>();
    }
    case Type::Code::UInt128:
    {
        return std::make_shared<ColumnUInt128>();
    }
    case Type::Code::Decimal:
    case Type::Code::Decimal32:
    case Type::Code::Decimal64:
    case Type::Code::Decimal128:
    {
        auto dt = type->As<DecimalType>();
        return std::make_shared<ColumnDecimal>(dt->GetPrecision(), dt->GetScale());
    }

    case Type::Code::Array:
    {
        return std::make_shared<ColumnArray>(createColumn(type->As<ArrayType>()->GetItemType()));
    }

    case Type::Code::Enum8:
    {
        return std::make_shared<ColumnEnum8>(type);
    }
    case Type::Code::Enum16:
    {
        return std::make_shared<ColumnEnum16>(type);
    }

    case Type::Code::Nullable:
    {
        return std::make_shared<ColumnNullable>(createColumn(type->As<NullableType>()->GetNestedType()), std::make_shared<ColumnUInt8>());
    }

    case Type::Code::LowCardinality:
    {
        TypeRef nested = type->As<LowCardinalityType>()->GetNestedType();
        bool is_nullable = (nested->GetCode() == Type::Code::Nullable);
        TypeRef inner = is_nullable
            ? nested->As<NullableType>()->GetNestedType()
            : nested;
        if (inner->GetCode() == Type::Code::String) {
            if (is_nullable) {
                return std::make_shared<ColumnLowCardinalityT<ColumnNullableT<ColumnString>>>();
            }
            return std::make_shared<ColumnLowCardinalityT<ColumnString>>();
        }
        if (inner->GetCode() == Type::Code::FixedString) {
            string typeName = inner->GetName();
            typeName.erase(typeName.find("FixedString("), 12);
            typeName.erase(typeName.find(")"), 1);
            int width = std::stoi(typeName);
            if (is_nullable) {
                return std::make_shared<ColumnLowCardinalityT<ColumnNullableT<ColumnFixedString>>>(width);
            }
            return std::make_shared<ColumnLowCardinalityT<ColumnFixedString>>(width);
        }
        throw std::runtime_error("LowCardinality only supported over String / FixedString (Nullable allowed)");
    }

    case Type::Code::Map:
    {
        TypeRef k = type->As<MapType>()->GetKeyType();
        TypeRef v = type->As<MapType>()->GetValueType();
        Type::Code kc = k->GetCode();
        Type::Code vc = v->GetCode();
        if (kc == Type::Code::String && vc == Type::Code::String) {
            return std::make_shared<ColumnMapT<ColumnString, ColumnString>>(
                std::make_shared<ColumnString>(), std::make_shared<ColumnString>());
        }
        if (kc == Type::Code::String && vc == Type::Code::Int64) {
            return std::make_shared<ColumnMapT<ColumnString, ColumnInt64>>(
                std::make_shared<ColumnString>(), std::make_shared<ColumnInt64>());
        }
        if (kc == Type::Code::String && vc == Type::Code::UInt64) {
            return std::make_shared<ColumnMapT<ColumnString, ColumnUInt64>>(
                std::make_shared<ColumnString>(), std::make_shared<ColumnUInt64>());
        }
        if (kc == Type::Code::String && vc == Type::Code::Float64) {
            return std::make_shared<ColumnMapT<ColumnString, ColumnFloat64>>(
                std::make_shared<ColumnString>(), std::make_shared<ColumnFloat64>());
        }
        if (kc == Type::Code::Int64 && vc == Type::Code::String) {
            return std::make_shared<ColumnMapT<ColumnInt64, ColumnString>>(
                std::make_shared<ColumnInt64>(), std::make_shared<ColumnString>());
        }
        return CreateColumnByType(type->GetName());
    }

    case Type::Code::Tuple:
    {
        throw std::runtime_error("can't support Tuple");
    }

    case Type::Code::Void:
    {
        throw std::runtime_error("can't support Void");
    }

    default:
        return CreateColumnByType(type->GetName());
    }
}

// Build a column of plain integer cells from a PHP rows array. Used by
// every signed and unsigned integer type that doesn't accept hex
// strings (UInt8/16, Int8..Int64).
template <typename TCol>
static ColumnRef appendIntColumn(HashTable *values_ht)
{
    auto value = std::make_shared<TCol>();
    zval *array_value;
    ZEND_HASH_FOREACH_VAL(values_ht, array_value) {
        convert_to_long(array_value);
        value->Append(Z_LVAL_P(array_value));
    } ZEND_HASH_FOREACH_END();
    return value;
}

// Build an unsigned integer column with a hex-string fast path. UInt32
// and UInt64 both accept "0x..." strings as a way to land values in the
// upper half of the range that a PHP signed long can't represent.
template <typename TCol, typename TStrtoul>
static ColumnRef appendUIntColumnWithHex(HashTable *values_ht, TStrtoul strtoul_fn)
{
    auto value = std::make_shared<TCol>();
    zval *array_value;
    ZEND_HASH_FOREACH_VAL(values_ht, array_value) {
        if (Z_TYPE_P(array_value) == IS_STRING && Z_STRLEN_P(array_value) >= 3 &&
            *Z_STRVAL_P(array_value) == '0' &&
            (*(Z_STRVAL_P(array_value) + 1) == 'x' || *(Z_STRVAL_P(array_value) + 1) == 'X')) {
            value->Append(strtoul_fn(Z_STRVAL_P(array_value), NULL, 0));
        } else {
            convert_to_long(array_value);
            value->Append(Z_LVAL_P(array_value));
        }
    } ZEND_HASH_FOREACH_END();
    return value;
}

// Build a Float32/Float64 column from a PHP rows array.
template <typename TCol>
static ColumnRef appendFloatColumn(HashTable *values_ht)
{
    auto value = std::make_shared<TCol>();
    zval *array_value;
    ZEND_HASH_FOREACH_VAL(values_ht, array_value) {
        convert_to_double(array_value);
        value->Append(Z_DVAL_P(array_value));
    } ZEND_HASH_FOREACH_END();
    return value;
}

// Build a ColumnMapT<KCol, VCol> from PHP rows. Each row is an assoc
// array; the caller supplies extractors that turn (zend_string*, ulong)
// into K and (zval*) into V. Five near-identical Map(K, V) arms route
// through here.
template <typename K, typename V, typename KCol, typename VCol,
          typename KFn, typename VFn>
static ColumnRef appendMapColumn(HashTable *values_ht, KFn extract_key, VFn extract_val)
{
    auto col = std::make_shared<ColumnMapT<KCol, VCol>>(
        std::make_shared<KCol>(), std::make_shared<VCol>());
    zval *array_value;
    ZEND_HASH_FOREACH_VAL(values_ht, array_value) {
        if (Z_TYPE_P(array_value) != IS_ARRAY) {
            throw std::runtime_error("Map row must be a PHP array");
        }
        std::vector<std::pair<K, V>> entries;
        HashTable *mh = Z_ARRVAL_P(array_value);
        zend_string *zk;
        zend_ulong nk;
        zval *mv;
        ZEND_HASH_FOREACH_KEY_VAL(mh, nk, zk, mv) {
            entries.emplace_back(extract_key(zk, nk), extract_val(mv));
        } ZEND_HASH_FOREACH_END();
        col->Append(entries);
    } ZEND_HASH_FOREACH_END();
    return col;
}

// Coerce a PHP 2-element numeric array into a (double, double) point tuple.
// Used by Point/Ring/Polygon/MultiPolygon insert paths.
static std::tuple<double, double> phpToPoint(zval *zv)
{
    if (Z_TYPE_P(zv) != IS_ARRAY) {
        throw std::runtime_error("Point must be a PHP array of 2 numbers");
    }
    HashTable *ht = Z_ARRVAL_P(zv);
    if (zend_hash_num_elements(ht) != 2) {
        throw std::runtime_error("Point must have exactly 2 elements");
    }
    zval *x = sc_zend_hash_index_find(ht, 0);
    zval *y = sc_zend_hash_index_find(ht, 1);
    if (!x || !y) {
        throw std::runtime_error("Point is missing an element");
    }
    convert_to_double(x);
    convert_to_double(y);
    return std::make_tuple(Z_DVAL_P(x), Z_DVAL_P(y));
}

static std::vector<std::tuple<double, double>> phpToRing(zval *zv)
{
    if (Z_TYPE_P(zv) != IS_ARRAY) {
        throw std::runtime_error("Ring must be a PHP array of points");
    }
    std::vector<std::tuple<double, double>> ring;
    zval *pt;
    ZEND_HASH_FOREACH_VAL(Z_ARRVAL_P(zv), pt) {
        ring.push_back(phpToPoint(pt));
    } ZEND_HASH_FOREACH_END();
    return ring;
}

static std::vector<std::vector<std::tuple<double, double>>> phpToPolygon(zval *zv)
{
    if (Z_TYPE_P(zv) != IS_ARRAY) {
        throw std::runtime_error("Polygon must be a PHP array of rings");
    }
    std::vector<std::vector<std::tuple<double, double>>> poly;
    zval *r;
    ZEND_HASH_FOREACH_VAL(Z_ARRVAL_P(zv), r) {
        poly.push_back(phpToRing(r));
    } ZEND_HASH_FOREACH_END();
    return poly;
}

ColumnRef insertColumn(TypeRef type, zval *value_zval)
{
    zval *array_value;
    HashTable *values_ht = Z_ARRVAL_P(value_zval);

    switch (type->GetCode())
    {
    case Type::Code::UInt64:
        return appendUIntColumnWithHex<ColumnUInt64>(values_ht, strtoull);
    case Type::Code::UInt8:
        return appendIntColumn<ColumnUInt8>(values_ht);
    case Type::Code::UInt16:
        return appendIntColumn<ColumnUInt16>(values_ht);
    case Type::Code::UInt32:
        return appendUIntColumnWithHex<ColumnUInt32>(values_ht, strtoul);
    case Type::Code::Int8:
        return appendIntColumn<ColumnInt8>(values_ht);
    case Type::Code::Int16:
        return appendIntColumn<ColumnInt16>(values_ht);
    case Type::Code::Int32:
        return appendIntColumn<ColumnInt32>(values_ht);
    case Type::Code::Int64:
        return appendIntColumn<ColumnInt64>(values_ht);

    case Type::Code::UUID:
    {
        auto value = std::make_shared<ColumnUUID>();

        ZEND_HASH_FOREACH_VAL(values_ht, array_value)
        {
            if (Z_TYPE_P(array_value) == IS_NULL)
            {
                value->Append(UUID{0, 0});
            }
            else
            {
                convert_to_string(array_value);
                string value_string = (string)Z_STRVAL_P(array_value);

                value_string.erase(std::remove(value_string.begin(), value_string.end(), '-'), value_string.end());
                if (value_string.length() != 32)
                {
                    throw std::runtime_error("UUID format error");
                }

                string first = value_string.substr(0, 16);
                string second = value_string.substr(16, 16);
                uint64_t i_first = std::stoull(first, nullptr, 16);
                uint64_t i_second = std::stoull(second, nullptr, 16);
                value->Append(UUID{i_first, i_second});
            }
        }
        ZEND_HASH_FOREACH_END();
        return value;
    }

    case Type::Code::Float32:
        return appendFloatColumn<ColumnFloat32>(values_ht);
    case Type::Code::Float64:
        return appendFloatColumn<ColumnFloat64>(values_ht);

    case Type::Code::String:
    {
        auto value = std::make_shared<ColumnString>();

        ZEND_HASH_FOREACH_VAL(values_ht, array_value)
        {
            convert_to_string(array_value);
            value->Append((string)Z_STRVAL_P(array_value));
        }
        ZEND_HASH_FOREACH_END();

        return value;
    }
    case Type::Code::FixedString:
    {
        string typeName = type->GetName();
        typeName.erase(typeName.find("FixedString("), 12);
        typeName.erase(typeName.find(")"), 1);
        auto value = std::make_shared<ColumnFixedString>(std::stoi(typeName));

        ZEND_HASH_FOREACH_VAL(values_ht, array_value)
        {
            convert_to_string(array_value);
            value->Append((string)Z_STRVAL_P(array_value));
        }
        ZEND_HASH_FOREACH_END();

        return value;
    }

    case Type::Code::DateTime:
    {
        auto value = std::make_shared<ColumnDateTime>();

        ZEND_HASH_FOREACH_VAL(values_ht, array_value)
        {
            if (Z_TYPE_P(array_value) == IS_STRING && memchr(Z_STRVAL_P(array_value), '-', Z_STRLEN_P(array_value)) != NULL) {
                value->Append((long)to_time_t(Z_STRVAL_P(array_value), false));
            } else {
                convert_to_long(array_value);
                value->Append(Z_LVAL_P(array_value));
            }
        }
        ZEND_HASH_FOREACH_END();

        return value;
    }
    case Type::Code::DateTime64:
    {
        size_t precision = type->As<DateTime64Type>()->GetPrecision();
        auto value = std::make_shared<ColumnDateTime64>(precision);
        int64_t scale = 1;
        for (size_t i = 0; i < precision; ++i) scale *= 10;

        ZEND_HASH_FOREACH_VAL(values_ht, array_value)
        {
            if (Z_TYPE_P(array_value) == IS_STRING && memchr(Z_STRVAL_P(array_value), '-', Z_STRLEN_P(array_value)) != NULL) {
                value->Append((int64_t)to_time_t(Z_STRVAL_P(array_value), false) * scale);
            } else if (Z_TYPE_P(array_value) == IS_DOUBLE) {
                value->Append((int64_t)(Z_DVAL_P(array_value) * scale));
            } else {
                convert_to_long(array_value);
                value->Append((int64_t)Z_LVAL_P(array_value) * scale);
            }
        }
        ZEND_HASH_FOREACH_END();

        return value;
    }
    case Type::Code::Date:
    {
        auto value = std::make_shared<ColumnDate>();

        ZEND_HASH_FOREACH_VAL(values_ht, array_value)
        {
            if (Z_TYPE_P(array_value) == IS_STRING && memchr(Z_STRVAL_P(array_value), '-', Z_STRLEN_P(array_value)) != NULL) {
                value->Append((long)to_time_t(Z_STRVAL_P(array_value)));
            } else {
                convert_to_long(array_value);
                value->Append((std::time_t)Z_LVAL_P(array_value));
            }
        }
        ZEND_HASH_FOREACH_END();

        return value;
    }
    case Type::Code::Date32:
    {
        auto value = std::make_shared<ColumnDate32>();
        ZEND_HASH_FOREACH_VAL(values_ht, array_value)
        {
            if (Z_TYPE_P(array_value) == IS_STRING && memchr(Z_STRVAL_P(array_value), '-', Z_STRLEN_P(array_value)) != NULL) {
                value->Append((std::time_t)to_time_t(Z_STRVAL_P(array_value)));
            } else {
                convert_to_long(array_value);
                value->Append((std::time_t)Z_LVAL_P(array_value));
            }
        }
        ZEND_HASH_FOREACH_END();
        return value;
    }
    case Type::Code::Time:
    {
        auto value = std::make_shared<ColumnTime>();
        ZEND_HASH_FOREACH_VAL(values_ht, array_value)
        {
            convert_to_long(array_value);
            value->Append((int32_t)Z_LVAL_P(array_value));
        }
        ZEND_HASH_FOREACH_END();
        return value;
    }
    case Type::Code::Time64:
    {
        size_t precision = type->As<Time64Type>()->GetPrecision();
        auto value = std::make_shared<ColumnTime64>(precision);
        int64_t scale = 1;
        for (size_t i = 0; i < precision; ++i) scale *= 10;
        ZEND_HASH_FOREACH_VAL(values_ht, array_value)
        {
            if (Z_TYPE_P(array_value) == IS_DOUBLE) {
                value->Append((int64_t)(Z_DVAL_P(array_value) * scale));
            } else {
                convert_to_long(array_value);
                value->Append((int64_t)Z_LVAL_P(array_value) * scale);
            }
        }
        ZEND_HASH_FOREACH_END();
        return value;
    }
    case Type::Code::Int128:
    {
        auto value = std::make_shared<ColumnInt128>();
        // Int128 range: -2^127 .. 2^127-1. The signed magnitude fits in 39
        // decimal digits (2^127 = 170141183460469231731687303715884105728).
        ZEND_HASH_FOREACH_VAL(values_ht, array_value)
        {
            if (Z_TYPE_P(array_value) == IS_STRING) {
                const char *s = Z_STRVAL_P(array_value);
                size_t len = Z_STRLEN_P(array_value);
                size_t i = 0;
                bool neg = false;
                if (len > 0 && (s[0] == '-' || s[0] == '+')) { neg = (s[0] == '-'); i = 1; }
                size_t digits = len - i;
                if (digits == 0 || digits > 39) {
                    throw std::runtime_error("Int128 string is empty or too long");
                }
                Int128 v = 0;
                for (; i < len; ++i) {
                    if (s[i] < '0' || s[i] > '9') {
                        throw std::runtime_error("Int128 string contains non-digit characters");
                    }
                    Int128 next = v * 10 + (s[i] - '0');
                    if (next < v) {
                        throw std::runtime_error("Int128 string overflows the 128-bit range");
                    }
                    v = next;
                }
                value->Append(neg ? -v : v);
            } else {
                convert_to_long(array_value);
                value->Append(Int128(Z_LVAL_P(array_value)));
            }
        }
        ZEND_HASH_FOREACH_END();
        return value;
    }
    case Type::Code::UInt128:
    {
        auto value = std::make_shared<ColumnUInt128>();
        // UInt128 range: 0 .. 2^128-1, i.e. up to 39 decimal digits.
        ZEND_HASH_FOREACH_VAL(values_ht, array_value)
        {
            if (Z_TYPE_P(array_value) == IS_STRING) {
                const char *s = Z_STRVAL_P(array_value);
                size_t len = Z_STRLEN_P(array_value);
                size_t i = 0;
                if (len > 0 && s[0] == '+') { i = 1; }
                size_t digits = len - i;
                if (digits == 0 || digits > 39) {
                    throw std::runtime_error("UInt128 string is empty or too long");
                }
                UInt128 v = 0;
                for (; i < len; ++i) {
                    if (s[i] < '0' || s[i] > '9') {
                        throw std::runtime_error("UInt128 string contains non-digit characters");
                    }
                    UInt128 next = v * 10 + (s[i] - '0');
                    if (next < v) {
                        throw std::runtime_error("UInt128 string overflows the 128-bit range");
                    }
                    v = next;
                }
                value->Append(v);
            } else {
                convert_to_long(array_value);
                if (Z_LVAL_P(array_value) < 0) {
                    throw std::runtime_error("UInt128 cannot accept a negative integer");
                }
                value->Append(UInt128((uint64_t)Z_LVAL_P(array_value)));
            }
        }
        ZEND_HASH_FOREACH_END();
        return value;
    }
    case Type::Code::Decimal:
    case Type::Code::Decimal32:
    case Type::Code::Decimal64:
    case Type::Code::Decimal128:
    {
        auto dt = type->As<DecimalType>();
        auto value = std::make_shared<ColumnDecimal>(dt->GetPrecision(), dt->GetScale());
        ZEND_HASH_FOREACH_VAL(values_ht, array_value)
        {
            convert_to_string(array_value);
            value->Append(std::string(Z_STRVAL_P(array_value), Z_STRLEN_P(array_value)));
        }
        ZEND_HASH_FOREACH_END();
        return value;
    }

    case Type::Code::Array:
    {
        TypeRef item_type = type->As<ArrayType>()->GetItemType();
        if (item_type->GetCode() == Type::Array)
        {
            throw std::runtime_error("can't support Multidimensional Arrays");
        }

        auto value = std::make_shared<ColumnArray>(createColumn(item_type));
        auto child = createColumn(item_type);

        ZEND_HASH_FOREACH_VAL(values_ht, array_value)
        {
            if (Z_TYPE_P(array_value) != IS_ARRAY)
            {
                throw std::runtime_error("The inserted data is not an array type");
            }

            child->Append(insertColumn(item_type, array_value));

            value->AppendAsColumn(child);
            child->Clear();
        }
        ZEND_HASH_FOREACH_END();

        return value;
    }

    case Type::Code::Enum8:
    {
        auto value = std::make_shared<ColumnEnum8>(type);

        ZEND_HASH_FOREACH_VAL(values_ht, array_value)
        {
            // PHP NULLs reach here when the column is Nullable(Enum8); the row's
            // null mask makes the data slot meaningless, but ColumnEnum8::Append
            // validates names through std::map::at and throws on "". Append the
            // unchecked int8 overload (default checkValue=false) instead.
            if (Z_TYPE_P(array_value) == IS_NULL)
            {
                value->Append((int8_t)0);
            }
            else if (Z_TYPE_P(array_value) == IS_LONG)
            {
                convert_to_long(array_value);
                value->Append(Z_LVAL_P(array_value));
            }
            else
            {
                convert_to_string(array_value);
                value->Append((string)Z_STRVAL_P(array_value));
            }
        }
        ZEND_HASH_FOREACH_END();

        return value;
    }
    case Type::Code::Enum16:
    {
        auto value = std::make_shared<ColumnEnum16>(type);

        ZEND_HASH_FOREACH_VAL(values_ht, array_value)
        {
            if (Z_TYPE_P(array_value) == IS_NULL)
            {
                value->Append((int16_t)0);
            }
            else if (Z_TYPE_P(array_value) == IS_LONG)
            {
                convert_to_long(array_value);
                value->Append(Z_LVAL_P(array_value));
            }
            else
            {
                convert_to_string(array_value);
                value->Append((string)Z_STRVAL_P(array_value));
            }
        }
        ZEND_HASH_FOREACH_END();

        return value;
    }

    case Type::Code::Nullable:
    {
        auto nulls = std::make_shared<ColumnUInt8>();

        ZEND_HASH_FOREACH_VAL(values_ht, array_value)
        {
            if (Z_TYPE_P(array_value) == IS_NULL)
            {
                nulls->Append(1);
            }
            else
            {
                nulls->Append(0);
            }
        }
        ZEND_HASH_FOREACH_END();

        ColumnRef child = insertColumn(type->As<NullableType>()->GetNestedType(), value_zval);

        return std::make_shared<ColumnNullable>(child, nulls);
    }

    case Type::Code::Tuple:
    {
        // Build one transposed list per tuple field (arity), iterating
        // every input row to pull row[field]. The previous version
        // looped by row count instead of arity, so multi-row tuple
        // inserts walked off the end of tupleType when rowcount != arity.
        auto tupleType = type->As<TupleType>()->GetTupleType();
        size_t arity = tupleType.size();

        zval *return_should;
        SC_MAKE_STD_ZVAL(return_should);
        array_init(return_should);

        zval *fzval;
        zval *pzval;

        zval *return_tmp;
        for (size_t field = 0; field < arity; field++)
        {
            SC_MAKE_STD_ZVAL(return_tmp);
            array_init(return_tmp);

            ZEND_HASH_FOREACH_VAL(values_ht, pzval)
            {
                if (Z_TYPE_P(pzval) != IS_ARRAY)
                {
                    throw std::runtime_error("Tuple row must be a PHP array");
                }
                if (zend_hash_num_elements(Z_ARRVAL_P(pzval)) != arity) {
                    throw std::runtime_error(
                        "Tuple row arity does not match the column type");
                }
                fzval = sc_zend_hash_index_find(Z_ARRVAL_P(pzval), field);
                if (NULL == fzval)
                {
                    throw std::runtime_error(
                        "Tuple row is missing a field value");
                }
                sc_zval_add_ref(fzval);
                add_next_index_zval(return_tmp, fzval);
            }
            ZEND_HASH_FOREACH_END();

            add_next_index_zval(return_should, return_tmp);
        }

        std::vector<ColumnRef> columns;
        size_t tupleTypeIndex = 0;

        ZEND_HASH_FOREACH_VAL(Z_ARRVAL_P(return_should), array_value)
        {
            if (Z_TYPE_P(array_value) != IS_ARRAY)
            {
                throw std::runtime_error("The inserted data is not an array type");
            }

            columns.push_back(insertColumn(tupleType[tupleTypeIndex], array_value));
            tupleTypeIndex++;
        }
        ZEND_HASH_FOREACH_END();

        sc_zval_ptr_dtor(&return_should);

        return std::make_shared<ColumnTuple>(columns);
    }

    case Type::Code::LowCardinality:
    {
        TypeRef nested = type->As<LowCardinalityType>()->GetNestedType();
        bool is_nullable = (nested->GetCode() == Type::Code::Nullable);
        TypeRef inner = is_nullable
            ? nested->As<NullableType>()->GetNestedType()
            : nested;

        if (inner->GetCode() == Type::Code::String) {
            if (is_nullable) {
                auto value = std::make_shared<ColumnLowCardinalityT<ColumnNullableT<ColumnString>>>();
                ZEND_HASH_FOREACH_VAL(values_ht, array_value)
                {
                    if (Z_TYPE_P(array_value) == IS_NULL) {
                        value->Append(std::nullopt);
                    } else {
                        convert_to_string(array_value);
                        value->Append(std::string_view(Z_STRVAL_P(array_value), Z_STRLEN_P(array_value)));
                    }
                }
                ZEND_HASH_FOREACH_END();
                return value;
            }
            auto value = std::make_shared<ColumnLowCardinalityT<ColumnString>>();
            ZEND_HASH_FOREACH_VAL(values_ht, array_value)
            {
                convert_to_string(array_value);
                value->Append(std::string_view(Z_STRVAL_P(array_value), Z_STRLEN_P(array_value)));
            }
            ZEND_HASH_FOREACH_END();
            return value;
        }
        if (inner->GetCode() == Type::Code::FixedString) {
            string typeName = inner->GetName();
            typeName.erase(typeName.find("FixedString("), 12);
            typeName.erase(typeName.find(")"), 1);
            int width = std::stoi(typeName);
            if (is_nullable) {
                auto value = std::make_shared<ColumnLowCardinalityT<ColumnNullableT<ColumnFixedString>>>(width);
                ZEND_HASH_FOREACH_VAL(values_ht, array_value)
                {
                    if (Z_TYPE_P(array_value) == IS_NULL) {
                        value->Append(std::nullopt);
                    } else {
                        convert_to_string(array_value);
                        value->Append(std::string_view(Z_STRVAL_P(array_value), Z_STRLEN_P(array_value)));
                    }
                }
                ZEND_HASH_FOREACH_END();
                return value;
            }
            auto value = std::make_shared<ColumnLowCardinalityT<ColumnFixedString>>(width);
            ZEND_HASH_FOREACH_VAL(values_ht, array_value)
            {
                convert_to_string(array_value);
                value->Append(std::string_view(Z_STRVAL_P(array_value), Z_STRLEN_P(array_value)));
            }
            ZEND_HASH_FOREACH_END();
            return value;
        }
        throw std::runtime_error("LowCardinality only supported over String / FixedString (Nullable allowed)");
    }

    case Type::Code::Map:
    {
        TypeRef k = type->As<MapType>()->GetKeyType();
        TypeRef v = type->As<MapType>()->GetValueType();
        Type::Code kc = k->GetCode();
        Type::Code vc = v->GetCode();

        // Key extractors: string keys reject integer-keyed PHP entries
        // outright; Int64 keys parse the string form or fall back to the
        // numeric key.
        auto strKey = [](zend_string *zk, zend_ulong) -> std::string {
            if (!zk) {
                throw std::runtime_error("Map(String, *) row entry must have a string key");
            }
            return std::string(ZSTR_VAL(zk), ZSTR_LEN(zk));
        };
        auto i64Key = [](zend_string *zk, zend_ulong nk) -> int64_t {
            return zk ? (int64_t)strtoll(ZSTR_VAL(zk), NULL, 10) : (int64_t)nk;
        };
        // Value extractors mutate the zval to coerce its type, then read.
        auto strVal = [](zval *mv) -> std::string {
            convert_to_string(mv);
            return std::string(Z_STRVAL_P(mv), Z_STRLEN_P(mv));
        };
        auto i64Val = [](zval *mv) -> int64_t {
            convert_to_long(mv);
            return (int64_t)Z_LVAL_P(mv);
        };
        auto u64Val = [](zval *mv) -> uint64_t {
            convert_to_long(mv);
            return (uint64_t)Z_LVAL_P(mv);
        };
        auto f64Val = [](zval *mv) -> double {
            convert_to_double(mv);
            return Z_DVAL_P(mv);
        };

        if (kc == Type::Code::String && vc == Type::Code::String) {
            return appendMapColumn<std::string, std::string,
                                   ColumnString, ColumnString>(values_ht, strKey, strVal);
        }
        if (kc == Type::Code::String && vc == Type::Code::Int64) {
            return appendMapColumn<std::string, int64_t,
                                   ColumnString, ColumnInt64>(values_ht, strKey, i64Val);
        }
        if (kc == Type::Code::String && vc == Type::Code::UInt64) {
            return appendMapColumn<std::string, uint64_t,
                                   ColumnString, ColumnUInt64>(values_ht, strKey, u64Val);
        }
        if (kc == Type::Code::String && vc == Type::Code::Float64) {
            return appendMapColumn<std::string, double,
                                   ColumnString, ColumnFloat64>(values_ht, strKey, f64Val);
        }
        if (kc == Type::Code::Int64 && vc == Type::Code::String) {
            return appendMapColumn<int64_t, std::string,
                                   ColumnInt64, ColumnString>(values_ht, i64Key, strVal);
        }
        throw std::runtime_error("Unsupported Map(K, V) for row write: " + type->GetName());
    }

    case Type::Code::Point:
    {
        auto col = std::make_shared<ColumnPoint>();
        ZEND_HASH_FOREACH_VAL(values_ht, array_value) {
            col->Append(phpToPoint(array_value));
        } ZEND_HASH_FOREACH_END();
        return col;
    }
    case Type::Code::Ring:
    {
        auto col = std::make_shared<ColumnRing>();
        ZEND_HASH_FOREACH_VAL(values_ht, array_value) {
            col->Append(phpToRing(array_value));
        } ZEND_HASH_FOREACH_END();
        return col;
    }
    case Type::Code::Polygon:
    {
        auto col = std::make_shared<ColumnPolygon>();
        ZEND_HASH_FOREACH_VAL(values_ht, array_value) {
            col->Append(phpToPolygon(array_value));
        } ZEND_HASH_FOREACH_END();
        return col;
    }
    case Type::Code::MultiPolygon:
    {
        auto col = std::make_shared<ColumnMultiPolygon>();
        ZEND_HASH_FOREACH_VAL(values_ht, array_value) {
            if (Z_TYPE_P(array_value) != IS_ARRAY) {
                throw std::runtime_error("MultiPolygon must be a PHP array of polygons");
            }
            std::vector<std::vector<std::vector<std::tuple<double, double>>>> mp;
            zval *poly;
            ZEND_HASH_FOREACH_VAL(Z_ARRVAL_P(array_value), poly) {
                mp.push_back(phpToPolygon(poly));
            } ZEND_HASH_FOREACH_END();
            col->Append(mp);
        } ZEND_HASH_FOREACH_END();
        return col;
    }

    case Type::Code::Void:
    {
        throw std::runtime_error("can't support Void");
    }
    default:
        throw std::runtime_error("insertColumn: unsupported type code: " + type->GetName());
    }
}

// Cast through (zend_long) so signed types (Int8..Int64) keep their
// sign on the way into PHP, instead of getting reinterpreted as huge
// unsigned values. Unsigned types up to UINT64_MAX preserve their bit
// pattern either way; PHP integers are signed 64-bit regardless.
#define SC_SINGLE_LONG()  \
    if (fetch_mode & SC_FETCH_ONE) { \
        ZVAL_LONG(arr, (zend_long)col); \
    } else { \
        sc_add_assoc_long_ex(arr, column_name.c_str(), column_name.length(), (zend_long)col); \
    }

#define SC_SINGLE_DOUBLE(val)  \
    if (fetch_mode & SC_FETCH_ONE) { \
        ZVAL_DOUBLE(arr, val); \
    } else { \
        sc_add_assoc_double_ex(arr, column_name.c_str(), column_name.length(), val); \
    }

#define SC_SINGLE_STRING(val, len)  \
    if (fetch_mode & SC_FETCH_ONE) { \
        ZVAL_STRINGL(arr, val, len); \
    } else { \
        sc_add_assoc_stringl_ex(arr, column_name.c_str(), column_name.length(), val, len, 1); \
    }

// Emit a Unix epoch as either a long or a strftime-formatted string,
// dispatched on fetch_mode and is_array. Used by DateTime, Date, and
// Date32 reads which all share the same shape modulo the format string
// and whether to emit NULL on a non-positive timestamp.
static void emitEpoch(zval *arr, std::time_t t, const char *fmt,
                      const string& column_name, int8_t is_array, long fetch_mode,
                      bool null_if_nonpositive)
{
    if (fetch_mode & SC_FETCH_DATE_AS_STRINGS) {
        if (null_if_nonpositive && t <= 0) {
            if (is_array) {
                add_next_index_null(arr);
            } else if (fetch_mode & SC_FETCH_ONE) {
                ZVAL_NULL(arr);
            } else {
                sc_add_assoc_null_ex(arr, column_name.c_str(), column_name.length());
            }
            return;
        }
        char buffer[32];
        size_t l = strftime(buffer, sizeof(buffer), fmt, gmtime(&t));
        if (is_array) {
            sc_add_next_index_stringl(arr, buffer, l, 1);
        } else {
            SC_SINGLE_STRING(buffer, l);
        }
    } else {
        if (is_array) {
            add_next_index_long(arr, (zend_long)t);
        } else if (fetch_mode & SC_FETCH_ONE) {
            ZVAL_LONG(arr, (zend_long)t);
        } else {
            sc_add_assoc_long_ex(arr, column_name.c_str(), column_name.length(), (zend_long)t);
        }
    }
}

// Read one integer column cell (UInt8..UInt64, Int8..Int64, IPv4) and
// emit it as a PHP long. The fetch-mode dispatch is identical across
// all eight integer column types, so they all route through here.
template <typename TCol>
static inline void emitIntColumn(zval *arr, const ColumnRef& columnRef, int row,
                                 const string& column_name, int8_t is_array, long fetch_mode)
{
    auto col_ptr = columnRef->As<TCol>();
    if (!col_ptr) {
        throw std::runtime_error("Integer column downcast failed");
    }
    auto col = (*col_ptr)[row];
    if (is_array) {
        add_next_index_long(arr, (long)col);
    } else {
        SC_SINGLE_LONG();
    }
}

// Build a PHP 2-element numeric array for a Point. Output is a freshly
// initialized zval owned by the caller; the caller decides how to attach
// it (next_index, assoc, or write-into-arr).
static void pointToZval(zval *out, const std::tuple<double, double>& pt)
{
    array_init(out);
    add_next_index_double(out, std::get<0>(pt));
    add_next_index_double(out, std::get<1>(pt));
}

// Geo nested types come back as clickhouse::ColumnArrayT::ArrayValueView,
// not std::vector. The view is STL-iterable but not assignable to a
// vector reference, so the helpers below take templated iterables.
template <typename PointRange>
static void ringRangeToZval(zval *out, const PointRange& ring)
{
    array_init(out);
    for (auto pt : ring) {
        zval pt_zv;
        pointToZval(&pt_zv, pt);
        add_next_index_zval(out, &pt_zv);
    }
}

template <typename RingRange>
static void polygonRangeToZval(zval *out, const RingRange& poly)
{
    array_init(out);
    for (auto ring : poly) {
        zval r_zv;
        ringRangeToZval(&r_zv, ring);
        add_next_index_zval(out, &r_zv);
    }
}

// Attach a built-up nested zval to the parent according to (is_array,
// fetch_mode, column_name). Mirrors the dispatch pattern Array/Tuple use.
static void emitNestedZval(zval *arr, zval *built, const string& column_name, int8_t is_array, long fetch_mode)
{
    if (is_array) {
        add_next_index_zval(arr, built);
    } else if (fetch_mode & SC_FETCH_ONE) {
        ZVAL_COPY_VALUE(arr, built);
    } else {
        sc_add_assoc_zval_ex(arr, column_name.c_str(), column_name.length(), built);
    }
}

void convertToZval(zval *arr, const ColumnRef& columnRef, int row, string column_name, int8_t is_array, long fetch_mode)
{
    switch (columnRef->Type()->GetCode())
    {
    case Type::Code::UInt64:
        emitIntColumn<ColumnUInt64>(arr, columnRef, row, column_name, is_array, fetch_mode);
        break;
    case Type::Code::UInt8:
        emitIntColumn<ColumnUInt8>(arr, columnRef, row, column_name, is_array, fetch_mode);
        break;
    case Type::Code::UInt16:
        emitIntColumn<ColumnUInt16>(arr, columnRef, row, column_name, is_array, fetch_mode);
        break;
    case Type::Code::UInt32:
        emitIntColumn<ColumnUInt32>(arr, columnRef, row, column_name, is_array, fetch_mode);
        break;
    case Type::Code::IPv4:
    {
        /* As above, ColumnIPv4 is no longer a ColumnUInt32 subclass in
         * v2.6.1; emit as canonical dotted-quad string via AsString(). */
        auto col_ip = columnRef->As<ColumnIPv4>();
        std::string s = col_ip ? col_ip->AsString(row) : std::string();
        if (is_array)
        {
            sc_add_next_index_stringl(arr, (char*)s.data(), s.size(), 1);
        }
        else
        {
            SC_SINGLE_STRING((char*)s.data(), s.size());
        }
        break;
    }
    case Type::Code::Int8:
        emitIntColumn<ColumnInt8>(arr, columnRef, row, column_name, is_array, fetch_mode);
        break;
    case Type::Code::Int16:
        emitIntColumn<ColumnInt16>(arr, columnRef, row, column_name, is_array, fetch_mode);
        break;
    case Type::Code::Int32:
        emitIntColumn<ColumnInt32>(arr, columnRef, row, column_name, is_array, fetch_mode);
        break;
    case Type::Code::Int64:
        emitIntColumn<ColumnInt64>(arr, columnRef, row, column_name, is_array, fetch_mode);
        break;
    case Type::Code::UUID:
    {
        stringstream first;
        stringstream second;
        auto col = (*columnRef->As<ColumnUUID>())[row];
        first<<std::setw(16)<<std::setfill('0')<<hex<<col.first;
        second<<std::setw(16)<<std::setfill('0')<<hex<<col.second;
        if (is_array)
        {
            sc_add_next_index_stringl(arr, (char*)(first.str() + second.str()).c_str(), (first.str() + second.str()).length(), 1);
        }
        else
        {
            SC_SINGLE_STRING((char*)(first.str() + second.str()).c_str(), (first.str() + second.str()).length());
        }
        break;
    }
    case Type::Code::Float32:
    {
        auto col = (*columnRef->As<ColumnFloat32>())[row];
        stringstream stream;
        stream<<col;
        double d;
        stream>>d;
        if (is_array)
        {
            add_next_index_double(arr, d);
        }
        else
        {
            SC_SINGLE_DOUBLE(d);
        }
        break;
    }
    case Type::Code::Float64:
    {
        auto col = (*columnRef->As<ColumnFloat64>())[row];
        if (is_array)
        {
            add_next_index_double(arr, (double)col);
        }
        else
        {
            SC_SINGLE_DOUBLE((double)col);
        }
        break;
    }
    case Type::Code::Decimal:
    case Type::Code::Decimal32:
    case Type::Code::Decimal64:
    case Type::Code::Decimal128:
    {
        auto col = columnRef->As<ColumnDecimal>();
        if (!col) {
            throw std::runtime_error("Decimal read: column downcast failed");
        }
        // Format with the scale point so a value inserted as "12.34" reads
        // back as "12.34", not the unscaled storage integer 1234.
        auto dec_type = columnRef->Type()->As<DecimalType>();
        size_t scale = dec_type ? dec_type->GetScale() : 0;
        Int128 raw = col->At(row);
        std::stringstream ss;
        ss << raw;
        std::string s = ss.str();
        if (scale > 0) {
            bool neg = !s.empty() && s[0] == '-';
            std::string abs = neg ? s.substr(1) : s;
            if (abs.size() <= scale) {
                abs.insert(0, scale + 1 - abs.size(), '0');
            }
            abs.insert(abs.size() - scale, ".");
            s = neg ? ("-" + abs) : abs;
        }
        if (is_array) {
            sc_add_next_index_stringl(arr, (char*)s.c_str(), s.length(), 1);
        } else {
            SC_SINGLE_STRING((char*)s.c_str(), s.length());
        }
        break;
    }
    case Type::Code::String:
    {
        auto col = (*columnRef->As<ColumnString>())[row];
        if (is_array)
        {
            sc_add_next_index_stringl(arr, (char*)col.data(), col.length(), 1);
        }
        else
        {
            SC_SINGLE_STRING((char*)col.data(), col.length());
        }
        break;
    }
    case Type::Code::FixedString:
    {
        // ColumnFixedString::At returns a string_view over the full fixed-size
        // buffer, including trailing NULs added by ClickHouse to pad short
        // values up to the column's declared width. Trim trailing NULs so the
        // PHP-side value matches the original input.
        auto col = (*columnRef->As<ColumnFixedString>())[row];
        size_t len = col.length();
        while (len > 0 && col.data()[len - 1] == '\0') {
            --len;
        }
        if (is_array)
        {
            sc_add_next_index_stringl(arr, (char*)col.data(), len, 1);
        }
        else
        {
            SC_SINGLE_STRING((char*)col.data(), len);
        }
        break;
    }
    case Type::Code::IPv6:
    {
        /* clickhouse-cpp v2.6.1 made ColumnIPv6 a sibling of ColumnFixedString
         * (composition, not inheritance), so As<ColumnFixedString>() returns
         * null and crashed every IPv6 read. Use AsString() to get the
         * canonical "::1" form. */
        auto col_ip = columnRef->As<ColumnIPv6>();
        std::string s = col_ip ? col_ip->AsString(row) : std::string();
        if (is_array)
        {
            sc_add_next_index_stringl(arr, (char*)s.data(), s.size(), 1);
        }
        else
        {
            SC_SINGLE_STRING((char*)s.data(), s.size());
        }
        break;
    }

    case Type::Code::DateTime:
    {
        auto col = columnRef->As<ColumnDateTime>();
        emitEpoch(arr, (std::time_t)col->At(row), "%Y-%m-%d %H:%M:%S",
                  column_name, is_array, fetch_mode, /*null_if_nonpositive=*/true);
        break;
    }
    case Type::Code::DateTime64:
    {
        auto col = columnRef->As<ColumnDateTime64>();
        size_t precision = columnRef->Type()->As<DateTime64Type>()->GetPrecision();
        int64_t scale = 1;
        for (size_t i = 0; i < precision; ++i) scale *= 10;
        int64_t raw = col->At(row);
        std::time_t whole = (std::time_t)(raw / scale);
        int64_t frac = raw % scale;
        if (frac < 0) { frac = -frac; }

        if (fetch_mode & SC_FETCH_DATE_AS_STRINGS) {
            char buffer[64];
            size_t l = strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", gmtime(&whole));
            if (precision > 0) {
                int written = snprintf(buffer + l, sizeof(buffer) - l, ".%0*lld",
                                       (int)precision, (long long)frac);
                if (written > 0) l += (size_t)written;
            }
            if (is_array) {
                sc_add_next_index_stringl(arr, buffer, l, 1);
            } else {
                SC_SINGLE_STRING(buffer, l);
            }
        } else {
            if (is_array) {
                add_next_index_long(arr, (long)raw);
            } else if (fetch_mode & SC_FETCH_ONE) {
                ZVAL_LONG(arr, (long)raw);
            } else {
                sc_add_assoc_long_ex(arr, column_name.c_str(), column_name.length(), (zend_long)raw);
            }
        }
        break;
    }
    case Type::Code::Date:
    {
        auto col = columnRef->As<ColumnDate>();
        emitEpoch(arr, (std::time_t)col->At(row), "%Y-%m-%d",
                  column_name, is_array, fetch_mode, /*null_if_nonpositive=*/true);
        break;
    }
    case Type::Code::Date32:
    {
        auto col = columnRef->As<ColumnDate32>();
        emitEpoch(arr, (std::time_t)col->At(row), "%Y-%m-%d",
                  column_name, is_array, fetch_mode, /*null_if_nonpositive=*/false);
        break;
    }
    case Type::Code::Time:
    {
        auto col = columnRef->As<ColumnTime>();
        int32_t v = col->At(row);
        if (fetch_mode & SC_FETCH_DATE_AS_STRINGS) {
            int abs_v = v < 0 ? -v : v;
            char buffer[16];
            int l = snprintf(buffer, sizeof(buffer), "%s%02d:%02d:%02d",
                             v < 0 ? "-" : "", abs_v / 3600, (abs_v / 60) % 60, abs_v % 60);
            if (is_array) {
                sc_add_next_index_stringl(arr, buffer, l, 1);
            } else {
                SC_SINGLE_STRING(buffer, l);
            }
        } else {
            if (is_array) {
                add_next_index_long(arr, (long)v);
            } else if (fetch_mode & SC_FETCH_ONE) {
                ZVAL_LONG(arr, (long)v);
            } else {
                sc_add_assoc_long_ex(arr, column_name.c_str(), column_name.length(), (zend_long)v);
            }
        }
        break;
    }
    case Type::Code::Time64:
    {
        auto col = columnRef->As<ColumnTime64>();
        size_t precision = columnRef->Type()->As<Time64Type>()->GetPrecision();
        int64_t scale = 1;
        for (size_t i = 0; i < precision; ++i) scale *= 10;
        int64_t raw = col->At(row);
        if (fetch_mode & SC_FETCH_DATE_AS_STRINGS) {
            int64_t whole = raw / scale;
            int64_t frac = raw % scale;
            if (frac < 0) frac = -frac;
            int64_t abs_whole = whole < 0 ? -whole : whole;
            char buffer[64];
            int l = snprintf(buffer, sizeof(buffer), "%s%02lld:%02lld:%02lld",
                             whole < 0 ? "-" : "",
                             (long long)(abs_whole / 3600),
                             (long long)((abs_whole / 60) % 60),
                             (long long)(abs_whole % 60));
            if (precision > 0 && l > 0) {
                int w = snprintf(buffer + l, sizeof(buffer) - l, ".%0*lld",
                                 (int)precision, (long long)frac);
                if (w > 0) l += w;
            }
            if (is_array) {
                sc_add_next_index_stringl(arr, buffer, l, 1);
            } else {
                SC_SINGLE_STRING(buffer, l);
            }
        } else {
            if (is_array) {
                add_next_index_long(arr, (long)raw);
            } else if (fetch_mode & SC_FETCH_ONE) {
                ZVAL_LONG(arr, (long)raw);
            } else {
                sc_add_assoc_long_ex(arr, column_name.c_str(), column_name.length(), (zend_long)raw);
            }
        }
        break;
    }
    case Type::Code::Int128:
    {
        auto col = columnRef->As<ColumnInt128>();
        Int128 v = col->At(row);
        std::stringstream ss;
        ss << v;
        std::string s = ss.str();
        if (is_array) {
            sc_add_next_index_stringl(arr, (char*)s.c_str(), s.length(), 1);
        } else {
            SC_SINGLE_STRING((char*)s.c_str(), s.length());
        }
        break;
    }
    case Type::Code::UInt128:
    {
        auto col = columnRef->As<ColumnUInt128>();
        UInt128 v = col->At(row);
        std::stringstream ss;
        ss << v;
        std::string s = ss.str();
        if (is_array) {
            sc_add_next_index_stringl(arr, (char*)s.c_str(), s.length(), 1);
        } else {
            SC_SINGLE_STRING((char*)s.c_str(), s.length());
        }
        break;
    }
    case Type::Code::Array:
    {
        auto array = columnRef->As<ColumnArray>();
        auto col = array->GetAsColumn(row);
        if (fetch_mode & SC_FETCH_ONE) {
            array_init(arr);
            for (size_t i = 0; i < col->Size(); ++i)
            {
                convertToZval(arr, col, i, "array", 1, 0);
            }
        } else {
            zval *return_tmp;
            SC_MAKE_STD_ZVAL(return_tmp);
            array_init(return_tmp);
            for (size_t i = 0; i < col->Size(); ++i)
            {
                convertToZval(return_tmp, col, i, "array", 1, 0);
            }
            if (is_array)
            {
                add_next_index_zval(arr, return_tmp);
            }
            else
            {
                sc_add_assoc_zval_ex(arr, column_name.c_str(), column_name.length(), return_tmp);
            }
        }
        break;
    }

    case Type::Code::Enum8:
    {
        auto array = columnRef->As<ColumnEnum8>();
        if (is_array)
        {
            sc_add_next_index_stringl(arr, (char*)array->NameAt(row).data(), array->NameAt(row).length(), 1);
        }
        else
        {
            SC_SINGLE_STRING((char*)array->NameAt(row).data(), array->NameAt(row).length());
        }
        break;
    }
    case Type::Code::Enum16:
    {
        auto array = columnRef->As<ColumnEnum16>();
        if (is_array)
        {
            sc_add_next_index_stringl(arr, (char*)array->NameAt(row).data(), array->NameAt(row).length(), 1);
        }
        else
        {
            SC_SINGLE_STRING((char*)array->NameAt(row).data(), array->NameAt(row).length());
        }
        break;
    }

    case Type::Code::Nullable:
    {
        auto nullable = columnRef->As<ColumnNullable>();
        if (nullable->IsNull(row))
        {
            if (is_array)
            {
                add_next_index_null(arr);
            }
            else
            {
                if (fetch_mode & SC_FETCH_ONE) {
                    ZVAL_NULL(arr);
                } else {
                    sc_add_assoc_null_ex(arr, column_name.c_str(), column_name.length());
                }
            }
        }
        else
        {
            convertToZval(arr, nullable->Nested(), row, column_name, is_array, fetch_mode);
        }
        break;
    }

    case Type::Code::Tuple:
    {
        auto tuple = columnRef->As<ColumnTuple>();
        if (fetch_mode & SC_FETCH_ONE) {
            array_init(arr);
            for (size_t i = 0; i < tuple->TupleSize(); ++i)
            {
                convertToZval(arr, (*tuple)[i], row, "tuple", 1, 0);
            }
        } else {
            zval *return_tmp;
            SC_MAKE_STD_ZVAL(return_tmp);
            array_init(return_tmp);
            for (size_t i = 0; i < tuple->TupleSize(); ++i)
            {
                convertToZval(return_tmp, (*tuple)[i], row, "tuple", 1, 0);
            }
            if (is_array)
            {
                add_next_index_zval(arr, return_tmp);
            }
            else
            {
                sc_add_assoc_zval_ex(arr, column_name.c_str(), column_name.length(), return_tmp);
            }
        }
        break;
    }

    case Type::Code::LowCardinality:
    {
        // Drop through ColumnLowCardinality::GetItem so the same code
        // path covers LC(String), LC(FixedString), LC(Nullable(String)),
        // and LC(Nullable(FixedString)) -- a NULL entry returns an
        // ItemView with type Void regardless of the nested column.
        auto lc = columnRef->As<ColumnLowCardinality>();
        if (!lc) {
            throw std::runtime_error("LowCardinality column downcast failed");
        }
        TypeRef nested = columnRef->Type()->As<LowCardinalityType>()->GetNestedType();
        bool is_nullable = (nested->GetCode() == Type::Code::Nullable);
        TypeRef inner = is_nullable
            ? nested->As<NullableType>()->GetNestedType()
            : nested;
        if (inner->GetCode() != Type::Code::String &&
            inner->GetCode() != Type::Code::FixedString) {
            throw std::runtime_error("LowCardinality read only supports String / FixedString");
        }

        ItemView iv = lc->GetItem(row);
        if (is_nullable && iv.type == Type::Code::Void) {
            if (is_array) {
                add_next_index_null(arr);
            } else if (fetch_mode & SC_FETCH_ONE) {
                ZVAL_NULL(arr);
            } else {
                sc_add_assoc_null_ex(arr, column_name.c_str(), column_name.length());
            }
            break;
        }

        std::string_view sv = iv.AsBinaryData();
        // FixedString views include trailing NULs from server-side padding;
        // trim them so the round-trip preserves the original input.
        if (inner->GetCode() == Type::Code::FixedString) {
            size_t len = sv.length();
            while (len > 0 && sv.data()[len - 1] == '\0') {
                --len;
            }
            sv = std::string_view(sv.data(), len);
        }
        if (is_array) {
            sc_add_next_index_stringl(arr, (char*)sv.data(), sv.length(), 1);
        } else {
            SC_SINGLE_STRING((char*)sv.data(), sv.length());
        }
        break;
    }

    case Type::Code::Map:
    {
        TypeRef map_type = columnRef->Type();
        Type::Code key_code = map_type->As<MapType>()->GetKeyType()->GetCode();
        Type::Code value_code = map_type->As<MapType>()->GetValueType()->GetCode();
        auto map_col = columnRef->As<ColumnMap>();
        ColumnRef tuple_col = map_col->GetAsColumn(row);
        auto tup = tuple_col->As<ColumnTuple>();
        ColumnRef keys_any = (*tup)[0];
        ColumnRef values_any = (*tup)[1];
        size_t entry_count = keys_any->Size();

        zval *map_zv;
        SC_MAKE_STD_ZVAL(map_zv);
        array_init(map_zv);

        for (size_t i = 0; i < entry_count; ++i) {
            // Build PHP key.
            std::string str_key_buf;
            zend_long long_key = 0;
            bool key_is_string = (key_code == Type::Code::String);
            if (key_is_string) {
                std::string_view kv = (*keys_any->As<ColumnString>())[i];
                str_key_buf.assign(kv.data(), kv.length());
            } else if (key_code == Type::Code::Int64) {
                long_key = (zend_long)keys_any->As<ColumnInt64>()->At(i);
            } else if (key_code == Type::Code::UInt64) {
                long_key = (zend_long)keys_any->As<ColumnUInt64>()->At(i);
            } else if (key_code == Type::Code::Int32) {
                long_key = (zend_long)keys_any->As<ColumnInt32>()->At(i);
            } else if (key_code == Type::Code::UInt32) {
                long_key = (zend_long)keys_any->As<ColumnUInt32>()->At(i);
            } else {
                throw std::runtime_error("Map read: unsupported key type " + map_type->As<MapType>()->GetKeyType()->GetName());
            }

            // Build PHP value and add under the right key.
            if (value_code == Type::Code::String) {
                std::string_view vv = (*values_any->As<ColumnString>())[i];
                if (key_is_string) {
                    sc_add_assoc_stringl_ex(map_zv, str_key_buf.c_str(), str_key_buf.length(),
                                            (char*)vv.data(), vv.length(), 1);
                } else {
                    add_index_stringl(map_zv, long_key, (char*)vv.data(), vv.length());
                }
            } else if (value_code == Type::Code::Int64 || value_code == Type::Code::UInt64
                    || value_code == Type::Code::Int32 || value_code == Type::Code::UInt32
                    || value_code == Type::Code::Int16 || value_code == Type::Code::UInt16
                    || value_code == Type::Code::Int8  || value_code == Type::Code::UInt8) {
                zend_long lv = 0;
                switch (value_code) {
                    case Type::Code::Int64:  lv = (zend_long)values_any->As<ColumnInt64>()->At(i); break;
                    case Type::Code::UInt64: lv = (zend_long)values_any->As<ColumnUInt64>()->At(i); break;
                    case Type::Code::Int32:  lv = (zend_long)values_any->As<ColumnInt32>()->At(i); break;
                    case Type::Code::UInt32: lv = (zend_long)values_any->As<ColumnUInt32>()->At(i); break;
                    case Type::Code::Int16:  lv = (zend_long)values_any->As<ColumnInt16>()->At(i); break;
                    case Type::Code::UInt16: lv = (zend_long)values_any->As<ColumnUInt16>()->At(i); break;
                    case Type::Code::Int8:   lv = (zend_long)values_any->As<ColumnInt8>()->At(i); break;
                    case Type::Code::UInt8:  lv = (zend_long)values_any->As<ColumnUInt8>()->At(i); break;
                    default: break;
                }
                if (key_is_string) {
                    add_assoc_long_ex(map_zv, str_key_buf.c_str(), str_key_buf.length(), lv);
                } else {
                    add_index_long(map_zv, long_key, lv);
                }
            } else if (value_code == Type::Code::Float64 || value_code == Type::Code::Float32) {
                double dv = (value_code == Type::Code::Float64)
                    ? (double)values_any->As<ColumnFloat64>()->At(i)
                    : (double)values_any->As<ColumnFloat32>()->At(i);
                if (key_is_string) {
                    add_assoc_double_ex(map_zv, str_key_buf.c_str(), str_key_buf.length(), dv);
                } else {
                    add_index_double(map_zv, long_key, dv);
                }
            } else {
                throw std::runtime_error("Map read: unsupported value type " + map_type->As<MapType>()->GetValueType()->GetName());
            }
        }

        if (is_array) {
            add_next_index_zval(arr, map_zv);
        } else if (fetch_mode & SC_FETCH_ONE) {
            ZVAL_COPY_VALUE(arr, map_zv);
        } else {
            sc_add_assoc_zval_ex(arr, column_name.c_str(), column_name.length(), map_zv);
        }
        break;
    }

    case Type::Code::Point:
    {
        auto col = columnRef->As<ColumnPoint>();
        if (!col) throw std::runtime_error("Point column downcast failed");
        zval pt_zv;
        pointToZval(&pt_zv, col->At(row));
        emitNestedZval(arr, &pt_zv, column_name, is_array, fetch_mode);
        break;
    }
    case Type::Code::Ring:
    {
        auto col = columnRef->As<ColumnRing>();
        if (!col) throw std::runtime_error("Ring column downcast failed");
        zval r_zv;
        ringRangeToZval(&r_zv, col->At(row));
        emitNestedZval(arr, &r_zv, column_name, is_array, fetch_mode);
        break;
    }
    case Type::Code::Polygon:
    {
        auto col = columnRef->As<ColumnPolygon>();
        if (!col) throw std::runtime_error("Polygon column downcast failed");
        zval p_zv;
        polygonRangeToZval(&p_zv, col->At(row));
        emitNestedZval(arr, &p_zv, column_name, is_array, fetch_mode);
        break;
    }
    case Type::Code::MultiPolygon:
    {
        auto col = columnRef->As<ColumnMultiPolygon>();
        if (!col) throw std::runtime_error("MultiPolygon column downcast failed");
        zval mp_zv;
        array_init(&mp_zv);
        for (auto poly : col->At(row)) {
            zval poly_zv;
            polygonRangeToZval(&poly_zv, poly);
            add_next_index_zval(&mp_zv, &poly_zv);
        }
        emitNestedZval(arr, &mp_zv, column_name, is_array, fetch_mode);
        break;
    }

    case Type::Code::Void:
    {
        throw std::runtime_error("can't support Void");
    }
    default:
        throw std::runtime_error("convertToZval: unsupported type code: " + columnRef->Type()->GetName());
    }
}

void zvalToBlock(Block& blockDes, Block& blockSrc, zend_ulong num_key, zval *value_zval)
{
    ColumnRef column = insertColumn(blockSrc[num_key]->Type(), value_zval);

    blockDes.AppendColumn(blockSrc.GetColumnName(num_key), column);
}

/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: noet sw=4 ts=4 fdm=marker
 * vim<600: noet sw=4 ts=4
 */
