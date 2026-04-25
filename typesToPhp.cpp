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
#include "lib/clickhouse-cpp/clickhouse/columns/lowcardinality.h"
#include "lib/clickhouse-cpp/clickhouse/columns/map.h"
#include <iostream>
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
    return timegm(&t);
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
        if (nested->GetCode() == Type::Code::String) {
            return std::make_shared<ColumnLowCardinalityT<ColumnString>>();
        }
        if (nested->GetCode() == Type::Code::FixedString) {
            string typeName = nested->GetName();
            typeName.erase(typeName.find("FixedString("), 12);
            typeName.erase(typeName.find(")"), 1);
            return std::make_shared<ColumnLowCardinalityT<ColumnFixedString>>(std::stoi(typeName));
        }
        throw std::runtime_error("LowCardinality only supported over String / FixedString");
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

    throw std::runtime_error("createColumn runtime error.");
}

ColumnRef insertColumn(TypeRef type, zval *value_zval)
{
    zval *array_value;
    char *str_key;
    uint32_t str_keylen;
    int keytype;
    // SC_HASHTABLE_FOREACH_START2 writes into these locals on every iteration;
    // most case arms below only consume array_value, so silence the unused-set
    // warning at function scope rather than per call site.
    (void)str_key; (void)str_keylen; (void)keytype;

    HashTable *values_ht = Z_ARRVAL_P(value_zval);

    switch (type->GetCode())
    {
    case Type::Code::UInt64:
    {
        auto value = std::make_shared<ColumnUInt64>();

        SC_HASHTABLE_FOREACH_START2(values_ht, str_key, str_keylen, keytype, array_value)
        {
            if (
                Z_TYPE_P(array_value) == IS_STRING && Z_STRLEN_P(array_value) >= 3
                && (
                    *Z_STRVAL_P(array_value) == '0' &&
                    ((*(Z_STRVAL_P(array_value) + 1) == 'x') || *(Z_STRVAL_P(array_value) + 1) == 'X')
                )
            ) {
                value->Append(strtoull(Z_STRVAL_P(array_value), NULL, 0));
            } else {
                convert_to_long(array_value);
                value->Append(Z_LVAL_P(array_value));
            }
        }
        SC_HASHTABLE_FOREACH_END();

        return value;
        break;
    }
    case Type::Code::UInt8:
    {
        auto value = std::make_shared<ColumnUInt8>();

        SC_HASHTABLE_FOREACH_START2(values_ht, str_key, str_keylen, keytype, array_value)
        {
            convert_to_long(array_value);
            value->Append(Z_LVAL_P(array_value));
        }
        SC_HASHTABLE_FOREACH_END();

        return value;
        break;
    }
    case Type::Code::UInt16:
    {
        auto value = std::make_shared<ColumnUInt16>();

        SC_HASHTABLE_FOREACH_START2(values_ht, str_key, str_keylen, keytype, array_value)
        {
            convert_to_long(array_value);
            value->Append(Z_LVAL_P(array_value));
        }
        SC_HASHTABLE_FOREACH_END();

        return value;
        break;
    }
    case Type::Code::UInt32:
    {
        auto value = std::make_shared<ColumnUInt32>();

        SC_HASHTABLE_FOREACH_START2(values_ht, str_key, str_keylen, keytype, array_value)
        {
            if (
                Z_TYPE_P(array_value) == IS_STRING && Z_STRLEN_P(array_value) >= 3
                && (
                    *Z_STRVAL_P(array_value) == '0' &&
                    ((*(Z_STRVAL_P(array_value) + 1) == 'x') || *(Z_STRVAL_P(array_value) + 1) == 'X')
                )) {
                    value->Append(strtoul(Z_STRVAL_P(array_value), NULL, 0));
                } else {
                    convert_to_long(array_value);
                    value->Append(Z_LVAL_P(array_value));
                }
        }
        SC_HASHTABLE_FOREACH_END();

        return value;
        break;
    }

    case Type::Code::Int8:
    {
        auto value = std::make_shared<ColumnInt8>();

        SC_HASHTABLE_FOREACH_START2(values_ht, str_key, str_keylen, keytype, array_value)
        {
            convert_to_long(array_value);
            value->Append(Z_LVAL_P(array_value));
        }
        SC_HASHTABLE_FOREACH_END();

        return value;
        break;
    }
    case Type::Code::Int16:
    {
        auto value = std::make_shared<ColumnInt16>();

        SC_HASHTABLE_FOREACH_START2(values_ht, str_key, str_keylen, keytype, array_value)
        {
            convert_to_long(array_value);
            value->Append(Z_LVAL_P(array_value));
        }
        SC_HASHTABLE_FOREACH_END();

        return value;
        break;
    }
    case Type::Code::Int32:
    {
        auto value = std::make_shared<ColumnInt32>();

        SC_HASHTABLE_FOREACH_START2(values_ht, str_key, str_keylen, keytype, array_value)
        {
            convert_to_long(array_value);
            value->Append(Z_LVAL_P(array_value));
        }
        SC_HASHTABLE_FOREACH_END();

        return value;
        break;
    }
    case Type::Code::Int64:
    {
        auto value = std::make_shared<ColumnInt64>();

        SC_HASHTABLE_FOREACH_START2(values_ht, str_key, str_keylen, keytype, array_value)
        {
            convert_to_long(array_value);
            value->Append(Z_LVAL_P(array_value));
        }
        SC_HASHTABLE_FOREACH_END();

        return value;
        break;
    }

    case Type::Code::UUID:
    {
        auto value = std::make_shared<ColumnUUID>();

        SC_HASHTABLE_FOREACH_START2(values_ht, str_key, str_keylen, keytype, array_value)
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
        SC_HASHTABLE_FOREACH_END();
        return value;
        break;
    }

    case Type::Code::Float32:
    {
        auto value = std::make_shared<ColumnFloat32>();

        SC_HASHTABLE_FOREACH_START2(values_ht, str_key, str_keylen, keytype, array_value)
        {
            convert_to_double(array_value);
            value->Append(Z_DVAL_P(array_value));
        }
        SC_HASHTABLE_FOREACH_END();

        return value;
        break;
    }
    case Type::Code::Float64:
    {
        auto value = std::make_shared<ColumnFloat64>();

        SC_HASHTABLE_FOREACH_START2(values_ht, str_key, str_keylen, keytype, array_value)
        {
            convert_to_double(array_value);
            value->Append(Z_DVAL_P(array_value));
        }
        SC_HASHTABLE_FOREACH_END();

        return value;
        break;
    }

    case Type::Code::String:
    {
        auto value = std::make_shared<ColumnString>();

        SC_HASHTABLE_FOREACH_START2(values_ht, str_key, str_keylen, keytype, array_value)
        {
            convert_to_string(array_value);
            value->Append((string)Z_STRVAL_P(array_value));
        }
        SC_HASHTABLE_FOREACH_END();

        return value;
    }
    case Type::Code::FixedString:
    {
        string typeName = type->GetName();
        typeName.erase(typeName.find("FixedString("), 12);
        typeName.erase(typeName.find(")"), 1);
        auto value = std::make_shared<ColumnFixedString>(std::stoi(typeName));

        SC_HASHTABLE_FOREACH_START2(values_ht, str_key, str_keylen, keytype, array_value)
        {
            convert_to_string(array_value);
            value->Append((string)Z_STRVAL_P(array_value));
        }
        SC_HASHTABLE_FOREACH_END();

        return value;
    }

    case Type::Code::DateTime:
    {
        auto value = std::make_shared<ColumnDateTime>();

        SC_HASHTABLE_FOREACH_START2(values_ht, str_key, str_keylen, keytype, array_value)
        {
            if (Z_TYPE_P(array_value) == IS_STRING && memchr(Z_STRVAL_P(array_value), '-', Z_STRLEN_P(array_value)) != NULL) {
                value->Append((long)to_time_t(Z_STRVAL_P(array_value), false));
            } else {
                convert_to_long(array_value);
                value->Append(Z_LVAL_P(array_value));
            }
        }
        SC_HASHTABLE_FOREACH_END();

        return value;
        break;
    }
    case Type::Code::DateTime64:
    {
        size_t precision = type->As<DateTime64Type>()->GetPrecision();
        auto value = std::make_shared<ColumnDateTime64>(precision);
        int64_t scale = 1;
        for (size_t i = 0; i < precision; ++i) scale *= 10;

        SC_HASHTABLE_FOREACH_START2(values_ht, str_key, str_keylen, keytype, array_value)
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
        SC_HASHTABLE_FOREACH_END();

        return value;
        break;
    }
    case Type::Code::Date:
    {
        auto value = std::make_shared<ColumnDate>();

        SC_HASHTABLE_FOREACH_START2(values_ht, str_key, str_keylen, keytype, array_value)
        {
            if (Z_TYPE_P(array_value) == IS_STRING && memchr(Z_STRVAL_P(array_value), '-', Z_STRLEN_P(array_value)) != NULL) {
                value->Append((long)to_time_t(Z_STRVAL_P(array_value)));
            } else {
                convert_to_long(array_value);
                value->Append((std::time_t)Z_LVAL_P(array_value));
            }
        }
        SC_HASHTABLE_FOREACH_END();

        return value;
        break;
    }
    case Type::Code::Date32:
    {
        auto value = std::make_shared<ColumnDate32>();
        SC_HASHTABLE_FOREACH_START2(values_ht, str_key, str_keylen, keytype, array_value)
        {
            if (Z_TYPE_P(array_value) == IS_STRING && memchr(Z_STRVAL_P(array_value), '-', Z_STRLEN_P(array_value)) != NULL) {
                value->Append((std::time_t)to_time_t(Z_STRVAL_P(array_value)));
            } else {
                convert_to_long(array_value);
                value->Append((std::time_t)Z_LVAL_P(array_value));
            }
        }
        SC_HASHTABLE_FOREACH_END();
        return value;
    }
    case Type::Code::Time:
    {
        auto value = std::make_shared<ColumnTime>();
        SC_HASHTABLE_FOREACH_START2(values_ht, str_key, str_keylen, keytype, array_value)
        {
            convert_to_long(array_value);
            value->Append((int32_t)Z_LVAL_P(array_value));
        }
        SC_HASHTABLE_FOREACH_END();
        return value;
    }
    case Type::Code::Time64:
    {
        size_t precision = type->As<Time64Type>()->GetPrecision();
        auto value = std::make_shared<ColumnTime64>(precision);
        int64_t scale = 1;
        for (size_t i = 0; i < precision; ++i) scale *= 10;
        SC_HASHTABLE_FOREACH_START2(values_ht, str_key, str_keylen, keytype, array_value)
        {
            if (Z_TYPE_P(array_value) == IS_DOUBLE) {
                value->Append((int64_t)(Z_DVAL_P(array_value) * scale));
            } else {
                convert_to_long(array_value);
                value->Append((int64_t)Z_LVAL_P(array_value) * scale);
            }
        }
        SC_HASHTABLE_FOREACH_END();
        return value;
    }
    case Type::Code::Int128:
    {
        auto value = std::make_shared<ColumnInt128>();
        // Int128 range: -2^127 .. 2^127-1. The signed magnitude fits in 39
        // decimal digits (2^127 = 170141183460469231731687303715884105728).
        SC_HASHTABLE_FOREACH_START2(values_ht, str_key, str_keylen, keytype, array_value)
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
        SC_HASHTABLE_FOREACH_END();
        return value;
    }
    case Type::Code::UInt128:
    {
        auto value = std::make_shared<ColumnUInt128>();
        // UInt128 range: 0 .. 2^128-1, i.e. up to 39 decimal digits.
        SC_HASHTABLE_FOREACH_START2(values_ht, str_key, str_keylen, keytype, array_value)
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
        SC_HASHTABLE_FOREACH_END();
        return value;
    }
    case Type::Code::Decimal:
    case Type::Code::Decimal32:
    case Type::Code::Decimal64:
    case Type::Code::Decimal128:
    {
        auto dt = type->As<DecimalType>();
        auto value = std::make_shared<ColumnDecimal>(dt->GetPrecision(), dt->GetScale());
        SC_HASHTABLE_FOREACH_START2(values_ht, str_key, str_keylen, keytype, array_value)
        {
            convert_to_string(array_value);
            value->Append(std::string(Z_STRVAL_P(array_value), Z_STRLEN_P(array_value)));
        }
        SC_HASHTABLE_FOREACH_END();
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

        SC_HASHTABLE_FOREACH_START2(values_ht, str_key, str_keylen, keytype, array_value)
        {
            if (Z_TYPE_P(array_value) != IS_ARRAY)
            {
                throw std::runtime_error("The inserted data is not an array type");
            }

            child->Append(insertColumn(item_type, array_value));

            value->AppendAsColumn(child);
            child->Clear();
        }
        SC_HASHTABLE_FOREACH_END();

        return value;
        break;
    }

    case Type::Code::Enum8:
    {
        auto value = std::make_shared<ColumnEnum8>(type);

        SC_HASHTABLE_FOREACH_START2(values_ht, str_key, str_keylen, keytype, array_value)
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
        SC_HASHTABLE_FOREACH_END();

        return value;
        break;
    }
    case Type::Code::Enum16:
    {
        auto value = std::make_shared<ColumnEnum16>(type);

        SC_HASHTABLE_FOREACH_START2(values_ht, str_key, str_keylen, keytype, array_value)
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
        SC_HASHTABLE_FOREACH_END();

        return value;
        break;
    }

    case Type::Code::Nullable:
    {
        auto nulls = std::make_shared<ColumnUInt8>();

        SC_HASHTABLE_FOREACH_START2(values_ht, str_key, str_keylen, keytype, array_value)
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
        SC_HASHTABLE_FOREACH_END();

        ColumnRef child = insertColumn(type->As<NullableType>()->GetNestedType(), value_zval);

        return std::make_shared<ColumnNullable>(child, nulls);
        break;
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

            SC_HASHTABLE_FOREACH_START2(values_ht, str_key, str_keylen, keytype, pzval)
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
            SC_HASHTABLE_FOREACH_END();

            add_next_index_zval(return_should, return_tmp);
        }

        std::vector<ColumnRef> columns;
        size_t tupleTypeIndex = 0;

        SC_HASHTABLE_FOREACH_START2(Z_ARRVAL_P(return_should), str_key, str_keylen, keytype, array_value)
        {
            if (Z_TYPE_P(array_value) != IS_ARRAY)
            {
                throw std::runtime_error("The inserted data is not an array type");
            }

            columns.push_back(insertColumn(tupleType[tupleTypeIndex], array_value));
            tupleTypeIndex++;
        }
        SC_HASHTABLE_FOREACH_END();

        sc_zval_ptr_dtor(&return_should);

        return std::make_shared<ColumnTuple>(columns);
    }

    case Type::Code::LowCardinality:
    {
        TypeRef nested = type->As<LowCardinalityType>()->GetNestedType();
        if (nested->GetCode() == Type::Code::String) {
            auto value = std::make_shared<ColumnLowCardinalityT<ColumnString>>();
            SC_HASHTABLE_FOREACH_START2(values_ht, str_key, str_keylen, keytype, array_value)
            {
                convert_to_string(array_value);
                value->Append(std::string_view(Z_STRVAL_P(array_value), Z_STRLEN_P(array_value)));
            }
            SC_HASHTABLE_FOREACH_END();
            return value;
        }
        if (nested->GetCode() == Type::Code::FixedString) {
            string typeName = nested->GetName();
            typeName.erase(typeName.find("FixedString("), 12);
            typeName.erase(typeName.find(")"), 1);
            auto value = std::make_shared<ColumnLowCardinalityT<ColumnFixedString>>(std::stoi(typeName));
            SC_HASHTABLE_FOREACH_START2(values_ht, str_key, str_keylen, keytype, array_value)
            {
                convert_to_string(array_value);
                value->Append(std::string_view(Z_STRVAL_P(array_value), Z_STRLEN_P(array_value)));
            }
            SC_HASHTABLE_FOREACH_END();
            return value;
        }
        throw std::runtime_error("LowCardinality only supported over String / FixedString");
    }

    case Type::Code::Map:
    {
        TypeRef k = type->As<MapType>()->GetKeyType();
        TypeRef v = type->As<MapType>()->GetValueType();
        Type::Code kc = k->GetCode();
        Type::Code vc = v->GetCode();

        if (kc == Type::Code::String && vc == Type::Code::String) {
            auto col = std::make_shared<ColumnMapT<ColumnString, ColumnString>>(
                std::make_shared<ColumnString>(), std::make_shared<ColumnString>());
            SC_HASHTABLE_FOREACH_START2(values_ht, str_key, str_keylen, keytype, array_value) {
                if (Z_TYPE_P(array_value) != IS_ARRAY) throw std::runtime_error("Map row must be a PHP array");
                std::vector<std::pair<std::string, std::string>> entries;
                HashTable *mh = Z_ARRVAL_P(array_value);
                zval *mv; char *mk; uint32_t ml; int mt; (void)mt;
                SC_HASHTABLE_FOREACH_START2(mh, mk, ml, mt, mv) {
                    if (!mk) continue;
                    convert_to_string(mv);
                    entries.emplace_back(std::string(mk, ml),
                                         std::string(Z_STRVAL_P(mv), Z_STRLEN_P(mv)));
                } SC_HASHTABLE_FOREACH_END();
                col->Append(entries);
            } SC_HASHTABLE_FOREACH_END();
            return col;
        }
        if (kc == Type::Code::String && vc == Type::Code::Int64) {
            auto col = std::make_shared<ColumnMapT<ColumnString, ColumnInt64>>(
                std::make_shared<ColumnString>(), std::make_shared<ColumnInt64>());
            SC_HASHTABLE_FOREACH_START2(values_ht, str_key, str_keylen, keytype, array_value) {
                if (Z_TYPE_P(array_value) != IS_ARRAY) throw std::runtime_error("Map row must be a PHP array");
                std::vector<std::pair<std::string, int64_t>> entries;
                HashTable *mh = Z_ARRVAL_P(array_value);
                zval *mv; char *mk; uint32_t ml; int mt; (void)mt;
                SC_HASHTABLE_FOREACH_START2(mh, mk, ml, mt, mv) {
                    if (!mk) continue;
                    convert_to_long(mv);
                    entries.emplace_back(std::string(mk, ml), (int64_t)Z_LVAL_P(mv));
                } SC_HASHTABLE_FOREACH_END();
                col->Append(entries);
            } SC_HASHTABLE_FOREACH_END();
            return col;
        }
        if (kc == Type::Code::String && vc == Type::Code::UInt64) {
            auto col = std::make_shared<ColumnMapT<ColumnString, ColumnUInt64>>(
                std::make_shared<ColumnString>(), std::make_shared<ColumnUInt64>());
            SC_HASHTABLE_FOREACH_START2(values_ht, str_key, str_keylen, keytype, array_value) {
                if (Z_TYPE_P(array_value) != IS_ARRAY) throw std::runtime_error("Map row must be a PHP array");
                std::vector<std::pair<std::string, uint64_t>> entries;
                HashTable *mh = Z_ARRVAL_P(array_value);
                zval *mv; char *mk; uint32_t ml; int mt; (void)mt;
                SC_HASHTABLE_FOREACH_START2(mh, mk, ml, mt, mv) {
                    if (!mk) continue;
                    convert_to_long(mv);
                    entries.emplace_back(std::string(mk, ml), (uint64_t)Z_LVAL_P(mv));
                } SC_HASHTABLE_FOREACH_END();
                col->Append(entries);
            } SC_HASHTABLE_FOREACH_END();
            return col;
        }
        if (kc == Type::Code::String && vc == Type::Code::Float64) {
            auto col = std::make_shared<ColumnMapT<ColumnString, ColumnFloat64>>(
                std::make_shared<ColumnString>(), std::make_shared<ColumnFloat64>());
            SC_HASHTABLE_FOREACH_START2(values_ht, str_key, str_keylen, keytype, array_value) {
                if (Z_TYPE_P(array_value) != IS_ARRAY) throw std::runtime_error("Map row must be a PHP array");
                std::vector<std::pair<std::string, double>> entries;
                HashTable *mh = Z_ARRVAL_P(array_value);
                zval *mv; char *mk; uint32_t ml; int mt; (void)mt;
                SC_HASHTABLE_FOREACH_START2(mh, mk, ml, mt, mv) {
                    if (!mk) continue;
                    convert_to_double(mv);
                    entries.emplace_back(std::string(mk, ml), (double)Z_DVAL_P(mv));
                } SC_HASHTABLE_FOREACH_END();
                col->Append(entries);
            } SC_HASHTABLE_FOREACH_END();
            return col;
        }
        if (kc == Type::Code::Int64 && vc == Type::Code::String) {
            auto col = std::make_shared<ColumnMapT<ColumnInt64, ColumnString>>(
                std::make_shared<ColumnInt64>(), std::make_shared<ColumnString>());
            SC_HASHTABLE_FOREACH_START2(values_ht, str_key, str_keylen, keytype, array_value) {
                if (Z_TYPE_P(array_value) != IS_ARRAY) throw std::runtime_error("Map row must be a PHP array");
                std::vector<std::pair<int64_t, std::string>> entries;
                HashTable *mh = Z_ARRVAL_P(array_value);
                zend_string *zk;
                zend_ulong nk;
                zval *mv;
                ZEND_HASH_FOREACH_KEY_VAL(mh, nk, zk, mv) {
                    convert_to_string(mv);
                    int64_t kk = zk ? (int64_t)strtoll(ZSTR_VAL(zk), NULL, 10) : (int64_t)nk;
                    entries.emplace_back(kk, std::string(Z_STRVAL_P(mv), Z_STRLEN_P(mv)));
                } ZEND_HASH_FOREACH_END();
                col->Append(entries);
            } SC_HASHTABLE_FOREACH_END();
            return col;
        }
        throw std::runtime_error("Unsupported Map(K, V) for row write: " + type->GetName());
    }

    case Type::Code::Void:
    {
        throw std::runtime_error("can't support Void");
    }
    default:
        throw std::runtime_error("insertColumn: unsupported type code: " + type->GetName());
    }

    throw std::runtime_error("insertColumn runtime error.");
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

void convertToZval(zval *arr, const ColumnRef& columnRef, int row, string column_name, int8_t is_array, long fetch_mode)
{
    switch (columnRef->Type()->GetCode())
    {
    case Type::Code::UInt64:
    {
        auto col = (*columnRef->As<ColumnUInt64>())[row];
        if (is_array)
        {
            add_next_index_long(arr, (long)col);
        }
        else
        {
            SC_SINGLE_LONG();
        }
        break;
    }
    case Type::Code::UInt8:
    {
        auto col = (*columnRef->As<ColumnUInt8>())[row];
        if (is_array)
        {
            add_next_index_long(arr, (long)col);
        }
        else
        {
            SC_SINGLE_LONG();
        }
        break;
    }
    case Type::Code::UInt16:
    {
        auto col = (*columnRef->As<ColumnUInt16>())[row];
        if (is_array)
        {
            add_next_index_long(arr, (long)col);
        }
        else
        {
            SC_SINGLE_LONG();
        }
        break;
    }
    case Type::Code::UInt32:
    case Type::Code::IPv4:
    {
        auto col = (*columnRef->As<ColumnUInt32>())[row];
        if (is_array)
        {
            add_next_index_long(arr, (long)col);
        }
        else
        {
            SC_SINGLE_LONG();
        }
        break;
    }
    case Type::Code::Int8:
    {
        auto col = (*columnRef->As<ColumnInt8>())[row];
        if (is_array)
        {
            add_next_index_long(arr, (long)col);
        }
        else
        {
            SC_SINGLE_LONG();
        }
        break;
    }
    case Type::Code::Int16:
    {
        auto col = (*columnRef->As<ColumnInt16>())[row];
        if (is_array)
        {
            add_next_index_long(arr, (long)col);
        }
        else
        {
            SC_SINGLE_LONG();
        }
        break;
    }
    case Type::Code::Int32:
    {
        auto col = (*columnRef->As<ColumnInt32>())[row];
        if (is_array)
        {
            add_next_index_long(arr, (long)col);
        }
        else
        {
            SC_SINGLE_LONG();
        }
        break;
    }
    case Type::Code::Int64:
    {
        auto col = (*columnRef->As<ColumnInt64>())[row];
        if (is_array)
        {
            add_next_index_long(arr, (long)col);
        }
        else
        {
            SC_SINGLE_LONG();
        }
        break;
    }
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
        // IPv6 is always 16 raw bytes; trailing NULs are meaningful, don't trim.
        auto col = (*columnRef->As<ColumnFixedString>())[row];
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

    case Type::Code::DateTime:
    {
        auto col = columnRef->As<ColumnDateTime>();
        if (is_array)
        {
            if (fetch_mode & SC_FETCH_DATE_AS_STRINGS) {
                char buffer[32];
                size_t l;
                std::time_t t = (long)col->As<ColumnDateTime>()->At(row);
                if (t > 0) {
                    l = strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", gmtime(&t));
                    sc_add_next_index_stringl(arr, buffer, l, 1);
                } else {
                    add_next_index_null(arr);
                }
            } else {
                add_next_index_long(arr, (long)col->As<ColumnDateTime>()->At(row));
            }
        }
        else
        {
            if (fetch_mode & SC_FETCH_DATE_AS_STRINGS) {
                char buffer[32];
                size_t l;
                std::time_t t = (long)col->As<ColumnDateTime>()->At(row);
                if (t > 0) {
                    l = strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", gmtime(&t));
                    SC_SINGLE_STRING(buffer, l);
                } else {
                    if (fetch_mode & SC_FETCH_ONE) {
                        ZVAL_NULL(arr);
                    } else {
                        sc_add_assoc_null_ex(arr, column_name.c_str(), column_name.length());
                    }
                }
            } else {
                if (fetch_mode & SC_FETCH_ONE) {
                    ZVAL_LONG(arr, (long)col->As<ColumnDateTime>()->At(row));
                } else {
                    sc_add_assoc_long_ex(arr, column_name.c_str(), column_name.length(), (zend_long)col->As<ColumnDateTime>()->At(row));
                }
            }
        }
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
        if (is_array)
        {
            if (fetch_mode & SC_FETCH_DATE_AS_STRINGS) {
                char buffer[16];
                size_t l;
                std::time_t t = (long)col->As<ColumnDate>()->At(row);
                if (t > 0) {
                    l = strftime(buffer, sizeof(buffer), "%Y-%m-%d", gmtime(&t));
                    sc_add_next_index_stringl(arr, buffer, l, 1);
                } else {
                    add_next_index_null(arr);
                }
            } else {
                add_next_index_long(arr, (long)col->As<ColumnDate>()->At(row));
            }
        }
        else
        {
            if (fetch_mode & SC_FETCH_DATE_AS_STRINGS) {
                char buffer[16];
                size_t l;
                std::time_t t = (long)col->As<ColumnDate>()->At(row);
                if (t > 0) {
                    l = strftime(buffer, sizeof(buffer), "%Y-%m-%d", gmtime(&t));
                    SC_SINGLE_STRING(buffer, l);
                } else {
                    if (fetch_mode & SC_FETCH_ONE) {
                        ZVAL_NULL(arr);
                    } else {
                        sc_add_assoc_null_ex(arr, column_name.c_str(), column_name.length());
                    }
                }
            } else {
                if (fetch_mode & SC_FETCH_ONE) {
                    ZVAL_LONG(arr, (long)col->As<ColumnDate>()->At(row));
                } else {
                    sc_add_assoc_long_ex(arr, column_name.c_str(), column_name.length(), (zend_long)col->As<ColumnDate>()->At(row));
                }
            }
        }
        break;
    }
    case Type::Code::Date32:
    {
        auto col = columnRef->As<ColumnDate32>();
        std::time_t t = col->At(row);
        if (fetch_mode & SC_FETCH_DATE_AS_STRINGS) {
            char buffer[16];
            size_t l = strftime(buffer, sizeof(buffer), "%Y-%m-%d", gmtime(&t));
            if (is_array) {
                sc_add_next_index_stringl(arr, buffer, l, 1);
            } else {
                SC_SINGLE_STRING(buffer, l);
            }
        } else {
            if (is_array) {
                add_next_index_long(arr, (long)t);
            } else if (fetch_mode & SC_FETCH_ONE) {
                ZVAL_LONG(arr, (long)t);
            } else {
                sc_add_assoc_long_ex(arr, column_name.c_str(), column_name.length(), (zend_long)t);
            }
        }
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
        TypeRef nested = columnRef->Type()->As<LowCardinalityType>()->GetNestedType();
        std::string_view sv;
        if (nested->GetCode() == Type::Code::String) {
            sv = columnRef->As<ColumnLowCardinalityT<ColumnString>>()->At(row);
        } else if (nested->GetCode() == Type::Code::FixedString) {
            sv = columnRef->As<ColumnLowCardinalityT<ColumnFixedString>>()->At(row);
        } else {
            throw std::runtime_error("LowCardinality read only supports String / FixedString");
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
