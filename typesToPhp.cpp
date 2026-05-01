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
#include <sstream>
#include <iomanip>
#include <algorithm>
#include <cmath>
#include <cerrno>
#include <cinttypes>

#include "typesToPhp.hpp"

using namespace clickhouse;
using namespace std;

/*
 * Format a 128-bit integer into a decimal string using a stack buffer.
 * Avoids the heap allocation a stringstream incurs per cell on the read
 * paths for Int128 / UInt128 / Decimal columns. Returns the number of
 * bytes written into `out` (which must hold at least 41 bytes: sign +
 * 39 digits + NUL margin).
 */
static size_t format_uint128_dec(absl::uint128 v, char *out)
{
    char tmp[40];
    int len = 0;
    do {
        tmp[len++] = (char)('0' + (int)(v % 10));
        v /= 10;
    } while (v != 0);
    for (int i = 0; i < len; ++i) {
        out[i] = tmp[len - 1 - i];
    }
    return (size_t)len;
}

static size_t format_int128_dec(absl::int128 v, char *out)
{
    if (v >= 0) {
        return format_uint128_dec(absl::uint128(v), out);
    }
    out[0] = '-';
    /* Use unsigned negation to handle INT128_MIN (whose magnitude is
     * representable in uint128 but not in int128). */
    absl::uint128 mag = absl::uint128(0) - absl::uint128(v);
    return 1 + format_uint128_dec(mag, out + 1);
}

/*
 * Parse a decimal-digit string into a 128-bit unsigned. Both ColumnInt128
 * and ColumnUInt128 inserts share the body; the signed wrapper composes
 * with negate handled by the caller. Throws on overflow / non-digit.
 *
 * `out_label` is the type name for the error message ("Int128" / "UInt128").
 */
static absl::uint128 parse_uint128_dec(const char *s, size_t len, const char *out_label)
{
    if (len == 0 || len > 39) {
        throw std::runtime_error(std::string(out_label) + " string is empty or too long");
    }
    absl::uint128 v = 0;
    for (size_t i = 0; i < len; ++i) {
        if (s[i] < '0' || s[i] > '9') {
            throw std::runtime_error(std::string(out_label) + " string contains non-digit characters");
        }
        absl::uint128 next = v * 10 + absl::uint128((unsigned)(s[i] - '0'));
        if (next < v) {
            throw std::runtime_error(std::string(out_label) + " string overflows the 128-bit range");
        }
        v = next;
    }
    return v;
}

/*
 * dynamic_pointer_cast helper that throws a contextual error when the
 * cast returns null, instead of leaving the caller to deref nullptr.
 * The clickhouse-cpp Block schema metadata is server-supplied; a
 * mismatch between the declared type code and the actual ColumnRef
 * concrete type used to crash the worker. Callers in convertToZval
 * (especially Map / Tuple decoders) wrap every typed cast through here.
 */
template <typename TCol>
static inline std::shared_ptr<TCol> as_or_throw(const ColumnRef &c, const char *what)
{
    auto p = c->As<TCol>();
    if (!p) {
        throw std::runtime_error(std::string(what) + ": column type mismatch");
    }
    return p;
}

/*
 * Strict numeric coercion for INSERT cells. PHP's `zval_get_long` and
 * `zval_get_double` happily produce 0 / 0.0 for non-numeric strings,
 * arrays, objects, etc., which used to land "abc" as 0 in an Int32
 * column with no diagnostic. The strict variants below reject every
 * non-numeric input and require full string consumption, mirroring
 * the strict parsers we already use for Map keys (CR-306) and hex
 * literals (CR-508). Range-checking against the destination column's
 * width is still the caller's responsibility (appendIntColumn passes
 * MinV/MaxV, narrow-int Map dispatch wraps with its own checks).
 *
 * IS_NULL handling: rejected by default (storing 0 silently corrupts
 * non-Nullable columns). The Nullable insert path bumps
 * `g_allow_null_in_strict` via AllowNullGuard so its recursive child
 * build can accept NULL cells (the null mask makes the placeholder
 * value irrelevant).
 */
static thread_local int g_allow_null_in_strict = 0;
struct AllowNullGuard {
    AllowNullGuard()  { ++g_allow_null_in_strict; }
    ~AllowNullGuard() { --g_allow_null_in_strict; }
    AllowNullGuard(const AllowNullGuard&) = delete;
    AllowNullGuard& operator=(const AllowNullGuard&) = delete;
};
static zend_long strict_zval_long(zval *z, const char *type_label)
{
    switch (Z_TYPE_P(z)) {
        case IS_LONG:  return Z_LVAL_P(z);
        case IS_TRUE:  return 1;
        case IS_FALSE: return 0;
        case IS_NULL:
            if (g_allow_null_in_strict > 0) return 0;
            throw std::runtime_error(
                std::string("null cannot be assigned to non-Nullable column ") + type_label);
        case IS_DOUBLE: {
            double d = Z_DVAL_P(z);
            if (std::isnan(d) || std::isinf(d)) {
                throw std::runtime_error(
                    std::string("non-finite double cannot be assigned to ") + type_label);
            }
            double frac, intpart;
            frac = std::modf(d, &intpart);
            if (frac != 0.0) {
                throw std::runtime_error(
                    std::string("fractional double cannot be assigned to integer column ") + type_label);
            }
            if (d < (double)ZEND_LONG_MIN || d > (double)ZEND_LONG_MAX) {
                throw std::runtime_error(
                    std::string("double out of range for integer column ") + type_label);
            }
            return (zend_long)d;
        }
        case IS_STRING: {
            const char *s = Z_STRVAL_P(z);
            size_t slen = Z_STRLEN_P(z);
            if (slen == 0) {
                throw std::runtime_error(
                    std::string("empty string cannot be assigned to ") + type_label);
            }
            char *endp = NULL;
            errno = 0;
            long long v = strtoll(s, &endp, 10);
            if (errno == ERANGE || endp == s ||
                (size_t)(endp - s) != slen) {
                throw std::runtime_error(
                    std::string("invalid integer string for ") + type_label);
            }
            return (zend_long)v;
        }
        default:
            throw std::runtime_error(
                std::string("array / object / resource cannot be assigned to integer column ") + type_label);
    }
}

/* UInt64 needs a strict parser of its own because strict_zval_long
 * tops out at ZEND_LONG_MAX (2^63-1): values above that arrive as
 * decimal strings (PHP can't fit them in a zend_long) and must be
 * parsed via strtoull, not strtoll. Same shape as strict_zval_long
 * — full-consumption check, NULL handled under AllowNullGuard,
 * fractional / non-finite doubles rejected — but with the unsigned
 * range and an additional `0x` hex form. */
static uint64_t strict_zval_u64(zval *z, const char *type_label)
{
    switch (Z_TYPE_P(z)) {
        case IS_LONG: {
            zend_long n = Z_LVAL_P(z);
            if (n < 0) {
                throw std::runtime_error(
                    std::string("negative value cannot fit in ") + type_label);
            }
            return (uint64_t)n;
        }
        case IS_TRUE:  return 1;
        case IS_FALSE: return 0;
        case IS_NULL:
            if (g_allow_null_in_strict > 0) return 0;
            throw std::runtime_error(
                std::string("null cannot be assigned to non-Nullable column ") + type_label);
        case IS_DOUBLE: {
            double d = Z_DVAL_P(z);
            if (std::isnan(d) || std::isinf(d)) {
                throw std::runtime_error(
                    std::string("non-finite double cannot be assigned to ") + type_label);
            }
            double frac, intpart;
            frac = std::modf(d, &intpart);
            if (frac != 0.0) {
                throw std::runtime_error(
                    std::string("fractional double cannot be assigned to integer column ") + type_label);
            }
            /* 18446744073709551616.0 is the next representable double
             * above 2^64. Anything >= it overflows uint64_t. Negatives
             * are rejected explicitly. */
            if (d < 0.0 || d >= 18446744073709551616.0) {
                throw std::runtime_error(
                    std::string("double out of range for integer column ") + type_label);
            }
            return (uint64_t)d;
        }
        case IS_STRING: {
            const char *s = Z_STRVAL_P(z);
            size_t slen = Z_STRLEN_P(z);
            if (slen == 0) {
                throw std::runtime_error(
                    std::string("empty string cannot be assigned to ") + type_label);
            }
            int base = 10;
            const char *p = s;
            size_t plen = slen;
            if (slen >= 3 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X')) {
                base = 16;
                p = s + 2;
                plen = slen - 2;
            }
            if (*p == '-' || *p == '+') {
                throw std::runtime_error(
                    std::string("invalid integer string for ") + type_label);
            }
            char *endp = NULL;
            errno = 0;
            unsigned long long v = strtoull(p, &endp, base);
            if (errno == ERANGE || endp == p ||
                (size_t)(endp - p) != plen) {
                throw std::runtime_error(
                    std::string("invalid integer string for ") + type_label);
            }
            return (uint64_t)v;
        }
        default:
            throw std::runtime_error(
                std::string("array / object / resource cannot be assigned to integer column ") + type_label);
    }
}

static double strict_zval_double(zval *z, const char *type_label)
{
    switch (Z_TYPE_P(z)) {
        case IS_LONG:  return (double)Z_LVAL_P(z);
        case IS_TRUE:  return 1.0;
        case IS_FALSE: return 0.0;
        case IS_NULL:
            if (g_allow_null_in_strict > 0) return 0.0;
            throw std::runtime_error(
                std::string("null cannot be assigned to non-Nullable column ") + type_label);
        case IS_DOUBLE: {
            double d = Z_DVAL_P(z);
            if (std::isnan(d) || std::isinf(d)) {
                throw std::runtime_error(
                    std::string("non-finite double cannot be assigned to ") + type_label);
            }
            return d;
        }
        case IS_STRING: {
            const char *s = Z_STRVAL_P(z);
            size_t slen = Z_STRLEN_P(z);
            if (slen == 0) {
                throw std::runtime_error(
                    std::string("empty string cannot be assigned to ") + type_label);
            }
            char *endp = NULL;
            errno = 0;
            double v = strtod(s, &endp);
            if (errno == ERANGE || endp == s ||
                (size_t)(endp - s) != slen ||
                std::isnan(v) || std::isinf(v)) {
                throw std::runtime_error(
                    std::string("invalid float string for ") + type_label);
            }
            return v;
        }
        default:
            throw std::runtime_error(
                std::string("array / object / resource cannot be assigned to float column ") + type_label);
    }
}

/*
 * RAII guard that owns a zend_string* obtained from zval_get_string and
 * releases it in the destructor. Used at PHP-to-C boundaries where the
 * surrounding code can throw (validation, recursive insertColumn, etc.)
 * without forcing every site to write try { ... } catch { release; throw; }.
 */
/*
 * Extract the width from a "FixedString(N)" type name. The previous
 * inline form did `typeName.erase(typeName.find("FixedString("), 12)` —
 * if find() returned npos, erase(npos, 12) is undefined. This helper
 * validates the prefix and parses the digit run.
 */
static int parseFixedStringWidth(TypeRef type)
{
    const std::string &name = type->GetName();
    static const char prefix[] = "FixedString(";
    static const size_t prefix_len = sizeof(prefix) - 1;
    if (name.size() < prefix_len + 2 ||
        name.compare(0, prefix_len, prefix) != 0 ||
        name.back() != ')') {
        throw std::runtime_error("Invalid FixedString type name: " + name);
    }
    const char *p = name.c_str() + prefix_len;
    char *endp = nullptr;
    errno = 0;
    long w = strtol(p, &endp, 10);
    if (errno == ERANGE || endp == p || w <= 0 || w > INT_MAX || endp != name.c_str() + name.size() - 1) {
        throw std::runtime_error("Invalid FixedString width: " + name);
    }
    return (int)w;
}

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
    std::tm t = {};
    std::istringstream ss(str);
    ss >> std::get_time(&t, is_date ? "%Y-%m-%d" : "%Y-%m-%d %H:%M:%S");
    if (ss.fail()) {
        /* Reject malformed date/datetime strings instead of silently
         * coercing to (time_t)-1, which then read back as 1969-12-31. */
        throw std::runtime_error(
            std::string("Invalid ") + (is_date ? "Date" : "DateTime") +
            " string: " + str);
    }
    /* std::get_time stops at the first non-matching character without
     * raising failbit, so "2024-01-01abc" parses cleanly as 2024-01-01.
     * Require EOF after the expected format so trailing garbage is
     * rejected at the boundary. peek() returns EOF when the stream is
     * fully consumed; anything else is an extra character we didn't
     * sign up for. */
    if (ss.peek() != std::char_traits<char>::eof()) {
        throw std::runtime_error(
            std::string("Invalid ") + (is_date ? "Date" : "DateTime") +
            " string (trailing characters): " + str);
    }
    /* Capture the parsed components before timegm so we can detect the
     * normalization that February 30 → March 2 silently performs. */
    int p_year = t.tm_year, p_mon = t.tm_mon, p_mday = t.tm_mday,
        p_hour = t.tm_hour, p_min = t.tm_min, p_sec = t.tm_sec;
#ifdef _WIN32
    /* MSVC has no timegm(); _mkgmtime() is the documented equivalent. */
    std::time_t out = _mkgmtime(&t);
#else
    std::time_t out = timegm(&t);
#endif
    if (out == (std::time_t)-1) {
        throw std::runtime_error(
            std::string("Invalid ") + (is_date ? "Date" : "DateTime") +
            " string (out of range): " + str);
    }
    /* Round-trip-validate: convert back to UTC components and compare.
     * This catches "2024-02-30" → 2024-03-01 normalization that timegm
     * does silently. */
    std::tm rt = {};
#ifdef _WIN32
    if (gmtime_s(&rt, &out) != 0) {
#else
    if (!gmtime_r(&out, &rt)) {
#endif
        throw std::runtime_error(
            std::string("Invalid ") + (is_date ? "Date" : "DateTime") +
            " string (gmtime failed): " + str);
    }
    if (rt.tm_year != p_year || rt.tm_mon != p_mon || rt.tm_mday != p_mday ||
        rt.tm_hour != p_hour || rt.tm_min != p_min || rt.tm_sec != p_sec) {
        throw std::runtime_error(
            std::string("Invalid ") + (is_date ? "Date" : "DateTime") +
            " string (normalized to a different value): " + str);
    }
    return out;
}

/*
 * Parse "YYYY-MM-DD HH:MM:SS[.ffffff...]" into (whole-seconds, fractional)
 * pair. The fractional component is multiplied by 10^precision so the
 * caller can encode straight into ColumnDateTime64. The previous insert
 * path used to_time_t alone, which dropped any sub-second part of the
 * string entirely.
 */
static std::pair<std::time_t, int64_t> to_time_t_with_frac(const std::string &str, size_t precision, int64_t scale)
{
    auto dot = str.find('.');
    std::time_t whole = to_time_t(dot == std::string::npos ? str : str.substr(0, dot), false);
    int64_t frac = 0;
    if (dot != std::string::npos) {
        if (precision == 0) {
            /* DateTime64(0) has no fractional component; any text after
             * the dot is invalid. The prior pass silently dropped the
             * suffix at precision 0, which let "00:00:00.garbage" land
             * as a clean DateTime64(0). */
            throw std::runtime_error(
                "Invalid DateTime64(0) string (fractional suffix on a "
                "zero-precision column): " + str);
        }
        const char *p = str.c_str() + dot + 1;
        const char *end = str.c_str() + str.size();
        if (p >= end) {
            /* Bare "12:34:56." with no digits after the dot. Previously
             * accepted (consumed=0 took the no-op path). Reject. */
            throw std::runtime_error(
                "Invalid DateTime64 string (bare dot without fractional digits): " + str);
        }
        size_t consumed = 0;
        while (p < end && consumed < precision && *p >= '0' && *p <= '9') {
            frac = frac * 10 + (*p - '0');
            ++p;
            ++consumed;
        }
        if (consumed == 0) {
            /* The first character after the dot wasn't a digit. */
            throw std::runtime_error(
                "Invalid DateTime64 string (non-digit after dot): " + str);
        }
        // Pad missing digits up to precision so "12:34:56.5" with precision 3
        // contributes 500 (ms), not 5.
        for (size_t pad = consumed; pad < precision; ++pad) frac *= 10;
        /* Reject trailing non-digit characters after the fractional part.
         * Without this "2024-01-01 00:00:00.123abc" silently truncated to
         * .123 and dropped the abc. */
        if (p < end) {
            throw std::runtime_error(
                "Invalid DateTime64 string (trailing characters after fraction): " + str);
        }
    } else {
        frac = 0;
    }
    (void)scale;
    return {whole, frac};
}

/* Cap recursion through nested column types (Tuple/Array/Map/Nullable/
 * LowCardinality) so a server-supplied schema like Tuple(Tuple(Tuple(...)))
 * cannot stack-overflow the worker. Used by both the read path
 * (convertToZval) and the write path (createColumn / insertColumn);
 * BeginInsert returns a server-built block schema, so an adversarial or
 * MITM'd server can craft a deeply-nested type just like on the read side.
 *
 * thread_local because clickhouse-cpp may dispatch from worker threads. */
static thread_local int convert_depth = 0;
static const int MAX_CONVERT_DEPTH = 32;

struct ConvertDepthGuard {
    ConvertDepthGuard() {
        if (++convert_depth > MAX_CONVERT_DEPTH) {
            --convert_depth;
            throw std::runtime_error("ClickHouse column nested-type depth exceeds limit");
        }
    }
    ~ConvertDepthGuard() { --convert_depth; }
};

ColumnRef createColumn(TypeRef type)
{
    ConvertDepthGuard _depth_guard;
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
        return std::make_shared<ColumnFixedString>(parseFixedStringWidth(type));
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
            int width = parseFixedStringWidth(inner);
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
//
// MinV/MaxV bound the destination column's representable range so that an
// out-of-range PHP value throws instead of silently wrapping in the
// narrowing assignment to ClickHouse's int8/int16/int32. Values are
// pulled non-mutatingly via zval_get_long so the caller's row arrays
// don't get their types coerced in place.
template <typename TCol>
static ColumnRef appendIntColumn(HashTable *values_ht,
                                 zend_long MinV, zend_long MaxV,
                                 const char *type_label)
{
    auto value = std::make_shared<TCol>();
    zval *array_value;
    ZEND_HASH_FOREACH_VAL(values_ht, array_value) {
        zend_long n = strict_zval_long(array_value, type_label);
        if (n < MinV || n > MaxV) {
            throw std::runtime_error(
                std::string("value out of range for ") + type_label);
        }
        value->Append((typename TCol::ValueType)n);
    } ZEND_HASH_FOREACH_END();
    return value;
}

// Build an unsigned integer column with a hex-string fast path. UInt32
// and UInt64 both accept "0x..." strings as a way to land values in the
// upper half of the range that a PHP signed long can't represent.
// `MaxV` bounds the destination column width: strtoul on 64-bit Linux
// returns 64-bit values regardless of the target column, so without a
// width check "0x100000000" silently truncated to UInt32 0.
template <typename TCol, typename TStrtoul>
static ColumnRef appendUIntColumnWithHex(HashTable *values_ht,
                                         TStrtoul strtoul_fn,
                                         uint64_t MaxV,
                                         const char *type_label)
{
    auto value = std::make_shared<TCol>();
    zval *array_value;
    ZEND_HASH_FOREACH_VAL(values_ht, array_value) {
        if (Z_TYPE_P(array_value) == IS_STRING && Z_STRLEN_P(array_value) >= 3 &&
            *Z_STRVAL_P(array_value) == '0' &&
            (*(Z_STRVAL_P(array_value) + 1) == 'x' || *(Z_STRVAL_P(array_value) + 1) == 'X')) {
            const char *s = Z_STRVAL_P(array_value);
            size_t slen = Z_STRLEN_P(array_value);
            char *endp = NULL;
            errno = 0;
            auto n = strtoul_fn(s, &endp, 0);
            /* PHP zend_string is length-prefixed and may carry embedded
             * NUL bytes. Comparing endp against ZSTR_LEN is the right
             * "fully consumed" check; checking *endp == '\0' would let
             * "0xABCD\0garbage" silently parse as 0xABCD because endp
             * lands on the NUL. Same fix CR-306 applied to Map keys. */
            if (errno == ERANGE || endp == s ||
                (size_t)(endp - s) != slen) {
                throw std::runtime_error(
                    std::string("invalid hex literal for ") + type_label);
            }
            if ((uint64_t)n > MaxV) {
                throw std::runtime_error(
                    std::string("hex literal out of range for ") + type_label);
            }
            value->Append((typename TCol::ValueType)n);
        } else {
            zend_long n = strict_zval_long(array_value, type_label);
            if (n < 0) {
                throw std::runtime_error(
                    std::string("negative value cannot fit in ") + type_label);
            }
            if ((uint64_t)n > MaxV) {
                throw std::runtime_error(
                    std::string("value out of range for ") + type_label);
            }
            value->Append((typename TCol::ValueType)n);
        }
    } ZEND_HASH_FOREACH_END();
    return value;
}

// Build an Enum8 / Enum16 column from a PHP rows array. Integer cells
// validate against the type's declared value set; the prior unchecked
// Append silently stored values like 0 / 3 / 127 inside an
// `Enum8('One'=1,'Two'=2)` column, after which normal reads threw
// `map::at` because the read path looks up the name for the stored
// integer. String cells go through ColumnEnum*::Append(name) which
// validates internally.
//
// IS_NULL handling: rejected for non-Nullable enums (would otherwise
// store raw 0, which is usually not a declared enum value and poisons
// reads). The Nullable insert path bumps AllowNullGuard so its
// recursive child build accepts NULL → declared-value placeholder; the
// null mask captures the actual NULL.
template <typename TCol, typename TInt>
static ColumnRef appendEnumColumn(TypeRef type, HashTable *values_ht)
{
    auto value = std::make_shared<TCol>(type);
    auto enum_type = type->As<clickhouse::EnumType>();
    /* Choose a placeholder int that's actually declared in the enum so
     * NULL cells under AllowNullGuard land safely. EnumType exposes
     * begin()/end() iterators over (name, value) pairs; just take the
     * first one. enum_type can be null on an unexpected schema; in
     * that case we conservatively use 0 and let HasEnumValue reject. */
    TInt placeholder = 0;
    if (enum_type) {
        auto it = enum_type->BeginValueToName();
        if (it != enum_type->EndValueToName()) {
            placeholder = (TInt)it->first;
        }
    }
    zval *array_value;
    ZEND_HASH_FOREACH_VAL(values_ht, array_value) {
        if (Z_TYPE_P(array_value) == IS_NULL) {
            if (g_allow_null_in_strict <= 0) {
                throw std::runtime_error(
                    "null cannot be assigned to non-Nullable Enum column");
            }
            value->Append(placeholder);
        } else if (Z_TYPE_P(array_value) == IS_LONG) {
            zend_long n = Z_LVAL_P(array_value);
            int16_t narrow = (int16_t)n;
            if ((zend_long)narrow != n || !enum_type || !enum_type->HasEnumValue(narrow)) {
                throw std::runtime_error(
                    "Enum integer value " + std::to_string(n) +
                    " is not declared in " + type->GetName());
            }
            value->Append((TInt)narrow);
        } else {
            /* String path: ColumnEnum*::Append(name) validates internally
             * and throws on unknown names. */
            ZStrGuard sg(array_value);
            value->Append(std::string(sg.val(), sg.len()));
        }
    } ZEND_HASH_FOREACH_END();
    return value;
}

// Build a ColumnDate / ColumnDate32 / ColumnDateTime column from a PHP
// rows array. Each row is either an int (epoch seconds) or a "YYYY-MM-DD"
// (or "YYYY-MM-DD HH:MM:SS" for is_date=false) string. The string path
// goes through to_time_t which throws on parse failure.
template <typename TCol>
static ColumnRef appendDateColumn(HashTable *values_ht, bool is_date)
{
    auto value = std::make_shared<TCol>();
    zval *array_value;
    const char *type_label = is_date ? "Date" : "DateTime";
    ZEND_HASH_FOREACH_VAL(values_ht, array_value) {
        /* Any string is treated as a formatted date/datetime. The prior
         * dash-only gate routed dashless strings through zval_get_long,
         * which silently coerced "abc" to 0 and landed it as the epoch.
         * to_time_t now does full validation (EOF after format, gmtime
         * round-trip). Numeric inputs go through strict_zval_long so
         * non-numeric, fractional, NaN/Inf are rejected. */
        if (Z_TYPE_P(array_value) == IS_STRING) {
            value->Append((std::time_t)to_time_t(
                std::string(Z_STRVAL_P(array_value), Z_STRLEN_P(array_value)),
                is_date));
        } else {
            value->Append((std::time_t)strict_zval_long(array_value, type_label));
        }
    } ZEND_HASH_FOREACH_END();
    return value;
}

// Build a LowCardinality(String) / LowCardinality(FixedString) column,
// optionally wrapped in Nullable. The four code paths used to be
// near-identical 12-line ZEND_HASH_FOREACH blocks; the template
// parameterizes on the column type and a compile-time `nullable` flag
// that decides whether IS_NULL maps to std::nullopt.
template <typename TCol, bool nullable>
static ColumnRef appendLowCardinalityColumn(HashTable *values_ht, std::shared_ptr<TCol> value)
{
    zval *array_value;
    ZEND_HASH_FOREACH_VAL(values_ht, array_value) {
        if constexpr (nullable) {
            if (Z_TYPE_P(array_value) == IS_NULL) {
                value->Append(std::nullopt);
                continue;
            }
        }
        ZStrGuard sg(array_value);
        value->Append(std::string_view(sg.val(), sg.len()));
    } ZEND_HASH_FOREACH_END();
    return value;
}

// Build a Float32/Float64 column from a PHP rows array.
template <typename TCol>
static ColumnRef appendFloatColumn(HashTable *values_ht, const char *type_label)
{
    auto value = std::make_shared<TCol>();
    zval *array_value;
    ZEND_HASH_FOREACH_VAL(values_ht, array_value) {
        value->Append((typename TCol::ValueType)strict_zval_double(array_value, type_label));
    } ZEND_HASH_FOREACH_END();
    return value;
}

// Build a ColumnMapT<KCol, VCol> from PHP rows. Each row is an assoc
// array; the caller supplies extractors that turn (zend_string*, ulong)
// into K and (zval*) into V.
template <typename K, typename V, typename KCol, typename VCol,
          typename KFn, typename VFn>
static ColumnRef appendMapColumn(HashTable *values_ht, KFn extract_key, VFn extract_val)
{
    auto col = std::make_shared<ColumnMapT<KCol, VCol>>(
        std::make_shared<KCol>(), std::make_shared<VCol>());
    /* Reuse the entries vector across rows so the per-row push_back
     * path doesn't fresh-heap-allocate; clear() preserves capacity. */
    std::vector<std::pair<K, V>> entries;
    zval *array_value;
    ZEND_HASH_FOREACH_VAL(values_ht, array_value) {
        if (Z_TYPE_P(array_value) != IS_ARRAY) {
            throw std::runtime_error("Map row must be a PHP array");
        }
        entries.clear();
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

// Parse a PHP zval into a clickhouse UUID. Mirrors the standalone-UUID
// insert path; used by Map(*, UUID) value extraction.
static UUID phpToUUID(zval *zv)
{
    if (Z_TYPE_P(zv) == IS_NULL) {
        return UUID{0, 0};
    }
    ZStrGuard sg(zv);
    std::string s(sg.val(), sg.len());
    s.erase(std::remove(s.begin(), s.end(), '-'), s.end());
    if (s.length() != 32) {
        throw std::runtime_error("UUID format error");
    }
    return UUID{
        std::stoull(s.substr(0, 16), nullptr, 16),
        std::stoull(s.substr(16, 16), nullptr, 16),
    };
}

// Second-stage Map dispatch: key column type already resolved at the
// call site, dispatch on value type code. Kept as a function template so
// each (KCol, K) tuple instantiates its own value-side switch and the
// compiler can fold identical extractor lambdas across instantiations.
template <typename KCol, typename K, typename KFn>
static ColumnRef appendMapByValueType(HashTable *values_ht, TypeRef vtype, KFn key_fn)
{
    auto strVal = [](zval *mv) -> std::string {
        ZStrGuard sg(mv);
        return std::string(sg.val(), sg.len());
    };
    /* Narrow-typed int extractors range-check before truncation. The
     * non-Map insert path has had these via appendIntColumn since pass 1;
     * the Map dispatch was using a single i64Val/u64Val for all widths
     * which silently wrapped Map(K, Int8) value 1000 to int8_t -24. */
    /* All Map value extractors go through strict_zval_long /
     * strict_zval_double so non-numeric strings, fractional doubles, and
     * non-finite floats throw instead of silently coercing to 0 / 0.0
     * inside the Map. Mirrors CR-003 for the non-Map path. */
    auto i64Val = [](zval *mv) -> int64_t {
        return (int64_t)strict_zval_long(mv, "Map value Int64");
    };
    auto u64Val = [](zval *mv) -> uint64_t {
        return strict_zval_u64(mv, "Map value UInt64");
    };
    auto narrowI = [](zval *mv, zend_long lo, zend_long hi, const char *t) -> int64_t {
        zend_long n = strict_zval_long(mv, t);
        if (n < lo || n > hi) {
            throw std::runtime_error(std::string("Map value out of range for ") + t);
        }
        return (int64_t)n;
    };
    auto narrowU = [](zval *mv, zend_ulong hi, const char *t) -> uint64_t {
        zend_long n = strict_zval_long(mv, t);
        if (n < 0 || (zend_ulong)n > hi) {
            throw std::runtime_error(std::string("Map value out of range for ") + t);
        }
        return (uint64_t)n;
    };
    auto f64Val = [](zval *mv) -> double {
        return strict_zval_double(mv, "Map value Float");
    };

    Type::Code vc = vtype->GetCode();
    switch (vc) {
        case Type::Code::String:
            return appendMapColumn<K, std::string, KCol, ColumnString>(values_ht, key_fn, strVal);
        case Type::Code::Int8: {
            auto v = [&](zval *mv) { return narrowI(mv, INT8_MIN, INT8_MAX, "Int8"); };
            return appendMapColumn<K, int64_t,    KCol, ColumnInt8>(values_ht, key_fn, v);
        }
        case Type::Code::Int16: {
            auto v = [&](zval *mv) { return narrowI(mv, INT16_MIN, INT16_MAX, "Int16"); };
            return appendMapColumn<K, int64_t,    KCol, ColumnInt16>(values_ht, key_fn, v);
        }
        case Type::Code::Int32: {
            auto v = [&](zval *mv) { return narrowI(mv, INT32_MIN, INT32_MAX, "Int32"); };
            return appendMapColumn<K, int64_t,    KCol, ColumnInt32>(values_ht, key_fn, v);
        }
        case Type::Code::Int64:
            return appendMapColumn<K, int64_t,    KCol, ColumnInt64>(values_ht, key_fn, i64Val);
        case Type::Code::UInt8: {
            auto v = [&](zval *mv) { return narrowU(mv, UINT8_MAX, "UInt8"); };
            return appendMapColumn<K, uint64_t,   KCol, ColumnUInt8>(values_ht, key_fn, v);
        }
        case Type::Code::UInt16: {
            auto v = [&](zval *mv) { return narrowU(mv, UINT16_MAX, "UInt16"); };
            return appendMapColumn<K, uint64_t,   KCol, ColumnUInt16>(values_ht, key_fn, v);
        }
        case Type::Code::UInt32: {
            auto v = [&](zval *mv) { return narrowU(mv, UINT32_MAX, "UInt32"); };
            return appendMapColumn<K, uint64_t,   KCol, ColumnUInt32>(values_ht, key_fn, v);
        }
        case Type::Code::UInt64:
            return appendMapColumn<K, uint64_t,   KCol, ColumnUInt64>(values_ht, key_fn, u64Val);
        case Type::Code::Float32:
            return appendMapColumn<K, double,     KCol, ColumnFloat32>(values_ht, key_fn, f64Val);
        case Type::Code::Float64:
            return appendMapColumn<K, double,     KCol, ColumnFloat64>(values_ht, key_fn, f64Val);
        case Type::Code::UUID:
            return appendMapColumn<K, UUID,       KCol, ColumnUUID>(values_ht, key_fn, phpToUUID);
        case Type::Code::LowCardinality: {
            TypeRef inner = vtype->As<LowCardinalityType>()->GetNestedType();
            if (inner->GetCode() == Type::Code::String) {
                return appendMapColumn<K, std::string, KCol, ColumnLowCardinalityT<ColumnString>>(values_ht, key_fn, strVal);
            }
            throw std::runtime_error("Unsupported Map value type LowCardinality(" + inner->GetName() + ")");
        }
        default:
            throw std::runtime_error("Unsupported Map value type: " + vtype->GetName());
    }
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
    return std::make_tuple(
        strict_zval_double(x, "Point coordinate"),
        strict_zval_double(y, "Point coordinate"));
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
    ConvertDepthGuard _depth_guard;  // shared with createColumn / convertToZval
    zval *array_value;
    HashTable *values_ht = Z_ARRVAL_P(value_zval);

    switch (type->GetCode())
    {
    case Type::Code::UInt64: {
        /* Direct call into strict_zval_u64 instead of going through
         * appendUIntColumnWithHex<ColumnUInt64>. The templated path
         * casts through zend_long for the non-hex branch and rejects
         * decimal strings above ZEND_LONG_MAX. UInt64 values up to
         * UINT64_MAX must be insertable from PHP — readers already
         * surface them as decimal strings — so the writer needs to
         * accept the same form. */
        auto value = std::make_shared<ColumnUInt64>();
        ZEND_HASH_FOREACH_VAL(values_ht, array_value) {
            value->Append(strict_zval_u64(array_value, "UInt64"));
        } ZEND_HASH_FOREACH_END();
        return value;
    }
    case Type::Code::UInt8:
        return appendIntColumn<ColumnUInt8>(values_ht, 0, 0xFF, "UInt8");
    case Type::Code::UInt16:
        return appendIntColumn<ColumnUInt16>(values_ht, 0, 0xFFFF, "UInt16");
    case Type::Code::UInt32:
        return appendUIntColumnWithHex<ColumnUInt32>(values_ht, strtoul, UINT32_MAX, "UInt32");
    case Type::Code::Int8:
        return appendIntColumn<ColumnInt8>(values_ht, INT8_MIN, INT8_MAX, "Int8");
    case Type::Code::Int16:
        return appendIntColumn<ColumnInt16>(values_ht, INT16_MIN, INT16_MAX, "Int16");
    case Type::Code::Int32:
        return appendIntColumn<ColumnInt32>(values_ht, INT32_MIN, INT32_MAX, "Int32");
    case Type::Code::Int64:
        return appendIntColumn<ColumnInt64>(values_ht, INT64_MIN, INT64_MAX, "Int64");

    case Type::Code::UUID:
    {
        auto value = std::make_shared<ColumnUUID>();
        ZEND_HASH_FOREACH_VAL(values_ht, array_value)
        {
            value->Append(phpToUUID(array_value));
        }
        ZEND_HASH_FOREACH_END();
        return value;
    }

    case Type::Code::Float32:
        return appendFloatColumn<ColumnFloat32>(values_ht, "Float32");
    case Type::Code::Float64:
        return appendFloatColumn<ColumnFloat64>(values_ht, "Float64");

    case Type::Code::String:
    {
        auto value = std::make_shared<ColumnString>();

        ZEND_HASH_FOREACH_VAL(values_ht, array_value)
        {
            ZStrGuard sg(array_value);
            value->Append(std::string(sg.val(), sg.len()));
        }
        ZEND_HASH_FOREACH_END();

        return value;
    }
    case Type::Code::FixedString:
    {
        auto value = std::make_shared<ColumnFixedString>(parseFixedStringWidth(type));

        ZEND_HASH_FOREACH_VAL(values_ht, array_value)
        {
            ZStrGuard sg(array_value);
            value->Append(std::string(sg.val(), sg.len()));
        }
        ZEND_HASH_FOREACH_END();

        return value;
    }

    case Type::Code::DateTime:
        return appendDateColumn<ColumnDateTime>(values_ht, /*is_date=*/false);
    case Type::Code::DateTime64:
    {
        size_t precision = type->As<DateTime64Type>()->GetPrecision();
        auto value = std::make_shared<ColumnDateTime64>(precision);
        int64_t scale = 1;
        for (size_t i = 0; i < precision; ++i) scale *= 10;

        ZEND_HASH_FOREACH_VAL(values_ht, array_value)
        {
            /* Any string is treated as a formatted timestamp; the prior
             * dash-only gate let "abc" fall through zval_get_long to 0
             * (epoch). to_time_t_with_frac validates fully. Numeric
             * inputs go through strict_zval_long / strict_zval_double. */
            if (Z_TYPE_P(array_value) == IS_STRING) {
                auto [whole, frac] = to_time_t_with_frac(
                    std::string(Z_STRVAL_P(array_value), Z_STRLEN_P(array_value)),
                    precision, scale);
                value->Append((int64_t)whole * scale + frac);
            } else if (Z_TYPE_P(array_value) == IS_DOUBLE) {
                double d = strict_zval_double(array_value, "DateTime64");
                value->Append((int64_t)(d * scale));
            } else {
                value->Append((int64_t)strict_zval_long(array_value, "DateTime64") * scale);
            }
        }
        ZEND_HASH_FOREACH_END();

        return value;
    }
    case Type::Code::Date:
        return appendDateColumn<ColumnDate>(values_ht, /*is_date=*/true);
    case Type::Code::Date32:
        return appendDateColumn<ColumnDate32>(values_ht, /*is_date=*/true);
    case Type::Code::Time:
    {
        auto value = std::make_shared<ColumnTime>();
        ZEND_HASH_FOREACH_VAL(values_ht, array_value)
        {
            /* Time is stored as int seconds-since-midnight; accept only
             * numeric inputs. String inputs would need an "HH:MM:SS"
             * parser which the column type doesn't currently expose, so
             * reject strings explicitly instead of letting zval_get_long
             * coerce "abc" to 0. */
            if (Z_TYPE_P(array_value) == IS_STRING) {
                throw std::runtime_error(
                    "Time column inserts require numeric seconds; "
                    "string formatted-time input is not currently supported");
            }
            value->Append((int32_t)strict_zval_long(array_value, "Time"));
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
            if (Z_TYPE_P(array_value) == IS_STRING) {
                throw std::runtime_error(
                    "Time64 column inserts require numeric seconds; "
                    "string formatted-time input is not currently supported");
            }
            if (Z_TYPE_P(array_value) == IS_DOUBLE) {
                double d = strict_zval_double(array_value, "Time64");
                value->Append((int64_t)(d * scale));
            } else {
                value->Append((int64_t)strict_zval_long(array_value, "Time64") * scale);
            }
        }
        ZEND_HASH_FOREACH_END();
        return value;
    }
    case Type::Code::Int128:
    {
        auto value = std::make_shared<ColumnInt128>();
        /* Int128 range is [-2^127, 2^127-1]. parse_uint128_dec accepts up
         * to 2^128-1, so an unbounded magnitude in (2^127, 2^128-1] used
         * to silently wrap to negative via the static_cast. Bound the
         * magnitude before casting; the negative-INT128_MIN edge needs
         * special handling because -INT128_MIN is undefined. */
        const absl::uint128 abs_int128_min = absl::uint128(1) << 127;
        const absl::uint128 int128_max     = abs_int128_min - 1;
        ZEND_HASH_FOREACH_VAL(values_ht, array_value)
        {
            if (Z_TYPE_P(array_value) == IS_STRING) {
                const char *s = Z_STRVAL_P(array_value);
                size_t len = Z_STRLEN_P(array_value);
                size_t i = 0;
                bool neg = false;
                if (len > 0 && (s[0] == '-' || s[0] == '+')) { neg = (s[0] == '-'); i = 1; }
                absl::uint128 mag = parse_uint128_dec(s + i, len - i, "Int128");
                if (neg) {
                    if (mag > abs_int128_min) {
                        throw std::runtime_error("Int128 string is below -2^127");
                    }
                    if (mag == abs_int128_min) {
                        /* INT128_MIN: -2^127. Constructing via -static_cast
                         * <Int128>(2^127) would be UB on the negation. */
                        value->Append(static_cast<Int128>(mag));
                    } else {
                        value->Append(-static_cast<Int128>(mag));
                    }
                } else {
                    if (mag > int128_max) {
                        throw std::runtime_error("Int128 string exceeds 2^127-1");
                    }
                    value->Append(static_cast<Int128>(mag));
                }
            } else {
                value->Append(Int128(strict_zval_long(array_value, "Int128")));
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
                value->Append(parse_uint128_dec(s + i, len - i, "UInt128"));
            } else {
                zend_long n = strict_zval_long(array_value, "UInt128");
                if (n < 0) {
                    throw std::runtime_error("UInt128 cannot accept a negative integer");
                }
                value->Append(UInt128((uint64_t)n));
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
            ZStrGuard sg(array_value);
            value->Append(std::string(sg.val(), sg.len()));
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
        return appendEnumColumn<ColumnEnum8, int8_t>(type, values_ht);
    case Type::Code::Enum16:
        return appendEnumColumn<ColumnEnum16, int16_t>(type, values_ht);

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

        /* The null mask captures IS_NULL cells, so the recursive child
         * build can accept NULL → typed-zero placeholder. Without the
         * guard, strict_zval_long / strict_zval_double would now
         * (post-CR-002) reject IS_NULL outright and break every
         * Nullable insert. */
        AllowNullGuard nulls_ok;
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

        zval return_should_storage;
        ZVAL_UNDEF(&return_should_storage);
        zval *return_should = &return_should_storage;
        array_init(return_should);

        zval return_tmp_storage;
        ZVAL_UNDEF(&return_tmp_storage);
        zval *return_tmp = &return_tmp_storage;

        try {
            zval *fzval;
            zval *pzval;
            for (size_t field = 0; field < arity; field++)
            {
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

                /* Transfer ownership: clear the slot so the catch handler
                 * doesn't double-free what is now owned by return_should. */
                add_next_index_zval(return_should, return_tmp);
                ZVAL_UNDEF(return_tmp);
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

            zval_ptr_dtor(return_should);
            return std::make_shared<ColumnTuple>(columns);
        } catch (...) {
            if (Z_TYPE(return_tmp_storage) != IS_UNDEF) zval_ptr_dtor(return_tmp);
            if (Z_TYPE(return_should_storage) != IS_UNDEF) zval_ptr_dtor(return_should);
            throw;
        }
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
                return appendLowCardinalityColumn<ColumnLowCardinalityT<ColumnNullableT<ColumnString>>, /*nullable=*/true>(
                    values_ht, std::make_shared<ColumnLowCardinalityT<ColumnNullableT<ColumnString>>>());
            }
            return appendLowCardinalityColumn<ColumnLowCardinalityT<ColumnString>, /*nullable=*/false>(
                values_ht, std::make_shared<ColumnLowCardinalityT<ColumnString>>());
        }
        if (inner->GetCode() == Type::Code::FixedString) {
            int width = parseFixedStringWidth(inner);
            if (is_nullable) {
                return appendLowCardinalityColumn<ColumnLowCardinalityT<ColumnNullableT<ColumnFixedString>>, /*nullable=*/true>(
                    values_ht, std::make_shared<ColumnLowCardinalityT<ColumnNullableT<ColumnFixedString>>>(width));
            }
            return appendLowCardinalityColumn<ColumnLowCardinalityT<ColumnFixedString>, /*nullable=*/false>(
                values_ht, std::make_shared<ColumnLowCardinalityT<ColumnFixedString>>(width));
        }
        throw std::runtime_error("LowCardinality only supported over String / FixedString (Nullable allowed)");
    }

    case Type::Code::Map:
    {
        TypeRef k = type->As<MapType>()->GetKeyType();
        TypeRef v = type->As<MapType>()->GetValueType();
        Type::Code kc = k->GetCode();

        // String keys reject integer-keyed PHP entries outright; integer
        // keys parse the string form or fall back to the numeric key.
        // Numeric parsers reject anything that doesn't consume the full
        // string (PHP's strtoll silently returned 0 for "abc" before).
        auto strKey = [](zend_string *zk, zend_ulong) -> std::string {
            if (!zk) {
                throw std::runtime_error("Map(String, *) row entry must have a string key");
            }
            return std::string(ZSTR_VAL(zk), ZSTR_LEN(zk));
        };
        /* PHP zend_string is length-prefixed and may contain embedded
         * NUL bytes. Comparing endp against ZSTR_LEN is the right
         * "fully consumed" check; checking *endp == '\0' would let
         * "123\x00garbage" silently parse as 123 because endp would
         * land on the NUL. */
        auto i64Key = [](zend_string *zk, zend_ulong nk) -> int64_t {
            if (!zk) return (int64_t)nk;
            const char *s = ZSTR_VAL(zk);
            char *endp = NULL;
            errno = 0;
            long long v = strtoll(s, &endp, 10);
            if (errno == ERANGE || endp == s || (size_t)(endp - s) != ZSTR_LEN(zk)) {
                throw std::runtime_error(
                    std::string("Map integer key is not a valid number: ") +
                    std::string(s, ZSTR_LEN(zk)));
            }
            return (int64_t)v;
        };
        auto u64Key = [](zend_string *zk, zend_ulong nk) -> uint64_t {
            if (!zk) return (uint64_t)nk;
            const char *s = ZSTR_VAL(zk);
            char *endp = NULL;
            errno = 0;
            unsigned long long v = strtoull(s, &endp, 10);
            if (errno == ERANGE || endp == s || (size_t)(endp - s) != ZSTR_LEN(zk)) {
                throw std::runtime_error(
                    std::string("Map unsigned key is not a valid number: ") +
                    std::string(s, ZSTR_LEN(zk)));
            }
            return (uint64_t)v;
        };
        auto f64Key = [](zend_string *zk, zend_ulong nk) -> double {
            if (!zk) return (double)nk;
            const char *s = ZSTR_VAL(zk);
            char *endp = NULL;
            errno = 0;
            double v = strtod(s, &endp);
            if (errno == ERANGE || endp == s || (size_t)(endp - s) != ZSTR_LEN(zk)) {
                throw std::runtime_error(
                    std::string("Map float key is not a valid number: ") +
                    std::string(s, ZSTR_LEN(zk)));
            }
            return v;
        };
        auto uuidKey = [](zend_string *zk, zend_ulong) -> UUID {
            if (!zk) {
                throw std::runtime_error("Map(UUID, *) row entry must have a string key");
            }
            std::string s(ZSTR_VAL(zk), ZSTR_LEN(zk));
            s.erase(std::remove(s.begin(), s.end(), '-'), s.end());
            if (s.length() != 32) {
                throw std::runtime_error("UUID key format error");
            }
            return UUID{
                std::stoull(s.substr(0, 16), nullptr, 16),
                std::stoull(s.substr(16, 16), nullptr, 16),
            };
        };

        /* Narrow-key wrappers: same range-check the value side gained for
         * narrow Map columns. Keys arrive as decimal strings; the parsed
         * int64 must still fit the destination column width. */
        auto narrowKeyI = [&](zend_string *zk, zend_ulong nk,
                              int64_t lo, int64_t hi, const char *t) -> int64_t {
            int64_t parsed = i64Key(zk, nk);
            if (parsed < lo || parsed > hi) {
                throw std::runtime_error(std::string("Map key out of range for ") + t);
            }
            return parsed;
        };
        auto narrowKeyU = [&](zend_string *zk, zend_ulong nk,
                              uint64_t hi, const char *t) -> uint64_t {
            uint64_t parsed = u64Key(zk, nk);
            if (parsed > hi) {
                throw std::runtime_error(std::string("Map key out of range for ") + t);
            }
            return parsed;
        };

        switch (kc) {
            case Type::Code::String:
                return appendMapByValueType<ColumnString,  std::string>(values_ht, v, strKey);
            case Type::Code::Int8: {
                auto kf = [&](zend_string *zk, zend_ulong nk) {
                    return narrowKeyI(zk, nk, INT8_MIN, INT8_MAX, "Int8");
                };
                return appendMapByValueType<ColumnInt8,    int64_t>(values_ht, v, kf);
            }
            case Type::Code::Int16: {
                auto kf = [&](zend_string *zk, zend_ulong nk) {
                    return narrowKeyI(zk, nk, INT16_MIN, INT16_MAX, "Int16");
                };
                return appendMapByValueType<ColumnInt16,   int64_t>(values_ht, v, kf);
            }
            case Type::Code::Int32: {
                auto kf = [&](zend_string *zk, zend_ulong nk) {
                    return narrowKeyI(zk, nk, INT32_MIN, INT32_MAX, "Int32");
                };
                return appendMapByValueType<ColumnInt32,   int64_t>(values_ht, v, kf);
            }
            case Type::Code::Int64:
                return appendMapByValueType<ColumnInt64,   int64_t>(values_ht, v, i64Key);
            case Type::Code::UInt8: {
                auto kf = [&](zend_string *zk, zend_ulong nk) {
                    return narrowKeyU(zk, nk, UINT8_MAX, "UInt8");
                };
                return appendMapByValueType<ColumnUInt8,   uint64_t>(values_ht, v, kf);
            }
            case Type::Code::UInt16: {
                auto kf = [&](zend_string *zk, zend_ulong nk) {
                    return narrowKeyU(zk, nk, UINT16_MAX, "UInt16");
                };
                return appendMapByValueType<ColumnUInt16,  uint64_t>(values_ht, v, kf);
            }
            case Type::Code::UInt32: {
                auto kf = [&](zend_string *zk, zend_ulong nk) {
                    return narrowKeyU(zk, nk, UINT32_MAX, "UInt32");
                };
                return appendMapByValueType<ColumnUInt32,  uint64_t>(values_ht, v, kf);
            }
            case Type::Code::UInt64:
                return appendMapByValueType<ColumnUInt64,  uint64_t>(values_ht, v, u64Key);
            case Type::Code::Float32:
                return appendMapByValueType<ColumnFloat32, double>(values_ht, v, f64Key);
            case Type::Code::Float64:
                return appendMapByValueType<ColumnFloat64, double>(values_ht, v, f64Key);
            case Type::Code::UUID:
                return appendMapByValueType<ColumnUUID,    UUID>(values_ht, v, uuidKey);
            case Type::Code::LowCardinality: {
                TypeRef inner = k->As<LowCardinalityType>()->GetNestedType();
                if (inner->GetCode() == Type::Code::String) {
                    return appendMapByValueType<ColumnLowCardinalityT<ColumnString>, std::string>(values_ht, v, strKey);
                }
                throw std::runtime_error("Unsupported Map key type LowCardinality(" + inner->GetName() + ")");
            }
            default:
                throw std::runtime_error("Unsupported Map(K, V) for row write: " + type->GetName());
        }
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
// Date32 reads which all share the same shape modulo the format string.
//
// All three wire types either prohibit negative values (Date is uint16,
// DateTime is uint32) or treat them as valid pre-epoch dates (Date32);
// `t == 0` is 1970-01-01, a valid value. We don't emit NULL for any
// non-NULL server value here.
static void emitEpoch(zval *arr, std::time_t t, const char *fmt,
                      const string& column_name, int8_t is_array, long fetch_mode)
{
    if (fetch_mode & SC_FETCH_DATE_AS_STRINGS) {
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
        add_next_index_long(arr, (zend_long)col);
    } else {
        SC_SINGLE_LONG();
    }
}

// UInt64 specialization. Values above ZEND_LONG_MAX (2^63-1) lose
// unsigned semantics when cast to zend_long — they read back as
// negatives in PHP, and Map(UInt64,*) keys collapse distinct
// unsigned values onto the same PHP-signed key. For values that
// don't fit a signed PHP integer, emit a decimal string instead so
// the user can round-trip safely. Values <= ZEND_LONG_MAX continue
// to come back as PHP int for backward compatibility.
static inline void emitUInt64Cell(zval *arr, uint64_t v,
                                  const string& column_name, int8_t is_array, long fetch_mode)
{
    if (v <= (uint64_t)ZEND_LONG_MAX) {
        if (is_array) {
            add_next_index_long(arr, (zend_long)v);
        } else if (fetch_mode & SC_FETCH_ONE) {
            ZVAL_LONG(arr, (zend_long)v);
        } else {
            sc_add_assoc_long_ex(arr, column_name.c_str(), column_name.length(),
                                 (zend_long)v);
        }
        return;
    }
    char buf[32];
    int len = snprintf(buf, sizeof(buf), "%" PRIu64, v);
    if (is_array) {
        sc_add_next_index_stringl(arr, buf, len, 1);
    } else if (fetch_mode & SC_FETCH_ONE) {
        ZVAL_STRINGL(arr, buf, len);
    } else {
        sc_add_assoc_stringl_ex(arr, column_name.c_str(), column_name.length(),
                                buf, len, 1);
    }
}

template <>
inline void emitIntColumn<ColumnUInt64>(zval *arr, const ColumnRef& columnRef, int row,
                                        const string& column_name, int8_t is_array, long fetch_mode)
{
    auto col_ptr = columnRef->As<ColumnUInt64>();
    if (!col_ptr) {
        throw std::runtime_error("Integer column downcast failed");
    }
    emitUInt64Cell(arr, (uint64_t)(*col_ptr)[row], column_name, is_array, fetch_mode);
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

void convertToZval(zval *arr, const ColumnRef& columnRef, int row, const string& column_name, int8_t is_array, long fetch_mode)
{
    ConvertDepthGuard _depth_guard;  // shared with createColumn / insertColumn
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
        auto col_ip = as_or_throw<ColumnIPv4>(columnRef, "IPv4 read");
        std::string s = col_ip->AsString(row);
        if (is_array)
        {
            sc_add_next_index_stringl(arr, s.data(), s.size(), 1);
        }
        else
        {
            SC_SINGLE_STRING(s.data(), s.size());
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
        auto uuid_col = columnRef->As<ColumnUUID>();
        if (!uuid_col) {
            throw std::runtime_error("UUID read: column type mismatch");
        }
        auto col = (*uuid_col)[row];
        char buf[33];
        snprintf(buf, sizeof(buf), "%016llx%016llx",
                 (unsigned long long)col.first,
                 (unsigned long long)col.second);
        if (is_array)
        {
            sc_add_next_index_stringl(arr, buf, 32, 1);
        }
        else
        {
            SC_SINGLE_STRING(buf, 32);
        }
        break;
    }
    case Type::Code::Float32:
    {
        auto f32_col = columnRef->As<ColumnFloat32>();
        if (!f32_col) {
            throw std::runtime_error("Float32 read: column type mismatch");
        }
        /* Round-trip through %.6g (matching the prior `stringstream<<float`
         * default precision) so 0.55f surfaces as 0.55 to PHP, not
         * 0.5500000119. snprintf+strtod is heap-free vs the old stringstream
         * pair. */
        char buf[32];
        snprintf(buf, sizeof(buf), "%.6g", (double)(*f32_col)[row]);
        double d = strtod(buf, nullptr);
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
        auto f64_col = columnRef->As<ColumnFloat64>();
        if (!f64_col) {
            throw std::runtime_error("Float64 read: column type mismatch");
        }
        double col = (double)(*f64_col)[row];
        if (is_array)
        {
            add_next_index_double(arr, col);
        }
        else
        {
            SC_SINGLE_DOUBLE(col);
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
        // back as "12.34", not the unscaled storage integer 1234. The
        // decimal point and any leading-zero padding are inserted in
        // place in the stack buffer so no std::string allocations fire
        // per cell. Worst case: 1 byte sign + 39 digits + 1 '.' = 41.
        auto dec_type = columnRef->Type()->As<DecimalType>();
        size_t scale = dec_type ? dec_type->GetScale() : 0;
        Int128 raw = col->At(row);
        char buf[64];
        size_t l = format_int128_dec(raw, buf);
        if (scale > 0) {
            bool neg = (buf[0] == '-');
            size_t sign_off = neg ? 1 : 0;
            size_t dlen = l - sign_off;
            // If dlen <= scale we need to pad: insert (scale+1 - dlen)
            // zeros after the sign, so "5" with scale 3 → "0.005"
            // (sign_off + 5 chars: 0 . 0 0 5).
            if (dlen <= scale) {
                size_t pad = scale + 1 - dlen;
                memmove(buf + sign_off + pad, buf + sign_off, dlen);
                memset(buf + sign_off, '0', pad);
                l += pad;
                dlen += pad;
            }
            // Insert '.' before the last `scale` digits.
            size_t dot_pos = sign_off + dlen - scale;
            memmove(buf + dot_pos + 1, buf + dot_pos, scale);
            buf[dot_pos] = '.';
            ++l;
        }
        if (is_array) {
            sc_add_next_index_stringl(arr, buf, l, 1);
        } else {
            SC_SINGLE_STRING(buf, l);
        }
        break;
    }
    case Type::Code::String:
    {
        auto s_col = columnRef->As<ColumnString>();
        if (!s_col) {
            throw std::runtime_error("String read: column type mismatch");
        }
        auto col = (*s_col)[row];
        if (is_array)
        {
            sc_add_next_index_stringl(arr, col.data(), col.length(), 1);
        }
        else
        {
            SC_SINGLE_STRING(col.data(), col.length());
        }
        break;
    }
    case Type::Code::FixedString:
    {
        // ColumnFixedString::At returns a string_view over the full fixed-size
        // buffer, including trailing NULs added by ClickHouse to pad short
        // values up to the column's declared width. Trim trailing NULs so the
        // PHP-side value matches the original input.
        auto fs_col = columnRef->As<ColumnFixedString>();
        if (!fs_col) {
            throw std::runtime_error("FixedString read: column type mismatch");
        }
        auto col = (*fs_col)[row];
        size_t len = col.length();
        while (len > 0 && col.data()[len - 1] == '\0') {
            --len;
        }
        if (is_array)
        {
            sc_add_next_index_stringl(arr, col.data(), len, 1);
        }
        else
        {
            SC_SINGLE_STRING(col.data(), len);
        }
        break;
    }
    case Type::Code::IPv6:
    {
        /* clickhouse-cpp v2.6.1 made ColumnIPv6 a sibling of ColumnFixedString
         * (composition, not inheritance), so As<ColumnFixedString>() returns
         * null and crashed every IPv6 read. Use AsString() to get the
         * canonical "::1" form. */
        auto col_ip = as_or_throw<ColumnIPv6>(columnRef, "IPv6 read");
        std::string s = col_ip->AsString(row);
        if (is_array)
        {
            sc_add_next_index_stringl(arr, s.data(), s.size(), 1);
        }
        else
        {
            SC_SINGLE_STRING(s.data(), s.size());
        }
        break;
    }

    case Type::Code::DateTime:
    {
        auto col = columnRef->As<ColumnDateTime>();
        if (!col) {
            throw std::runtime_error("DateTime read: column type mismatch");
        }
        emitEpoch(arr, (std::time_t)col->At(row), "%Y-%m-%d %H:%M:%S",
                  column_name, is_array, fetch_mode);
        break;
    }
    case Type::Code::DateTime64:
    {
        auto col = columnRef->As<ColumnDateTime64>();
        if (!col) {
            throw std::runtime_error("DateTime64 read: column type mismatch");
        }
        size_t precision = columnRef->Type()->As<DateTime64Type>()->GetPrecision();
        if (precision > 9) {
            throw std::runtime_error("DateTime64 precision out of spec range (0..9)");
        }
        int64_t scale = 1;
        for (size_t i = 0; i < precision; ++i) scale *= 10;
        int64_t raw = col->At(row);
        std::time_t whole = (std::time_t)(raw / scale);
        int64_t frac = raw % scale;
        if (frac < 0) { frac = -frac; }

        if (fetch_mode & SC_FETCH_DATE_AS_STRINGS) {
            char buffer[64];
            size_t l = strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", gmtime(&whole));
            if (precision > 0 && l < sizeof(buffer)) {
                int written = snprintf(buffer + l, sizeof(buffer) - l, ".%0*lld",
                                       (int)precision, (long long)frac);
                if (written > 0 && (size_t)written < sizeof(buffer) - l) {
                    l += (size_t)written;
                }
            }
            if (is_array) {
                sc_add_next_index_stringl(arr, buffer, l, 1);
            } else {
                SC_SINGLE_STRING(buffer, l);
            }
        } else {
            if (is_array) {
                add_next_index_long(arr, (zend_long)raw);
            } else if (fetch_mode & SC_FETCH_ONE) {
                ZVAL_LONG(arr, (zend_long)raw);
            } else {
                sc_add_assoc_long_ex(arr, column_name.c_str(), column_name.length(), (zend_long)raw);
            }
        }
        break;
    }
    case Type::Code::Date:
    {
        auto col = columnRef->As<ColumnDate>();
        if (!col) {
            throw std::runtime_error("Date read: column type mismatch");
        }
        emitEpoch(arr, (std::time_t)col->At(row), "%Y-%m-%d",
                  column_name, is_array, fetch_mode);
        break;
    }
    case Type::Code::Date32:
    {
        auto col = columnRef->As<ColumnDate32>();
        if (!col) {
            throw std::runtime_error("Date32 read: column type mismatch");
        }
        emitEpoch(arr, (std::time_t)col->At(row), "%Y-%m-%d",
                  column_name, is_array, fetch_mode);
        break;
    }
    case Type::Code::Time:
    {
        auto col = columnRef->As<ColumnTime>();
        if (!col) {
            throw std::runtime_error("Time read: column type mismatch");
        }
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
                add_next_index_long(arr, (zend_long)v);
            } else if (fetch_mode & SC_FETCH_ONE) {
                ZVAL_LONG(arr, (zend_long)v);
            } else {
                sc_add_assoc_long_ex(arr, column_name.c_str(), column_name.length(), (zend_long)v);
            }
        }
        break;
    }
    case Type::Code::Time64:
    {
        auto col = columnRef->As<ColumnTime64>();
        if (!col) {
            throw std::runtime_error("Time64 read: column type mismatch");
        }
        size_t precision = columnRef->Type()->As<Time64Type>()->GetPrecision();
        if (precision > 9) {
            throw std::runtime_error("Time64 precision out of spec range (0..9)");
        }
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
            if (l < 0 || (size_t)l >= sizeof(buffer)) l = (int)sizeof(buffer) - 1;
            if (precision > 0 && l > 0 && (size_t)l < sizeof(buffer)) {
                int w = snprintf(buffer + l, sizeof(buffer) - l, ".%0*lld",
                                 (int)precision, (long long)frac);
                if (w > 0 && (size_t)w < sizeof(buffer) - (size_t)l) l += w;
            }
            if (is_array) {
                sc_add_next_index_stringl(arr, buffer, l, 1);
            } else {
                SC_SINGLE_STRING(buffer, l);
            }
        } else {
            if (is_array) {
                add_next_index_long(arr, (zend_long)raw);
            } else if (fetch_mode & SC_FETCH_ONE) {
                ZVAL_LONG(arr, (zend_long)raw);
            } else {
                sc_add_assoc_long_ex(arr, column_name.c_str(), column_name.length(), (zend_long)raw);
            }
        }
        break;
    }
    case Type::Code::Int128:
    {
        auto col = columnRef->As<ColumnInt128>();
        if (!col) {
            throw std::runtime_error("Int128 read: column type mismatch");
        }
        char buf[41];
        size_t l = format_int128_dec(col->At(row), buf);
        if (is_array) {
            sc_add_next_index_stringl(arr, buf, l, 1);
        } else {
            SC_SINGLE_STRING(buf, l);
        }
        break;
    }
    case Type::Code::UInt128:
    {
        auto col = columnRef->As<ColumnUInt128>();
        if (!col) {
            throw std::runtime_error("UInt128 read: column type mismatch");
        }
        char buf[40];
        size_t l = format_uint128_dec(col->At(row), buf);
        if (is_array) {
            sc_add_next_index_stringl(arr, buf, l, 1);
        } else {
            SC_SINGLE_STRING(buf, l);
        }
        break;
    }
    case Type::Code::Array:
    {
        auto array = columnRef->As<ColumnArray>();
        if (!array) {
            throw std::runtime_error("Array read: column type mismatch");
        }
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
        if (!array) {
            throw std::runtime_error("Enum8 read: column type mismatch");
        }
        std::string_view name = array->NameAt(row);
        if (is_array)
        {
            sc_add_next_index_stringl(arr, name.data(), name.length(), 1);
        }
        else
        {
            SC_SINGLE_STRING(name.data(), name.length());
        }
        break;
    }
    case Type::Code::Enum16:
    {
        auto array = columnRef->As<ColumnEnum16>();
        if (!array) {
            throw std::runtime_error("Enum16 read: column type mismatch");
        }
        std::string_view name = array->NameAt(row);
        if (is_array)
        {
            sc_add_next_index_stringl(arr, name.data(), name.length(), 1);
        }
        else
        {
            SC_SINGLE_STRING(name.data(), name.length());
        }
        break;
    }

    case Type::Code::Nullable:
    {
        auto nullable = columnRef->As<ColumnNullable>();
        if (!nullable) {
            throw std::runtime_error("Nullable read: column type mismatch");
        }
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
        if (!tuple) {
            throw std::runtime_error("Tuple read: column type mismatch");
        }
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
            sc_add_next_index_stringl(arr, sv.data(), sv.length(), 1);
        } else {
            SC_SINGLE_STRING(sv.data(), sv.length());
        }
        break;
    }

    case Type::Code::Map:
    {
        TypeRef map_type = columnRef->Type();
        auto map_type_ref = map_type->As<MapType>();
        if (!map_type_ref) {
            throw std::runtime_error("Map read: type metadata is not MapType");
        }
        TypeRef key_type_ref = map_type_ref->GetKeyType();
        TypeRef value_type_ref = map_type_ref->GetValueType();
        Type::Code key_code = key_type_ref->GetCode();
        Type::Code value_code = value_type_ref->GetCode();
        auto map_col = columnRef->As<ColumnMap>();
        if (!map_col) {
            throw std::runtime_error("Map read: column type mismatch");
        }
        ColumnRef tuple_col = map_col->GetAsColumn(row);
        auto tup = tuple_col->As<ColumnTuple>();
        if (!tup) {
            throw std::runtime_error("Map read: inner tuple type mismatch");
        }
        ColumnRef keys_any = (*tup)[0];
        ColumnRef values_any = (*tup)[1];
        size_t entry_count = keys_any->Size();
        /* Defensive: a malformed/malicious server response could report
         * key-count > value-count; subsequent At(i) on values would read
         * past the value column. */
        if (values_any->Size() < entry_count) {
            throw std::runtime_error("Map column key/value size mismatch");
        }

        zval *map_zv;
        SC_MAKE_STD_ZVAL(map_zv);
        array_init(map_zv);

        /* Pre-cast the key and value columns once per row instead of per
         * entry. The keys_any / values_any column slices don't change
         * across the entry loop; only the row index inside them does.
         * Doing as_or_throw inside decodeKey would re-run a
         * dynamic_pointer_cast for every map entry. For Map(Int64, V)
         * with 100 entries × 1M rows that was 100M unnecessary casts.
         * Only one of these typed pointers is populated for any given
         * cell; the others stay null. */
        std::shared_ptr<ColumnString>  k_str_col;
        std::shared_ptr<ColumnInt64>   k_i64_col;
        std::shared_ptr<ColumnUInt64>  k_u64_col;
        std::shared_ptr<ColumnInt32>   k_i32_col;
        std::shared_ptr<ColumnUInt32>  k_u32_col;
        std::shared_ptr<ColumnInt16>   k_i16_col;
        std::shared_ptr<ColumnUInt16>  k_u16_col;
        std::shared_ptr<ColumnInt8>    k_i8_col;
        std::shared_ptr<ColumnUInt8>   k_u8_col;
        std::shared_ptr<ColumnFloat32> k_f32_col;
        std::shared_ptr<ColumnFloat64> k_f64_col;
        std::shared_ptr<ColumnUUID>    k_uuid_col;
        switch (key_code) {
            case Type::Code::String:  k_str_col  = as_or_throw<ColumnString>(keys_any, "Map key String"); break;
            case Type::Code::Int64:   k_i64_col  = as_or_throw<ColumnInt64>(keys_any, "Map key Int64"); break;
            case Type::Code::UInt64:  k_u64_col  = as_or_throw<ColumnUInt64>(keys_any, "Map key UInt64"); break;
            case Type::Code::Int32:   k_i32_col  = as_or_throw<ColumnInt32>(keys_any, "Map key Int32"); break;
            case Type::Code::UInt32:  k_u32_col  = as_or_throw<ColumnUInt32>(keys_any, "Map key UInt32"); break;
            case Type::Code::Int16:   k_i16_col  = as_or_throw<ColumnInt16>(keys_any, "Map key Int16"); break;
            case Type::Code::UInt16:  k_u16_col  = as_or_throw<ColumnUInt16>(keys_any, "Map key UInt16"); break;
            case Type::Code::Int8:    k_i8_col   = as_or_throw<ColumnInt8>(keys_any, "Map key Int8"); break;
            case Type::Code::UInt8:   k_u8_col   = as_or_throw<ColumnUInt8>(keys_any, "Map key UInt8"); break;
            case Type::Code::Float32: k_f32_col  = as_or_throw<ColumnFloat32>(keys_any, "Map key Float32"); break;
            case Type::Code::Float64: k_f64_col  = as_or_throw<ColumnFloat64>(keys_any, "Map key Float64"); break;
            case Type::Code::UUID:    k_uuid_col = as_or_throw<ColumnUUID>(keys_any, "Map key UUID"); break;
            default:
                throw std::runtime_error("Map read: unsupported key type " + key_type_ref->GetName());
        }

        // Same hoist for value column: one cast per Map cell, not per
        // entry. Only the pointer matching value_code is populated.
        std::shared_ptr<ColumnString>  v_str_col;
        std::shared_ptr<ColumnInt64>   v_i64_col;
        std::shared_ptr<ColumnUInt64>  v_u64_col;
        std::shared_ptr<ColumnInt32>   v_i32_col;
        std::shared_ptr<ColumnUInt32>  v_u32_col;
        std::shared_ptr<ColumnInt16>   v_i16_col;
        std::shared_ptr<ColumnUInt16>  v_u16_col;
        std::shared_ptr<ColumnInt8>    v_i8_col;
        std::shared_ptr<ColumnUInt8>   v_u8_col;
        std::shared_ptr<ColumnFloat32> v_f32_col;
        std::shared_ptr<ColumnFloat64> v_f64_col;
        std::shared_ptr<ColumnUUID>    v_uuid_col;
        switch (value_code) {
            case Type::Code::String:  v_str_col  = as_or_throw<ColumnString>(values_any, "Map value String"); break;
            case Type::Code::Int64:   v_i64_col  = as_or_throw<ColumnInt64>(values_any, "Map value Int64"); break;
            case Type::Code::UInt64:  v_u64_col  = as_or_throw<ColumnUInt64>(values_any, "Map value UInt64"); break;
            case Type::Code::Int32:   v_i32_col  = as_or_throw<ColumnInt32>(values_any, "Map value Int32"); break;
            case Type::Code::UInt32:  v_u32_col  = as_or_throw<ColumnUInt32>(values_any, "Map value UInt32"); break;
            case Type::Code::Int16:   v_i16_col  = as_or_throw<ColumnInt16>(values_any, "Map value Int16"); break;
            case Type::Code::UInt16:  v_u16_col  = as_or_throw<ColumnUInt16>(values_any, "Map value UInt16"); break;
            case Type::Code::Int8:    v_i8_col   = as_or_throw<ColumnInt8>(values_any, "Map value Int8"); break;
            case Type::Code::UInt8:   v_u8_col   = as_or_throw<ColumnUInt8>(values_any, "Map value UInt8"); break;
            case Type::Code::Float32: v_f32_col  = as_or_throw<ColumnFloat32>(values_any, "Map value Float32"); break;
            case Type::Code::Float64: v_f64_col  = as_or_throw<ColumnFloat64>(values_any, "Map value Float64"); break;
            case Type::Code::UUID:    v_uuid_col = as_or_throw<ColumnUUID>(values_any, "Map value UUID"); break;
            default:
                throw std::runtime_error("Map read: unsupported value type " + value_type_ref->GetName());
        }

        // Decode a key column at row i into one of three forms: string,
        // long integer, or double. PHP arrays only key by string or
        // long; doubles get formatted to a canonical string key.
        auto decodeKey = [&](size_t i, std::string &str_buf, zend_long &long_out, double &dbl_out) -> int {
            // Returns 0 = string, 1 = long, 2 = double-as-string.
            switch (key_code) {
                case Type::Code::String: {
                    std::string_view kv = (*k_str_col)[i];
                    str_buf.assign(kv.data(), kv.length());
                    return 0;
                }
                case Type::Code::Int64:   long_out = (zend_long)k_i64_col->At(i);  return 1;
                case Type::Code::UInt64: {
                    /* UInt64 values above ZEND_LONG_MAX (2^63-1) lose
                     * unsigned semantics if cast to zend_long, and PHP
                     * array keys can't be unsigned, so distinct large
                     * UInt64 keys would otherwise collapse to the same
                     * negative signed-key. Promote to string when the
                     * value doesn't fit a signed PHP integer. */
                    uint64_t uk = (uint64_t)k_u64_col->At(i);
                    if (uk > (uint64_t)ZEND_LONG_MAX) {
                        char buf[32];
                        int len = snprintf(buf, sizeof(buf), "%" PRIu64, uk);
                        str_buf.assign(buf, len);
                        return 0;
                    }
                    long_out = (zend_long)uk;
                    return 1;
                }
                case Type::Code::Int32:   long_out = (zend_long)k_i32_col->At(i);  return 1;
                case Type::Code::UInt32:  long_out = (zend_long)k_u32_col->At(i);  return 1;
                case Type::Code::Int16:   long_out = (zend_long)k_i16_col->At(i);  return 1;
                case Type::Code::UInt16:  long_out = (zend_long)k_u16_col->At(i);  return 1;
                case Type::Code::Int8:    long_out = (zend_long)k_i8_col->At(i);   return 1;
                case Type::Code::UInt8:   long_out = (zend_long)k_u8_col->At(i);   return 1;
                case Type::Code::Float32: dbl_out  = (double)k_f32_col->At(i);     return 2;
                case Type::Code::Float64: dbl_out  = (double)k_f64_col->At(i);     return 2;
                case Type::Code::UUID: {
                    UUID u = k_uuid_col->At(i);
                    char buf[64];
                    snprintf(buf, sizeof(buf), "%08x-%04x-%04x-%04x-%012llx",
                             (uint32_t)(u.first >> 32),
                             (uint16_t)((u.first >> 16) & 0xffff),
                             (uint16_t)(u.first & 0xffff),
                             (uint16_t)(u.second >> 48),
                             (unsigned long long)(u.second & 0xffffffffffffull));
                    str_buf.assign(buf);
                    return 0;
                }
                default:
                    throw std::runtime_error("Map read: unsupported key type " + key_type_ref->GetName());
            }
        };

        /* Format a Float key as a locale-independent decimal string. The
         * naive snprintf("%.17g") honors LC_NUMERIC, so the same Float64
         * map key would surface under a different PHP array key under
         * setlocale(LC_NUMERIC, 'de_DE'). php_gcvt with explicit '.' is
         * the same fix CR-303 applied at the SQL parameter boundary. */
        auto fmtFloatKey = [](double dk, char *buf, size_t bufsz) -> int {
            php_gcvt(dk, 17, '.', 'e', buf);
            (void)bufsz;
            return (int)strlen(buf);
        };

        // Helper: add (string|long-as-string) keyed value into map_zv.
        // Handles all three key categories returned by decodeKey.
        auto addStrL = [&](int kkind, const std::string &sb, zend_long lk, double dk,
                           const char *vptr, size_t vlen) {
            if (kkind == 0) {
                sc_add_assoc_stringl_ex(map_zv, sb.c_str(), sb.length(), vptr, vlen, 1);
            } else if (kkind == 1) {
                add_index_stringl(map_zv, lk, vptr, vlen);
            } else {
                char kbuf[64];
                int klen = fmtFloatKey(dk, kbuf, sizeof(kbuf));
                sc_add_assoc_stringl_ex(map_zv, kbuf, klen, vptr, vlen, 1);
            }
        };
        auto addLong = [&](int kkind, const std::string &sb, zend_long lk, double dk, zend_long lv) {
            if (kkind == 0) {
                add_assoc_long_ex(map_zv, sb.c_str(), sb.length(), lv);
            } else if (kkind == 1) {
                add_index_long(map_zv, lk, lv);
            } else {
                char kbuf[64];
                int klen = fmtFloatKey(dk, kbuf, sizeof(kbuf));
                add_assoc_long_ex(map_zv, kbuf, klen, lv);
            }
        };
        auto addDbl = [&](int kkind, const std::string &sb, zend_long lk, double dk, double dv) {
            if (kkind == 0) {
                add_assoc_double_ex(map_zv, sb.c_str(), sb.length(), dv);
            } else if (kkind == 1) {
                add_index_double(map_zv, lk, dv);
            } else {
                char kbuf[64];
                int klen = fmtFloatKey(dk, kbuf, sizeof(kbuf));
                add_assoc_double_ex(map_zv, kbuf, klen, dv);
            }
        };

        for (size_t i = 0; i < entry_count; ++i) {
            std::string str_key_buf;
            zend_long long_key = 0;
            double dbl_key = 0.0;
            int kkind = decodeKey(i, str_key_buf, long_key, dbl_key);

            // Decode value, dispatch by value type.
            if (value_code == Type::Code::String) {
                std::string_view vv = (*v_str_col)[i];
                addStrL(kkind, str_key_buf, long_key, dbl_key, vv.data(), vv.length());
            } else if (value_code == Type::Code::UInt64) {
                /* UInt64 values above ZEND_LONG_MAX surface as
                 * negatives if cast to zend_long; emit as a string
                 * value so callers can round-trip safely. Same fix
                 * as the scalar UInt64 read path. */
                uint64_t uv = (uint64_t)v_u64_col->At(i);
                if (uv > (uint64_t)ZEND_LONG_MAX) {
                    char buf[32];
                    int len = snprintf(buf, sizeof(buf), "%" PRIu64, uv);
                    addStrL(kkind, str_key_buf, long_key, dbl_key, buf, len);
                } else {
                    addLong(kkind, str_key_buf, long_key, dbl_key, (zend_long)uv);
                }
            } else if (value_code == Type::Code::Int64
                    || value_code == Type::Code::Int32 || value_code == Type::Code::UInt32
                    || value_code == Type::Code::Int16 || value_code == Type::Code::UInt16
                    || value_code == Type::Code::Int8  || value_code == Type::Code::UInt8) {
                zend_long lv = 0;
                switch (value_code) {
                    case Type::Code::Int64:  lv = (zend_long)v_i64_col->At(i); break;
                    case Type::Code::Int32:  lv = (zend_long)v_i32_col->At(i); break;
                    case Type::Code::UInt32: lv = (zend_long)v_u32_col->At(i); break;
                    case Type::Code::Int16:  lv = (zend_long)v_i16_col->At(i); break;
                    case Type::Code::UInt16: lv = (zend_long)v_u16_col->At(i); break;
                    case Type::Code::Int8:   lv = (zend_long)v_i8_col->At(i);  break;
                    case Type::Code::UInt8:  lv = (zend_long)v_u8_col->At(i);  break;
                    default: break;
                }
                addLong(kkind, str_key_buf, long_key, dbl_key, lv);
            } else if (value_code == Type::Code::Float64 || value_code == Type::Code::Float32) {
                double dv = (value_code == Type::Code::Float64)
                    ? (double)v_f64_col->At(i)
                    : (double)v_f32_col->At(i);
                addDbl(kkind, str_key_buf, long_key, dbl_key, dv);
            } else if (value_code == Type::Code::UUID) {
                UUID u = v_uuid_col->At(i);
                char buf[64];
                int blen = snprintf(buf, sizeof(buf), "%08x-%04x-%04x-%04x-%012llx",
                         (uint32_t)(u.first >> 32),
                         (uint16_t)((u.first >> 16) & 0xffff),
                         (uint16_t)(u.first & 0xffff),
                         (uint16_t)(u.second >> 48),
                         (unsigned long long)(u.second & 0xffffffffffffull));
                addStrL(kkind, str_key_buf, long_key, dbl_key, buf, blen);
            } else {
                throw std::runtime_error("Map read: unsupported value type " + value_type_ref->GetName());
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
