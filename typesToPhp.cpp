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
#include "ext/json/php_json.h"
#include "zend_smart_str.h"
#include "zend_exceptions.h"
#include "php7_wrapper.h"
#include "main/snprintf.h"
};

#include "php_clickhouse.h"

#include "lib/clickhouse-cpp/clickhouse/client.h"
#include "lib/clickhouse-cpp/clickhouse/error_codes.h"
#include "lib/clickhouse-cpp/clickhouse/types/type_parser.h"
#include "lib/clickhouse-cpp/clickhouse/columns/factory.h"
#include "lib/clickhouse-cpp/clickhouse/columns/bool.h"
#include "lib/clickhouse-cpp/clickhouse/columns/geo.h"
#include "lib/clickhouse-cpp/clickhouse/columns/ip4.h"
#include "lib/clickhouse-cpp/clickhouse/columns/ip6.h"
#include "lib/clickhouse-cpp/clickhouse/columns/lowcardinality.h"
#include "lib/clickhouse-cpp/clickhouse/columns/map.h"
#include "lib/clickhouse-cpp/clickhouse/base/socket.h"
#include <algorithm>
#include <cmath>
#include <cerrno>
#include <cinttypes>
#include <cstring>
#include <limits>
#include <system_error>
#include <type_traits>

#include "typesToPhp.hpp"

using namespace clickhouse;
using namespace std;

/* out needs 41 bytes: sign, 39 digits, and NUL margin. Returns bytes written. */
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

static absl::uint128 parse_uint128_dec(const char *s, size_t len, const char *out_label)
{
    if (len == 0 || len > 39) {
        throw std::runtime_error(std::string(out_label) + " string is empty or too long");
    }
    /* Detect overflow before multiplying: wrapped results can still exceed v. */
    const absl::uint128 umax = ~absl::uint128(0);
    const absl::uint128 umax_div10 = umax / 10;
    const unsigned umax_mod10 = (unsigned)(umax % 10);
    absl::uint128 v = 0;
    for (size_t i = 0; i < len; ++i) {
        if (s[i] < '0' || s[i] > '9') {
            throw std::runtime_error(std::string(out_label) + " string contains non-digit characters");
        }
        unsigned d = (unsigned)(s[i] - '0');
        if (v > umax_div10 || (v == umax_div10 && d > umax_mod10)) {
            throw std::runtime_error(std::string(out_label) + " string overflows the 128-bit range");
        }
        v = v * 10 + absl::uint128(d);
    }
    return v;
}

/* Server-supplied type codes may disagree with the concrete column class. */
template <typename TCol>
static inline std::shared_ptr<TCol> as_or_throw(const ColumnRef &c, const char *what)
{
    auto p = c->As<TCol>();
    if (!p) {
        throw std::runtime_error(std::string(what) + ": column type mismatch");
    }
    return p;
}

/* Avoid per-cell RTTI/refcount work only for stable, one-to-one Type::Code
 * mappings: numeric ColumnVector<T> and ColumnString. Representation-changing
 * types (IP, FixedString, geo, Enum, nested wrappers) need as_or_throw. */
template <typename TCol>
static inline const TCol *fast_scalar_col(const ColumnRef &c)
{
    return static_cast<const TCol *>(c.get());
}

/* Validate concrete metadata types as well as column types. */
template <typename TType>
static inline auto type_as_or_throw(const TypeRef &t, const char *what)
{
    /* Type::As returns a raw pointer; Column::As returns a shared_ptr. */
    auto p = t->As<TType>();
    if (!p) {
        throw std::runtime_error(std::string(what) + ": type metadata mismatch");
    }
    return p;
}

/* Reject nonnumeric input instead of PHP coercion to zero. Callers enforce
 * destination widths. AllowNullGuard permits masked NULL placeholders only
 * while building a Nullable child column. */
static thread_local int g_allow_null_in_strict = 0;
struct AllowNullGuard {
    AllowNullGuard()  { ++g_allow_null_in_strict; }
    ~AllowNullGuard() { --g_allow_null_in_strict; }
    AllowNullGuard(const AllowNullGuard&) = delete;
    AllowNullGuard& operator=(const AllowNullGuard&) = delete;
};

/* Userland coercion may reenter another client. Top-level conversion guards
 * isolate NULL strictness and depth, restoring outer state on exit. */
static thread_local int convert_depth = 0;
static const int MAX_CONVERT_DEPTH = 32;
/* Bound JSON decode amplification; oversized cells remain readable as raw strings. */
static const size_t MAX_JSON_CELL_BYTES = 16u * 1024u * 1024u;

InsertConversionScopeGuard::InsertConversionScopeGuard()
    : saved_null(g_allow_null_in_strict), saved_depth(convert_depth) {
    g_allow_null_in_strict = 0;
    convert_depth = 0;
}
InsertConversionScopeGuard::~InsertConversionScopeGuard() {
    g_allow_null_in_strict = saved_null;
    convert_depth = saved_depth;
}

ConvertDepthScopeGuard::ConvertDepthScopeGuard() : saved_depth(convert_depth) {
    convert_depth = 0;
}
ConvertDepthScopeGuard::~ConvertDepthScopeGuard() {
    convert_depth = saved_depth;
}
static int64_t strict_zval_i64(zval *z, const char *type_label)
{
    ZVAL_DEREF(z);
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
            if (d < -9223372036854775808.0 || d >= 9223372036854775808.0) {
                throw std::runtime_error(
                    std::string("double out of range for integer column ") + type_label);
            }
            return (int64_t)d;
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
            return (int64_t)v;
        }
        default:
            throw std::runtime_error(
                std::string("array / object / resource cannot be assigned to integer column ") + type_label);
    }
}

static uint64_t strict_u64_string(const char *s, size_t slen,
                                 const char *type_label)
{
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
    auto invalid = [&]() {
        throw std::runtime_error(
            std::string("invalid integer string for ") + type_label);
    };
    if (plen == 0) invalid();
    for (size_t i = 0; i < plen; ++i) {
        const unsigned char c = static_cast<unsigned char>(p[i]);
        const bool decimal_digit = c >= '0' && c <= '9';
        const bool hex_digit = decimal_digit ||
            (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
        if (!(base == 10 ? decimal_digit : hex_digit)) invalid();
    }
    char *endp = NULL;
    errno = 0;
    unsigned long long v = strtoull(p, &endp, base);
    if (errno == ERANGE || endp == p ||
        (size_t)(endp - p) != plen) {
        invalid();
    }
    return (uint64_t)v;
}

/* UInt64 values above INT64_MAX arrive as decimal strings; use unsigned parsing. */
static uint64_t strict_zval_u64(zval *z, const char *type_label)
{
    ZVAL_DEREF(z);
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
            /* 2^64 is exactly representable as double, unlike UINT64_MAX. */
            if (d < 0.0 || d >= 18446744073709551616.0) {
                throw std::runtime_error(
                    std::string("double out of range for integer column ") + type_label);
            }
            return (uint64_t)d;
        }
        case IS_STRING:
            return strict_u64_string(Z_STRVAL_P(z), Z_STRLEN_P(z), type_label);
        default:
            throw std::runtime_error(
                std::string("array / object / resource cannot be assigned to integer column ") + type_label);
    }
}

static double strict_zval_double(zval *z, const char *type_label)
{
    ZVAL_DEREF(z);
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

/* Out-of-range double-to-int64 conversion is UB. The exclusive upper
 * bound is 2^63; the negated range check also rejects NaN/Inf. */
static int64_t checked_double_to_int64(double v, const char *type_label)
{
    if (!(v >= -9223372036854775808.0 && v < 9223372036854775808.0)) {
        throw std::runtime_error(
            std::string(type_label) + " value out of representable 64-bit range");
    }
    return (int64_t)v;
}

static std::string strict_zval_string(zval *z, const char *type_label)
{
    ZVAL_DEREF(z);
    if (Z_TYPE_P(z) == IS_NULL) {
        if (g_allow_null_in_strict > 0) return std::string();
        throw std::runtime_error(
            std::string("null cannot be assigned to non-Nullable column ") + type_label);
    }
    if (Z_TYPE_P(z) == IS_ARRAY) {
        throw std::runtime_error(
            std::string("array cannot be assigned to string column ") + type_label +
            " (scalar or Stringable object required)");
    }
    if (Z_TYPE_P(z) == IS_RESOURCE) {
        throw std::runtime_error(
            std::string("resource cannot be assigned to string column ") + type_label);
    }
    ZStrGuard sg(z);  // throws if a __toString() left EG(exception) pending
    return std::string(sg.val(), sg.len());
}

/* Return bytes written, excluding NUL; dashed and dashless forms encode identical bytes. */
static int format_uuid(UUID u, bool dashed, char *buf, size_t bufsz)
{
    if (dashed) {
        return snprintf(buf, bufsz, "%08x-%04x-%04x-%04x-%012llx",
                        (uint32_t)(u.first >> 32),
                        (uint16_t)((u.first >> 16) & 0xffff),
                        (uint16_t)(u.first & 0xffff),
                        (uint16_t)(u.second >> 48),
                        (unsigned long long)(u.second & 0xffffffffffffull));
    }
    return snprintf(buf, bufsz, "%016llx%016llx",
                    (unsigned long long)u.first,
                    (unsigned long long)u.second);
}

static int uuid_hex_value(char c)
{
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

static UUID parseUUIDString(const char *s, size_t len, const char *error_msg)
{
    bool dashed;
    if (len == 32) {
        dashed = false;
    } else if (len == 36 && s[8] == '-' && s[13] == '-' &&
               s[18] == '-' && s[23] == '-') {
        dashed = true;
    } else {
        throw std::runtime_error(error_msg);
    }

    uint64_t high = 0;
    uint64_t low = 0;
    size_t digits = 0;
    for (size_t i = 0; i < len; ++i) {
        if (dashed && (i == 8 || i == 13 || i == 18 || i == 23)) {
            continue; // validated as '-' above
        }
        int nibble = uuid_hex_value(s[i]);
        if (nibble < 0) {
            throw std::runtime_error(error_msg);
        }
        if (digits < 16) {
            high = (high << 4) | (uint64_t)nibble;
        } else {
            low = (low << 4) | (uint64_t)nibble;
        }
        ++digits;
    }
    /* digits is exactly 32 by construction for both accepted lengths. */
    return UUID{high, low};
}

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

/* Parse zero-padded civil timestamps directly to UTC epoch seconds.
 * timegm conflates failure with valid epoch -1; Windows rejects pre-epoch input. */
static int64_t to_time_t(const char *s, size_t len, bool is_date = true)
{
    const char *kind = is_date ? "Date" : "DateTime";
    auto fail = [&](const char *detail) -> int64_t {
        throw std::runtime_error(
            std::string("Invalid ") + kind + " string" + detail +
            ": " + std::string(s, len));
    };
    auto is_digit = [](char c) { return c >= '0' && c <= '9'; };
    auto date_shape_at = [&](const char *p) {
        return is_digit(p[0]) && is_digit(p[1]) && is_digit(p[2]) && is_digit(p[3]) &&
               p[4] == '-' && is_digit(p[5]) && is_digit(p[6]) && p[7] == '-' &&
               is_digit(p[8]) && is_digit(p[9]);
    };
    auto time_shape_at = [&](const char *p) {
        return p[0] == ' ' && is_digit(p[1]) && is_digit(p[2]) && p[3] == ':' &&
               is_digit(p[4]) && is_digit(p[5]) && p[6] == ':' &&
               is_digit(p[7]) && is_digit(p[8]);
    };
    size_t want = is_date ? 10 : 19;
    /* len == want short-circuits first, so the shape probes never read
     * past the buffer; in the len > want branch the prefix is in bounds. */
    bool exact = (len == want) && date_shape_at(s) &&
        (is_date || time_shape_at(s + 10));
    if (!exact) {
        if (len > want && date_shape_at(s) && (is_date || time_shape_at(s + 10))) {
            return fail(" (trailing characters)");
        }
        return fail("");
    }
    int64_t year = (int64_t)((s[0] - '0') * 1000 + (s[1] - '0') * 100 +
                             (s[2] - '0') * 10 + (s[3] - '0'));
    unsigned month = (unsigned)((s[5] - '0') * 10 + (s[6] - '0'));
    unsigned day = (unsigned)((s[8] - '0') * 10 + (s[9] - '0'));
    unsigned hour = 0, minute = 0, second = 0;
    if (!is_date) {
        hour = (unsigned)((s[11] - '0') * 10 + (s[12] - '0'));
        minute = (unsigned)((s[14] - '0') * 10 + (s[15] - '0'));
        second = (unsigned)((s[17] - '0') * 10 + (s[18] - '0'));
    }
    static const unsigned month_days[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    bool leap = (year % 4 == 0) && (year % 100 != 0 || year % 400 == 0);
    unsigned max_day = month >= 1 && month <= 12
        ? month_days[month - 1] + (month == 2 && leap ? 1U : 0U)
        : 0;
    if (day < 1 || day > max_day || hour > 23 || minute > 59 || second > 59) {
        return fail(" (invalid civil time)");
    }

    int64_t adjusted_year = year - (month <= 2 ? 1 : 0);
    int64_t era = (adjusted_year >= 0 ? adjusted_year : adjusted_year - 399) / 400;
    unsigned year_of_era = (unsigned)(adjusted_year - era * 400);
    unsigned shifted_month = month > 2 ? month - 3 : month + 9;
    unsigned day_of_year = (153 * shifted_month + 2) / 5 + day - 1;
    unsigned day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    int64_t days = era * 146097 + (int64_t)day_of_era - 719468;
    int64_t seconds = days * 86400 + (int64_t)hour * 3600 + (int64_t)minute * 60 + second;
    /* Keep int64 epochs: 32-bit time_t would reject valid Date32/DateTime values.
     * Callers enforce column storage limits. */
    return seconds;
}

/* Return whole seconds and fractional ticks scaled by 10^precision. */
static std::pair<int64_t, int64_t> to_time_t_with_frac(const char *s, size_t len, size_t precision)
{
    const char *dot = (const char *)memchr(s, '.', len);
    size_t whole_len = dot ? (size_t)(dot - s) : len;
    int64_t whole = to_time_t(s, whole_len, false);
    int64_t frac = 0;
    if (dot) {
        std::string input(s, len);
        if (precision == 0) {
            throw std::runtime_error(
                "Invalid DateTime64(0) string (fractional suffix on a "
                "zero-precision column): " + input);
        }
        const char *p = dot + 1;
        const char *end = s + len;
        if (p >= end) {
            throw std::runtime_error(
                "Invalid DateTime64 string (bare dot without fractional digits): " + input);
        }
        size_t consumed = 0;
        while (p < end && consumed < precision && *p >= '0' && *p <= '9') {
            frac = frac * 10 + (*p - '0');
            ++p;
            ++consumed;
        }
        if (consumed == 0) {
            throw std::runtime_error(
                "Invalid DateTime64 string (non-digit after dot): " + input);
        }
        // Pad missing digits up to precision so "12:34:56.5" with precision 3
        // contributes 500 (ms), not 5.
        for (size_t pad = consumed; pad < precision; ++pad) frac *= 10;
        if (p < end) {
            throw std::runtime_error(
                "Invalid DateTime64 string (trailing characters after fraction): " + input);
        }
    }
    return {whole, frac};
}


/* Bound recursion through server-supplied nested types on both read and insert paths. */
struct ConvertDepthGuard {
    ConvertDepthGuard() {
        if (++convert_depth > MAX_CONVERT_DEPTH) {
            --convert_depth;
            throw std::runtime_error("ClickHouse column nested-type depth exceeds limit");
        }
    }
    ~ConvertDepthGuard() { --convert_depth; }
};


/* Callers bound precision to 0..9, so every scale fits int64. */
static int64_t pow10_i64(size_t precision)
{
    static constexpr int64_t kPow10[10] = {
        1, 10, 100, 1000, 10000, 100000,
        1000000, 10000000, 100000000, 1000000000,
    };
    return kPow10[precision];
}

/* A column shares one Type object; cache parameters to avoid per-cell casts. */
struct ColParamMemo {
    const void *type_ptr = nullptr;
    size_t precision = 0;
    size_t scale = 0;
};
static thread_local ColParamMemo g_dt64_memo;
static thread_local ColParamMemo g_t64_memo;
static thread_local ColParamMemo g_dec_memo;

static inline size_t cachedDateTime64Precision(const TypeRef &t)
{
    if (g_dt64_memo.type_ptr != (const void *)t.get()) {
        size_t p = type_as_or_throw<DateTime64Type>(t, "DateTime64")->GetPrecision();
        if (p > 9) {
            throw std::runtime_error("DateTime64 precision out of spec range (0..9)");
        }
        g_dt64_memo.type_ptr = (const void *)t.get();
        g_dt64_memo.precision = p;
    }
    return g_dt64_memo.precision;
}

static inline size_t cachedTime64Precision(const TypeRef &t)
{
    if (g_t64_memo.type_ptr != (const void *)t.get()) {
        size_t p = type_as_or_throw<Time64Type>(t, "Time64")->GetPrecision();
        if (p > 9) {
            throw std::runtime_error("Time64 precision out of spec range (0..9)");
        }
        g_t64_memo.type_ptr = (const void *)t.get();
        g_t64_memo.precision = p;
    }
    return g_t64_memo.precision;
}

static inline size_t cachedDecimalScale(const TypeRef &t)
{
    if (g_dec_memo.type_ptr != (const void *)t.get()) {
        /* Preserve scale 0 for non-Decimal metadata under a Decimal code. */
        auto dt = t->As<DecimalType>();
        size_t s = dt ? dt->GetScale() : 0;
        if (s > 38) {
            throw std::runtime_error("Decimal scale out of supported range (max 38)");
        }
        g_dec_memo.type_ptr = (const void *)t.get();
        g_dec_memo.scale = s;
    }
    return g_dec_memo.scale;
}

/* LowCardinality creation matrix: only String / FixedString payloads
 * (each Nullable-wrapped or bare) have a vendored column class; anything
 * else throws here rather than falling through to the generic factory. */
static ColumnRef makeLowCardinalityColumn(TypeRef type)
{
    TypeRef nested = type_as_or_throw<LowCardinalityType>(type, "LowCardinality")->GetNestedType();
    bool is_nullable = (nested->GetCode() == Type::Code::Nullable);
    TypeRef inner = is_nullable
        ? type_as_or_throw<NullableType>(nested, "Nullable")->GetNestedType()
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

static ColumnRef makeMapColumn(TypeRef type)
{
    TypeRef k = type_as_or_throw<MapType>(type, "Map")->GetKeyType();
    TypeRef v = type_as_or_throw<MapType>(type, "Map")->GetValueType();
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

ColumnRef createColumn(TypeRef type)
{
    ConvertDepthGuard depth_guard;
    switch (type->GetCode())
    {
    case Type::Code::UInt64:
        return std::make_shared<ColumnUInt64>();
    case Type::Code::UInt8:
        return std::make_shared<ColumnUInt8>();
    case Type::Code::UInt16:
        return std::make_shared<ColumnUInt16>();
    case Type::Code::UInt32:
        return std::make_shared<ColumnUInt32>();

    case Type::Code::Int8:
        return std::make_shared<ColumnInt8>();
    case Type::Code::Int16:
        return std::make_shared<ColumnInt16>();
    case Type::Code::Int32:
        return std::make_shared<ColumnInt32>();
    case Type::Code::Int64:
        return std::make_shared<ColumnInt64>();

    case Type::Code::UUID:
        return std::make_shared<ColumnUUID>();

    case Type::Code::Float32:
        return std::make_shared<ColumnFloat32>();
    case Type::Code::Float64:
        return std::make_shared<ColumnFloat64>();

    case Type::Code::String:
        return std::make_shared<ColumnString>();
    case Type::Code::FixedString:
        return std::make_shared<ColumnFixedString>(parseFixedStringWidth(type));

    case Type::Code::DateTime:
        return std::make_shared<ColumnDateTime>();
    case Type::Code::DateTime64:
        return std::make_shared<ColumnDateTime64>(type_as_or_throw<DateTime64Type>(type, "DateTime64")->GetPrecision());
    case Type::Code::Date:
        return std::make_shared<ColumnDate>();
    case Type::Code::Date32:
        return std::make_shared<ColumnDate32>();
    case Type::Code::Time:
        return std::make_shared<ColumnTime>();
    case Type::Code::Time64:
        return std::make_shared<ColumnTime64>(type_as_or_throw<Time64Type>(type, "Time64")->GetPrecision());
    case Type::Code::Int128:
        return std::make_shared<ColumnInt128>();
    case Type::Code::UInt128:
        return std::make_shared<ColumnUInt128>();
    case Type::Code::Decimal:
    case Type::Code::Decimal32:
    case Type::Code::Decimal64:
    case Type::Code::Decimal128:
    {
        auto dt = type_as_or_throw<DecimalType>(type, "Decimal");
        return std::make_shared<ColumnDecimal>(dt->GetPrecision(), dt->GetScale());
    }

    case Type::Code::JSON:
        return std::make_shared<ColumnJSON>();

    case Type::Code::Bool:
        return std::make_shared<ColumnBool>();
    case Type::Code::IPv4:
        return std::make_shared<ColumnIPv4>();
    case Type::Code::IPv6:
        return std::make_shared<ColumnIPv6>();

    case Type::Code::Array:
        return std::make_shared<ColumnArray>(createColumn(type_as_or_throw<ArrayType>(type, "Array")->GetItemType()));

    case Type::Code::Enum8:
        return std::make_shared<ColumnEnum8>(type);
    case Type::Code::Enum16:
        return std::make_shared<ColumnEnum16>(type);

    case Type::Code::Nullable:
        return std::make_shared<ColumnNullable>(createColumn(type_as_or_throw<NullableType>(type, "Nullable")->GetNestedType()), std::make_shared<ColumnUInt8>());

    case Type::Code::LowCardinality:
        return makeLowCardinalityColumn(type);

    case Type::Code::Map:
        return makeMapColumn(type);

    case Type::Code::Tuple:
    {
        auto tupleType = type_as_or_throw<TupleType>(type, "Tuple")->GetTupleType();
        std::vector<ColumnRef> columns;
        columns.reserve(tupleType.size());
        for (const auto &field : tupleType) {
            columns.push_back(createColumn(field));
        }
        return std::make_shared<ColumnTuple>(columns);
    }

    case Type::Code::Void:
    {
        throw std::runtime_error("can't support Void");
    }

    default:
        return CreateColumnByType(type->GetName());
    }
}

static bool canReuseArrayChild(const TypeRef& type)
{
    switch (type->GetCode()) {
    case Type::Code::Nullable:
        return canReuseArrayChild(
            type_as_or_throw<NullableType>(type, "Nullable")->GetNestedType());
    case Type::Code::Tuple:
    case Type::Code::Map:
    case Type::Code::Point:
    case Type::Code::Ring:
    case Type::Code::Polygon:
    case Type::Code::MultiPolygon:
        return false;
    default:
        return true;
    }
}

template <typename TCol>
static inline void appendIntCell(TCol *value, zval *cell,
                                 int64_t MinV, int64_t MaxV, const char *type_label)
{
    int64_t n = strict_zval_i64(cell, type_label);
    if (n < MinV || n > MaxV) {
        throw std::runtime_error(std::string("value out of range for ") + type_label);
    }
    value->Append((typename TCol::ValueType)n);
}

template <typename TCol>
static inline void appendFloatCell(TCol *value, zval *cell, const char *type_label)
{
    double n = strict_zval_double(cell, type_label);
    double max = (double)std::numeric_limits<typename TCol::ValueType>::max();
    if (n < -max || n > max) {
        throw std::runtime_error(std::string("value out of range for ") + type_label);
    }
    value->Append((typename TCol::ValueType)n);
}
static inline void appendStringCell(ColumnString *value, zval *cell, const char *type_label)
{
    ZVAL_DEREF(cell);
    if (Z_TYPE_P(cell) == IS_STRING) {
        value->Append(std::string_view(Z_STRVAL_P(cell), Z_STRLEN_P(cell)));
        return;
    }
    value->Append(strict_zval_string(cell, type_label));
}

static inline void appendFixedStringCell(ColumnFixedString *value, zval *cell,
                                         size_t width, const char *type_label)
{
    ZVAL_DEREF(cell);
    if (Z_TYPE_P(cell) == IS_STRING) {
        size_t slen = Z_STRLEN_P(cell);
        if (slen > width) {
            throw std::runtime_error(
                "FixedString value exceeds the declared column width");
        }
        value->Append(std::string_view(Z_STRVAL_P(cell), slen));
        return;
    }
    std::string s = strict_zval_string(cell, type_label);
    if (s.size() > width) {
        throw std::runtime_error(
            "FixedString value exceeds the declared column width");
    }
    value->Append(s);
}

/* AppendRaw avoids narrowing through 32-bit time_t. Convert seconds to
 * column units first. Neither string parsing nor numeric coercion invokes PHP. */
template <typename TCol>
static inline void appendDateCell(TCol *value, zval *cell, bool is_date,
                                  const char *type_label,
                                  int64_t min_epoch, int64_t max_epoch)
{
    ZVAL_DEREF(cell);
    int64_t t;
    if (Z_TYPE_P(cell) == IS_STRING) {
        t = to_time_t(Z_STRVAL_P(cell), Z_STRLEN_P(cell), is_date);
    } else {
        t = strict_zval_i64(cell, type_label);
    }
    if (t < min_epoch || t > max_epoch) {
        throw std::runtime_error(
            std::string(type_label) + " value is outside the representable "
            "range for this column type");
    }
    if constexpr (std::is_same_v<TCol, ColumnDate>) {
        /* uint16 days; min_epoch is 0 so t >= 0 and truncation is exact. */
        value->AppendRaw((uint16_t)(t / 86400));
    } else if constexpr (std::is_same_v<TCol, ColumnDate32>) {
        /* int32 days; floor-divide so pre-epoch seconds land on the right day. */
        int64_t days = t >= 0 ? t / 86400 : -((-t + 86399) / 86400);
        value->AppendRaw((int32_t)days);
    } else {
        static_assert(std::is_same_v<TCol, ColumnDateTime>,
            "appendDateCell supports Date, Date32 and DateTime only");
        value->AppendRaw((uint32_t)t);
    }
}

static inline void appendTimeCell(ColumnTime *value, zval *cell)
{
    ZVAL_DEREF(cell);
    /* No time-string parser is available; reject strings rather than coerce them to zero. */
    if (Z_TYPE_P(cell) == IS_STRING) {
        throw std::runtime_error(
            "Time column inserts require numeric seconds; "
            "string formatted-time input is not currently supported");
    }
    int64_t t = strict_zval_i64(cell, "Time");
    if (t < INT32_MIN || t > INT32_MAX) {
        throw std::runtime_error(
            "Time column value out of representable int32 range");
    }
    value->Append((int32_t)t);
}

static inline void appendDateTime64Cell(ColumnDateTime64 *value, zval *cell,
                                        size_t precision, int64_t scale)
{
    ZVAL_DEREF(cell);
    if (Z_TYPE_P(cell) == IS_STRING) {
        auto [whole, frac] = to_time_t_with_frac(
            Z_STRVAL_P(cell), Z_STRLEN_P(cell), precision);
        /* frac is in [0, scale); reserve one tick of multiply headroom for the add. */
        int64_t w = whole;
        if (w > (INT64_MAX - frac) / scale || w < INT64_MIN / scale) {
            throw std::runtime_error(
                "DateTime64 value out of representable range for this precision");
        }
        value->Append(w * scale + frac);
    } else if (Z_TYPE_P(cell) == IS_DOUBLE) {
        /* Floats lose epoch precision beyond microseconds; require formatted
         * strings for precision >= 7. */
        if (precision >= 7) {
            throw std::runtime_error(
                "DateTime64 precision >= 7 cannot be set from a float without "
                "precision loss; pass a formatted date string for sub-microsecond precision");
        }
        double d = strict_zval_double(cell, "DateTime64");
        value->Append(checked_double_to_int64(d * scale, "DateTime64"));
    } else {
        int64_t secs = strict_zval_i64(cell, "DateTime64");
        if (secs > INT64_MAX / scale || secs < INT64_MIN / scale) {
            throw std::runtime_error(
                "DateTime64 seconds value out of representable range for this precision");
        }
        value->Append(secs * scale);
    }
}

static inline void appendTime64Cell(ColumnTime64 *value, zval *cell,
                                    size_t precision, int64_t scale)
{
    ZVAL_DEREF(cell);
    if (Z_TYPE_P(cell) == IS_STRING) {
        throw std::runtime_error(
            "Time64 column inserts require numeric seconds; "
            "string formatted-time input is not currently supported");
    }
    if (Z_TYPE_P(cell) == IS_DOUBLE) {
        if (precision >= 7) {
            throw std::runtime_error(
                "Time64 precision >= 7 cannot be set from a float without "
                "precision loss; pass an integer number of seconds");
        }
        double d = strict_zval_double(cell, "Time64");
        value->Append(checked_double_to_int64(d * scale, "Time64"));
    } else {
        int64_t secs = strict_zval_i64(cell, "Time64");
        if (secs > INT64_MAX / scale || secs < INT64_MIN / scale) {
            throw std::runtime_error(
                "Time64 seconds value out of representable range for this precision");
        }
        value->Append(secs * scale);
    }
}

template <typename TCol>
static ColumnRef appendIntColumn(HashTable *values_ht,
                                 int64_t MinV, int64_t MaxV,
                                 const char *type_label)
{
    auto value = std::make_shared<TCol>();
    zval *array_value;
    ZEND_HASH_FOREACH_VAL(values_ht, array_value) {
        appendIntCell(value.get(), array_value, MinV, MaxV, type_label);
    } ZEND_HASH_FOREACH_END();
    return value;
}

template <typename TCol, typename TStrtoul>
static inline void appendUIntHexCell(TCol *value, zval *array_value,
                                     TStrtoul strtoul_fn, uint64_t MaxV,
                                     const char *type_label)
{
    ZVAL_DEREF(array_value);
    if (Z_TYPE_P(array_value) == IS_STRING && Z_STRLEN_P(array_value) >= 3 &&
        *Z_STRVAL_P(array_value) == '0' &&
        (*(Z_STRVAL_P(array_value) + 1) == 'x' || *(Z_STRVAL_P(array_value) + 1) == 'X')) {
        const char *s = Z_STRVAL_P(array_value);
        size_t slen = Z_STRLEN_P(array_value);
        char *endp = NULL;
        errno = 0;
        auto n = strtoul_fn(s, &endp, 0);
        /* Check the length, not just NUL termination: PHP strings may contain embedded NULs. */
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
        int64_t n = strict_zval_i64(array_value, type_label);
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
}

template <typename TCol, typename TStrtoul>
static ColumnRef appendUIntColumnWithHex(HashTable *values_ht,
                                         TStrtoul strtoul_fn,
                                         uint64_t MaxV,
                                         const char *type_label)
{
    auto value = std::make_shared<TCol>();
    zval *array_value;
    ZEND_HASH_FOREACH_VAL(values_ht, array_value) {
        appendUIntHexCell(value.get(), array_value, strtoul_fn, MaxV, type_label);
    } ZEND_HASH_FOREACH_END();
    return value;
}

// Decimal strings above ZEND_LONG_MAX must round-trip from UInt64 reads.
static inline void appendUInt64HexCell(ColumnUInt64 *value, zval *array_value,
                                      const char *type_label)
{
    ZVAL_DEREF(array_value);
    if (Z_TYPE_P(array_value) == IS_STRING && Z_STRLEN_P(array_value) >= 3 &&
        *Z_STRVAL_P(array_value) == '0' &&
        (*(Z_STRVAL_P(array_value) + 1) == 'x' || *(Z_STRVAL_P(array_value) + 1) == 'X')) {
        const char *s = Z_STRVAL_P(array_value);
        size_t slen = Z_STRLEN_P(array_value);
        char *endp = NULL;
        errno = 0;
        unsigned long long n = strtoull(s, &endp, 0);
        if (errno == ERANGE || endp == s ||
            (size_t)(endp - s) != slen) {
            throw std::runtime_error(
                std::string("invalid hex literal for ") + type_label);
        }
        value->Append((ColumnUInt64::ValueType)n);
    } else {
        value->Append(strict_zval_u64(array_value, type_label));
    }
}

static ColumnRef appendUInt64Column(HashTable *values_ht,
                                   const char *type_label)
{
    auto value = std::make_shared<ColumnUInt64>();
    zval *array_value;
    ZEND_HASH_FOREACH_VAL(values_ht, array_value) {
        appendUInt64HexCell(value.get(), array_value, type_label);
    } ZEND_HASH_FOREACH_END();
    return value;
}

// Undeclared enum integers break name lookup on read; validate before Append.
template <typename TCol, typename TInt>
static ColumnRef appendEnumColumn(TypeRef type, HashTable *values_ht)
{
    auto value = std::make_shared<TCol>(type);
    auto enum_type = type->As<clickhouse::EnumType>();
    /* Masked NULLs still need a declared enum value; HasEnumValue checks the fallback. */
    TInt placeholder = 0;
    if (enum_type) {
        auto it = enum_type->BeginValueToName();
        if (it != enum_type->EndValueToName()) {
            placeholder = (TInt)it->first;
        }
    }
    zval *array_value;
    ZEND_HASH_FOREACH_VAL(values_ht, array_value) {
        ZVAL_DEREF(array_value);
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
            ZStrGuard sg(array_value);
            value->Append(std::string(sg.val(), sg.len()));
        }
    } ZEND_HASH_FOREACH_END();
    return value;
}

template <typename TCol>
static ColumnRef appendDateColumn(HashTable *values_ht, bool is_date,
                                  const char *type_label,
                                  int64_t min_epoch, int64_t max_epoch)
{
    auto value = std::make_shared<TCol>();
    zval *array_value;
    ZEND_HASH_FOREACH_VAL(values_ht, array_value) {
        appendDateCell(value.get(), array_value, is_date, type_label,
                       min_epoch, max_epoch);
    } ZEND_HASH_FOREACH_END();
    return value;
}

template <typename TCol, bool nullable>
static ColumnRef appendLowCardinalityColumn(HashTable *values_ht, std::shared_ptr<TCol> value, const char *type_label)
{
    zval *array_value;
    ZEND_HASH_FOREACH_VAL(values_ht, array_value) {
        ZVAL_DEREF(array_value);
        if constexpr (nullable) {
            if (Z_TYPE_P(array_value) == IS_NULL) {
                value->Append(std::nullopt);
                continue;
            }
        }
        std::string s = strict_zval_string(array_value, type_label);
        value->Append(std::string_view(s.data(), s.size()));
    } ZEND_HASH_FOREACH_END();
    return value;
}

template <typename TCol>
static ColumnRef appendFloatColumn(HashTable *values_ht, const char *type_label)
{
    auto value = std::make_shared<TCol>();
    zval *array_value;
    ZEND_HASH_FOREACH_VAL(values_ht, array_value) {
        appendFloatCell(value.get(), array_value, type_label);
    } ZEND_HASH_FOREACH_END();
    return value;
}

template <typename K, typename V, typename KCol, typename VCol,
          typename KFn, typename VFn>
static ColumnRef appendMapColumn(HashTable *values_ht, KFn extract_key, VFn extract_val)
{
    auto col = std::make_shared<ColumnMapT<KCol, VCol>>(
        std::make_shared<KCol>(), std::make_shared<VCol>());
    std::vector<std::pair<K, V>> entries;
    zval *array_value;
    ZEND_HASH_FOREACH_VAL(values_ht, array_value) {
        ZVAL_DEREF(array_value);
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

enum class MapInputShape {
    Assoc,
    Pairs,
    Mixed,
};

static bool isPackedList(HashTable *ht)
{
    zend_ulong expected = 0;
    zend_ulong index;
    zend_string *key;
    ZEND_HASH_FOREACH_KEY(ht, index, key) {
        if (key || index != expected++) {
            return false;
        }
    } ZEND_HASH_FOREACH_END();
    return true;
}

static bool isMapPairList(HashTable *ht)
{
    if (zend_hash_num_elements(ht) == 0 || !isPackedList(ht)) {
        return false;
    }
    zval *entry;
    ZEND_HASH_FOREACH_VAL(ht, entry) {
        ZVAL_DEREF(entry);
        if (Z_TYPE_P(entry) != IS_ARRAY ||
            zend_hash_num_elements(Z_ARRVAL_P(entry)) != 2 ||
            !zend_hash_index_exists(Z_ARRVAL_P(entry), 0) ||
            !zend_hash_index_exists(Z_ARRVAL_P(entry), 1)) {
            return false;
        }
    } ZEND_HASH_FOREACH_END();
    return true;
}

static MapInputShape classifyMapInput(HashTable *values_ht)
{
    bool saw_assoc = false;
    bool saw_pairs = false;
    zval *row;
    ZEND_HASH_FOREACH_VAL(values_ht, row) {
        ZVAL_DEREF(row);
        if (Z_TYPE_P(row) != IS_ARRAY) {
            throw std::runtime_error("Map row must be a PHP array");
        }
        HashTable *row_ht = Z_ARRVAL_P(row);
        if (zend_hash_num_elements(row_ht) == 0) {
            continue;
        }
        if (isMapPairList(row_ht)) {
            saw_pairs = true;
        } else {
            saw_assoc = true;
        }
    } ZEND_HASH_FOREACH_END();
    if (saw_assoc && saw_pairs) {
        return MapInputShape::Mixed;
    }
    return saw_pairs ? MapInputShape::Pairs : MapInputShape::Assoc;
}

static ColumnRef appendMapPairsColumn(HashTable *values_ht,
                                      const TypeRef &key_type,
                                      const TypeRef &value_type)
{
    auto tuple_data = std::make_shared<ColumnTuple>(
        std::vector<ColumnRef>{createColumn(key_type), createColumn(value_type)});
    auto rows = std::make_shared<ColumnArray>(tuple_data);

    zval *row;
    ZEND_HASH_FOREACH_VAL(values_ht, row) {
        ZVAL_DEREF(row);
        HashTable *row_ht = Z_ARRVAL_P(row);
        zval keys;
        zval values;
        array_init_size(&keys, zend_hash_num_elements(row_ht));
        array_init_size(&values, zend_hash_num_elements(row_ht));
        try {
            zval *pair;
            ZEND_HASH_FOREACH_VAL(row_ht, pair) {
                ZVAL_DEREF(pair);
                zval *key = zend_hash_index_find(Z_ARRVAL_P(pair), 0);
                zval *value = zend_hash_index_find(Z_ARRVAL_P(pair), 1);
                zval key_copy;
                zval value_copy;
                ZVAL_COPY_DEREF(&key_copy, key);
                ZVAL_COPY_DEREF(&value_copy, value);
                add_next_index_zval(&keys, &key_copy);
                add_next_index_zval(&values, &value_copy);
            } ZEND_HASH_FOREACH_END();

            auto row_tuple = std::make_shared<ColumnTuple>(
                std::vector<ColumnRef>{
                    insertColumn(key_type, &keys),
                    insertColumn(value_type, &values),
                });
            rows->AppendAsColumn(row_tuple);
            zval_ptr_dtor(&keys);
            zval_ptr_dtor(&values);
        } catch (...) {
            zval_ptr_dtor(&keys);
            zval_ptr_dtor(&values);
            throw;
        }
    } ZEND_HASH_FOREACH_END();

    return std::make_shared<ColumnMap>(rows);
}

static UUID phpToUUID(zval *zv)
{
    ZVAL_DEREF(zv);
    if (Z_TYPE_P(zv) == IS_NULL) {
        if (g_allow_null_in_strict <= 0) {
            throw std::runtime_error("null cannot be assigned to non-Nullable column UUID");
        }
        return UUID{0, 0};
    }
    ZStrGuard sg(zv);
    return parseUUIDString(sg.val(), sg.len(), "UUID format error");
}

template <typename KCol, typename K, typename KFn>
static ColumnRef appendMapByValueType(HashTable *values_ht, TypeRef vtype, KFn key_fn)
{
    auto strVal = [](zval *mv) -> std::string {
        return strict_zval_string(mv, "Map value String");
    };
    auto i64Val = [](zval *mv) -> int64_t {
        return strict_zval_i64(mv, "Map value Int64");
    };
    auto u64Val = [](zval *mv) -> uint64_t {
        return strict_zval_u64(mv, "Map value UInt64");
    };
    auto narrowI = [](zval *mv, int64_t lo, int64_t hi, const char *t) -> int64_t {
        int64_t n = strict_zval_i64(mv, t);
        if (n < lo || n > hi) {
            throw std::runtime_error(std::string("Map value out of range for ") + t);
        }
        return (int64_t)n;
    };
    auto narrowU = [](zval *mv, uint64_t hi, const char *t) -> uint64_t {
        uint64_t n = strict_zval_u64(mv, t);
        if (n > hi) {
            throw std::runtime_error(std::string("Map value out of range for ") + t);
        }
        return (uint64_t)n;
    };
    auto f64Val = [](zval *mv) -> double {
        return strict_zval_double(mv, "Map value Float");
    };
    auto f32Val = [](zval *mv) -> double {
        double n = strict_zval_double(mv, "Map value Float32");
        double max = (double)std::numeric_limits<float>::max();
        if (n < -max || n > max) {
            throw std::runtime_error("Map value out of range for Float32");
        }
        return n;
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
            return appendMapColumn<K, double,     KCol, ColumnFloat32>(values_ht, key_fn, f32Val);
        case Type::Code::Float64:
            return appendMapColumn<K, double,     KCol, ColumnFloat64>(values_ht, key_fn, f64Val);
        case Type::Code::UUID:
            return appendMapColumn<K, UUID,       KCol, ColumnUUID>(values_ht, key_fn, phpToUUID);
        case Type::Code::LowCardinality: {
            TypeRef inner = type_as_or_throw<LowCardinalityType>(vtype, "LowCardinality")->GetNestedType();
            if (inner->GetCode() == Type::Code::String) {
                return appendMapColumn<K, std::string, KCol, ColumnLowCardinalityT<ColumnString>>(values_ht, key_fn, strVal);
            }
            throw std::runtime_error("Unsupported Map value type LowCardinality(" + inner->GetName() + ")");
        }
        default:
            throw std::runtime_error("Unsupported Map value type: " + vtype->GetName());
    }
}

static std::tuple<double, double> phpToPoint(zval *zv)
{
    ZVAL_DEREF(zv);
    if (Z_TYPE_P(zv) != IS_ARRAY) {
        throw std::runtime_error("Point must be a PHP array of 2 numbers");
    }
    HashTable *ht = Z_ARRVAL_P(zv);
    if (zend_hash_num_elements(ht) != 2) {
        throw std::runtime_error("Point must have exactly 2 elements");
    }
    zval *x = zend_hash_index_find(ht, 0);
    zval *y = zend_hash_index_find(ht, 1);
    if (!x || !y) {
        throw std::runtime_error("Point is missing an element");
    }
    return std::make_tuple(
        strict_zval_double(x, "Point coordinate"),
        strict_zval_double(y, "Point coordinate"));
}

static std::vector<std::tuple<double, double>> phpToRing(zval *zv)
{
    ZVAL_DEREF(zv);
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
    ZVAL_DEREF(zv);
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

/* Recheck row type: a referenced row can change during userland coercion. */
zval *extractRowCell(zval *row_pz, size_t col_index,
                     const std::vector<zend_string*> *col_names)
{
    ZVAL_DEREF(row_pz);
    if (Z_TYPE_P(row_pz) != IS_ARRAY) {
        throw std::runtime_error(
            "The insert function needs to pass in a two-dimensional array");
    }
    zval *cell = zend_hash_index_find(Z_ARRVAL_P(row_pz), col_index);
    if (!cell && col_names) {
        zend_string *cn = (*col_names)[col_index];
        cell = zend_hash_str_find(Z_ARRVAL_P(row_pz), ZSTR_VAL(cn), ZSTR_LEN(cn));
    }
    if (!cell) {
        throw std::runtime_error(
            "The number of parameters inserted per line is inconsistent");
    }
    ZVAL_DEREF(cell);
    return cell;
}

/* Fuse only appenders that cannot invoke PHP; iterating live rows is then safe.
 * Types using __toString/jsonSerialize need the snapshotting transpose path.
 * Return nullptr to request that fallback. */
ColumnRef tryBuildScalarColumnFromRows(HashTable *rows_ht, size_t col_index,
                                       const std::vector<zend_string*> *col_names,
                                       TypeRef type)
{
    auto build = [&](auto column, auto per_cell) -> ColumnRef {
        zval *row_pz;
        ZEND_HASH_FOREACH_VAL(rows_ht, row_pz) {
            per_cell(column.get(), extractRowCell(row_pz, col_index, col_names));
        } ZEND_HASH_FOREACH_END();
        return column;
    };

    switch (type->GetCode()) {
    case Type::Code::Int8:   return build(std::make_shared<ColumnInt8>(),   [](ColumnInt8 *c, zval *v)  { appendIntCell(c, v, INT8_MIN,  INT8_MAX,  "Int8"); });
    case Type::Code::Int16:  return build(std::make_shared<ColumnInt16>(),  [](ColumnInt16 *c, zval *v) { appendIntCell(c, v, INT16_MIN, INT16_MAX, "Int16"); });
    case Type::Code::Int32:  return build(std::make_shared<ColumnInt32>(),  [](ColumnInt32 *c, zval *v) { appendIntCell(c, v, INT32_MIN, INT32_MAX, "Int32"); });
    case Type::Code::Int64:  return build(std::make_shared<ColumnInt64>(),  [](ColumnInt64 *c, zval *v) { appendIntCell(c, v, INT64_MIN, INT64_MAX, "Int64"); });
    case Type::Code::UInt8:  return build(std::make_shared<ColumnUInt8>(),  [](ColumnUInt8 *c, zval *v) { appendIntCell(c, v, 0, 0xFF,   "UInt8"); });
    case Type::Code::UInt16: return build(std::make_shared<ColumnUInt16>(), [](ColumnUInt16 *c, zval *v){ appendIntCell(c, v, 0, 0xFFFF, "UInt16"); });
    case Type::Code::UInt32: return build(std::make_shared<ColumnUInt32>(), [](ColumnUInt32 *c, zval *v){ appendUIntHexCell(c, v, strtoul, UINT32_MAX, "UInt32"); });
    case Type::Code::UInt64: return build(std::make_shared<ColumnUInt64>(), [](ColumnUInt64 *c, zval *v){ c->Append(strict_zval_u64(v, "UInt64")); });
    case Type::Code::Float32:return build(std::make_shared<ColumnFloat32>(),[](ColumnFloat32 *c, zval *v){ appendFloatCell(c, v, "Float32"); });
    case Type::Code::Float64:return build(std::make_shared<ColumnFloat64>(),[](ColumnFloat64 *c, zval *v){ appendFloatCell(c, v, "Float64"); });
    case Type::Code::Date:    return build(std::make_shared<ColumnDate>(),   [](ColumnDate *c, zval *v)    { appendDateCell(c, v, true, "Date", 0, 65535LL * 86400 + 86399); });
    case Type::Code::Date32:  return build(std::make_shared<ColumnDate32>(), [](ColumnDate32 *c, zval *v)  { appendDateCell(c, v, true, "Date32", -25567LL * 86400, 120529LL * 86400 + 86399); });
    case Type::Code::DateTime:return build(std::make_shared<ColumnDateTime>(),[](ColumnDateTime *c, zval *v){ appendDateCell(c, v, false, "DateTime", 0, 4294967295LL); });
    case Type::Code::Time:    return build(std::make_shared<ColumnTime>(),   [](ColumnTime *c, zval *v)    { appendTimeCell(c, v); });
    case Type::Code::DateTime64: {
        size_t precision = type_as_or_throw<DateTime64Type>(type, "DateTime64")->GetPrecision();
        if (precision > 9) {
            throw std::runtime_error("DateTime64 precision out of spec range (0..9)");
        }
        int64_t scale = pow10_i64(precision);
        auto column = std::make_shared<ColumnDateTime64>(precision);
        return build(column, [precision, scale](ColumnDateTime64 *c, zval *v){ appendDateTime64Cell(c, v, precision, scale); });
    }
    case Type::Code::Time64: {
        size_t precision = type_as_or_throw<Time64Type>(type, "Time64")->GetPrecision();
        if (precision > 9) {
            throw std::runtime_error("Time64 precision out of spec range (0..9)");
        }
        int64_t scale = pow10_i64(precision);
        auto column = std::make_shared<ColumnTime64>(precision);
        return build(column, [precision, scale](ColumnTime64 *c, zval *v){ appendTime64Cell(c, v, precision, scale); });
    }
    default:
        return nullptr;
    }
}

/* ColumnDecimal::Append does not enforce precision/scale; validate plain
 * decimals here and leave unusual forms to the native parser. */
static void validateDecimalText(const std::string &s, size_t precision,
                                size_t scale, const char *label)
{
    size_t i = 0, n = s.size();
    if (i < n && (s[i] == '+' || s[i] == '-')) ++i;
    size_t int_start = i;
    while (i < n && s[i] >= '0' && s[i] <= '9') ++i;
    size_t int_digits = i - int_start;
    size_t frac_digits = 0;
    if (i < n && s[i] == '.') {
        ++i;
        size_t f0 = i;
        while (i < n && s[i] >= '0' && s[i] <= '9') ++i;
        frac_digits = i - f0;
    }
    if (i != n || (int_digits == 0 && frac_digits == 0)) return;

    size_t sig_int = int_digits;
    for (size_t k = int_start; k < int_start + int_digits && s[k] == '0'; ++k) {
        --sig_int;
    }
    if (frac_digits > scale) {
        throw std::runtime_error(
            std::string(label) + " value has more fractional digits than the "
            "column scale allows");
    }
    if (precision >= scale && sig_int > precision - scale) {
        throw std::runtime_error(
            std::string(label) + " value exceeds the range of its Decimal "
            "precision/scale");
    }
}

static ColumnRef insertNumericColumn(Type::Code code, HashTable *values_ht)
{
    switch (code) {
    case Type::Code::UInt64:
        return appendUInt64Column(values_ht, "UInt64");
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
    case Type::Code::Float32:
        return appendFloatColumn<ColumnFloat32>(values_ht, "Float32");
    case Type::Code::Float64:
        return appendFloatColumn<ColumnFloat64>(values_ht, "Float64");
    default:
        throw std::logic_error("insertNumericColumn: non-numeric type code");
    }
}

static ColumnRef insertTemporalColumn(TypeRef type, HashTable *values_ht)
{
    zval *array_value;
    switch (type->GetCode()) {
    case Type::Code::DateTime:
        /* DateTime: uint32 seconds, 1970-01-01 .. 2106-02-07 06:28:15. */
        return appendDateColumn<ColumnDateTime>(values_ht, /*is_date=*/false,
                                                "DateTime", 0, 4294967295LL);
    case Type::Code::DateTime64:
    {
        size_t precision = type_as_or_throw<DateTime64Type>(type, "DateTime64")->GetPrecision();
        /* Bound server precision before scaling to prevent signed overflow. */
        if (precision > 9) {
            throw std::runtime_error("DateTime64 precision out of spec range (0..9)");
        }
        auto value = std::make_shared<ColumnDateTime64>(precision);
        int64_t scale = pow10_i64(precision);

        ZEND_HASH_FOREACH_VAL(values_ht, array_value)
        {
            appendDateTime64Cell(value.get(), array_value, precision, scale);
        }
        ZEND_HASH_FOREACH_END();

        return value;
    }
    case Type::Code::Date:
        /* Date: uint16 days, 1970-01-01 .. 2149-06-06 (day 0..65535). */
        return appendDateColumn<ColumnDate>(values_ht, /*is_date=*/true,
                                            "Date", 0, 65535LL * 86400 + 86399);
    case Type::Code::Date32:
        /* Date32: int32 days, 1900-01-01 .. 2299-12-31 (day -25567..120529). */
        return appendDateColumn<ColumnDate32>(values_ht, /*is_date=*/true,
                                              "Date32",
                                              -25567LL * 86400,
                                              120529LL * 86400 + 86399);
    case Type::Code::Time:
    {
        auto value = std::make_shared<ColumnTime>();
        ZEND_HASH_FOREACH_VAL(values_ht, array_value)
        {
            appendTimeCell(value.get(), array_value);
        }
        ZEND_HASH_FOREACH_END();
        return value;
    }
    case Type::Code::Time64:
    {
        size_t precision = type_as_or_throw<Time64Type>(type, "Time64")->GetPrecision();
        if (precision > 9) {
            throw std::runtime_error("Time64 precision out of spec range (0..9)");
        }
        auto value = std::make_shared<ColumnTime64>(precision);
        int64_t scale = pow10_i64(precision);
        ZEND_HASH_FOREACH_VAL(values_ht, array_value)
        {
            appendTime64Cell(value.get(), array_value, precision, scale);
        }
        ZEND_HASH_FOREACH_END();
        return value;
    }
    default:
        throw std::logic_error("insertTemporalColumn: non-temporal type code");
    }
}

static ColumnRef insertGeoIpColumn(Type::Code code, HashTable *values_ht)
{
    zval *array_value;
    switch (code) {
    case Type::Code::IPv4:
    {
        /* IPv4 rejects the empty-string NULL placeholder; use a valid masked address. */
        auto value = std::make_shared<ColumnIPv4>();
        ZEND_HASH_FOREACH_VAL(values_ht, array_value)
        {
            zval *v = array_value;
            ZVAL_DEREF(v);
            if (Z_TYPE_P(v) == IS_NULL && g_allow_null_in_strict > 0) {
                value->Append(std::string("0.0.0.0"));
            } else if (Z_TYPE_P(v) == IS_LONG || Z_TYPE_P(v) == IS_DOUBLE) {
                /* Match toIPv4(N): 16909060 -> 1.2.3.4. The native uint32 Append
                 * has different byte-order handling, so use the validated text path. */
                uint32_t u;
                if (Z_TYPE_P(v) == IS_DOUBLE) {
                    double d = Z_DVAL_P(v), intpart;
                    if (std::isnan(d) || std::isinf(d) || std::modf(d, &intpart) != 0.0) {
                        throw std::runtime_error("IPv4 float input must be an integral value");
                    }
                    if (d < 0.0 || d > (double)UINT32_MAX) {
                        throw std::runtime_error("IPv4 integer out of range (0 .. 4294967295)");
                    }
                    u = (uint32_t)d;
                } else {
                    zend_long n = Z_LVAL_P(v);
                    if (n < 0 || (uint64_t)n > UINT32_MAX) {
                        throw std::runtime_error("IPv4 integer out of range (0 .. 4294967295)");
                    }
                    u = (uint32_t)n;
                }
                char ipbuf[16];
                snprintf(ipbuf, sizeof(ipbuf), "%u.%u.%u.%u",
                         (unsigned)((u >> 24) & 0xFF), (unsigned)((u >> 16) & 0xFF),
                         (unsigned)((u >> 8) & 0xFF),  (unsigned)(u & 0xFF));
                value->Append(std::string(ipbuf));
            } else {
                value->Append(strict_zval_string(v, "IPv4"));
            }
        }
        ZEND_HASH_FOREACH_END();
        return value;
    }
    case Type::Code::IPv6:
    {
        auto value = std::make_shared<ColumnIPv6>();
        ZEND_HASH_FOREACH_VAL(values_ht, array_value)
        {
            zval *v = array_value;
            ZVAL_DEREF(v);
            if (Z_TYPE_P(v) == IS_NULL && g_allow_null_in_strict > 0) {
                value->Append(std::string_view("::"));
            } else {
                /* Append(string_view) calls inet_pton on .data(); a view of
                 * a std::string is NUL-terminated, a bare view need not be. */
                std::string s = strict_zval_string(v, "IPv6");
                value->Append(std::string_view(s));
            }
        }
        ZEND_HASH_FOREACH_END();
        return value;
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
            ZVAL_DEREF(array_value);
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
    default:
        throw std::logic_error("insertGeoIpColumn: non-geo/IP type code");
    }
}

static ColumnRef insertMapColumn(TypeRef type, HashTable *values_ht)
{
        TypeRef k = type_as_or_throw<MapType>(type, "Map")->GetKeyType();
        TypeRef v = type_as_or_throw<MapType>(type, "Map")->GetValueType();
        Type::Code kc = k->GetCode();

        MapInputShape input_shape = classifyMapInput(values_ht);
        if (input_shape == MapInputShape::Mixed) {
            throw std::runtime_error(
                "Map rows must consistently use associative arrays or ordered key/value pairs");
        }
        if (input_shape == MapInputShape::Pairs) {
            return appendMapPairsColumn(values_ht, k, v);
        }

        auto strKey = [](zend_string *zk, zend_ulong) -> std::string {
            if (!zk) {
                throw std::runtime_error("Map(String, *) row entry must have a string key");
            }
            return std::string(ZSTR_VAL(zk), ZSTR_LEN(zk));
        };
        /* Embedded NULs must not truncate a length-prefixed PHP key. */
        auto i64Key = [](zend_string *zk, zend_ulong nk) -> int64_t {
            if (!zk) return (int64_t)(zend_long)nk;
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
            if (!zk) {
                zend_long signed_key = (zend_long)nk;
                if (signed_key < 0) {
                    throw std::runtime_error("Map unsigned key cannot be negative");
                }
                return (uint64_t)signed_key;
            }
            return strict_u64_string(ZSTR_VAL(zk), ZSTR_LEN(zk), "Map UInt64 key");
        };
        auto f64Key = [](zend_string *zk, zend_ulong nk) -> double {
            if (!zk) return (double)(zend_long)nk;
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
        auto f32Key = [&](zend_string *zk, zend_ulong nk) -> double {
            double v = f64Key(zk, nk);
            double max = (double)std::numeric_limits<float>::max();
            if (v < -max || v > max) {
                throw std::runtime_error("Map key out of range for Float32");
            }
            return v;
        };
        auto uuidKey = [](zend_string *zk, zend_ulong) -> UUID {
            if (!zk) {
                throw std::runtime_error("Map(UUID, *) row entry must have a string key");
            }
            return parseUUIDString(ZSTR_VAL(zk), ZSTR_LEN(zk), "UUID key format error");
        };

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
                return appendMapByValueType<ColumnFloat32, double>(values_ht, v, f32Key);
            case Type::Code::Float64:
                return appendMapByValueType<ColumnFloat64, double>(values_ht, v, f64Key);
            case Type::Code::UUID:
                return appendMapByValueType<ColumnUUID,    UUID>(values_ht, v, uuidKey);
            case Type::Code::LowCardinality: {
                TypeRef inner = type_as_or_throw<LowCardinalityType>(k, "LowCardinality")->GetNestedType();
                if (inner->GetCode() == Type::Code::String) {
                    return appendMapByValueType<ColumnLowCardinalityT<ColumnString>, std::string>(values_ht, v, strKey);
                }
                throw std::runtime_error("Unsupported Map key type LowCardinality(" + inner->GetName() + ")");
            }
            default:
                throw std::runtime_error("Unsupported Map(K, V) for row write: " + type->GetName());
        }
}
ColumnRef insertColumn(TypeRef type, zval *value_zval)
{
    ConvertDepthGuard depth_guard;
    zval *array_value;
    HashTable *values_ht = Z_ARRVAL_P(value_zval);

    switch (type->GetCode())
    {
    case Type::Code::UInt64:
    case Type::Code::UInt8:
    case Type::Code::UInt16:
    case Type::Code::UInt32:
    case Type::Code::Int8:
    case Type::Code::Int16:
    case Type::Code::Int32:
    case Type::Code::Int64:
        return insertNumericColumn(type->GetCode(), values_ht);

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
    case Type::Code::Float64:
        return insertNumericColumn(type->GetCode(), values_ht);

    case Type::Code::String:
    {
        auto value = std::make_shared<ColumnString>();

        ZEND_HASH_FOREACH_VAL(values_ht, array_value)
        {
            appendStringCell(value.get(), array_value, "String");
        }
        ZEND_HASH_FOREACH_END();

        return value;
    }
    case Type::Code::JSON:
    {
        /* Raw JSON strings need validation before wire transmission. Nullable
         * children use a masked {} placeholder; bare NULL is invalid. */
        auto value = std::make_shared<ColumnJSON>();
        ZEND_HASH_FOREACH_VAL(values_ht, array_value)
        {
            zval *v = array_value;
            ZVAL_DEREF(v);
            if (Z_TYPE_P(v) == IS_ARRAY || Z_TYPE_P(v) == IS_OBJECT) {
                smart_str buf = {0};
                if (php_json_encode(&buf, v, 0) == FAILURE || EG(exception)) {
                    smart_str_free(&buf);
                    /* Preserve any PHP exception from jsonSerialize/__toString for the boundary catch. */
                    if (EG(exception)) {
                        throw std::runtime_error(
                            "JSON insert: value serialization threw an exception");
                    }
                    throw std::runtime_error("JSON insert: failed to encode value to JSON");
                }
                smart_str_0(&buf);
                value->Append(std::string_view(ZSTR_VAL(buf.s), ZSTR_LEN(buf.s)));
                smart_str_free(&buf);
            } else if (Z_TYPE_P(v) == IS_STRING) {
#if PHP_VERSION_ID >= 80300
                if (!php_json_validate_ex(Z_STRVAL_P(v), Z_STRLEN_P(v), 0,
                                          PHP_JSON_PARSER_DEFAULT_DEPTH)) {
                    if (EG(exception)) zend_clear_exception();
                    throw std::runtime_error("JSON insert: string value is not valid JSON");
                }
#else
                zval probe;
                /* php_json_decode leaves output untouched on failure; initialize before cleanup. */
                ZVAL_UNDEF(&probe);
                if (php_json_decode(&probe, Z_STRVAL_P(v), Z_STRLEN_P(v),
                                    /*assoc=*/true, PHP_JSON_PARSER_DEFAULT_DEPTH) == FAILURE) {
                    if (EG(exception)) zend_clear_exception();
                    throw std::runtime_error("JSON insert: string value is not valid JSON");
                }
                zval_ptr_dtor(&probe);
#endif
                value->Append(std::string_view(Z_STRVAL_P(v), Z_STRLEN_P(v)));
            } else if (Z_TYPE_P(v) == IS_NULL) {
                if (g_allow_null_in_strict == 0) {
                    throw std::runtime_error(
                        "null cannot be assigned to non-Nullable column JSON");
                }
                value->Append(std::string("{}"));
            } else {
                throw std::runtime_error(
                    "JSON insert requires an array, object, or JSON string value");
            }
        }
        ZEND_HASH_FOREACH_END();
        return value;
    }
    case Type::Code::Bool:
    {
        /* zend_is_true("false") is true; parse boolean spellings explicitly. */
        auto value = std::make_shared<ColumnBool>();
        ZEND_HASH_FOREACH_VAL(values_ht, array_value)
        {
            zval *v = array_value;
            ZVAL_DEREF(v);
            if (Z_TYPE_P(v) == IS_NULL) {
                if (g_allow_null_in_strict == 0) {
                    throw std::runtime_error("null cannot be assigned to non-Nullable column Bool");
                }
                value->Append(false);
                continue;
            }
            bool bit = false;
            if (Z_TYPE_P(v) == IS_TRUE) {
                bit = true;
            } else if (Z_TYPE_P(v) == IS_FALSE) {
                bit = false;
            } else if (Z_TYPE_P(v) == IS_LONG) {
                if (Z_LVAL_P(v) != 0 && Z_LVAL_P(v) != 1) {
                    throw std::runtime_error(
                        "Bool insert requires true/false, 0/1, or \"true\"/\"false\"/\"0\"/\"1\"");
                }
                bit = Z_LVAL_P(v) != 0;
            } else if (Z_TYPE_P(v) == IS_STRING) {
                const char *s = Z_STRVAL_P(v);
                size_t n = Z_STRLEN_P(v);
                auto ieq = [&](const char *lit) {
                    size_t ln = strlen(lit);
                    if (n != ln) return false;
                    for (size_t i = 0; i < n; ++i) {
                        if (tolower((unsigned char)s[i]) != (unsigned char)lit[i]) {
                            return false;
                        }
                    }
                    return true;
                };
                if (ieq("1") || ieq("true")) bit = true;
                else if (ieq("0") || ieq("false")) bit = false;
                else {
                    throw std::runtime_error(
                        "Bool insert requires true/false, 0/1, or \"true\"/\"false\"/\"0\"/\"1\"");
                }
            } else {
                throw std::runtime_error(
                    "Bool insert requires true/false, 0/1, or \"true\"/\"false\"/\"0\"/\"1\"");
            }
            value->Append(bit);
        }
        ZEND_HASH_FOREACH_END();
        return value;
    }
    case Type::Code::IPv4:
    case Type::Code::IPv6:
        return insertGeoIpColumn(type->GetCode(), values_ht);
    case Type::Code::FixedString:
    {
        size_t width = parseFixedStringWidth(type);
        auto value = std::make_shared<ColumnFixedString>(width);

        ZEND_HASH_FOREACH_VAL(values_ht, array_value)
        {
            appendFixedStringCell(value.get(), array_value, width, "FixedString");
        }
        ZEND_HASH_FOREACH_END();

        return value;
    }

    case Type::Code::DateTime:
    case Type::Code::DateTime64:
    case Type::Code::Date:
    case Type::Code::Date32:
    case Type::Code::Time:
    case Type::Code::Time64:
        return insertTemporalColumn(type, values_ht);
    case Type::Code::Int128:
    {
        auto value = std::make_shared<ColumnInt128>();
        /* Bound unsigned magnitude before the signed cast; negating INT128_MIN is UB. */
        const absl::uint128 abs_int128_min = absl::uint128(1) << 127;
        const absl::uint128 int128_max     = abs_int128_min - 1;
        ZEND_HASH_FOREACH_VAL(values_ht, array_value)
        {
            ZVAL_DEREF(array_value);
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
                value->Append(Int128(strict_zval_i64(array_value, "Int128")));
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
            ZVAL_DEREF(array_value);
            if (Z_TYPE_P(array_value) == IS_STRING) {
                const char *s = Z_STRVAL_P(array_value);
                size_t len = Z_STRLEN_P(array_value);
                size_t i = 0;
                if (len > 0 && s[0] == '+') { i = 1; }
                value->Append(parse_uint128_dec(s + i, len - i, "UInt128"));
            } else {
                value->Append(UInt128(strict_zval_u64(array_value, "UInt128")));
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
        auto dt = type_as_or_throw<DecimalType>(type, "Decimal");
        size_t dec_precision = dt->GetPrecision();
        size_t dec_scale = dt->GetScale();
        auto value = std::make_shared<ColumnDecimal>(dec_precision, dec_scale);
        ZEND_HASH_FOREACH_VAL(values_ht, array_value)
        {
            zval *v = array_value;
            ZVAL_DEREF(v);
            if (Z_TYPE_P(v) == IS_NULL) {
                /* Decimal parses empty strings as zero; permit NULL only as a masked placeholder. */
                if (g_allow_null_in_strict == 0) {
                    throw std::runtime_error(
                        "null cannot be assigned to non-Nullable column Decimal");
                }
                value->Append(std::string("0"));
            } else if (Z_TYPE_P(v) == IS_ARRAY || Z_TYPE_P(v) == IS_OBJECT ||
                Z_TYPE_P(v) == IS_RESOURCE) {
                throw std::runtime_error(
                    "Decimal insert requires a scalar value (string, int, or float)");
            } else {
                ZStrGuard sg(v);
                std::string dv(sg.val(), sg.len());
                validateDecimalText(dv, dec_precision, dec_scale, "Decimal");
                value->Append(dv);
            }
        }
        ZEND_HASH_FOREACH_END();
        return value;
    }

    case Type::Code::Array:
    {
        TypeRef item_type = type_as_or_throw<ArrayType>(type, "Array")->GetItemType();
        if (item_type->GetCode() == Type::Array)
        {
            throw std::runtime_error(
                "Multidimensional Arrays are not supported for insert "
                "(Array(Array(...))); select of nested arrays works. "
                "Flatten the data or use a single-level Array column.");
        }

        auto value = std::make_shared<ColumnArray>(createColumn(item_type));

        /* Tuple-backed columns lose their nested shape when Clear() runs. */
        bool reuse_child = canReuseArrayChild(item_type);
        ColumnRef child = reuse_child ? createColumn(item_type) : nullptr;

        ZEND_HASH_FOREACH_VAL(values_ht, array_value)
        {
            ZVAL_DEREF(array_value);
            if (Z_TYPE_P(array_value) != IS_ARRAY)
            {
                throw std::runtime_error("The inserted data is not an array type");
            }

            if (reuse_child) {
                child->Append(insertColumn(item_type, array_value));
                value->AppendAsColumn(child);
                child->Clear();
            } else {
                value->AppendAsColumn(insertColumn(item_type, array_value));
            }
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
            /* Dereference before masking, or a referenced NULL would be stored as zero. */
            zval *nv = array_value;
            ZVAL_DEREF(nv);
            nulls->Append(Z_TYPE_P(nv) == IS_NULL ? 1 : 0);
        }
        ZEND_HASH_FOREACH_END();

        /* The bitmap masks NULL cells, so child conversion may use typed-zero placeholders. */
        AllowNullGuard nulls_ok;
        ColumnRef child = insertColumn(type_as_or_throw<NullableType>(type, "Nullable")->GetNestedType(), value_zval);

        return std::make_shared<ColumnNullable>(child, nulls);
    }

    case Type::Code::Tuple:
    {
        auto tupleType = type_as_or_throw<TupleType>(type, "Tuple")->GetTupleType();
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
                    ZVAL_DEREF(pzval);
                    if (Z_TYPE_P(pzval) != IS_ARRAY)
                    {
                        throw std::runtime_error("Tuple row must be a PHP array");
                    }
                    if (zend_hash_num_elements(Z_ARRVAL_P(pzval)) != arity) {
                        throw std::runtime_error(
                            "Tuple row arity does not match the column type");
                    }
                    fzval = zend_hash_index_find(Z_ARRVAL_P(pzval), field);
                    if (NULL == fzval)
                    {
                        throw std::runtime_error(
                            "Tuple row is missing a field value");
                    }
                    ZVAL_DEREF(fzval);
                    Z_TRY_ADDREF_P(fzval);
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
        TypeRef nested = type_as_or_throw<LowCardinalityType>(type, "LowCardinality")->GetNestedType();
        bool is_nullable = (nested->GetCode() == Type::Code::Nullable);
        TypeRef inner = is_nullable
            ? type_as_or_throw<NullableType>(nested, "Nullable")->GetNestedType()
            : nested;

        if (inner->GetCode() == Type::Code::String) {
            if (is_nullable) {
                return appendLowCardinalityColumn<ColumnLowCardinalityT<ColumnNullableT<ColumnString>>, /*nullable=*/true>(
                    values_ht, std::make_shared<ColumnLowCardinalityT<ColumnNullableT<ColumnString>>>(), "LowCardinality Nullable String");
            }
            return appendLowCardinalityColumn<ColumnLowCardinalityT<ColumnString>, /*nullable=*/false>(
                values_ht, std::make_shared<ColumnLowCardinalityT<ColumnString>>(), "LowCardinality String");
        }
        if (inner->GetCode() == Type::Code::FixedString) {
            int width = parseFixedStringWidth(inner);
            if (is_nullable) {
                return appendLowCardinalityColumn<ColumnLowCardinalityT<ColumnNullableT<ColumnFixedString>>, /*nullable=*/true>(
                    values_ht, std::make_shared<ColumnLowCardinalityT<ColumnNullableT<ColumnFixedString>>>(width), "LowCardinality Nullable FixedString");
            }
            return appendLowCardinalityColumn<ColumnLowCardinalityT<ColumnFixedString>, /*nullable=*/false>(
                values_ht, std::make_shared<ColumnLowCardinalityT<ColumnFixedString>>(width), "LowCardinality FixedString");
        }
        throw std::runtime_error("LowCardinality only supported over String / FixedString (Nullable allowed)");
    }

    case Type::Code::Map:
        return insertMapColumn(type, values_ht);

    case Type::Code::Point:
    case Type::Code::Ring:
    case Type::Code::Polygon:
    case Type::Code::MultiPolygon:
        return insertGeoIpColumn(type->GetCode(), values_ht);

    case Type::Code::Void:
    {
        throw std::runtime_error("can't support Void");
    }
    default:
        throw std::runtime_error("insertColumn: unsupported type code: " + type->GetName());
    }
}

static void emitStringCell(zval *arr, const char *s, size_t len,
                           const string& column_name, int8_t is_array, long fetch_mode)
{
    if (is_array) {
        add_next_index_stringl(arr, s, len);
    } else if (fetch_mode & SC_FETCH_ONE) {
        ZVAL_STRINGL(arr, s, len);
    } else {
        add_assoc_stringl_ex(arr, column_name.c_str(), column_name.length(), s, len);
    }
}

static void emitLongCell(zval *arr, zend_long v,
                         const string& column_name, int8_t is_array, long fetch_mode)
{
    if (is_array) {
        add_next_index_long(arr, v);
    } else if (fetch_mode & SC_FETCH_ONE) {
        ZVAL_LONG(arr, v);
    } else {
        add_assoc_long_ex(arr, column_name.c_str(), column_name.length(), v);
    }
}

static void emitSigned64Cell(zval *arr, int64_t v,
                             const string& column_name, int8_t is_array, long fetch_mode)
{
    if (v >= (int64_t)ZEND_LONG_MIN && v <= (int64_t)ZEND_LONG_MAX) {
        emitLongCell(arr, (zend_long)v, column_name, is_array, fetch_mode);
        return;
    }
    char buf[32];
    int len = snprintf(buf, sizeof(buf), "%" PRId64, v);
    emitStringCell(arr, buf, len, column_name, is_array, fetch_mode);
}

static inline void emitUInt64Cell(zval *arr, uint64_t v,
                                  const string& column_name, int8_t is_array,
                                  long fetch_mode);

static void emitDoubleCell(zval *arr, double v,
                           const string& column_name, int8_t is_array, long fetch_mode)
{
    if (is_array) {
        add_next_index_double(arr, v);
    } else if (fetch_mode & SC_FETCH_ONE) {
        ZVAL_DOUBLE(arr, v);
    } else {
        add_assoc_double_ex(arr, column_name.c_str(), column_name.length(), v);
    }
}

/* Avoid 32-bit time_t truncation in gmtime; use Howard Hinnant's inverse
 * civil-date arithmetic, paired with days_from_civil in to_time_t. */
static inline int64_t floor_div_86400(int64_t epoch, int64_t &secs_of_day)
{
    int64_t days = epoch >= 0 ? epoch / 86400 : -((-epoch + 86399) / 86400);
    secs_of_day = epoch - days * 86400;
    return days;
}

static inline void civil_from_days(int64_t z, int &y, unsigned &m, unsigned &d)
{
    z += 719468;
    int64_t era = (z >= 0 ? z : z - 146096) / 146097;
    unsigned doe = (unsigned)(z - era * 146097);
    unsigned yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    int64_t y_ = (int64_t)yoe + era * 400;
    unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    unsigned mp = (5 * doy + 2) / 153;
    d = doy - (153 * mp + 2) / 5 + 1;
    m = mp < 10 ? mp + 3 : mp - 9;
    y = (int)(y_ + (m <= 2 ? 1 : 0));
}

static inline size_t format_epoch_utc(int64_t epoch, char *buf, size_t bufsz, bool with_time)
{
    int64_t secs_of_day = 0;
    int64_t days = floor_div_86400(epoch, secs_of_day);
    int y = 0;
    unsigned m = 0, d = 0;
    civil_from_days(days, y, m, d);
    if (!with_time) {
        return (size_t)snprintf(buf, bufsz, "%04d-%02u-%02u", y, m, d);
    }
    unsigned hh = (unsigned)(secs_of_day / 3600);
    unsigned mm = (unsigned)((secs_of_day / 60) % 60);
    unsigned ss = (unsigned)(secs_of_day % 60);
    return (size_t)snprintf(buf, bufsz, "%04d-%02u-%02u %02u:%02u:%02u",
                            y, m, d, hh, mm, ss);
}

// RawAt widens storage directly; At narrows through time_t on 32-bit platforms.
static void emitEpoch(zval *arr, int64_t t, const char *fmt,
                      const string& column_name, int8_t is_array, long fetch_mode)
{
    if (fetch_mode & SC_FETCH_DATE_AS_STRINGS) {
        char buffer[32];
        bool with_time = strchr(fmt, 'H') != nullptr;
        size_t l = format_epoch_utc(t, buffer, sizeof(buffer), with_time);
        emitStringCell(arr, buffer, l, column_name, is_array, fetch_mode);
    } else {
        emitSigned64Cell(arr, t, column_name, is_array, fetch_mode);
    }
}


template <typename TCol>
static inline void emitIntColumn(zval *arr, const ColumnRef& columnRef, int row,
                                 const string& column_name, int8_t is_array, long fetch_mode)
{
    const TCol *col_ptr = fast_scalar_col<TCol>(columnRef);
    using ValueType = typename TCol::ValueType;
    ValueType value = (*col_ptr)[row];
    if constexpr (std::numeric_limits<ValueType>::is_signed) {
        emitSigned64Cell(arr, (int64_t)value, column_name, is_array, fetch_mode);
    } else {
        emitUInt64Cell(arr, (uint64_t)value, column_name, is_array, fetch_mode);
    }
}


// Values above ZEND_LONG_MAX need decimal strings for lossless PHP round-trips.
static inline void emitUInt64Cell(zval *arr, uint64_t v,
                                  const string& column_name, int8_t is_array, long fetch_mode)
{
    if (v <= (uint64_t)ZEND_LONG_MAX) {
        emitLongCell(arr, (zend_long)v, column_name, is_array, fetch_mode);
        return;
    }
    char buf[32];
    int len = snprintf(buf, sizeof(buf), "%" PRIu64, v);
    emitStringCell(arr, buf, len, column_name, is_array, fetch_mode);
}


static void pointToZval(zval *out, const std::tuple<double, double>& pt)
{
    array_init_size(out, 2);
    add_next_index_double(out, std::get<0>(pt));
    add_next_index_double(out, std::get<1>(pt));
}

// Geo ArrayValueView is iterable but cannot bind to a vector reference.
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

static void emitNestedZval(zval *arr, zval *built, const string& column_name, int8_t is_array, long fetch_mode)
{
    if (is_array) {
        add_next_index_zval(arr, built);
    } else if (fetch_mode & SC_FETCH_ONE) {
        ZVAL_COPY_VALUE(arr, built);
    } else {
        add_assoc_zval_ex(arr, column_name.c_str(), column_name.length(), built);
    }
}


template <typename TCol>
static void emitEnumColumn(zval *arr, const ColumnRef& columnRef, int row,
                           const string& column_name, int8_t is_array, long fetch_mode,
                           const char *what)
{
    auto col = as_or_throw<TCol>(columnRef, what);
    std::string_view name = col->NameAt(row);
    emitStringCell(arr, name.data(), name.length(), column_name, is_array, fetch_mode);
}

static void readNumericCell(Type::Code code, zval *arr, const ColumnRef& columnRef, int row,
                            const string& column_name, int8_t is_array, long fetch_mode)
{
    switch (code) {
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
    case Type::Code::Float32:
        emitDoubleCell(arr, static_cast<double>((*fast_scalar_col<ColumnFloat32>(columnRef))[row]),
                       column_name, is_array, fetch_mode);
        break;
    case Type::Code::Float64:
        emitDoubleCell(arr, (double)(*fast_scalar_col<ColumnFloat64>(columnRef))[row],
                       column_name, is_array, fetch_mode);
        break;
    default:
        throw std::logic_error("readNumericCell: non-numeric type code");
    }
}

void convertToZval(zval *arr, const ColumnRef& columnRef, int row, const string& column_name, int8_t is_array, long fetch_mode)
{
    ConvertDepthGuard depth_guard;
    switch (columnRef->Type()->GetCode())
    {
    case Type::Code::UInt64:
    case Type::Code::UInt8:
    case Type::Code::UInt16:
    case Type::Code::UInt32:
        readNumericCell(columnRef->Type()->GetCode(), arr, columnRef, row, column_name, is_array, fetch_mode);
        break;
    case Type::Code::IPv4:
    {
        /* IPv4 no longer inherits ColumnUInt32; render via a stack buffer to avoid AsString allocation. */
        auto col_ip = as_or_throw<ColumnIPv4>(columnRef, "IPv4 read");
        in_addr addr = col_ip->At(row);
        char buf[INET_ADDRSTRLEN];
        if (!inet_ntop(AF_INET, &addr, buf, sizeof(buf))) {
            throw std::system_error(
                std::error_code(errno, std::generic_category()),
                "Invalid IPv4 data");
        }
        emitStringCell(arr, buf, strlen(buf), column_name, is_array, fetch_mode);
        break;
    }
    case Type::Code::Int8:
    case Type::Code::Int16:
    case Type::Code::Int32:
    case Type::Code::Int64:
        readNumericCell(columnRef->Type()->GetCode(), arr, columnRef, row, column_name, is_array, fetch_mode);
        break;
    case Type::Code::UUID:
    {
        auto uuid_col = as_or_throw<ColumnUUID>(columnRef, "UUID read");
        auto col = (*uuid_col)[row];
        char buf[37];
        int blen = format_uuid(col, (fetch_mode & SC_FETCH_UUID_WITH_DASHES) != 0,
                               buf, sizeof(buf));
        emitStringCell(arr, buf, blen, column_name, is_array, fetch_mode);
        break;
    }
    case Type::Code::Float32:
    case Type::Code::Float64:
        readNumericCell(columnRef->Type()->GetCode(), arr, columnRef, row, column_name, is_array, fetch_mode);
        break;
    case Type::Code::Decimal:
    case Type::Code::Decimal32:
    case Type::Code::Decimal64:
    case Type::Code::Decimal128:
    {
        auto col = as_or_throw<ColumnDecimal>(columnRef, "Decimal read");
        // Worst case: sign + 39 digits + decimal point = 41 bytes.
        size_t scale = cachedDecimalScale(columnRef->Type());
        Int128 raw = col->At(row);
        char buf[64];
        size_t l = format_int128_dec(raw, buf);
        if (scale > 0) {
            bool neg = (buf[0] == '-');
            size_t sign_off = neg ? 1 : 0;
            size_t dlen = l - sign_off;
            // Pad before inserting the decimal point: 5 at scale 3 becomes 0.005.
            if (dlen <= scale) {
                size_t pad = scale + 1 - dlen;
                memmove(buf + sign_off + pad, buf + sign_off, dlen);
                memset(buf + sign_off, '0', pad);
                l += pad;
                dlen += pad;
            }
            size_t dot_pos = sign_off + dlen - scale;
            memmove(buf + dot_pos + 1, buf + dot_pos, scale);
            buf[dot_pos] = '.';
            ++l;
        }
        emitStringCell(arr, buf, l, column_name, is_array, fetch_mode);
        break;
    }
    case Type::Code::Bool:
    {
        auto b_col = as_or_throw<ColumnBool>(columnRef, "Bool read");
        bool v = b_col->At(row);
        if (is_array)
        {
            add_next_index_bool(arr, v);
        }
        else if (fetch_mode & SC_FETCH_ONE)
        {
            ZVAL_BOOL(arr, v);
        }
        else
        {
            add_assoc_bool_ex(arr, column_name.c_str(), column_name.length(), v);
        }
        break;
    }
    case Type::Code::String:
    {
        const ColumnString *s_col = fast_scalar_col<ColumnString>(columnRef);
        auto col = (*s_col)[row];
        emitStringCell(arr, col.data(), col.length(), column_name, is_array, fetch_mode);
        break;
    }
    case Type::Code::JSON:
    {
        /* Native JSON reads require output_format_native_write_json_as_string=1.
         * JSON_AS_ARRAY wins when both decode flags are set. */
        auto j_col = as_or_throw<ColumnJSON>(columnRef, "JSON read");
        auto sv = j_col->At(row);
        if (fetch_mode & (SC_FETCH_JSON_AS_ARRAY | SC_FETCH_JSON_AS_OBJECT))
        {
            bool assoc = (fetch_mode & SC_FETCH_JSON_AS_ARRAY) != 0;
            zval decoded;
            /* Packed string views lack a NUL terminator required by the PHP JSON scanner. */
            /* php_json_decode takes char* (not const) on PHP 7.4; &str[0]
             * is a mutable, NUL-terminated pointer on every target. */
            std::string json_str(sv);
            if (json_str.size() > MAX_JSON_CELL_BYTES) {
                throw std::runtime_error("JSON read: cell exceeds maximum JSON decode size (16 MiB)");
            }
            /* php_json_decode leaves output untouched on failure. */
            ZVAL_UNDEF(&decoded);
            if (php_json_decode(&decoded, &json_str[0], json_str.size(), assoc,
                                PHP_JSON_PARSER_DEFAULT_DEPTH) == FAILURE)
            {
                throw std::runtime_error("JSON read: failed to decode server JSON value");
            }
            if (is_array)
            {
                add_next_index_zval(arr, &decoded);
            }
            else if (fetch_mode & SC_FETCH_ONE)
            {
                ZVAL_COPY_VALUE(arr, &decoded);
            }
            else
            {
                add_assoc_zval_ex(arr, column_name.c_str(), column_name.length(), &decoded);
            }
        }
        else
        {
            emitStringCell(arr, sv.data(), sv.length(), column_name, is_array, fetch_mode);
        }
        break;
    }
    case Type::Code::FixedString:
    {
        // FIXEDSTRING_BINARY preserves trailing NULs that may be payload rather than padding.
        auto fs_col = as_or_throw<ColumnFixedString>(columnRef, "FixedString read");
        auto col = (*fs_col)[row];
        size_t len = col.length();
        if (!(fetch_mode & SC_FETCH_FIXEDSTRING_BINARY)) {
            while (len > 0 && col[len - 1] == '\0') {
                --len;
            }
        }
        emitStringCell(arr, col.data(), len, column_name, is_array, fetch_mode);
        break;
    }
    case Type::Code::IPv6:
    {
        /* IPv6 uses composition since v2.6.1; render directly to avoid AsString allocation. */
        auto col_ip = as_or_throw<ColumnIPv6>(columnRef, "IPv6 read");
        in6_addr addr = col_ip->At(row);
        char buf[INET6_ADDRSTRLEN];
        if (!inet_ntop(AF_INET6, &addr, buf, sizeof(buf))) {
            throw std::system_error(
                std::error_code(errno, std::generic_category()),
                "Invalid IPv6 data");
        }
        emitStringCell(arr, buf, strlen(buf), column_name, is_array, fetch_mode);
        break;
    }

    case Type::Code::DateTime:
    {
        auto col = as_or_throw<ColumnDateTime>(columnRef, "DateTime read");
        emitEpoch(arr, (int64_t)col->RawAt(row), "%Y-%m-%d %H:%M:%S",
                  column_name, is_array, fetch_mode);
        break;
    }
    case Type::Code::DateTime64:
    {
        auto col = as_or_throw<ColumnDateTime64>(columnRef, "DateTime64 read");
        size_t precision = cachedDateTime64Precision(columnRef->Type());
        int64_t scale = pow10_i64(precision);
        int64_t raw = col->At(row);
        /* Floor-divide pre-epoch timestamps so the fraction stays in [0, scale). */
        int64_t whole_i = raw / scale;
        int64_t frac = raw % scale;
        if (frac < 0) { frac += scale; --whole_i; }
        if (fetch_mode & SC_FETCH_DATE_AS_STRINGS) {
            char buffer[64];
            size_t l = format_epoch_utc(whole_i, buffer, sizeof(buffer), true);
            if (precision > 0 && l < sizeof(buffer)) {
                int written = snprintf(buffer + l, sizeof(buffer) - l, ".%0*lld",
                                       (int)precision, (long long)frac);
                if (written > 0 && (size_t)written < sizeof(buffer) - l) {
                    l += (size_t)written;
                }
            }
            emitStringCell(arr, buffer, l, column_name, is_array, fetch_mode);
        } else {
            emitSigned64Cell(arr, raw, column_name, is_array, fetch_mode);
        }
        break;
    }
    case Type::Code::Date:
    {
        auto col = as_or_throw<ColumnDate>(columnRef, "Date read");
        emitEpoch(arr, (int64_t)col->RawAt(row) * 86400, "%Y-%m-%d",
                  column_name, is_array, fetch_mode);
        break;
    }
    case Type::Code::Date32:
    {
        auto col = as_or_throw<ColumnDate32>(columnRef, "Date32 read");
        emitEpoch(arr, (int64_t)col->RawAt(row) * 86400, "%Y-%m-%d",
                  column_name, is_array, fetch_mode);
        break;
    }
    case Type::Code::Time:
    {
        auto col = as_or_throw<ColumnTime>(columnRef, "Time read");
        int32_t v = col->At(row);
        if (fetch_mode & SC_FETCH_DATE_AS_STRINGS) {
            uint32_t abs_v = v < 0 ? uint32_t(0) - uint32_t(v) : uint32_t(v);
            char buffer[16];
            int l = snprintf(buffer, sizeof(buffer), "%s%02" PRIu32 ":%02" PRIu32 ":%02" PRIu32,
                             v < 0 ? "-" : "", abs_v / 3600, (abs_v / 60) % 60, abs_v % 60);
            emitStringCell(arr, buffer, l, column_name, is_array, fetch_mode);
        } else {
            if (is_array) {
                add_next_index_long(arr, (zend_long)v);
            } else if (fetch_mode & SC_FETCH_ONE) {
                ZVAL_LONG(arr, (zend_long)v);
            } else {
                add_assoc_long_ex(arr, column_name.c_str(), column_name.length(), (zend_long)v);
            }
        }
        break;
    }
    case Type::Code::Time64:
    {
        auto col = as_or_throw<ColumnTime64>(columnRef, "Time64 read");
        size_t precision = cachedTime64Precision(columnRef->Type());
        int64_t scale = pow10_i64(precision);
        int64_t raw = col->At(row);
        if (fetch_mode & SC_FETCH_DATE_AS_STRINGS) {
            /* Take sign from raw: negative sub-second durations have whole == 0. */
            bool neg = raw < 0;
            uint64_t araw = neg ? uint64_t(0) - uint64_t(raw) : uint64_t(raw);
            uint64_t abs_whole = araw / (uint64_t)scale;
            uint64_t frac = araw % (uint64_t)scale;
            char buffer[64];
            int l = snprintf(buffer, sizeof(buffer), "%s%02" PRIu64 ":%02" PRIu64 ":%02" PRIu64,
                             neg ? "-" : "",
                             abs_whole / 3600,
                             (abs_whole / 60) % 60,
                             abs_whole % 60);
            if (l < 0 || (size_t)l >= sizeof(buffer)) l = (int)sizeof(buffer) - 1;
            if (precision > 0 && l > 0 && (size_t)l < sizeof(buffer)) {
                int w = snprintf(buffer + l, sizeof(buffer) - l, ".%0*" PRIu64,
                                 (int)precision, frac);
                if (w > 0 && (size_t)w < sizeof(buffer) - (size_t)l) l += w;
            }
            emitStringCell(arr, buffer, l, column_name, is_array, fetch_mode);
        } else {
            emitSigned64Cell(arr, raw, column_name, is_array, fetch_mode);
        }
        break;
    }
    case Type::Code::Int128:
    {
        auto col = as_or_throw<ColumnInt128>(columnRef, "Int128 read");
        char buf[41];
        size_t l = format_int128_dec(col->At(row), buf);
        emitStringCell(arr, buf, l, column_name, is_array, fetch_mode);
        break;
    }
    case Type::Code::UInt128:
    {
        auto col = as_or_throw<ColumnUInt128>(columnRef, "UInt128 read");
        char buf[40];
        size_t l = format_uint128_dec(col->At(row), buf);
        emitStringCell(arr, buf, l, column_name, is_array, fetch_mode);
        break;
    }
    case Type::Code::Array:
    {
        auto array = as_or_throw<ColumnArray>(columnRef, "Array read");
        auto col = array->GetAsColumn(row);
        /* Nested cells accept value-shaping flags only, never result-shape flags. */
        long nested_mode = fetch_mode & SC_FETCH_VALUE_FLAGS;
        if (fetch_mode & SC_FETCH_ONE) {
            array_init_size(arr, (uint32_t)col->Size());
            for (size_t i = 0; i < col->Size(); ++i)
            {
                convertToZval(arr, col, i, "array", 1, nested_mode);
            }
        } else {
            zval *return_tmp;
            SC_MAKE_STD_ZVAL(return_tmp);
            array_init_size(return_tmp, (uint32_t)col->Size());
            /* Free partial nested output on throw before ownership transfers to arr. */
            try {
                for (size_t i = 0; i < col->Size(); ++i)
                {
                    convertToZval(return_tmp, col, i, "array", 1, nested_mode);
                }
            } catch (...) {
                zval_ptr_dtor(return_tmp);
                throw;
            }
            if (is_array)
            {
                add_next_index_zval(arr, return_tmp);
            }
            else
            {
                add_assoc_zval_ex(arr, column_name.c_str(), column_name.length(), return_tmp);
            }
        }
        break;
    }

    case Type::Code::Enum8:
        emitEnumColumn<ColumnEnum8>(arr, columnRef, row, column_name, is_array, fetch_mode, "Enum8 read");
        break;
    case Type::Code::Enum16:
        emitEnumColumn<ColumnEnum16>(arr, columnRef, row, column_name, is_array, fetch_mode, "Enum16 read");
        break;

    case Type::Code::Nullable:
    {
        auto nullable = as_or_throw<ColumnNullable>(columnRef, "Nullable read");
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
                    add_assoc_null_ex(arr, column_name.c_str(), column_name.length());
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
        auto tuple = as_or_throw<ColumnTuple>(columnRef, "Tuple read");
        long nested_mode = fetch_mode & SC_FETCH_VALUE_FLAGS;
        if (fetch_mode & SC_FETCH_ONE) {
            array_init_size(arr, (uint32_t)tuple->TupleSize());
            for (size_t i = 0; i < tuple->TupleSize(); ++i)
            {
                convertToZval(arr, (*tuple)[i], row, "tuple", 1, nested_mode);
            }
        } else {
            zval *return_tmp;
            SC_MAKE_STD_ZVAL(return_tmp);
            array_init_size(return_tmp, (uint32_t)tuple->TupleSize());
            /* Same orphan-on-throw guard as the Array read path. */
            try {
                for (size_t i = 0; i < tuple->TupleSize(); ++i)
                {
                    convertToZval(return_tmp, (*tuple)[i], row, "tuple", 1, nested_mode);
                }
            } catch (...) {
                zval_ptr_dtor(return_tmp);
                throw;
            }
            if (is_array)
            {
                add_next_index_zval(arr, return_tmp);
            }
            else
            {
                add_assoc_zval_ex(arr, column_name.c_str(), column_name.length(), return_tmp);
            }
        }
        break;
    }

    case Type::Code::LowCardinality:
    {
        // ColumnLowCardinality::GetItem covers LC(String), LC(FixedString),
        // LC(Nullable(String)), and LC(Nullable(FixedString)). A NULL entry
        // returns an ItemView with type Void regardless of the nested column.
        auto lc = columnRef->As<ColumnLowCardinality>();
        if (!lc) {
            throw std::runtime_error("LowCardinality column downcast failed");
        }
        TypeRef nested = type_as_or_throw<LowCardinalityType>(columnRef->Type(), "LowCardinality")->GetNestedType();
        bool is_nullable = (nested->GetCode() == Type::Code::Nullable);
        TypeRef inner = is_nullable
            ? type_as_or_throw<NullableType>(nested, "Nullable")->GetNestedType()
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
                add_assoc_null_ex(arr, column_name.c_str(), column_name.length());
            }
            break;
        }

        std::string_view sv = iv.AsBinaryData();
        // FixedString views include trailing NULs from server-side padding;
        // trim them so the round-trip preserves the original input, unless
        // FIXEDSTRING_BINARY asks for the raw padded width (see the standalone
        // FixedString case above).
        if (inner->GetCode() == Type::Code::FixedString &&
            !(fetch_mode & SC_FETCH_FIXEDSTRING_BINARY)) {
            size_t len = sv.length();
            while (len > 0 && sv[len - 1] == '\0') {
                --len;
            }
            sv = std::string_view(sv.data(), len);
        }
        emitStringCell(arr, sv.data(), sv.length(), column_name, is_array, fetch_mode);
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
        auto map_col = as_or_throw<ColumnMap>(columnRef, "Map read");
        ColumnRef tuple_col = map_col->GetAsColumn(row);
        auto tup = tuple_col->As<ColumnTuple>();
        if (!tup) {
            throw std::runtime_error("Map read: inner tuple type mismatch");
        }
        ColumnRef keys_any = (*tup)[0];
        ColumnRef values_any = (*tup)[1];
        size_t entry_count = keys_any->Size();
        /* Unequal column lengths would read out of bounds or silently drop map values. */
        if (values_any->Size() != entry_count) {
            throw std::runtime_error("Map column key/value size mismatch");
        }

        zval *map_zv;
        SC_MAKE_STD_ZVAL(map_zv);
        array_init_size(map_zv, (uint32_t)entry_count);
        /* Free partial map output on throw before ownership transfers to the parent. */
        struct MapZvGuard {
            zval *z;
            ~MapZvGuard() { if (z) zval_ptr_dtor(z); }
        } map_guard{map_zv};

        if (fetch_mode & SC_FETCH_MAP_AS_PAIRS) {
            long nested_mode = fetch_mode & SC_FETCH_VALUE_FLAGS;
            for (size_t i = 0; i < entry_count; ++i) {
                zval pair;
                array_init_size(&pair, 2);
                try {
                    zval key;
                    convertToZval(&key, keys_any, (int)i, "", 0,
                                  nested_mode | SC_FETCH_ONE);
                    add_next_index_zval(&pair, &key);

                    zval value;
                    convertToZval(&value, values_any, (int)i, "", 0,
                                  nested_mode | SC_FETCH_ONE);
                    add_next_index_zval(&pair, &value);
                } catch (...) {
                    zval_ptr_dtor(&pair);
                    throw;
                }
                add_next_index_zval(map_zv, &pair);
            }

            map_guard.z = nullptr;
            if (is_array) {
                add_next_index_zval(arr, map_zv);
                ZVAL_UNDEF(map_zv);
            } else if (fetch_mode & SC_FETCH_ONE) {
                ZVAL_COPY_VALUE(arr, map_zv);
                ZVAL_UNDEF(map_zv);
            } else {
                add_assoc_zval_ex(arr, column_name.c_str(), column_name.length(), map_zv);
                ZVAL_UNDEF(map_zv);
            }
            break;
        }

        /* Cast once per Map cell; the entry loop only changes the index. */
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

        // PHP keys are strings/integers; render floating keys as canonical strings.
        auto signedKey = [](int64_t value, std::string &str_buf,
                            zend_long &long_out) -> int {
            if (value >= (int64_t)ZEND_LONG_MIN &&
                value <= (int64_t)ZEND_LONG_MAX) {
                long_out = (zend_long)value;
                return 1;
            }
            char buf[32];
            int len = snprintf(buf, sizeof(buf), "%" PRId64, value);
            str_buf.assign(buf, len);
            return 0;
        };
        auto unsignedKey = [](uint64_t value, std::string &str_buf,
                              zend_long &long_out) -> int {
            if (value <= (uint64_t)ZEND_LONG_MAX) {
                long_out = (zend_long)value;
                return 1;
            }
            char buf[32];
            int len = snprintf(buf, sizeof(buf), "%" PRIu64, value);
            str_buf.assign(buf, len);
            return 0;
        };
        auto decodeKey = [&](size_t i, std::string &str_buf, zend_long &long_out, double &dbl_out) -> int {
            // Returns 0 = string, 1 = long, 2 = double-as-string.
            switch (key_code) {
                case Type::Code::String: {
                    std::string_view kv = (*k_str_col)[i];
                    str_buf.assign(kv.data(), kv.length());
                    return 0;
                }
                case Type::Code::Int64:
                    return signedKey((int64_t)k_i64_col->At(i), str_buf, long_out);
                case Type::Code::UInt64:
                    return unsignedKey((uint64_t)k_u64_col->At(i), str_buf, long_out);
                case Type::Code::Int32:
                    return signedKey((int64_t)k_i32_col->At(i), str_buf, long_out);
                case Type::Code::UInt32:
                    return unsignedKey((uint64_t)k_u32_col->At(i), str_buf, long_out);
                case Type::Code::Int16:   long_out = (zend_long)k_i16_col->At(i);  return 1;
                case Type::Code::UInt16:  long_out = (zend_long)k_u16_col->At(i);  return 1;
                case Type::Code::Int8:    long_out = (zend_long)k_i8_col->At(i);   return 1;
                case Type::Code::UInt8:   long_out = (zend_long)k_u8_col->At(i);   return 1;
                case Type::Code::Float32: dbl_out  = (double)k_f32_col->At(i);     return 2;
                case Type::Code::Float64: dbl_out  = (double)k_f64_col->At(i);     return 2;
                case Type::Code::UUID: {
                    UUID u = k_uuid_col->At(i);
                    char buf[37];
                    int blen = format_uuid(u, (fetch_mode & SC_FETCH_UUID_WITH_DASHES) != 0,
                                           buf, sizeof(buf));
                    str_buf.assign(buf, blen);
                    return 0;
                }
                default:
                    throw std::runtime_error("Map read: unsupported key type " + key_type_ref->GetName());
            }
        };

        /* Keep floating map keys stable across LC_NUMERIC locales. */
        auto fmtFloatKey = [](double dk, char *buf, size_t bufsz) -> int {
            php_gcvt(dk, 17, '.', 'e', buf);
            (void)bufsz;
            return (int)strlen(buf);
        };

        auto rejectLossyKey = [&]() {
            throw std::runtime_error(
                "Map decoding would collapse two entries onto the same PHP "
                "array key; pass ClickHouse::MAP_AS_PAIRS for an ordered "
                "lossless result");
        };
        /* Canonical decimal strings become integer keys. Use _add: _add_new
         * bypasses duplicate checks and can create multiple buckets for one key. */
        auto symtableAdd = [](HashTable *ht, const char *key, size_t len,
                              zval *value) -> zval * {
            zend_ulong numeric_index;
            if (ZEND_HANDLE_NUMERIC_STR(key, len, numeric_index)) {
                return zend_hash_index_add(ht, numeric_index, value);
            }
            return zend_hash_str_add(ht, key, len, value);
        };
        auto insertValue = [&](int kkind, const std::string &sb,
                               zend_long lk, double dk, zval *value) {
            HashTable *map_ht = Z_ARRVAL_P(map_zv);
            zval *inserted = nullptr;
            if (kkind == 0) {
                inserted = symtableAdd(map_ht, sb.data(), sb.size(), value);
            } else if (kkind == 1) {
                inserted = zend_hash_index_add(map_ht, (zend_ulong)lk, value);
            } else {
                char kbuf[64];
                int klen = fmtFloatKey(dk, kbuf, sizeof(kbuf));
                inserted = symtableAdd(map_ht, kbuf, (size_t)klen, value);
            }
            if (!inserted) {
                zval_ptr_dtor(value);
                rejectLossyKey();
            }
        };

        auto addStrL = [&](int kkind, const std::string &sb, zend_long lk, double dk,
                           const char *vptr, size_t vlen) {
            zval value;
            ZVAL_STRINGL(&value, vptr, vlen);
            insertValue(kkind, sb, lk, dk, &value);
        };
        auto addLong = [&](int kkind, const std::string &sb, zend_long lk, double dk, zend_long lv) {
            zval value;
            ZVAL_LONG(&value, lv);
            insertValue(kkind, sb, lk, dk, &value);
        };
        auto addDbl = [&](int kkind, const std::string &sb, zend_long lk, double dk, double dv) {
            zval value;
            ZVAL_DOUBLE(&value, dv);
            insertValue(kkind, sb, lk, dk, &value);
        };
        auto addSigned = [&](int kkind, const std::string &sb, zend_long lk,
                             double dk, int64_t value) {
            if (value >= (int64_t)ZEND_LONG_MIN &&
                value <= (int64_t)ZEND_LONG_MAX) {
                addLong(kkind, sb, lk, dk, (zend_long)value);
                return;
            }
            char buf[32];
            int len = snprintf(buf, sizeof(buf), "%" PRId64, value);
            addStrL(kkind, sb, lk, dk, buf, len);
        };
        auto addUnsigned = [&](int kkind, const std::string &sb, zend_long lk,
                               double dk, uint64_t value) {
            if (value <= (uint64_t)ZEND_LONG_MAX) {
                addLong(kkind, sb, lk, dk, (zend_long)value);
                return;
            }
            char buf[32];
            int len = snprintf(buf, sizeof(buf), "%" PRIu64, value);
            addStrL(kkind, sb, lk, dk, buf, len);
        };

        /* Reuse key buffer capacity. ColumnMap has no offset accessor to avoid GetAsColumn. */
        std::string str_key_buf;
        for (size_t i = 0; i < entry_count; ++i) {
            zend_long long_key = 0;
            double dbl_key = 0.0;
            int kkind = decodeKey(i, str_key_buf, long_key, dbl_key);

            if (value_code == Type::Code::String) {
                std::string_view vv = (*v_str_col)[i];
                addStrL(kkind, str_key_buf, long_key, dbl_key, vv.data(), vv.length());
            } else if (value_code == Type::Code::UInt64) {
                addUnsigned(kkind, str_key_buf, long_key, dbl_key,
                            (uint64_t)v_u64_col->At(i));
            } else if (value_code == Type::Code::Int64
                    || value_code == Type::Code::Int32 || value_code == Type::Code::UInt32
                    || value_code == Type::Code::Int16 || value_code == Type::Code::UInt16
                    || value_code == Type::Code::Int8  || value_code == Type::Code::UInt8) {
                switch (value_code) {
                    case Type::Code::Int64:
                        addSigned(kkind, str_key_buf, long_key, dbl_key,
                                  (int64_t)v_i64_col->At(i));
                        break;
                    case Type::Code::Int32:
                        addSigned(kkind, str_key_buf, long_key, dbl_key,
                                  (int64_t)v_i32_col->At(i));
                        break;
                    case Type::Code::UInt32:
                        addUnsigned(kkind, str_key_buf, long_key, dbl_key,
                                    (uint64_t)v_u32_col->At(i));
                        break;
                    case Type::Code::Int16:
                        addLong(kkind, str_key_buf, long_key, dbl_key,
                                (zend_long)v_i16_col->At(i));
                        break;
                    case Type::Code::UInt16:
                        addLong(kkind, str_key_buf, long_key, dbl_key,
                                (zend_long)v_u16_col->At(i));
                        break;
                    case Type::Code::Int8:
                        addLong(kkind, str_key_buf, long_key, dbl_key,
                                (zend_long)v_i8_col->At(i));
                        break;
                    case Type::Code::UInt8:
                        addLong(kkind, str_key_buf, long_key, dbl_key,
                                (zend_long)v_u8_col->At(i));
                        break;
                    default: break;
                }
            } else if (value_code == Type::Code::Float64 || value_code == Type::Code::Float32) {
                double dv = (value_code == Type::Code::Float64)
                    ? (double)v_f64_col->At(i)
                    : (double)v_f32_col->At(i);
                addDbl(kkind, str_key_buf, long_key, dbl_key, dv);
            } else if (value_code == Type::Code::UUID) {
                UUID u = v_uuid_col->At(i);
                char buf[37];
                int blen = format_uuid(u, (fetch_mode & SC_FETCH_UUID_WITH_DASHES) != 0,
                                       buf, sizeof(buf));
                addStrL(kkind, str_key_buf, long_key, dbl_key, buf, blen);
            } else {
                throw std::runtime_error("Map read: unsupported value type " + value_type_ref->GetName());
            }
        }

        /* Past every throwing step; ownership transfers to arr below. */
        map_guard.z = nullptr;
        if (is_array) {
            add_next_index_zval(arr, map_zv);
            ZVAL_UNDEF(map_zv);
        } else if (fetch_mode & SC_FETCH_ONE) {
            ZVAL_COPY_VALUE(arr, map_zv);
            ZVAL_UNDEF(map_zv);
        } else {
            add_assoc_zval_ex(arr, column_name.c_str(), column_name.length(), map_zv);
            ZVAL_UNDEF(map_zv);
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
