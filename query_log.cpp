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
#include "Zend/zend_exceptions.h"
#include "php7_wrapper.h"
}

#include "php_clickhouse.h"

#include "lib/clickhouse-cpp/clickhouse/client.h"
#include "lib/clickhouse-cpp/clickhouse/exceptions.h"
#include "clickhouse_internal.h"

#include <algorithm>
#include <cstring>
#include <string_view>
#include <unordered_map>

using namespace clickhouse;
using namespace std;

/* Bound retained memory even when getLogQueries() never drains the log. */
#define CLICKHOUSE_QUERY_LOG_MAX 1024
#define CLICKHOUSE_QUERY_LOG_STRING_MAX_BYTES 8192

std::string queryLogString(const std::string &value)
{
    if (value.size() <= CLICKHOUSE_QUERY_LOG_STRING_MAX_BYTES) {
        return value;
    }
    static const char suffix[] = "... (truncated)";
    constexpr size_t suffix_len = sizeof(suffix) - 1;
    constexpr size_t prefix_len = CLICKHOUSE_QUERY_LOG_STRING_MAX_BYTES - suffix_len;
    std::string out(value.data(), prefix_len);
    out.append(suffix, suffix_len);
    return out;
}

namespace {

bool isSqlWordChar(unsigned char c)
{
    return (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') ||
           (c >= 'a' && c <= 'z') || c == '_';
}

bool isHexDigitChar(unsigned char c)
{
    return (c >= '0' && c <= '9') || (c >= 'A' && c <= 'F') || (c >= 'a' && c <= 'f');
}

/* Past this many distinct `$tag$` spellings the heredoc index stops growing
 * and every heredoc opener is redacted through end of input. */
#define CLICKHOUSE_REDACT_MAX_HEREDOC_TAGS 1024

/* Token boundaries follow ClickHouse's Lexer::nextTokenImpl, including the
 * previous-significant-token state its number and '.' branches depend on: a
 * quote, heredoc, or comment opener inside another token must not start a new
 * one, or a single apostrophe would shift every later literal boundary. */
class SqlRedactor {
public:
    SqlRedactor(const std::string &sql, size_t max_bytes, bool distrust_braces = false)
        : sql_(sql), n_(sql.size()), max_(max_bytes), distrust_braces_(distrust_braces)
    {
        out_.reserve(std::min(n_, max_bytes));
    }

    std::string run()
    {
        size_t i = 0;
        while (i < n_ && !full()) {
            i = token(i);
        }
        return std::move(out_);
    }

private:
    /* After a `{name}` the template does not show whether it was substituted
     * (bare word or number) or left as braces (Other). MaybeDot adds Dot, for
     * a value such as `5.` that follows a Dot. dot() and number() fail closed
     * wherever these alternatives would lex differently. */
    enum class Kind { Other, Dot, Number, BareWord, QuotedIdent, CloseRound, CloseSquare,
                      MaybeBareWord, MaybeDot };

    const std::string &sql_;
    const size_t n_;
    const size_t max_;
    const bool distrust_braces_;
    std::string out_;
    Kind prev_ = Kind::Other;
    Kind cur_ = Kind::Other;
    bool significant_ = true;
    bool heredocs_indexed_ = false;
    bool heredoc_index_overflow_ = false;
    size_t next_close_brace_ = 0;
    size_t next_colon_ = 0;
    std::unordered_map<std::string_view, size_t> last_heredoc_tag_;

    bool full() const { return out_.size() >= max_; }

    void emit(const char *p, size_t len)
    {
        out_.append(p, std::min(len, max_ - out_.size()));
    }
    void emit(const char *s) { emit(s, strlen(s)); }
    void emitRange(size_t b, size_t e) { emit(sql_.data() + b, e - b); }

    bool digitAt(size_t p) const { return p < n_ && sql_[p] >= '0' && sql_[p] <= '9'; }
    bool hexDigitAt(size_t p) const { return p < n_ && isHexDigitChar((unsigned char)sql_[p]); }

    /* isNumberSeparator(): '_' between two digits, never first in a block. */
    bool separatorAt(bool start_of_block, bool hex, size_t p) const
    {
        if (sql_[p] != '_' || start_of_block || p + 1 >= n_) return false;
        return hex ? hexDigitAt(p + 1) : digitAt(p + 1);
    }

    size_t token(size_t i)
    {
        cur_ = Kind::Other;
        significant_ = true;
        size_t next = tokenAt(i);
        if (significant_) prev_ = cur_;
        return next;
    }

    /* Returns the offset just past the closing quote, or npos when the
     * quoted token runs past `limit`. */
    size_t quotedEnd(size_t i, char quote, size_t limit) const
    {
        size_t j = i + 1;
        while (j < limit) {
            char c = sql_[j];
            if (c == '\\') {
                if (j + 1 >= limit) return std::string::npos;
                j += 2;
            } else if (c == quote) {
                if (j + 1 < limit && sql_[j + 1] == quote) {
                    j += 2;
                } else {
                    return j + 1;
                }
            } else {
                ++j;
            }
        }
        return std::string::npos;
    }

    size_t tokenAt(size_t i)
    {
        unsigned char c = (unsigned char)sql_[i];
        switch (c) {
        case ' ': case '\t': case '\n': case '\r': case '\f': case '\v':
            significant_ = false;
            break;
        case '\'': {
            emit("'?'");
            if (full()) return n_;
            size_t e = quotedEnd(i, '\'', n_);
            return e == std::string::npos ? n_ : e;
        }
        case '"':
        case '`': {
            cur_ = Kind::QuotedIdent;
            size_t e = quotedEnd(i, (char)c, n_);
            if (e == std::string::npos) {
                char q[4] = {(char)c, '?', (char)c, '\0'};
                emit(q);
                return n_;
            }
            emitRange(i, e);
            return e;
        }
        case '{':
            return brace(i);
        case ')':
            cur_ = Kind::CloseRound;
            break;
        case ']':
            cur_ = Kind::CloseSquare;
            break;
        case '.':
            return dot(i);
        case '-':
            if (i + 1 < n_ && sql_[i + 1] == '-') return lineComment(i);
            break;
        case '/':
            if (i + 1 < n_ && sql_[i + 1] == '/') return lineComment(i);
            if (i + 1 < n_ && sql_[i + 1] == '*') return blockComment(i);
            break;
        case '#':
            if (i + 1 < n_ && (sql_[i + 1] == ' ' || sql_[i + 1] == '!')) return lineComment(i);
            break;
        case '\\':
            /* `\G` is one token; lexing the G as a bare word would turn a
             * following '.' into a Dot. */
            if (i + 1 < n_ && sql_[i + 1] == 'G') {
                emitRange(i, i + 2);
                return i + 2;
            }
            break;
        case '$':
            return dollar(i);
        case 0xE2:
            if (i + 5 < n_ && (unsigned char)sql_[i + 1] == 0x80 &&
                ((unsigned char)sql_[i + 2] == 0x98 || (unsigned char)sql_[i + 2] == 0x9C)) {
                return unicodeQuoted(i);
            }
            break;
        default:
            if ((c == 'x' || c == 'X' || c == 'b' || c == 'B') && i + 2 < n_ && sql_[i + 1] == '\'') {
                return hexOrBinString(i);
            }
            if (c >= '0' && c <= '9') return number(i);
            if (isSqlWordChar(c)) return bareWord(i);
            break;
        }
        if (c >= 0x80) {
            size_t w = utf8WhitespaceEnd(i);
            if (w > i) {
                significant_ = false;
                emitRange(i, w);
                return w;
            }
        }
        emitRange(i, i + 1);
        return i + 1;
    }

    /* skipWhitespacesUTF8(): U+0085 U+00A0 U+180E U+2000..U+200D U+2028
     * U+2029 U+202F U+205F U+2060 U+3000 U+FEFF, plus ASCII whitespace. */
    size_t utf8WhitespaceEnd(size_t p) const
    {
        while (p < n_) {
            const unsigned char *u = (const unsigned char *)sql_.data() + p;
            unsigned char a = u[0];
            if (a == ' ' || a == '\t' || a == '\n' || a == '\r' || a == '\f' || a == '\v') {
                ++p;
                continue;
            }
            if (p + 1 < n_ && a == 0xC2 && (u[1] == 0x85 || u[1] == 0xA0)) {
                p += 2;
                continue;
            }
            if (p + 2 < n_ &&
                ((a == 0xE1 && u[1] == 0xA0 && u[2] == 0x8E) ||
                 (a == 0xE2 && u[1] == 0x80 &&
                  ((u[2] >= 0x80 && u[2] <= 0x8D) || u[2] == 0xA8 || u[2] == 0xA9 || u[2] == 0xAF)) ||
                 (a == 0xE2 && u[1] == 0x81 && (u[2] == 0x9F || u[2] == 0xA0)) ||
                 (a == 0xE3 && u[1] == 0x80 && u[2] == 0x80) ||
                 (a == 0xEF && u[1] == 0xBB && u[2] == 0xBF))) {
                p += 3;
                continue;
            }
            break;
        }
        return p;
    }

    size_t lineComment(size_t i)
    {
        significant_ = false;
        size_t e = sql_.find('\n', i);
        if (e == std::string::npos) e = n_;
        commentBody(i, e);
        return e;
    }

    size_t blockComment(size_t i)
    {
        significant_ = false;
        size_t j = i + 2;
        size_t depth = 1;
        while (j + 2 <= n_) {
            if (sql_[j] == '/' && sql_[j + 1] == '*') {
                j += 2;
                ++depth;
            } else if (sql_[j] == '*' && sql_[j + 1] == '/') {
                j += 2;
                if (--depth == 0) {
                    commentBody(i, j - 2);
                    emitRange(j - 2, j);
                    return j;
                }
            } else {
                ++j;
            }
        }
        commentBody(i, n_);
        return n_;
    }

    /* Comment text stays readable, but quoted spans inside it are still
     * redacted (commented-out SQL often carries real values). */
    void commentBody(size_t b, size_t e)
    {
        size_t k = b;
        while (k < e && !full()) {
            if (sql_[k] == '\'') {
                emit("'?'");
                size_t q = quotedEnd(k, '\'', e);
                k = q == std::string::npos ? e : q;
                continue;
            }
            const void *q = memchr(sql_.data() + k, '\'', e - k);
            size_t next = q ? (size_t)((const char *)q - sql_.data()) : e;
            emitRange(k, next);
            k = next;
        }
    }

    size_t dollar(size_t i)
    {
        size_t k = i + 1;
        while (k < n_ && isSqlWordChar((unsigned char)sql_[k])) ++k;
        if (k < n_ && sql_[k] == '$') {
            std::string_view tag(sql_.data() + i, k + 1 - i);
            size_t close = heredocClose(tag, k + 1);
            if (close != std::string::npos) {
                emit(tag.data(), tag.size());
                emit("?");
                if (close >= n_) return n_;
                emit(tag.data(), tag.size());
                return close + tag.size();
            }
        }
        if (i + 1 >= n_ || !isSqlWordChar((unsigned char)sql_[i + 1])) {
            emitRange(i, i + 1);
            return i + 1;
        }
        return bareWord(i);
    }

    /* Index each `$tag$` by its last start so an unclosed opener costs O(1)
     * instead of a scan to end of input per opener. Returns npos when no
     * closer follows, or n_ (redact to end) once the index overflowed. */
    size_t heredocClose(std::string_view tag, size_t from)
    {
        if (!heredocs_indexed_) {
            heredocs_indexed_ = true;
            size_t p = sql_.find('$');
            while (p != std::string::npos) {
                size_t k = p + 1;
                while (k < n_ && isSqlWordChar((unsigned char)sql_[k])) ++k;
                if (k < n_ && sql_[k] == '$') {
                    std::string_view seg(sql_.data() + p, k + 1 - p);
                    auto it = last_heredoc_tag_.find(seg);
                    if (it != last_heredoc_tag_.end()) {
                        it->second = p;
                    } else if (last_heredoc_tag_.size() < CLICKHOUSE_REDACT_MAX_HEREDOC_TAGS) {
                        last_heredoc_tag_.emplace(seg, p);
                    } else {
                        heredoc_index_overflow_ = true;
                        std::unordered_map<std::string_view, size_t>().swap(last_heredoc_tag_);
                        break;
                    }
                }
                p = sql_.find('$', p + 1);
            }
        }
        if (heredoc_index_overflow_) return n_;
        auto it = last_heredoc_tag_.find(tag);
        if (it == last_heredoc_tag_.end() || it->second < from) return std::string::npos;
        return std::string_view(sql_).find(tag, from);
    }

    size_t unicodeQuoted(size_t i)
    {
        unsigned char open = (unsigned char)sql_[i + 2];
        char close = (char)(open + 1);
        bool is_string = open == 0x98;
        size_t j = i + 3;
        size_t e = std::string::npos;
        while (j + 2 < n_) {
            if ((unsigned char)sql_[j] == 0xE2 && (unsigned char)sql_[j + 1] == 0x80 &&
                sql_[j + 2] == close) {
                e = j + 3;
                break;
            }
            ++j;
        }
        if (is_string || e == std::string::npos) {
            emitRange(i, i + 3);
            emit("?");
            char tail[4] = {'\xE2', '\x80', close, '\0'};
            emit(tail);
            return e == std::string::npos ? n_ : e;
        }
        cur_ = Kind::QuotedIdent;
        emitRange(i, e);
        return e;
    }

    size_t hexOrBinString(size_t i)
    {
        bool hex = sql_[i] == 'x' || sql_[i] == 'X';
        size_t j = i + 2;
        while (j < n_ && (hex ? isHexDigitChar((unsigned char)sql_[j])
                              : (sql_[j] == '0' || sql_[j] == '1'))) {
            ++j;
        }
        emitRange(i, i + 1);
        emit("'?'");
        return (j < n_ && sql_[j] == '\'') ? j + 1 : n_;
    }

    /* Earliest offset >= from holding `c`, or npos; `cache` only moves forward
     * so repeated calls over one statement stay linear. */
    size_t nextByte(size_t from, char c, size_t &cache) const
    {
        if (cache != std::string::npos && cache < from) {
            const void *q = from < n_ ? memchr(sql_.data() + from, c, n_ - from) : nullptr;
            cache = q ? (size_t)((const char *)q - sql_.data()) : std::string::npos;
        }
        return cache;
    }

    size_t failClosed()
    {
        emit("?");
        return n_;
    }

    /* The log keeps the template, but the server lexes the substituted text.
     * A `{name}` value is a bare word or number, so it is modeled as one token;
     * where the value could merge with or change the lexing of a neighbor,
     * everything from the placeholder on is redacted. */
    size_t brace(size_t i)
    {
        size_t close = nextByte(i + 1, '}', next_close_brace_);
        if (close == std::string::npos || close == i + 1) {
            emitRange(i, i + 1);
            return i + 1;
        }
        if (distrust_braces_) return failClosed();
        bool word_key = true;
        for (size_t k = i + 1; k < close; ++k) {
            if (!isSqlWordChar((unsigned char)sql_[k])) {
                word_key = false;
                break;
            }
        }
        if (!word_key) {
            size_t colon = nextByte(i + 1, ':', next_colon_);
            if (colon != std::string::npos && colon < close) {
                emitRange(i, i + 1);
                return i + 1;
            }
            return failClosed();
        }
        bool after_dot = prev_ == Kind::Dot || prev_ == Kind::MaybeDot;
        if (i > 0) {
            unsigned char l = (unsigned char)sql_[i - 1];
            if (isSqlWordChar(l) || l == '$' || l == '\\' || l == '}' || (l == '.' && !after_dot)) {
                return failClosed();
            }
        }
        size_t r = close + 1;
        /* `{a}.{b}` may compose `1.e`, a number that takes an adjacent sign. */
        if (prev_ == Kind::Dot && i >= 2 && sql_[i - 1] == '.' && sql_[i - 2] == '}' &&
            r < n_ && (sql_[r] == '+' || sql_[r] == '-')) {
            return failClosed();
        }
        if (r < n_) {
            unsigned char c = (unsigned char)sql_[r];
            if (isSqlWordChar(c) || c == '$' || c == '\'' || c == '{') return failClosed();
            if (c == '.') {
                if (r + 1 >= n_) return failClosed();
                unsigned char d = (unsigned char)sql_[r + 1];
                if (d == '{') {
                    if (!wordPlaceholderAt(r + 1)) return failClosed();
                    emitRange(i, r);
                    cur_ = after_dot ? Kind::MaybeDot : Kind::MaybeBareWord;
                    return r;
                }
                bool ident = (d >= 'A' && d <= 'Z') || (d >= 'a' && d <= 'z') || d == '_';
                if (!ident || d == 'e' || d == 'E') return failClosed();
                size_t k = r + 2;
                while (k < n_ && isSqlWordChar((unsigned char)sql_[k])) ++k;
                if (k < n_ && (sql_[k] == '$' || sql_[k] == '\'')) return failClosed();
                emitRange(i, k);
                cur_ = Kind::MaybeBareWord;
                return k;
            }
        }
        emitRange(i, r);
        cur_ = after_dot ? Kind::MaybeDot : Kind::MaybeBareWord;
        return r;
    }

    bool wordPlaceholderAt(size_t p)
    {
        size_t close = nextByte(p + 1, '}', next_close_brace_);
        if (close == std::string::npos || close == p + 1) return false;
        for (size_t k = p + 1; k < close; ++k) {
            if (!isSqlWordChar((unsigned char)sql_[k])) return false;
        }
        return true;
    }

    /* A '.' is a Dot after an operand-like token, so `t.1e-5` reads 1e as a
     * tuple element; otherwise `.5e-3` is a number that owns its sign. */
    size_t dot(size_t i)
    {
        if (digitAt(i + 1) && (prev_ == Kind::MaybeBareWord || prev_ == Kind::MaybeDot)) {
            return failClosed();
        }
        if (i > 0 && (!digitAt(i + 1) || prev_ == Kind::CloseRound ||
                      prev_ == Kind::CloseSquare || prev_ == Kind::BareWord ||
                      prev_ == Kind::QuotedIdent || prev_ == Kind::Number ||
                      prev_ == Kind::MaybeBareWord || prev_ == Kind::MaybeDot)) {
            cur_ = Kind::Dot;
            emitRange(i, i + 1);
            return i + 1;
        }
        size_t p = i + 1;
        bool sob = true;
        while (p < n_ && (digitAt(p) || separatorAt(sob, false, p))) { ++p; sob = false; }
        if (p + 1 < n_ && (sql_[p] == 'e' || sql_[p] == 'E')) {
            sob = true;
            ++p;
            if (p + 1 < n_ && (sql_[p] == '-' || sql_[p] == '+')) ++p;
            while (p < n_ && (digitAt(p) || separatorAt(sob, false, p))) { ++p; sob = false; }
        }
        cur_ = Kind::Number;
        emitRange(i, p);
        return p;
    }

    size_t number(size_t i)
    {
        if (prev_ == Kind::MaybeDot) return failClosed();
        size_t p = i + 1;
        if (prev_ == Kind::Dot) {
            while (p < n_ && (digitAt(p) || separatorAt(false, false, p))) ++p;
        } else {
            bool sob = false;
            bool hex = false;
            if (i + 2 < n_ && sql_[i] == '0' &&
                (sql_[i + 1] == 'x' || sql_[i + 1] == 'X' || sql_[i + 1] == 'b' || sql_[i + 1] == 'B')) {
                bool valid = (sql_[i + 1] == 'x' || sql_[i + 1] == 'X')
                    ? hexDigitAt(i + 2)
                    : (sql_[i + 2] == '0' || sql_[i + 2] == '1');
                if (valid) {
                    hex = sql_[i + 1] == 'x' || sql_[i + 1] == 'X';
                    p = i + 2;
                    sob = true;
                }
            }
            while (p < n_ && ((hex ? hexDigitAt(p) : digitAt(p)) || separatorAt(sob, hex, p))) { ++p; sob = false; }
            if (p < n_ && sql_[p] == '.') {
                sob = true;
                ++p;
                while (p < n_ && ((hex ? hexDigitAt(p) : digitAt(p)) || separatorAt(sob, hex, p))) { ++p; sob = false; }
            }
            if (p + 1 < n_ && (hex ? (sql_[p] == 'p' || sql_[p] == 'P') : (sql_[p] == 'e' || sql_[p] == 'E'))) {
                sob = true;
                ++p;
                if (p + 1 < n_ && (sql_[p] == '-' || sql_[p] == '+')) ++p;
                while (p < n_ && (digitAt(p) || separatorAt(sob, false, p))) { ++p; sob = false; }
            }
        }
        cur_ = Kind::Number;
        /* `1abc` continues as a bare word; ErrorWrongNumber if it held '.', '+', '-'. */
        if (p < n_ && isSqlWordChar((unsigned char)sql_[p])) {
            ++p;
            while (p < n_ && isSqlWordChar((unsigned char)sql_[p])) ++p;
            cur_ = Kind::BareWord;
            for (size_t k = i; k < p; ++k) {
                if (!isSqlWordChar((unsigned char)sql_[k])) {
                    cur_ = Kind::Other;
                    break;
                }
            }
        }
        emitRange(i, p);
        return p;
    }

    size_t bareWord(size_t i)
    {
        cur_ = Kind::BareWord;
        size_t j = i + 1;
        while (j < n_ && (isSqlWordChar((unsigned char)sql_[j]) || sql_[j] == '$')) ++j;
        emitRange(i, j);
        return j;
    }
};

}  // namespace

/* Redact string literals, heredocs, and quoted text inside comments; keep
 * identifiers and comment text. Unterminated tokens are redacted through end
 * of input. Used for logs and traces, never wire SQL. */
static std::string redactSqlLiteralsBounded(const std::string &sql, size_t max_bytes,
                                            bool distrust_braces = false)
{
    return SqlRedactor(sql, max_bytes, distrust_braces).run();
}

std::string redactSqlLiterals(const std::string &sql)
{
    return redactSqlLiteralsBounded(sql, std::string::npos);
}

void appendQueryLogCapped(clickhouse_object *obj, QueryLog &&ql)
{
    if (obj->query_log.size() >= CLICKHOUSE_QUERY_LOG_MAX) {
        obj->query_log.pop_front();
    }
    obj->query_log.push_back(std::move(ql));
}

QueryLog buildQueryLog(const clickhouse_object *obj,
                       const std::string &sql, const std::string &qid,
                       bool distrust_braces)
{
    QueryLog ql;
    /* One byte beyond the cap distinguishes an exact fit from truncation.
     * Do not allocate or scan an unbounded redacted suffix just to discard it.
     * The unrestricted helper remains available for verbose callbacks. */
    ql.sql = queryLogString(redactSqlLiteralsBounded(
        sql, CLICKHOUSE_QUERY_LOG_STRING_MAX_BYTES + 1, distrust_braces));
    ql.query_id = queryLogString(qid);
    ql.elapsed_ms = obj->stats.elapsed_ms;
    ql.rows_read = obj->stats.rows_read;
    ql.bytes_read = obj->stats.bytes_read;
    return ql;
}

void recordQuerySuccess(clickhouse_object *obj, const std::string &sql, const std::string &qid)
{
    if (!obj->log_enabled) return;
    try {
        appendQueryLogCapped(obj, buildQueryLog(obj, sql, qid));
    } catch (...) { /* swallow; logging is best-effort */ }
}

void recordQueryError(clickhouse_object *obj, const std::string &sql, const std::string &qid, const std::exception &e)
{
    if (!obj->log_enabled) return;
    try {
        QueryLog ql = buildQueryLog(obj, sql, qid,
                                    dynamic_cast<const PlaceholderKeyError *>(&e) != nullptr);
        if (auto se = dynamic_cast<const clickhouse::ServerException*>(&e)) {
            ql.error_code = se->GetException().code;
        } else {
            ql.error_code = -1;
        }
        ql.error_message = sanitizeError(e.what());
        appendQueryLogCapped(obj, std::move(ql));
    } catch (...) { /* swallow; logging must not throw from inside a catch */ }
}

/* {{{ proto bool enableLogQueries(bool enabled = true)
 *
 * Toggle the query log accumulator. While enabled, each completed
 * select / insert / execute / writeStart appends an entry. Toggling
 * off does NOT clear; getLogQueries() returns and clears.
 */
PHP_METHOD(ClickHouse, enableLogQueries)
{
    zend_bool enabled = 1;
    ZEND_PARSE_PARAMETERS_START(0, 1)
        Z_PARAM_OPTIONAL
        Z_PARAM_BOOL(enabled)
    ZEND_PARSE_PARAMETERS_END();
    clickhouse_object *obj = Z_CLICKHOUSE_P(getThis());
    obj->log_enabled = (enabled != 0);
    RETURN_TRUE;
}
/* }}} */

/* {{{ proto array getLogQueries()
 *
 * Return all accumulated query log entries and clear the buffer. Each
 * entry is an associative array: sql, query_id, elapsed_ms, rows_read,
 * bytes_read, error_code (0 = success, server code on server error,
 * -1 on client/network error), error_message.
 */
PHP_METHOD(ClickHouse, getLogQueries)
{
    if (zend_parse_parameters_none() == FAILURE) return;
    clickhouse_object *obj = Z_CLICKHOUSE_P(getThis());
    array_init(return_value);
    for (const auto &ql : obj->query_log) {
        zval entry;
        array_init(&entry);
        add_assoc_stringl(&entry, "sql", (char*)ql.sql.c_str(), ql.sql.size());
        add_assoc_stringl(&entry, "query_id", (char*)ql.query_id.c_str(), ql.query_id.size());
        add_assoc_double(&entry, "elapsed_ms", ql.elapsed_ms);
        addAssocUInt64(&entry, "rows_read", ql.rows_read);
        addAssocUInt64(&entry, "bytes_read", ql.bytes_read);
        add_assoc_long(&entry, "error_code", (zend_long)ql.error_code);
        add_assoc_stringl(&entry, "error_message",
            (char*)ql.error_message.c_str(), ql.error_message.size());
        add_next_index_zval(return_value, &entry);
    }
    obj->query_log.clear();
}
/* }}} */

/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: noet sw=4 ts=4 fdm=marker
 * vim<600: noet sw=4 ts=4
 */
