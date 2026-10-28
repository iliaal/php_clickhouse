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

#include "clickhouse_c_internal.h"

/* Bound retained memory even when getLogQueries() never drains the log. */
#define CLICKHOUSE_QUERY_LOG_MAX 1024
#define CLICKHOUSE_QUERY_LOG_STRING_MAX_BYTES 8192

zend_string *ch_query_log_string(zend_string *value)
{
	if (!value) return zend_string_init("", 0, 0);
	if (ZSTR_LEN(value) <= CLICKHOUSE_QUERY_LOG_STRING_MAX_BYTES) return zend_string_copy(value);
	static const char suffix[] = "... (truncated)";
	const size_t prefix = CLICKHOUSE_QUERY_LOG_STRING_MAX_BYTES - (sizeof(suffix) - 1);
	zend_string *out = zend_string_alloc(CLICKHOUSE_QUERY_LOG_STRING_MAX_BYTES, 0);
	memcpy(ZSTR_VAL(out), ZSTR_VAL(value), prefix);
	memcpy(ZSTR_VAL(out) + prefix, suffix, sizeof(suffix));
	return out;
}

static bool ch_sql_word_char(unsigned char c)
{
	return (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') ||
		(c >= 'a' && c <= 'z') || c == '_';
}

static bool ch_hex_digit_char(unsigned char c)
{
	return (c >= '0' && c <= '9') || (c >= 'A' && c <= 'F') ||
		(c >= 'a' && c <= 'f');
}

/* Past this many distinct $tag$ spellings, redact each heredoc to the end. */
#define CLICKHOUSE_REDACT_MAX_HEREDOC_TAGS 1024

typedef enum {
	CH_TOKEN_Other, CH_TOKEN_Dot, CH_TOKEN_Number, CH_TOKEN_BareWord,
	CH_TOKEN_QuotedIdent, CH_TOKEN_CloseRound, CH_TOKEN_CloseSquare,
	CH_TOKEN_MaybeBareWord, CH_TOKEN_MaybeDot
} ch_token_kind;

/* Token boundaries mirror the original lexer-aware redactor; previous-token
 * alternatives account for text substituted into a logged SQL template. */
typedef struct {
	const char *sql;
	size_t length;
	size_t maximum;
	bool distrust_braces;
	smart_str out;
	ch_token_kind previous;
	ch_token_kind current;
	bool significant;
	bool heredocs_indexed;
	bool heredoc_index_overflow;
	size_t next_close_brace;
	size_t next_colon;
	HashTable heredoc_tags;
} ch_redactor;

static bool ch_redactor_full(ch_redactor *r)
{
	return r->out.s && ZSTR_LEN(r->out.s) >= r->maximum;
}

static void ch_redactor_emit(ch_redactor *r, const char *value, size_t length)
{
	size_t used = r->out.s ? ZSTR_LEN(r->out.s) : 0;
	size_t remaining = r->maximum - used;
	if (length > remaining) length = remaining;
	if (length) smart_str_appendl(&r->out, value, length);
}

static void ch_redactor_emits(ch_redactor *r, const char *value)
{
	ch_redactor_emit(r, value, strlen(value));
}

static void ch_redactor_emitRange(ch_redactor *r, size_t begin, size_t end)
{
	ch_redactor_emit(r, r->sql + begin, end - begin);
}

static bool ch_redactor_digitAt(ch_redactor *r, size_t p)
{
	return p < r->length && r->sql[p] >= '0' && r->sql[p] <= '9';
}

static bool ch_redactor_hexDigitAt(ch_redactor *r, size_t p)
{
	return p < r->length && ch_hex_digit_char((unsigned char)r->sql[p]);
}

static size_t ch_redactor_find_byte(ch_redactor *r, char byte, size_t from)
{
	const char *found = from < r->length ? memchr(r->sql + from, byte, r->length - from) : NULL;
	return found ? (size_t)(found - r->sql) : SIZE_MAX;
}

/* Store borrowed pointers into SQL, avoiding numeric narrowing on x86. */
static size_t ch_redactor_heredocClose(ch_redactor *r, const char *tag, size_t length, size_t from)
{
	if (!r->heredocs_indexed) {
		r->heredocs_indexed = true;
		zend_hash_init(&r->heredoc_tags, 0, NULL, NULL, 0);
		size_t p = ch_redactor_find_byte(r, '$', 0);
		while (p != SIZE_MAX) {
			size_t k = p + 1;
			while (k < r->length && ch_sql_word_char((unsigned char)r->sql[k])) ++k;
			if (k < r->length && r->sql[k] == '$') {
				size_t tag_length = k + 1 - p;
				if (zend_hash_str_exists(&r->heredoc_tags, r->sql + p, tag_length) ||
					zend_hash_num_elements(&r->heredoc_tags) < CLICKHOUSE_REDACT_MAX_HEREDOC_TAGS) {
					zend_hash_str_update_ptr(&r->heredoc_tags, r->sql + p, tag_length, (void *)(r->sql + p));
				} else {
					r->heredoc_index_overflow = true;
					zend_hash_clean(&r->heredoc_tags);
					break;
				}
			}
			p = ch_redactor_find_byte(r, '$', p + 1);
		}
	}
	if (r->heredoc_index_overflow) return r->length;
	const char *last = zend_hash_str_find_ptr(&r->heredoc_tags, tag, length);
	if (!last || (size_t)(last - r->sql) < from) return SIZE_MAX;
	size_t p = from;
	while (p <= r->length - length) {
		const char *found = memchr(r->sql + p, '$', r->length - length - p + 1);
		if (!found) break;
		p = (size_t)(found - r->sql);
		if (memcmp(found, tag, length) == 0) return p;
		++p;
	}
	return SIZE_MAX;
}

static size_t ch_redactor_dollar(ch_redactor *r, size_t i);

static bool ch_redactor_separatorAt(ch_redactor *r, bool start_of_block, bool hex, size_t p);
static size_t ch_redactor_token(ch_redactor *r, size_t i);
static size_t ch_redactor_quotedEnd(ch_redactor *r, size_t i, char quote, size_t limit);
static size_t ch_redactor_tokenAt(ch_redactor *r, size_t i);
static size_t ch_redactor_utf8WhitespaceEnd(ch_redactor *r, size_t p);
static size_t ch_redactor_lineComment(ch_redactor *r, size_t i);
static size_t ch_redactor_blockComment(ch_redactor *r, size_t i);
static void ch_redactor_commentBody(ch_redactor *r, size_t b, size_t e);
static size_t ch_redactor_unicodeQuoted(ch_redactor *r, size_t i);
static size_t ch_redactor_hexOrBinString(ch_redactor *r, size_t i);
static size_t ch_redactor_nextByte(ch_redactor *r, size_t from, char c, size_t *cache);
static size_t ch_redactor_failClosed(ch_redactor *r);
static size_t ch_redactor_brace(ch_redactor *r, size_t i);
static bool ch_redactor_wordPlaceholderAt(ch_redactor *r, size_t p);
static size_t ch_redactor_dot(ch_redactor *r, size_t i);
static size_t ch_redactor_number(ch_redactor *r, size_t i);
static size_t ch_redactor_bareWord(ch_redactor *r, size_t i);

static bool ch_redactor_separatorAt(ch_redactor *r, bool start_of_block, bool hex, size_t p)
{
	if (r->sql[p] != '_' || start_of_block || p + 1 >= r->length) return false;
	return hex ? ch_redactor_hexDigitAt(r, p + 1) : ch_redactor_digitAt(r, p + 1);
}

static size_t ch_redactor_token(ch_redactor *r, size_t i)
{
	r->current = CH_TOKEN_Other;
	r->significant = true;
	size_t next = ch_redactor_tokenAt(r, i);
	if (r->significant) r->previous = r->current;
	return next;
}

static size_t ch_redactor_quotedEnd(ch_redactor *r, size_t i, char quote, size_t limit)
{
	size_t j = i + 1;
	while (j < limit) {
		char c = r->sql[j];
		if (c == '\\') {
			if (j + 1 >= limit) return SIZE_MAX;
			j += 2;
		} else if (c == quote) {
			if (j + 1 < limit && r->sql[j + 1] == quote) {
				j += 2;
			} else {
				return j + 1;
			}
		} else {
			++j;
		}
	}
	return SIZE_MAX;
}

static size_t ch_redactor_tokenAt(ch_redactor *r, size_t i)
{
	unsigned char c = (unsigned char)r->sql[i];
	switch (c) {
	case ' ': case '\t': case '\n': case '\r': case '\f': case '\v':
		r->significant = false;
		break;
	case '\'': {
		ch_redactor_emits(r, "'?'");
		if (ch_redactor_full(r)) return r->length;
		size_t e = ch_redactor_quotedEnd(r, i, '\'', r->length);
		return e == SIZE_MAX ? r->length : e;
	}
	case '"':
	case '`': {
		r->current = CH_TOKEN_QuotedIdent;
		size_t e = ch_redactor_quotedEnd(r, i, (char)c, r->length);
		if (e == SIZE_MAX) {
			char q[4] = {(char)c, '?', (char)c, '\0'};
			ch_redactor_emits(r, q);
			return r->length;
		}
		ch_redactor_emitRange(r, i, e);
		return e;
	}
	case '{':
		return ch_redactor_brace(r, i);
	case ')':
		r->current = CH_TOKEN_CloseRound;
		break;
	case ']':
		r->current = CH_TOKEN_CloseSquare;
		break;
	case '.':
		return ch_redactor_dot(r, i);
	case '-':
		if (i + 1 < r->length && r->sql[i + 1] == '-') return ch_redactor_lineComment(r, i);
		break;
	case '/':
		if (i + 1 < r->length && r->sql[i + 1] == '/') return ch_redactor_lineComment(r, i);
		if (i + 1 < r->length && r->sql[i + 1] == '*') return ch_redactor_blockComment(r, i);
		break;
	case '#':
		if (i + 1 < r->length && (r->sql[i + 1] == ' ' || r->sql[i + 1] == '!')) return ch_redactor_lineComment(r, i);
		break;
	case '\\':
		/* `\G` is one token; lexing the G as a bare word would turn a
		 * following '.' into a Dot. */
		if (i + 1 < r->length && r->sql[i + 1] == 'G') {
			ch_redactor_emitRange(r, i, i + 2);
			return i + 2;
		}
		break;
	case '$':
		return ch_redactor_dollar(r, i);
	case 0xE2:
		if (i + 5 < r->length && (unsigned char)r->sql[i + 1] == 0x80 &&
			((unsigned char)r->sql[i + 2] == 0x98 || (unsigned char)r->sql[i + 2] == 0x9C)) {
			return ch_redactor_unicodeQuoted(r, i);
		}
		break;
	default:
		if ((c == 'x' || c == 'X' || c == 'b' || c == 'B') && i + 2 < r->length && r->sql[i + 1] == '\'') {
			return ch_redactor_hexOrBinString(r, i);
		}
		if (c >= '0' && c <= '9') return ch_redactor_number(r, i);
		if (ch_sql_word_char(c)) return ch_redactor_bareWord(r, i);
		break;
	}
	if (c >= 0x80) {
		size_t w = ch_redactor_utf8WhitespaceEnd(r, i);
		if (w > i) {
			r->significant = false;
			ch_redactor_emitRange(r, i, w);
			return w;
		}
	}
	ch_redactor_emitRange(r, i, i + 1);
	return i + 1;
}

static size_t ch_redactor_utf8WhitespaceEnd(ch_redactor *r, size_t p)
{
	while (p < r->length) {
		const unsigned char *u = (const unsigned char *)r->sql + p;
		unsigned char a = u[0];
		if (a == ' ' || a == '\t' || a == '\n' || a == '\r' || a == '\f' || a == '\v') {
			++p;
			continue;
		}
		if (p + 1 < r->length && a == 0xC2 && (u[1] == 0x85 || u[1] == 0xA0)) {
			p += 2;
			continue;
		}
		if (p + 2 < r->length &&
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

static size_t ch_redactor_lineComment(ch_redactor *r, size_t i)
{
	r->significant = false;
	size_t e = ch_redactor_find_byte(r, '\n', i);
	if (e == SIZE_MAX) e = r->length;
	ch_redactor_commentBody(r, i, e);
	return e;
}

static size_t ch_redactor_blockComment(ch_redactor *r, size_t i)
{
	r->significant = false;
	size_t j = i + 2;
	size_t depth = 1;
	while (j + 2 <= r->length) {
		if (r->sql[j] == '/' && r->sql[j + 1] == '*') {
			j += 2;
			++depth;
		} else if (r->sql[j] == '*' && r->sql[j + 1] == '/') {
			j += 2;
			if (--depth == 0) {
				ch_redactor_commentBody(r, i, j - 2);
				ch_redactor_emitRange(r, j - 2, j);
				return j;
			}
		} else {
			++j;
		}
	}
	ch_redactor_commentBody(r, i, r->length);
	return r->length;
}

static void ch_redactor_commentBody(ch_redactor *r, size_t b, size_t e)
{
	size_t k = b;
	while (k < e && !ch_redactor_full(r)) {
		if (r->sql[k] == '\'') {
			ch_redactor_emits(r, "'?'");
			size_t q = ch_redactor_quotedEnd(r, k, '\'', e);
			k = q == SIZE_MAX ? e : q;
			continue;
		}
		const void *q = memchr(r->sql + k, '\'', e - k);
		size_t next = q ? (size_t)((const char *)q - r->sql) : e;
		ch_redactor_emitRange(r, k, next);
		k = next;
	}
}

static size_t ch_redactor_unicodeQuoted(ch_redactor *r, size_t i)
{
	unsigned char open = (unsigned char)r->sql[i + 2];
	char close = (char)(open + 1);
	bool is_string = open == 0x98;
	size_t j = i + 3;
	size_t e = SIZE_MAX;
	while (j + 2 < r->length) {
		if ((unsigned char)r->sql[j] == 0xE2 && (unsigned char)r->sql[j + 1] == 0x80 &&
			r->sql[j + 2] == close) {
			e = j + 3;
			break;
		}
		++j;
	}
	if (is_string || e == SIZE_MAX) {
		ch_redactor_emitRange(r, i, i + 3);
		ch_redactor_emits(r, "?");
		char tail[4] = {'\xE2', '\x80', close, '\0'};
		ch_redactor_emits(r, tail);
		return e == SIZE_MAX ? r->length : e;
	}
	r->current = CH_TOKEN_QuotedIdent;
	ch_redactor_emitRange(r, i, e);
	return e;
}

static size_t ch_redactor_hexOrBinString(ch_redactor *r, size_t i)
{
	bool hex = r->sql[i] == 'x' || r->sql[i] == 'X';
	size_t j = i + 2;
	while (j < r->length && (hex ? ch_hex_digit_char((unsigned char)r->sql[j])
						  : (r->sql[j] == '0' || r->sql[j] == '1'))) {
		++j;
	}
	ch_redactor_emitRange(r, i, i + 1);
	ch_redactor_emits(r, "'?'");
	return (j < r->length && r->sql[j] == '\'') ? j + 1 : r->length;
}

static size_t ch_redactor_nextByte(ch_redactor *r, size_t from, char c, size_t *cache)
{
	if ((*cache) != SIZE_MAX && (*cache) < from) {
		const void *q = from < r->length ? memchr(r->sql + from, c, r->length - from) : NULL;
		(*cache) = q ? (size_t)((const char *)q - r->sql) : SIZE_MAX;
	}
	return (*cache);
}

static size_t ch_redactor_failClosed(ch_redactor *r)
{
	ch_redactor_emits(r, "?");
	return r->length;
}

static size_t ch_redactor_brace(ch_redactor *r, size_t i)
{
	size_t close = ch_redactor_nextByte(r, i + 1, '}', &r->next_close_brace);
	if (close == SIZE_MAX || close == i + 1) {
		ch_redactor_emitRange(r, i, i + 1);
		return i + 1;
	}
	if (r->distrust_braces) return ch_redactor_failClosed(r);
	bool word_key = true;
	for (size_t k = i + 1; k < close; ++k) {
		if (!ch_sql_word_char((unsigned char)r->sql[k])) {
			word_key = false;
			break;
		}
	}
	if (!word_key) {
		size_t colon = ch_redactor_nextByte(r, i + 1, ':', &r->next_colon);
		if (colon != SIZE_MAX && colon < close) {
			ch_redactor_emitRange(r, i, i + 1);
			return i + 1;
		}
		return ch_redactor_failClosed(r);
	}
	bool after_dot = r->previous == CH_TOKEN_Dot || r->previous == CH_TOKEN_MaybeDot;
	if (i > 0) {
		unsigned char l = (unsigned char)r->sql[i - 1];
		if (ch_sql_word_char(l) || l == '$' || l == '\\' || l == '}' || (l == '.' && !after_dot)) {
			return ch_redactor_failClosed(r);
		}
	}
	size_t right = close + 1;
	/* `{a}.{b}` may compose `1.e`, a number that takes an adjacent sign. */
	if (r->previous == CH_TOKEN_Dot && i >= 2 && r->sql[i - 1] == '.' && r->sql[i - 2] == '}' &&
		right < r->length && (r->sql[right] == '+' || r->sql[right] == '-')) {
		return ch_redactor_failClosed(r);
	}
	if (right < r->length) {
		unsigned char c = (unsigned char)r->sql[right];
		if (ch_sql_word_char(c) || c == '$' || c == '\'' || c == '{') return ch_redactor_failClosed(r);
		if (c == '.') {
			if (right + 1 >= r->length) return ch_redactor_failClosed(r);
			unsigned char d = (unsigned char)r->sql[right + 1];
			if (d == '{') {
				if (!ch_redactor_wordPlaceholderAt(r, right + 1)) return ch_redactor_failClosed(r);
				ch_redactor_emitRange(r, i, right);
				r->current = after_dot ? CH_TOKEN_MaybeDot : CH_TOKEN_MaybeBareWord;
				return right;
			}
			bool ident = (d >= 'A' && d <= 'Z') || (d >= 'a' && d <= 'z') || d == '_';
			if (!ident || d == 'e' || d == 'E') return ch_redactor_failClosed(r);
			size_t k = right + 2;
			while (k < r->length && ch_sql_word_char((unsigned char)r->sql[k])) ++k;
			if (k < r->length && (r->sql[k] == '$' || r->sql[k] == '\'')) return ch_redactor_failClosed(r);
			ch_redactor_emitRange(r, i, k);
			r->current = CH_TOKEN_MaybeBareWord;
			return k;
		}
	}
	ch_redactor_emitRange(r, i, right);
	r->current = after_dot ? CH_TOKEN_MaybeDot : CH_TOKEN_MaybeBareWord;
	return right;
}

static bool ch_redactor_wordPlaceholderAt(ch_redactor *r, size_t p)
{
	size_t close = ch_redactor_nextByte(r, p + 1, '}', &r->next_close_brace);
	if (close == SIZE_MAX || close == p + 1) return false;
	for (size_t k = p + 1; k < close; ++k) {
		if (!ch_sql_word_char((unsigned char)r->sql[k])) return false;
	}
	return true;
}

static size_t ch_redactor_dot(ch_redactor *r, size_t i)
{
	if (ch_redactor_digitAt(r, i + 1) && (r->previous == CH_TOKEN_MaybeBareWord || r->previous == CH_TOKEN_MaybeDot)) {
		return ch_redactor_failClosed(r);
	}
	if (i > 0 && (!ch_redactor_digitAt(r, i + 1) || r->previous == CH_TOKEN_CloseRound ||
				  r->previous == CH_TOKEN_CloseSquare || r->previous == CH_TOKEN_BareWord ||
				  r->previous == CH_TOKEN_QuotedIdent || r->previous == CH_TOKEN_Number ||
				  r->previous == CH_TOKEN_MaybeBareWord || r->previous == CH_TOKEN_MaybeDot)) {
		r->current = CH_TOKEN_Dot;
		ch_redactor_emitRange(r, i, i + 1);
		return i + 1;
	}
	size_t p = i + 1;
	bool sob = true;
	while (p < r->length && (ch_redactor_digitAt(r, p) || ch_redactor_separatorAt(r, sob, false, p))) { ++p; sob = false; }
	if (p + 1 < r->length && (r->sql[p] == 'e' || r->sql[p] == 'E')) {
		sob = true;
		++p;
		if (p + 1 < r->length && (r->sql[p] == '-' || r->sql[p] == '+')) ++p;
		while (p < r->length && (ch_redactor_digitAt(r, p) || ch_redactor_separatorAt(r, sob, false, p))) { ++p; sob = false; }
	}
	r->current = CH_TOKEN_Number;
	ch_redactor_emitRange(r, i, p);
	return p;
}

static size_t ch_redactor_number(ch_redactor *r, size_t i)
{
	if (r->previous == CH_TOKEN_MaybeDot) return ch_redactor_failClosed(r);
	size_t p = i + 1;
	if (r->previous == CH_TOKEN_Dot) {
		while (p < r->length && (ch_redactor_digitAt(r, p) || ch_redactor_separatorAt(r, false, false, p))) ++p;
	} else {
		bool sob = false;
		bool hex = false;
		if (i + 2 < r->length && r->sql[i] == '0' &&
			(r->sql[i + 1] == 'x' || r->sql[i + 1] == 'X' || r->sql[i + 1] == 'b' || r->sql[i + 1] == 'B')) {
			bool valid = (r->sql[i + 1] == 'x' || r->sql[i + 1] == 'X')
				? ch_redactor_hexDigitAt(r, i + 2)
				: (r->sql[i + 2] == '0' || r->sql[i + 2] == '1');
			if (valid) {
				hex = r->sql[i + 1] == 'x' || r->sql[i + 1] == 'X';
				p = i + 2;
				sob = true;
			}
		}
		while (p < r->length && ((hex ? ch_redactor_hexDigitAt(r, p) : ch_redactor_digitAt(r, p)) || ch_redactor_separatorAt(r, sob, hex, p))) { ++p; sob = false; }
		if (p < r->length && r->sql[p] == '.') {
			sob = true;
			++p;
			while (p < r->length && ((hex ? ch_redactor_hexDigitAt(r, p) : ch_redactor_digitAt(r, p)) || ch_redactor_separatorAt(r, sob, hex, p))) { ++p; sob = false; }
		}
		if (p + 1 < r->length && (hex ? (r->sql[p] == 'p' || r->sql[p] == 'P') : (r->sql[p] == 'e' || r->sql[p] == 'E'))) {
			sob = true;
			++p;
			if (p + 1 < r->length && (r->sql[p] == '-' || r->sql[p] == '+')) ++p;
			while (p < r->length && (ch_redactor_digitAt(r, p) || ch_redactor_separatorAt(r, sob, false, p))) { ++p; sob = false; }
		}
	}
	r->current = CH_TOKEN_Number;
	/* `1abc` continues as a bare word; ErrorWrongNumber if it held '.', '+', '-'. */
	if (p < r->length && ch_sql_word_char((unsigned char)r->sql[p])) {
		++p;
		while (p < r->length && ch_sql_word_char((unsigned char)r->sql[p])) ++p;
		r->current = CH_TOKEN_BareWord;
		for (size_t k = i; k < p; ++k) {
			if (!ch_sql_word_char((unsigned char)r->sql[k])) {
				r->current = CH_TOKEN_Other;
				break;
			}
		}
	}
	ch_redactor_emitRange(r, i, p);
	return p;
}

static size_t ch_redactor_bareWord(ch_redactor *r, size_t i)
{
	r->current = CH_TOKEN_BareWord;
	size_t j = i + 1;
	while (j < r->length && (ch_sql_word_char((unsigned char)r->sql[j]) || r->sql[j] == '$')) ++j;
	ch_redactor_emitRange(r, i, j);
	return j;
}

static size_t ch_redactor_dollar(ch_redactor *r, size_t i)
{
	size_t k = i + 1;
	while (k < r->length && ch_sql_word_char((unsigned char)r->sql[k])) ++k;
	if (k < r->length && r->sql[k] == '$') {
		const char *tag = r->sql + i;
		size_t length = k + 1 - i;
		size_t close = ch_redactor_heredocClose(r, tag, length, k + 1);
		if (close != SIZE_MAX) {
			ch_redactor_emit(r, tag, length);
			ch_redactor_emits(r, "?");
			if (close >= r->length) return r->length;
			ch_redactor_emit(r, tag, length);
			return close + length;
		}
	}
	if (i + 1 >= r->length || !ch_sql_word_char((unsigned char)r->sql[i + 1])) {
		ch_redactor_emitRange(r, i, i + 1);
		return i + 1;
	}
	return ch_redactor_bareWord(r, i);
}

/* Logs use a bounded prefix; verbose callbacks retain the entire redaction. */
static zend_string *ch_redact_sql_bounded(zend_string *sql, size_t maximum, bool distrust_braces)
{
	ch_redactor redactor = {0};
	redactor.sql = sql ? ZSTR_VAL(sql) : "";
	redactor.length = sql ? ZSTR_LEN(sql) : 0;
	redactor.maximum = maximum;
	redactor.distrust_braces = distrust_braces;
	size_t i = 0;
	while (i < redactor.length && !ch_redactor_full(&redactor)) i = ch_redactor_token(&redactor, i);
	if (redactor.heredocs_indexed) zend_hash_destroy(&redactor.heredoc_tags);
	smart_str_0(&redactor.out);
	return redactor.out.s ? redactor.out.s : zend_string_init("", 0, 0);
}

zend_string *ch_redact_sql_literals(zend_string *sql, bool distrust_braces)
{
	return ch_redact_sql_bounded(sql, SIZE_MAX, distrust_braces);
}

static void ch_log_entry(clickhouse_object *obj, zend_string *sql, zend_string *query_id,
	bool distrust_braces, zend_long error_code, zend_string *error_message)
{
	zend_string *redacted = ch_redact_sql_bounded(sql,
		CLICKHOUSE_QUERY_LOG_STRING_MAX_BYTES + 1, distrust_braces);
	zend_string *capped = ch_query_log_string(redacted);
	zend_string_release(redacted);
	zval entry;
	array_init(&entry);
	add_assoc_str(&entry, "sql", capped);
	add_assoc_str(&entry, "query_id", ch_query_log_string(query_id));
	add_assoc_double(&entry, "elapsed_ms", obj->stats.elapsed_ms);
	ch_add_assoc_uint64(&entry, "rows_read", obj->stats.rows_read);
	ch_add_assoc_uint64(&entry, "bytes_read", obj->stats.bytes_read);
	add_assoc_long(&entry, "error_code", error_code);
	if (error_message) add_assoc_str(&entry, "error_message", zend_string_copy(error_message));
	else add_assoc_stringl(&entry, "error_message", "", 0);
	if (zend_hash_num_elements(&obj->query_log) >= CLICKHOUSE_QUERY_LOG_MAX) {
		zend_ulong oldest;
		ZEND_HASH_FOREACH_NUM_KEY(&obj->query_log, oldest) {
			zend_hash_index_del(&obj->query_log, oldest);
			break;
		} ZEND_HASH_FOREACH_END();
	}
	zend_hash_index_update(&obj->query_log, obj->query_log_next++, &entry);
}

void ch_record_query_success(clickhouse_object *obj, zend_string *sql,
	zend_string *query_id, bool distrust_braces)
{
	if (!obj->log_enabled) return;
	ch_log_entry(obj, sql, query_id, distrust_braces, 0, NULL);
}

/* Read storage directly so logging an exception cannot invoke __get or hooks. */
static zval *ch_log_exception_property(HashTable *properties, const char *name, size_t length)
{
	zval *value = zend_hash_str_find(properties, name, length);
	if (value && Z_TYPE_P(value) == IS_INDIRECT) value = Z_INDIRECT_P(value);
	if (value) ZVAL_DEREF(value);
	return value;
}

static zend_string *ch_log_local_error_message(void)
{
	if (EG(exception)) {
#if PHP_VERSION_ID < 80000
		zval exception;
		ZVAL_OBJ(&exception, EG(exception));
		HashTable *properties = zend_std_get_properties(&exception);
#else
		HashTable *properties = zend_std_get_properties(EG(exception));
#endif
		zval *value = ch_log_exception_property(properties, ZEND_STRL("\0*\0message"));
		if (value && Z_TYPE_P(value) == IS_STRING) {
			return ch_sanitize_error(Z_STRVAL_P(value), Z_STRLEN_P(value));
		}
	}
	return NULL;
}

void ch_record_query_local_error(clickhouse_object *obj, zend_string *sql,
	zend_string *query_id, bool distrust_braces)
{
	if (!obj->log_enabled) return;
	zend_string *message = ch_log_local_error_message();
	ch_log_entry(obj, sql, query_id, distrust_braces, -1, message);
	if (message) zend_string_release(message);
}

void ch_record_query_error(clickhouse_object *obj, zend_string *sql,
	zend_string *query_id, bool distrust_braces)
{
	if (!obj->log_enabled) return;
	if (obj->failure_message) {
		ch_log_entry(obj, sql, query_id, distrust_braces, obj->failure_code, obj->failure_message);
	} else {
		ch_record_query_local_error(obj, sql, query_id, distrust_braces);
	}
}
