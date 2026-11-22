/*
 * test_common.h -- shared CHECK macros for the test binaries.
 *
 * Each test .c keeps its own file-local storage:
 *   static int        fail_count   = 0;
 *   static const char *current_test = "";
 *
 * CHECK_OK jumps to a label named `out` in the caller.
 */

#ifndef CLICKHOUSE_TEST_COMMON_H
#define CLICKHOUSE_TEST_COMMON_H

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(cond) do {                                            \
    if (!(cond)) {                                                  \
        fprintf(stderr, "%s:%d %s: FAIL: %s\n",                     \
                __FILE__, __LINE__, current_test, #cond);           \
        fail_count++;                                               \
    }                                                               \
} while (0)

#define CHECK_EQ_U64(actual, expected) do {                         \
    uint64_t a = (uint64_t) (actual), b = (uint64_t) (expected);    \
    if (a != b) {                                                   \
        fprintf(stderr, "%s:%d %s: FAIL: %s == %" PRIu64            \
                " (got %" PRIu64 ")\n",                             \
                __FILE__, __LINE__, current_test, #actual, b, a);   \
        fail_count++;                                               \
    }                                                               \
} while (0)

#define CHECK_EQ_I64(actual, expected) do {                         \
    int64_t a = (int64_t) (actual), b = (int64_t) (expected);       \
    if (a != b) {                                                   \
        fprintf(stderr, "%s:%d %s: FAIL: %s == %" PRId64            \
                " (got %" PRId64 ")\n",                             \
                __FILE__, __LINE__, current_test, #actual, b, a);   \
        fail_count++;                                               \
    }                                                               \
} while (0)

#define CHECK_STR_EQ(actual, alen, expected) do {                   \
    size_t elen = strlen(expected);                                 \
    if ((alen) != elen || memcmp((actual), (expected), elen) != 0) {\
        fprintf(stderr, "%s:%d %s: FAIL: \"%.*s\" != \"%s\"\n",     \
                __FILE__, __LINE__, current_test,                   \
                (int)(alen), (actual), (expected));                 \
        fail_count++;                                               \
    }                                                               \
} while (0)

/* CHECK_OK: jumps to `out:` on failure. Pair with chc_err err = {};
 * and an out: label that frees / closes / returns. */
#define CHECK_OK(rc, err) do {                                      \
    if ((rc) != CHC_OK) {                                           \
        fprintf(stderr, "%s:%d %s: FAIL: rc=%d err='%s'\n",         \
                __FILE__, __LINE__, current_test, (rc), (err).msg); \
        fail_count++; goto out;                                     \
    }                                                               \
} while (0)

typedef struct test_mem_src {
    const uint8_t *data;
    size_t len;
    size_t pos;
} test_mem_src;

typedef struct test_mem_sink {
    uint8_t *data;
    size_t len;
    size_t cap;
} test_mem_sink;

CHC_MAYBE_UNUSED static int
test_mem_err_set(chc_err *err, int code, const char *msg)
{
    if (err) snprintf(err->msg, sizeof err->msg, "%s", msg);
    return code;
}

CHC_MAYBE_UNUSED static int
test_mem_read(void *ud, void *buf, size_t len, size_t *out_n, chc_err *err)
{
    (void) err;
    test_mem_src *m = ud;
    size_t avail = m->len - m->pos;
    size_t take = len < avail ? len : avail;
    if (take) memcpy(buf, m->data + m->pos, take);
    m->pos += take;
    *out_n = take;
    return CHC_OK;
}

CHC_MAYBE_UNUSED static void
test_mem_src_init(test_mem_src *src, chc_io *io, const void *data, size_t len)
{
    *src = (test_mem_src) { .data = data, .len = len };
    *io = (chc_io) { .ud = src, .read = test_mem_read };
}

/* Read one block via a throwaway chc_in over io -- for single-block sources.
 * Streaming callers own a persistent chc_in and call chc_block_read directly. */
CHC_MAYBE_UNUSED static int
test_block_read_io(chc_io *io, const chc_alloc *al, const chc_block_opts *opts,
                   chc_block **out, chc_err *err)
{
    chc_in in;
    int rc = chc_in_init(&in, io, al, opts->read_buffer_bytes, err);
    if (rc != CHC_OK) return rc;
    rc = chc_block_read(&in, al, opts, out, err);
    chc_in_free(&in);
    return rc;
}

CHC_MAYBE_UNUSED static int
test_mem_sink_write(void *ud, const void *buf, size_t n, chc_err *err)
{
    test_mem_sink *s = ud;
    if (n > SIZE_MAX - s->len)
        return test_mem_err_set(err, CHC_ERR_OOM, "mem sink size overflow");

    size_t need = s->len + n;
    if (need > s->cap) {
        size_t nc = s->cap ? s->cap : 256;
        while (nc < need) {
            if (nc > SIZE_MAX / 2) { nc = need; break; }
            nc *= 2;
        }
        uint8_t *nb = realloc(s->data, nc);
        if (!nb) return test_mem_err_set(err, CHC_ERR_OOM, "mem sink oom");
        s->data = nb;
        s->cap = nc;
    }
    if (n) memcpy(s->data + s->len, buf, n);
    s->len += n;
    return CHC_OK;
}

CHC_MAYBE_UNUSED static void
test_mem_sink_init(test_mem_sink *sink, chc_io *io)
{
    *sink = (test_mem_sink) {};
    *io = (chc_io) { .ud = sink, .write = test_mem_sink_write };
}

CHC_MAYBE_UNUSED static void
test_mem_sink_free(test_mem_sink *sink)
{
    free(sink->data);
    *sink = (test_mem_sink) {};
}

/* Allocator that fails the nth request, counting alloc & realloc. Drives
 * OOM sweeps: run an operation once per n and assert every partial unwind
 * returns the allocator to zero live bytes. */
typedef struct test_fail_alloc {
    size_t calls;
    size_t fail_at;             /* SIZE_MAX: never fail */
    size_t live;
    size_t live_bytes;
} test_fail_alloc;

CHC_MAYBE_UNUSED static void *
test_fail_alloc_alloc(void *ud, size_t n)
{
    test_fail_alloc *a = ud;
    if (a->calls++ == a->fail_at) return NULL;
    void *p = malloc(n ? n : 1);
    if (p) { a->live++; a->live_bytes += n; }
    return p;
}

CHC_MAYBE_UNUSED static void *
test_fail_alloc_realloc(void *ud, void *p, size_t old_n, size_t new_n)
{
    test_fail_alloc *a = ud;
    if (a->calls++ == a->fail_at) return NULL;
    void *q = realloc(p, new_n ? new_n : 1);
    if (!q) return NULL;
    if (!p) a->live++;
    a->live_bytes += new_n - old_n;
    return q;
}

CHC_MAYBE_UNUSED static void
test_fail_alloc_free(void *ud, void *p, size_t n)
{
    test_fail_alloc *a = ud;
    if (p) { a->live--; a->live_bytes -= n; }
    free(p);
}

CHC_MAYBE_UNUSED static chc_alloc
test_fail_alloc_init(test_fail_alloc *a, size_t fail_at)
{
    *a = (test_fail_alloc) { .fail_at = fail_at };
    return (chc_alloc) { a, test_fail_alloc_alloc,
                         test_fail_alloc_realloc, test_fail_alloc_free };
}

#endif
