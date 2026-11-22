/*
 * test_wire_errors.c -- rejection paths of the reader & writer: malformed
 * Native bytes, column trees that disagree with their declared type, reader
 * misuse, and sweeps that fail one allocation or one sink write at a time.
 * Pure library, no server.
 */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHC_PROVIDE_STDLIB_ALLOC
#define CHC_IMPLEMENTATION
#include "clickhouse.h"

static int fail_count = 0;
static const char *current_test = "";

#include "test_common.h"

/* ---------------- byte builder ------------------------------------------- */

typedef struct { uint8_t d[1024]; size_t n; } wbuf;

static void w8(wbuf *b, uint8_t v)   { b->d[b->n++] = v; }
static void w32(wbuf *b, uint32_t v) { for (int i = 0; i < 4; i++) w8(b, (uint8_t) (v >> (8 * i))); }
static void w64(wbuf *b, uint64_t v) { for (int i = 0; i < 8; i++) w8(b, (uint8_t) (v >> (8 * i))); }

static void
wvar(wbuf *b, uint64_t v)
{
    while (v >= 0x80) { w8(b, (uint8_t) (v | 0x80)); v >>= 7; }
    w8(b, (uint8_t) v);
}

static void
wstr(wbuf *b, const char *s)
{
    size_t n = strlen(s);
    wvar(b, n);
    memcpy(b->d + b->n, s, n);
    b->n += n;
}

/* ---------------- readers ------------------------------------------------ */

typedef struct {
    test_mem_src src;
    chc_io       io;
    chc_in       in;
} reader;

static int
reader_open(reader *r, const chc_alloc *al, const wbuf *b, chc_err *err)
{
    test_mem_src_init(&r->src, &r->io, b->d, b->n);
    return chc_in_init(&r->in, &r->io, al, 0, err);
}

static chc_type *
type_of(const chc_alloc *al, const char *src)
{
    chc_type *t = NULL;
    chc_err err = {};
    if (chc_type_parse(src, strlen(src), al, &t, &err) != CHC_OK) {
        fprintf(stderr, "%s: FAIL: cannot parse \"%s\": %s\n",
                current_test, src, err.msg);
        fail_count++;
        return NULL;
    }
    return t;
}

/* Decode one column of `type_src` from `body` and assert the return code. */
static void
expect_col_read(const char *type_src, size_t n_rows, const wbuf *body, int want_rc)
{
    chc_alloc al = chc_alloc_stdlib();
    chc_type *t = type_of(&al, type_src);
    if (!t) return;

    reader r;
    chc_err err = {};
    chc_column *c = NULL;
    if (reader_open(&r, &al, body, &err) != CHC_OK) {
        fail_count++;
        chc_type_destroy(t, &al);
        return;
    }
    int rc = chc__col_read(&r.in, t, n_rows, &c, &err);
    if (rc != want_rc) {
        fprintf(stderr, "%s: FAIL: read %s rows=%zu rc=%d want %d err='%s'\n",
                current_test, type_src, n_rows, rc, want_rc, err.msg);
        fail_count++;
    }
    chc__column_destroy(c, &al);
    chc_in_free(&r.in);
    chc_type_destroy(t, &al);
}

/* ---------------- chc_in misuse & limits --------------------------------- */

static void
test_in_misuse(void)
{
    current_test = "in_misuse";
    chc_alloc al = chc_alloc_stdlib();
    wbuf b = {};
    w8(&b, 0x42);

    reader r;
    chc_err err = {};
    if (reader_open(&r, &al, &b, &err) != CHC_OK) { fail_count++; return; }

    CHECK(chc_in_submit(&r.in, "x", 1, &err) == CHC_ERR_USAGE);
    CHECK(chc__in_rewind(&r.in) == CHC_ERR_USAGE);      /* no checkpoint set */
    chc_in_free(&r.in);
}

static void
test_varint_and_string_limits(void)
{
    current_test = "varint_and_string_limits";
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {};

    wbuf b = {};
    for (int i = 0; i < 10; i++) w8(&b, 0x80);          /* never terminates */
    w8(&b, 0x00);
    reader r;
    if (reader_open(&r, &al, &b, &err) != CHC_OK) { fail_count++; return; }
    uint64_t v = 0;
    CHECK(chc__read_varuint(&r.in, &v, &err) == CHC_ERR_PROTOCOL);
    chc_in_free(&r.in);

    wbuf s = {};
    wvar(&s, CHC_MAX_STRING_SIZE + 1);
    if (reader_open(&r, &al, &s, &err) != CHC_OK) { fail_count++; return; }
    char *out = NULL;
    size_t out_len = 0;
    CHECK(chc__read_string(&r.in, &out, &out_len, &err) == CHC_ERR_PROTOCOL);
    chc_in_free(&r.in);
}

static bool g_cancel = false;
static int  cancel_cb(void *ud) { (void) ud; return g_cancel; }

/* Bypass path in chc__read_bytes: request larger than the staging buffer
 * reads straight into the caller's destination. */
static void
test_read_bytes_bypass(void)
{
    current_test = "read_bytes_bypass";
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {};
    uint8_t dst[512];

    wbuf b = {};
    for (size_t i = 0; i < sizeof b.d; i++) w8(&b, (uint8_t) i);

    /* Canceled before the first bypass read. */
    reader r;
    test_mem_src_init(&r.src, &r.io, b.d, b.n);
    r.io.check_cancel = cancel_cb;
    CHECK_OK(chc_in_init(&r.in, &r.io, &al, 16, &err), err);
    g_cancel = true;
    CHECK(chc__read_bytes(&r.in, dst, sizeof dst, &err) == CHC_ERR_CANCELLED);
    g_cancel = false;
    chc_in_free(&r.in);

    /* Source runs dry mid-bypass: short read, not a hang. */
    wbuf few = {};
    for (int i = 0; i < 40; i++) w8(&few, (uint8_t) i);
    test_mem_src_init(&r.src, &r.io, few.d, few.n);
    CHECK_OK(chc_in_init(&r.in, &r.io, &al, 16, &err), err);
    CHECK(chc__read_bytes(&r.in, dst, sizeof dst, &err) == CHC_ERR_EOF);
    chc_in_free(&r.in);

    /* Same underrun inside the staging-buffer loop (request fits the buffer). */
    test_mem_src_init(&r.src, &r.io, few.d, few.n);
    CHECK_OK(chc_in_init(&r.in, &r.io, &al, 4096, &err), err);
    CHECK(chc__read_bytes(&r.in, dst, sizeof dst, &err) == CHC_ERR_EOF);
    chc_in_free(&r.in);
out:
    return;
}

/* ---------------- chc_column_validate ------------------------------------ */

static void
test_validate(void)
{
    current_test = "validate";
    chc_err err = {};
    uint8_t  k1[2] = { 0, 1 };
    uint16_t k2[2] = { 0, 1 };
    uint32_t k4[2] = { 0, 1 };
    uint64_t k8[2] = { 0, 1 };
    chc_column dict = { .layout = CHC_COL_NOTHING, .n_rows = 2 };

    const struct { int key_size; const void *keys; } ok[] = {
        { 1, k1 }, { 2, k2 }, { 4, k4 }, { 8, k8 },
    };
    for (size_t i = 0; i < sizeof ok / sizeof *ok; i++) {
        chc_column c = { .layout = CHC_COL_LOW_CARDINALITY, .n_rows = 2,
                         .lc = { .key_size = ok[i].key_size, .keys = (void *) ok[i].keys,
                                 .dict = &dict, .dict_n = 2 } };
        CHECK(chc_column_validate(&c, &err) == CHC_OK);
    }

    chc_column bad = { .layout = CHC_COL_LOW_CARDINALITY, .n_rows = 2,
                       .lc = { .key_size = 3, .keys = k4, .dict = &dict, .dict_n = 2 } };
    CHECK(chc_column_validate(&bad, &err) == CHC_ERR_PROTOCOL);

    chc_column inner = { .layout = CHC_COL_NOTHING, .n_rows = 2 };
    chc_column nul = { .layout = CHC_COL_NULLABLE, .n_rows = 2,
                       .nullable = { .inner = &inner } };
    CHECK(chc_column_validate(&nul, &err) == CHC_OK);
}

static void
test_elem_size(void)
{
    current_test = "elem_size";
    chc_alloc al = chc_alloc_stdlib();
    static const struct { const char *src; size_t size; } cases[] = {
        { "Int256",        32 },
        { "UInt256",       32 },
        { "Decimal256(4)", 32 },
        { "FixedString(7)", 7 },
        { "String",         0 },
    };
    for (size_t i = 0; i < sizeof cases / sizeof *cases; i++) {
        chc_type *t = type_of(&al, cases[i].src);
        if (!t) continue;
        CHECK_EQ_U64(chc_type_elem_size(t), cases[i].size);
        chc_type_destroy(t, &al);
    }
}

/* ---------------- malformed column bodies -------------------------------- */

static void
test_col_read_rejects(void)
{
    current_test = "col_read_rejects";
    wbuf empty = {};

    /* Row counts that overflow the slab sizing before any byte is read. */
    expect_col_read("Int256", SIZE_MAX / 16, &empty, CHC_ERR_PROTOCOL);
    expect_col_read("String", SIZE_MAX / 4, &empty, CHC_ERR_PROTOCOL);
    expect_col_read("Array(Int32)", SIZE_MAX / 4, &empty, CHC_ERR_PROTOCOL);
    expect_col_read("Ring", SIZE_MAX / 4, &empty, CHC_ERR_PROTOCOL);

    /* Child counts the decoder cannot make sense of. */
    expect_col_read("Nullable(Int32, Int32)", 1, &empty, CHC_ERR_TYPE);
    expect_col_read("Array(Int32, Int32)", 1, &empty, CHC_ERR_TYPE);
    expect_col_read("Map(Int32)", 1, &empty, CHC_ERR_TYPE);
    expect_col_read("QBit", 1, &empty, CHC_ERR_TYPE);
    expect_col_read("LowCardinality(Int32, Int32)", 1, &empty, CHC_ERR_TYPE);
    expect_col_read("SimpleAggregateFunction", 1, &empty, CHC_ERR_TYPE);
    expect_col_read("Nested", 1, &empty, CHC_ERR_TYPE);
    expect_col_read("Variant(Int32)", 1, &empty, CHC_ERR_TYPE);

    /* String row longer than the wire cap. */
    wbuf long_row = {};
    wvar(&long_row, CHC_MAX_STRING_SIZE + 1);
    expect_col_read("String", 1, &long_row, CHC_ERR_PROTOCOL);

    /* SimpleAggregateFunction uses its storage type */
    wbuf u64row = {};
    w64(&u64row, 7);
    expect_col_read("SimpleAggregateFunction(sum, UInt64)", 1, &u64row, CHC_OK);
}

static void
test_low_cardinality_rejects(void)
{
    current_test = "low_cardinality_rejects";
    const uint64_t add_keys = 1ull << 9;
    wbuf b;

    /* Global dictionaries are not implemented. */
    b = (wbuf) {}; w64(&b, 1ull << 8);
    expect_col_read("LowCardinality(String)", 1, &b, CHC_ERR_PROTOCOL);

    /* Additional keys are the only supported dictionary source. */
    b = (wbuf) {}; w64(&b, 0);
    expect_col_read("LowCardinality(String)", 1, &b, CHC_ERR_PROTOCOL);

    /* Index width 4 is off the end of the 1/2/4/8-byte ladder. */
    b = (wbuf) {}; w64(&b, add_keys | 4);
    expect_col_read("LowCardinality(String)", 1, &b, CHC_ERR_PROTOCOL);

    /* Dictionary bigger than the row cap. */
    b = (wbuf) {}; w64(&b, add_keys); w64(&b, CHC_MAX_NUM_ROWS + 1);
    expect_col_read("LowCardinality(String)", 1, &b, CHC_ERR_PROTOCOL);

    /* Key count disagreeing with the block row count. */
    b = (wbuf) {};
    w64(&b, add_keys); w64(&b, 1); wstr(&b, "a"); w64(&b, 2);
    expect_col_read("LowCardinality(String)", 1, &b, CHC_ERR_PROTOCOL);

    /* Keys slab sizing overflows: 8-byte indices over a huge row count. */
    b = (wbuf) {};
    w64(&b, add_keys | 3); w64(&b, 1); wstr(&b, "a"); w64(&b, SIZE_MAX / 4);
    expect_col_read("LowCardinality(String)", SIZE_MAX / 4, &b, CHC_ERR_PROTOCOL);

    /* Every index width decodes. */
    for (unsigned idx = 0; idx < 4; idx++) {
        b = (wbuf) {};
        w64(&b, add_keys | idx); w64(&b, 1); wstr(&b, "a"); w64(&b, 1);
        for (unsigned i = 0; i < (1u << idx); i++) w8(&b, 0);
        expect_col_read("LowCardinality(String)", 1, &b, CHC_OK);
    }

    /* Empty LC columns carry no body; the dictionary layout still follows
     * the declared inner type. */
    wbuf none = {};
    expect_col_read("LowCardinality(String)", 0, &none, CHC_OK);
    expect_col_read("LowCardinality(UInt32)", 0, &none, CHC_OK);
    expect_col_read("LowCardinality(Array(Int32))", 0, &none, CHC_OK);

    /* Geo layer claiming more nested rows than the cap allows. */
    wbuf huge = {}; w64(&huge, CHC_MAX_NUM_ROWS + 1);
    expect_col_read("Ring", 1, &huge, CHC_ERR_PROTOCOL);

    /* Prefix carries a key version the decoder pins to 1. */
    chc_alloc al = chc_alloc_stdlib();
    chc_type *t = type_of(&al, "LowCardinality(String)");
    if (t) {
        wbuf ver = {}; w64(&ver, 2);
        reader r;
        chc_err err = {};
        if (reader_open(&r, &al, &ver, &err) == CHC_OK) {
            CHECK(chc__col_read_prefix(&r.in, t, &err) == CHC_ERR_PROTOCOL);
            chc_in_free(&r.in);
        } else
            fail_count++;
        chc_type_destroy(t, &al);
    }
}

/* ---------------- block header ------------------------------------------- */

static void
expect_block_read(const wbuf *b, const chc_block_opts *opts, int want_rc,
                  bool want_block)
{
    chc_alloc al = chc_alloc_stdlib();
    reader r;
    chc_err err = {};
    chc_block *blk = NULL;
    if (reader_open(&r, &al, b, &err) != CHC_OK) { fail_count++; return; }
    int rc = chc_block_read(&r.in, &al, opts, &blk, &err);
    if (rc != want_rc || (blk != NULL) != want_block) {
        fprintf(stderr, "%s: FAIL: block rc=%d want %d, blk=%p err='%s'\n",
                current_test, rc, want_rc, (void *) blk, err.msg);
        fail_count++;
    }
    chc_block_destroy(blk, &al);
    chc_in_free(&r.in);
}

static void
test_block_header_rejects(void)
{
    current_test = "block_header_rejects";
    chc_block_opts info = { .has_block_info = true };
    chc_block_opts custom = { .has_custom_serialization = true };
    chc_block_opts plain = {};
    wbuf b;

    /* Clean end of stream at a packet boundary is not an error. */
    b = (wbuf) {};
    expect_block_read(&b, &info, CHC_OK, false);

    b = (wbuf) {}; wvar(&b, 2);                         /* field 1 expected */
    expect_block_read(&b, &info, CHC_ERR_PROTOCOL, false);

    b = (wbuf) {}; wvar(&b, 1); w8(&b, 0); wvar(&b, 3);  /* field 2 expected */
    expect_block_read(&b, &info, CHC_ERR_PROTOCOL, false);

    b = (wbuf) {};
    wvar(&b, 1); w8(&b, 0); wvar(&b, 2); w32(&b, 0); wvar(&b, 9);
    expect_block_read(&b, &info, CHC_ERR_PROTOCOL, false);

    /* A well-formed BlockInfo followed by an empty block. */
    b = (wbuf) {};
    wvar(&b, 1); w8(&b, 1); wvar(&b, 2); w32(&b, 7); wvar(&b, 0);
    wvar(&b, 0); wvar(&b, 0);
    expect_block_read(&b, &info, CHC_OK, true);

    b = (wbuf) {}; wvar(&b, CHC_MAX_NUM_COLUMNS + 1); wvar(&b, 0);
    expect_block_read(&b, &plain, CHC_ERR_PROTOCOL, false);

    b = (wbuf) {}; wvar(&b, 1); wvar(&b, CHC_MAX_NUM_ROWS + 1);
    expect_block_read(&b, &plain, CHC_ERR_PROTOCOL, false);

    /* has_custom flag is 0 or 1 */
    b = (wbuf) {};
    wvar(&b, 1); wvar(&b, 1); wstr(&b, "c"); wstr(&b, "UInt8"); w8(&b, 2);
    expect_block_read(&b, &custom, CHC_ERR_PROTOCOL, false);

    b = (wbuf) {};
    wvar(&b, 1); wvar(&b, 1); wstr(&b, "c"); wstr(&b, "UInt8"); w8(&b, 0); w8(&b, 5);
    expect_block_read(&b, &custom, CHC_OK, true);
}

/* ---------------- writer -------------------------------------------------- */

typedef struct { size_t calls, fail_at; } fail_sink;

static int
fail_sink_write(void *ud, const void *buf, size_t n, chc_err *err)
{
    (void) buf; (void) n;
    fail_sink *s = ud;
    if (s->calls++ == s->fail_at)
        return chc__err_set(err, CHC_ERR_IO, "sink failure");
    return CHC_OK;
}

/* A column tree whose layout contradicts its declared type must be rejected
 * rather than serialized as garbage. */
static void
test_write_mismatch(void)
{
    current_test = "write_mismatch";
    chc_alloc al = chc_alloc_stdlib();
    test_mem_sink sink;
    chc_io io;
    chc_err err = {};

    uint64_t offs[2]  = { 1, 2 };
    uint8_t  data[2]  = { 'a', 'b' };
    uint8_t  nulls[2] = { 0, 0 };
    double   xy[2]    = { 1.0, 2.0 };

    chc_column fixed  = chc_build_fixed(xy, sizeof xy[0], 2);
    chc_column str    = chc_build_string(offs, data, 2);
    chc_column nul    = chc_build_nullable(nulls, &fixed);
    chc_column arr    = chc_build_array(offs, 2, &fixed);
    chc_column *pair[2] = { &fixed, &fixed };
    chc_column tup    = chc_build_tuple(pair, 2);
    chc_column arr_of_str = chc_build_array(offs, 2, &str);
    chc_column lc     = chc_build_lc(3, data, 2, &str);    /* 3 is not a width */
    chc_column nothing = { .layout = CHC_COL_NOTHING, .n_rows = 2 };

    static const char *const type_of_col[] = {
        "Int32", "String", "Nullable(Int32)", "Array(Int32)", "Tuple(Int32, Int32)",
        "Map(Int32, Int32)", "Map(Int32, Int32)", "QBit", "LowCardinality(String)",
        "LowCardinality(String)", "Point", "Ring",
        "Variant(Int32)", "SimpleAggregateFunction",
        "Nested(a Int32)", "Nested",
    };
    const chc_column *col_for[] = {
        &str, &fixed, &fixed, &fixed, &fixed,
        &fixed, &arr_of_str, &fixed, &str,
        &lc, &str, &fixed,
        &fixed, &fixed,
        &fixed, &arr,           /* wrong layout, then Nested without fields */
    };

    for (size_t i = 0; i < sizeof type_of_col / sizeof *type_of_col; i++) {
        chc_type *t = type_of(&al, type_of_col[i]);
        if (!t) continue;
        test_mem_sink_init(&sink, &io);
        int rc = chc__col_write(&io, col_for[i], t, &err);
        if (rc == CHC_OK) {
            fprintf(stderr, "%s: FAIL: %s accepted a mismatched column\n",
                    current_test, type_of_col[i]);
            fail_count++;
        }
        test_mem_sink_free(&sink);
        chc_type_destroy(t, &al);
    }

    /* Point with the right arity but non-FIXED coordinates. */
    chc_column *str_pair[2] = { &str, &str };
    chc_column str_tup = chc_build_tuple(str_pair, 2);
    chc_type *point = type_of(&al, "Point");
    if (point) {
        test_mem_sink_init(&sink, &io);
        CHECK(chc__col_write(&io, &str_tup, point, &err) == CHC_ERR_TYPE);
        test_mem_sink_free(&sink);
        chc_type_destroy(point, &al);
    }

    /* Nothing writes one zero byte per row, chunked. */
    chc_type *nt = type_of(&al, "Nothing");
    if (nt) {
        nothing.n_rows = 600;
        test_mem_sink_init(&sink, &io);
        CHECK(chc__col_write(&io, &nothing, nt, &err) == CHC_OK);
        CHECK_EQ_U64(sink.len, 600);
        test_mem_sink_free(&sink);
        chc_type_destroy(nt, &al);
    }
    (void) nul; (void) arr; (void) tup;
}

/* Write each shape once per sink call, failing that call: every write in the
 * encoder must propagate the error. */
static void
test_write_failure_sweep(void)
{
    current_test = "write_failure_sweep";
    chc_alloc al = chc_alloc_stdlib();

    uint64_t offs[2]   = { 1, 2 };
    uint64_t offs1[1]  = { 2 };
    uint8_t  data[2]   = { 'a', 'b' };
    uint8_t  nulls[2]  = { 0, 1 };
    uint8_t  keys[2]   = { 0, 1 };
    double   xy[4]     = { 1.0, 2.0, 3.0, 4.0 };
    uint8_t  planes[8] = {};

    chc_column f64    = chc_build_fixed(xy, sizeof xy[0], 2);
    chc_column str    = chc_build_string(offs, data, 2);
    chc_column nul    = chc_build_nullable(nulls, &str);
    chc_column arr    = chc_build_array(offs, 2, &str);
    chc_column *kv[2] = { &str, &str };
    chc_column kvtup  = chc_build_tuple(kv, 2);
    chc_column map    = chc_build_array(offs, 2, &kvtup);
    chc_column *pair[2] = { &f64, &f64 };
    chc_column tup    = chc_build_tuple(pair, 2);
    chc_column lc     = chc_build_lc(1, keys, 2, &str);
    chc_column point  = chc_build_tuple(pair, 2);
    chc_column ring   = chc_build_array(offs1, 1, &point);

    chc_column plane[16];
    chc_column *plane_ptr[16];
    for (size_t i = 0; i < 16; i++) {
        plane[i] = chc_build_fixed(planes, 2, 2);
        plane_ptr[i] = &plane[i];
    }
    chc_column qbit = chc_build_tuple(plane_ptr, 16);

    uint16_t keys2[2] = { 0, 1 };
    uint32_t keys4[2] = { 0, 1 };
    uint64_t keys8[2] = { 0, 1 };
    chc_column lc2 = chc_build_lc(2, keys2, 2, &str);
    chc_column lc4 = chc_build_lc(4, keys4, 2, &str);
    chc_column lc8 = chc_build_lc(8, keys8, 2, &str);
    chc_column empty = chc_build_fixed(NULL, 4, 0);

    static const struct { const char *type; const chc_column *col; } shapes[] = {
        { "String",                 NULL },
        { "Nullable(String)",       NULL },
        { "Array(String)",          NULL },
        { "Map(String, String)",    NULL },
        { "Tuple(Float64, Float64)",NULL },
        { "LowCardinality(String)", NULL },
        { "Point",                  NULL },
        { "Ring",                   NULL },
        { "QBit(BFloat16, 16)",     NULL },
        { "SimpleAggregateFunction(anyLast, String)", NULL },
        { "Nothing",                NULL },
        { "LowCardinality(String)", NULL },
        { "LowCardinality(String)", NULL },
        { "LowCardinality(String)", NULL },
        { "Int32",                  NULL },
    };
    const chc_column *cols[] = {
        &str, &nul, &arr, &map, &tup, &lc, &point, &ring, &qbit, &str, NULL,
        &lc2, &lc4, &lc8, &empty,
    };
    chc_column nothing = { .layout = CHC_COL_NOTHING, .n_rows = 2 };

    for (size_t i = 0; i < sizeof shapes / sizeof *shapes; i++) {
        chc_type *t = type_of(&al, shapes[i].type);
        if (!t) continue;
        const chc_column *c = cols[i] ? cols[i] : &nothing;
        for (size_t fail_at = 0; ; fail_at++) {
            fail_sink fs = { .fail_at = fail_at };
            chc_io io = { .ud = &fs, .write = fail_sink_write };
            chc_err err = {};
            int rc = chc__col_write(&io, c, t, &err);
            if (rc == CHC_OK) {
                if (fail_at >= fs.calls) break;         /* swept past the last write */
                continue;
            }
            if (rc != CHC_ERR_IO) {
                fprintf(stderr, "%s: FAIL: %s fail_at=%zu rc=%d err='%s'\n",
                        current_test, shapes[i].type, fail_at, rc, err.msg);
                fail_count++;
                break;
            }
        }
        chc_type_destroy(t, &al);
    }
}

/* Read paths that allocate: fail one allocation at a time & require the
 * unwind to report OOM without leaking. */
static void
test_read_oom_sweep(void)
{
    current_test = "read_oom_sweep";
    const uint64_t add_keys = 1ull << 9;

    wbuf lc = {};
    w64(&lc, add_keys); w64(&lc, 2); wstr(&lc, ""); wstr(&lc, "a");
    w64(&lc, 1); w8(&lc, 1);

    for (size_t fail_at = 0; ; fail_at++) {
        test_fail_alloc fa;
        chc_alloc al = test_fail_alloc_init(&fa, fail_at);
        chc_type *t = NULL;
        chc_err err = {};
        if (chc_type_parse("LowCardinality(Nullable(String))", 32, &al, &t, &err))
            continue;                               /* parser OOM covered elsewhere */
        reader r;
        chc_column *c = NULL;
        int rc = reader_open(&r, &al, &lc, &err);
        if (rc == CHC_OK) {
            rc = chc__col_read(&r.in, t, 1, &c, &err);
            chc__column_destroy(c, &al);
            chc_in_free(&r.in);
        }
        chc_type_destroy(t, &al);
        if (rc != CHC_OK && rc != CHC_ERR_OOM) {
            fprintf(stderr, "%s: FAIL: lc fail_at=%zu rc=%d err='%s'\n",
                    current_test, fail_at, rc, err.msg);
            fail_count++;
            break;
        }
        CHECK_EQ_U64(fa.live, 0);
        CHECK_EQ_U64(fa.live_bytes, 0);
        if (rc == CHC_OK && fail_at >= fa.calls) break;
    }

    wbuf blk = {};
    wvar(&blk, 1); wvar(&blk, 1); wstr(&blk, "c"); wstr(&blk, "UInt8"); w8(&blk, 5);
    for (size_t fail_at = 0; ; fail_at++) {
        test_fail_alloc fa;
        chc_alloc al = test_fail_alloc_init(&fa, fail_at);
        chc_err err = {};
        reader r;
        chc_block *b = NULL;
        chc_block_opts opts = {};
        int rc = reader_open(&r, &al, &blk, &err);
        if (rc == CHC_OK) {
            rc = chc_block_read(&r.in, &al, &opts, &b, &err);
            chc_block_destroy(b, &al);
            chc_in_free(&r.in);
        }
        if (rc != CHC_OK && rc != CHC_ERR_OOM) {
            fprintf(stderr, "%s: FAIL: block fail_at=%zu rc=%d err='%s'\n",
                    current_test, fail_at, rc, err.msg);
            fail_count++;
            break;
        }
        CHECK_EQ_U64(fa.live, 0);
        CHECK_EQ_U64(fa.live_bytes, 0);
        if (rc == CHC_OK && fail_at >= fa.calls) break;
    }
}

static void
test_block_write_long_type(void)
{
    current_test = "block_write_long_type";
    chc_alloc al = chc_alloc_stdlib();
    test_mem_sink sink;
    chc_io io;
    chc_err err = {};

    /* Valid type names retain their full spelling beyond 255 bytes. */
    char deep[600];
    size_t levels = 60, n = 0;
    for (size_t i = 0; i < levels; i++) n += (size_t) sprintf(deep + n, "Array(");
    n += (size_t) sprintf(deep + n, "Int32");
    for (size_t i = 0; i < levels; i++) deep[n++] = ')';
    deep[n] = '\0';

    chc_type *t = type_of(&al, deep);
    if (t) {
        chc_column leaf = chc_build_fixed(NULL, 4, 0);
        chc_column arr = chc_build_array(NULL, 0, &leaf);
        chc_block_col col = { .name = "c", .name_len = 1, .type = t, .col = &arr };
        test_mem_sink_init(&sink, &io);
        CHECK(chc_block_write_cols(&io, &col, 1, 0, NULL, &err) == CHC_OK);
        test_mem_src src;
        chc_io input;
        test_mem_src_init(&src, &input, sink.data, sink.len);
        chc_block_opts opts = {0};
        chc_block *block = NULL;
        CHECK(test_block_read_io(&input, &al, &opts, &block, &err) == CHC_OK);
        if (block) {
            char actual[sizeof deep];
            CHECK(chc_type_format(chc_block_column_type(block, 0), actual, sizeof actual) == n);
            CHECK(memcmp(actual, deep, n) == 0);
        }
        chc_block_destroy(block, &al);
        test_mem_sink_free(&sink);
        chc_type_destroy(t, &al);
    }
}

int
main(void)
{
    test_in_misuse();
    test_varint_and_string_limits();
    test_read_bytes_bypass();
    test_validate();
    test_elem_size();
    test_col_read_rejects();
    test_low_cardinality_rejects();
    test_block_header_rejects();
    test_write_mismatch();
    test_write_failure_sweep();
    test_read_oom_sweep();
    test_block_write_long_type();

    if (fail_count) {
        fprintf(stderr, "%d failure(s)\n", fail_count);
        return 1;
    }
    printf("all wire_errors tests passed\n");
    return 0;
}
