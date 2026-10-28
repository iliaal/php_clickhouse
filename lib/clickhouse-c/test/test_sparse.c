/*
 * test_sparse.c -- custom serialization descriptors & sparse column bodies
 * as servers send them from revision 54454 (23.3) or 54465 (current). Each
 * case decodes a sparse block & its dense equivalent and compares, resuming
 * byte by byte, over compressed frames, and failing each allocation. Pure
 * library, no server.
 */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHC_PROVIDE_STDLIB_ALLOC
#define CHC_IMPLEMENTATION
#define CHC_NO_ZSTD
#include "clickhouse.h"
#include "clickhouse-compression.h"
#include "clickhouse-client.h"

static int fail_count = 0;
static const char *current_test = "";

#include "test_common.h"
#include "test_block_compare.h"

#define END (UINT64_C(1) << 62)

typedef struct { uint8_t d[4096]; size_t n; } wbuf;

static void w8(wbuf *b, uint8_t v) { b->d[b->n++] = v; }
static void wle(wbuf *b, uint64_t v, int bytes) { for (int i = 0; i < bytes; i++) w8(b, (uint8_t) (v >> (8 * i))); }

static void
wvar(wbuf *b, uint64_t v)
{
    while (v >= 0x80) { w8(b, (uint8_t) (v | 0x80)); v >>= 7; }
    w8(b, (uint8_t) v);
}

static void
wmem(wbuf *b, const void *p, size_t n)
{
    memcpy(b->d + b->n, p, n);
    b->n += n;
}

static void
wstr(wbuf *b, const char *s)
{
    wvar(b, strlen(s));
    wmem(b, s, strlen(s));
}

static void
wblock(wbuf *b, uint64_t ncols, uint64_t nrows)
{
    wvar(b, 1); w8(b, 0); wvar(b, 2); wle(b, UINT32_MAX, 4); wvar(b, 0);
    wvar(b, ncols);
    wvar(b, nrows);
}

/* Column header, kinds NULL for has_custom = 0 */
static void
wcol(wbuf *b, const char *name, const char *type, const char *kinds)
{
    wstr(b, name);
    wstr(b, type);
    w8(b, kinds != NULL);
    for (const char *k = kinds; k && *k; k++) w8(b, (uint8_t) (*k - '0'));
}

static const chc_block_opts tcp = { .has_block_info = true, .has_custom_serialization = true };

static int
decode_io(const wbuf *b, const chc_alloc *al, chc_block **out, chc_err *err)
{
    test_mem_src src;
    chc_io io;
    test_mem_src_init(&src, &io, b->d, b->n);
    return test_block_read_io(&io, al, &tcp, out, err);
}

/* Ioless resume fed one byte at a time, counting would-blocks that kept a
 * completed earlier column */
static int
decode_bytewise(const wbuf *b, chc_block **out, int *retained, chc_err *err)
{
    chc_alloc al = chc_alloc_stdlib();
    chc_in in;
    if (chc_in_init_ioless(&in, &al)) return CHC_ERR_OOM;
    chc_block *blk = NULL;
    size_t next_col = 0, fed = 0;
    int rc;
    for (;;) {
        rc = chc__block_resume_in(&in, &al, &tcp, &blk, &next_col, err);
        if (rc != CHC_WOULD_BLOCK || fed == b->n) break;
        if (blk && next_col) (*retained)++;
        if ((rc = chc_in_submit(&in, b->d + fed++, 1, err))) break;
    }
    if (rc == CHC_WOULD_BLOCK) { chc_block_destroy(blk, &al); blk = NULL; }
    CHECK(rc != CHC_OK || chc_in_available(&in) == 0);
    chc_in_free(&in);
    *out = blk;
    return rc;
}

/* Compressed Data packet, one LZ4 frame per `frame` body bytes, decoded by
 * ioless client fed one byte at a time */
static int
decode_compressed(const wbuf *body, size_t frame, chc_block **out, chc_err *err)
{
    chc_alloc al = chc_alloc_stdlib();
    chc_codec codec;
    chc_lz4_codec_init(&codec);
    test_mem_sink pkt;
    chc_io pkt_io;
    test_mem_sink_init(&pkt, &pkt_io);
    int rc = chc__write_varuint(&pkt_io, CHC_PKT_DATA, err);
    if (rc == CHC_OK) rc = chc__write_string(&pkt_io, "", 0, err);
    for (size_t off = 0; rc == CHC_OK && off < body->n; off += frame) {
        size_t n = body->n - off < frame ? body->n - off : frame;
        rc = chc__comp_emit_chunks(&pkt_io, &codec, CHC_COMP_LZ4, body->d + off, n, &al, err);
    }
    chc_client c;
    memset(&c, 0, sizeof c);
    c.al = &al;
    c.compression = CHC_COMP_LZ4;
    c.codec = &codec;
    c.server.revision = CHC_CLIENT_REVISION;
    if (rc == CHC_OK) rc = chc_in_init_ioless(&c.in, &al);
    chc_packet p = {};
    for (size_t fed = 0; rc == CHC_OK; ) {
        rc = chc_client_recv_packet(&c, &p, err);
        if (rc != CHC_WOULD_BLOCK || fed == pkt.len) break;
        rc = chc_in_submit(&c.in, pkt.data + fed++, 1, err);
    }
    *out = rc == CHC_OK ? p.block : NULL;
    chc__client_recv_state_free(&c);
    chc_in_free(&c.in);
    test_mem_sink_free(&pkt);
    return rc;
}

/* Dense rewrite of decoded sparse column must reproduce dense bytes */
static void
check_rewrite(const chc_block *blk, const wbuf *dense)
{
    chc_block_col cols[4];
    size_t n = chc_block_n_columns(blk);
    for (size_t i = 0; i < n; i++) {
        cols[i].name = chc_block_column_name(blk, i, &cols[i].name_len);
        cols[i].type = chc_block_column_type(blk, i);
        cols[i].col = chc_block_column(blk, i);
    }
    test_mem_sink sink;
    chc_io io;
    chc_err err = {};
    test_mem_sink_init(&sink, &io);
    CHECK(chc_block_write_cols(&io, cols, n, chc_block_n_rows(blk), &tcp, &err) == CHC_OK);
    /* Writer emits BlockInfo bucket -1 like wblock */
    CHECK(sink.len == dense->n && memcmp(sink.data, dense->d, dense->n) == 0);
    test_mem_sink_free(&sink);
}

static void
check_oom(const wbuf *sparse)
{
    for (size_t fail_at = 0; ; fail_at++) {
        test_fail_alloc fa;
        chc_alloc fal = test_fail_alloc_init(&fa, fail_at);
        chc_block *blk = NULL;
        chc_err err = {};
        int rc = decode_io(sparse, &fal, &blk, &err);
        chc_block_destroy(blk, &fal);
        CHECK_EQ_U64(fa.live, 0);
        CHECK_EQ_U64(fa.live_bytes, 0);
        if (rc != CHC_OK && rc != CHC_ERR_OOM) {
            fprintf(stderr, "%s: FAIL: fail_at=%zu rc=%d err='%s'\n",
                    current_test, fail_at, rc, err.msg);
            fail_count++;
            return;
        }
        if (rc == CHC_OK) return;
    }
}

/* One column block of type, sparse body under kinds against dense body */
static void
expect_same(const char *what, const char *type, uint64_t nrows,
            const char *kinds, const wbuf *sparse_body, const wbuf *dense_body)
{
    current_test = what;
    chc_alloc al = chc_alloc_stdlib();
    wbuf sparse = {}, dense = {};
    wblock(&sparse, 1, nrows);
    wcol(&sparse, "c", type, kinds);
    wmem(&sparse, sparse_body->d, sparse_body->n);
    wblock(&dense, 1, nrows);
    wcol(&dense, "c", type, NULL);
    wmem(&dense, dense_body->d, dense_body->n);

    chc_block *want = NULL, *got = NULL;
    chc_err err = {};
    int retained = 0;
    CHECK(decode_io(&dense, &al, &want, &err) == CHC_OK);
    CHECK(decode_io(&sparse, &al, &got, &err) == CHC_OK);
    if (err.msg[0]) fprintf(stderr, "%s: %s\n", what, err.msg);
    CHECK(want && got && test_block_eq(want, got));
    if (got) check_rewrite(got, &dense);
    chc_block_destroy(got, &al);

    CHECK(decode_bytewise(&sparse, &got, &retained, &err) == CHC_OK);
    CHECK(want && got && test_block_eq(want, got));
    chc_block_destroy(got, &al);

    /* Frame boundary at every position within descriptor & body */
    wbuf body = sparse;
    for (size_t frame = 1; frame <= 5; frame++) {
        CHECK(decode_compressed(&body, frame, &got, &err) == CHC_OK);
        CHECK(want && got && test_block_eq(want, got));
        chc_block_destroy(got, &al);
    }
    chc_block_destroy(want, &al);
    check_oom(&sparse);
}

static void
test_fixed(void)
{
    wbuf s, d;

    /* [0, 7, 0, 0, 9] */
    s = (wbuf) {}; wvar(&s, 1); wvar(&s, 2); wvar(&s, END | 0); wle(&s, 7, 4); wle(&s, 9, 4);
    d = (wbuf) {}; wle(&d, 0, 4); wle(&d, 7, 4); wle(&d, 0, 4); wle(&d, 0, 4); wle(&d, 9, 4);
    expect_same("fixed_interior", "UInt32", 5, "1", &s, &d);

    s = (wbuf) {}; wvar(&s, END | 5);
    d = (wbuf) {}; for (int i = 0; i < 5; i++) wle(&d, 0, 4);
    expect_same("fixed_all_default", "UInt32", 5, "1", &s, &d);

    s = (wbuf) {}; for (int i = 0; i < 5; i++) wvar(&s, 0);
    wvar(&s, END | 0); for (int i = 1; i <= 5; i++) wle(&s, i, 4);
    d = (wbuf) {}; for (int i = 1; i <= 5; i++) wle(&d, i, 4);
    expect_same("fixed_all_non_default", "UInt32", 5, "1", &s, &d);

    /* [7, 0, 0, 0, 9] */
    s = (wbuf) {}; wvar(&s, 0); wvar(&s, 3); wvar(&s, END | 0); wle(&s, 7, 8); wle(&s, 9, 8);
    d = (wbuf) {}; wle(&d, 7, 8); for (int i = 0; i < 3; i++) wle(&d, 0, 8); wle(&d, 9, 8);
    expect_same("fixed_first_last", "Int64", 5, "1", &s, &d);

    /* [0, 0, 5, 0, 6, 0, 0, 0] trailing gap */
    s = (wbuf) {}; wvar(&s, 2); wvar(&s, 1); wvar(&s, END | 3); w8(&s, 5); w8(&s, 6);
    d = (wbuf) {}; w8(&d, 0); w8(&d, 0); w8(&d, 5); w8(&d, 0); w8(&d, 6);
    w8(&d, 0); w8(&d, 0); w8(&d, 0);
    expect_same("fixed_multiple_gaps", "UInt8", 8, "1", &s, &d);

    /* Gaps hold zero storage even though Enum default is first label */
    s = (wbuf) {}; wvar(&s, 1); wvar(&s, END | 1); w8(&s, 2);
    d = (wbuf) {}; w8(&d, 0); w8(&d, 2); w8(&d, 0);
    expect_same("enum_zero_gaps", "Enum8('a' = 1, 'b' = 2)", 3, "1", &s, &d);

    /* Semantic default sent as sparse value */
    s = (wbuf) {}; wvar(&s, 0); wvar(&s, END | 1); w8(&s, 1);
    d = (wbuf) {}; w8(&d, 1); w8(&d, 0);
    expect_same("enum_semantic_default", "Enum8('a' = 1, 'b' = 2)", 2, "1", &s, &d);

    s = (wbuf) {}; wvar(&s, 1); wvar(&s, END | 0); wmem(&s, "xyz", 3);
    d = (wbuf) {}; wmem(&d, "\0\0\0xyz", 6);
    expect_same("fixed_string", "FixedString(3)", 2, "1", &s, &d);

    s = (wbuf) {}; wvar(&s, 1); wvar(&s, END | 0); wle(&s, 6, 8);
    d = (wbuf) {}; wle(&d, 0, 8); wle(&d, 6, 8);
    expect_same("saf_sparse", "SimpleAggregateFunction(sum, UInt64)", 2, "1", &s, &d);

    s = (wbuf) {}; wvar(&s, END | 3);
    d = (wbuf) {}; w8(&d, 0); w8(&d, 0); w8(&d, 0);
    expect_same("nothing", "Nothing", 3, "1", &s, &d);

    /* has_custom set with every kind DEFAULT keeps dense body */
    s = (wbuf) {}; wle(&s, 3, 2); wle(&s, 4, 2);
    expect_same("custom_all_default", "UInt16", 2, "0", &s, &s);
}

static void
test_string(void)
{
    wbuf s, d;

    /* ["", "ab", "", "", "c\0d"] */
    s = (wbuf) {}; wvar(&s, 1); wvar(&s, 2); wvar(&s, END | 0);
    wstr(&s, "ab"); wvar(&s, 3); wmem(&s, "c\0d", 3);
    d = (wbuf) {}; wstr(&d, ""); wstr(&d, "ab"); wstr(&d, ""); wstr(&d, "");
    wvar(&d, 3); wmem(&d, "c\0d", 3);
    expect_same("string_interior", "String", 5, "1", &s, &d);

    s = (wbuf) {}; wvar(&s, END | 3);
    d = (wbuf) {}; wstr(&d, ""); wstr(&d, ""); wstr(&d, "");
    expect_same("string_all_default", "String", 3, "1", &s, &d);

    /* Empty non-default payload, only synthetic */
    s = (wbuf) {}; wvar(&s, 0); wvar(&s, 0); wvar(&s, END | 1); wstr(&s, "q"); wstr(&s, "");
    d = (wbuf) {}; wstr(&d, "q"); wstr(&d, ""); wstr(&d, "");
    expect_same("string_first_trailing", "String", 3, "1", &s, &d);
}

static void
test_tuple(void)
{
    wbuf s, d;

    /* Dense root, sparse UInt8 child, dense String child */
    s = (wbuf) {}; wvar(&s, 1); wvar(&s, END | 0); w8(&s, 4); wstr(&s, "a"); wstr(&s, "b");
    d = (wbuf) {}; w8(&d, 0); w8(&d, 4); wstr(&d, "a"); wstr(&d, "b");
    expect_same("tuple_sparse_child", "Tuple(UInt8, String)", 2, "010", &s, &d);

    /* Root SPARSE is metadata only, no outer offsets */
    s = (wbuf) {}; w8(&s, 1); w8(&s, 2); wvar(&s, END | 2);
    d = (wbuf) {}; w8(&d, 1); w8(&d, 2); wstr(&d, ""); wstr(&d, "");
    expect_same("tuple_root_sparse", "Tuple(a UInt8, b String)", 2, "101", &s, &d);

    s = (wbuf) {}; wvar(&s, END | 2); wle(&s, 1, 2); wle(&s, 2, 2);
    wvar(&s, 1); wvar(&s, END | 0); wstr(&s, "z");
    d = (wbuf) {}; w8(&d, 0); w8(&d, 0); wle(&d, 1, 2); wle(&d, 2, 2); wstr(&d, ""); wstr(&d, "z");
    expect_same("tuple_nested", "Tuple(UInt8, Tuple(UInt16, String))", 2, "11001", &s, &d);

    /* Point: Tuple(Float64, Float64) shape */
    s = (wbuf) {}; wvar(&s, 1); wvar(&s, END | 0); wle(&s, 0x4000000000000000u, 8);
    wle(&s, 1, 8); wle(&s, 2, 8);
    d = (wbuf) {}; wle(&d, 0, 8); wle(&d, 0x4000000000000000u, 8); wle(&d, 1, 8); wle(&d, 2, 8);
    expect_same("point_child", "Point", 2, "010", &s, &d);

    /* Tuple child under Array is not descended into */
    s = (wbuf) {}; wle(&s, 1, 8); w8(&s, 9); wstr(&s, "x");
    expect_same("array_tuple_one_kind", "Array(Tuple(UInt8, String))", 1, "0", &s, &s);
}

/* Sparse column read must fail with rc, io-backed & bytewise */
static void
expect_fail(const char *what, const wbuf *b, int want_io, const char *msg)
{
    current_test = what;
    chc_alloc al = chc_alloc_stdlib();
    chc_block *blk = NULL;
    chc_err err = {};
    int retained = 0;
    int rc = decode_io(b, &al, &blk, &err);
    if (rc != want_io || (msg && !strstr(err.msg, msg))) {
        fprintf(stderr, "%s: FAIL: rc=%d want %d err='%s'\n", what, rc, want_io, err.msg);
        fail_count++;
    }
    chc_block_destroy(blk, &al);
    int want_ioless = want_io == CHC_ERR_EOF ? CHC_WOULD_BLOCK : want_io;
    CHECK(decode_bytewise(b, &blk, &retained, &err) == want_ioless);
    chc_block_destroy(blk, &al);
}

static void
fail_col(const char *what, const char *type, uint64_t nrows, const char *kinds,
         const wbuf *body, int want, const char *msg)
{
    wbuf b = {};
    wblock(&b, 1, nrows);
    wcol(&b, "c", type, kinds);
    wmem(&b, body->d, body->n);
    expect_fail(what, &b, want, msg);
}

static void
test_rejects(void)
{
    wbuf e = {}, s;

    fail_col("array_root", "Array(UInt8)", 1, "1", &e, CHC_ERR_PROTOCOL, "unsupported");
    fail_col("nullable_root", "Nullable(UInt8)", 1, "1", &e, CHC_ERR_PROTOCOL, "unsupported");
    fail_col("lc_root", "LowCardinality(String)", 1, "1", &e, CHC_ERR_PROTOCOL, "unsupported");
    fail_col("map_root", "Map(String, UInt8)", 1, "1", &e, CHC_ERR_PROTOCOL, "unsupported");
    fail_col("json_root", "JSON", 1, "1", &e, CHC_ERR_PROTOCOL, "unsupported");
    fail_col("tuple_array_child", "Tuple(Array(UInt8))", 1, "01", &e, CHC_ERR_PROTOCOL, "unsupported");
    fail_col("detached_kind", "UInt8", 1, "2", &e, CHC_ERR_PROTOCOL, "kind 2");
    fail_col("combination_kind", "UInt8", 0, "5", &e, CHC_ERR_PROTOCOL, "kind 5");
    fail_col("truncated_child_kind", "Tuple(UInt8, UInt8)", 1, "01", &e, CHC_ERR_EOF, NULL);

    wbuf b = {};
    wblock(&b, 1, 1);
    wstr(&b, "c"); wstr(&b, "UInt8"); w8(&b, 2);
    expect_fail("flag_value", &b, CHC_ERR_PROTOCOL, "flag");

    s = (wbuf) {}; wvar(&s, UINT64_C(1) << 63);
    fail_col("bit63", "UInt8", 1, "1", &s, CHC_ERR_PROTOCOL, "overrun");

    s = (wbuf) {}; for (int i = 0; i < 10; i++) w8(&s, 0xff); w8(&s, 1);
    fail_col("varint_too_long", "UInt8", 1, "1", &s, CHC_ERR_PROTOCOL, "varint");

    s = (wbuf) {}; wvar(&s, 6);
    fail_col("gap_overrun", "UInt8", 5, "1", &s, CHC_ERR_PROTOCOL, "overrun");

    s = (wbuf) {}; wvar(&s, END | 6);
    fail_col("end_overrun", "UInt8", 5, "1", &s, CHC_ERR_PROTOCOL, "overrun");

    s = (wbuf) {}; wvar(&s, END | 2);
    fail_col("terminal_underrun", "UInt8", 5, "1", &s, CHC_ERR_PROTOCOL, "end at row 2");

    /* Non-default past last row */
    s = (wbuf) {}; wvar(&s, 0); wvar(&s, 0);
    fail_col("value_past_end", "UInt8", 1, "1", &s, CHC_ERR_PROTOCOL, "overrun");

    s = (wbuf) {}; wvar(&s, 0);
    fail_col("missing_end_marker", "UInt8", 1, "1", &s, CHC_ERR_EOF, NULL);

    s = (wbuf) {}; wvar(&s, 0); wvar(&s, END | 0); w8(&s, 1);
    fail_col("truncated_value", "UInt16", 1, "1", &s, CHC_ERR_EOF, NULL);

    /* Host size bound checked before any read */
    current_test = "size_overflow";
    chc_alloc al = chc_alloc_stdlib();
    chc_type *t = NULL;
    chc_err err = {};
    chc_in in;
    chc_column *c = NULL;
    if (chc_type_parse("UInt8", 5, &al, &t, &err) || chc_in_init_ioless(&in, &al)) {
        fail_count++;
        return;
    }
    CHECK(chc__col_read_sparse(&in, t, SIZE_MAX / 4, &c, &err) == CHC_ERR_PROTOCOL);
    CHECK(c == NULL);
    chc_in_free(&in);
    chc_type_destroy(t, &al);
}

/* Zero-row column carries kinds only, next column & block stay aligned */
static void
test_zero_rows(void)
{
    current_test = "zero_rows";
    chc_alloc al = chc_alloc_stdlib();
    wbuf b = {};
    wblock(&b, 2, 0);
    wcol(&b, "s", "Tuple(UInt8, String)", "111");
    wcol(&b, "t", "UInt8", NULL);
    wblock(&b, 1, 1);
    wcol(&b, "u", "UInt8", "1");
    wvar(&b, 0); wvar(&b, END | 0); w8(&b, 42);

    test_mem_src src;
    chc_io io;
    chc_in in;
    chc_err err = {};
    test_mem_src_init(&src, &io, b.d, b.n);
    if (chc_in_init(&in, &io, &al, 0, &err)) { fail_count++; return; }
    chc_block *blk = NULL;
    CHECK(chc_block_read(&in, &al, &tcp, &blk, &err) == CHC_OK);
    CHECK_EQ_U64(chc_block_n_columns(blk), 2);
    CHECK(chc_block_column(blk, 0) == NULL);
    chc_block_destroy(blk, &al);
    CHECK(chc_block_read(&in, &al, &tcp, &blk, &err) == CHC_OK);
    const chc_column *c = chc_block_column(blk, 0);
    CHECK(c && ((const uint8_t *) chc_column_fixed_data(c, NULL))[0] == 42);
    chc_block_destroy(blk, &al);
    chc_in_free(&in);
}

/* Completed dense column survives would-block inside following sparse one */
static void
test_resume_retains(void)
{
    current_test = "resume_retains";
    chc_alloc al = chc_alloc_stdlib();
    wbuf b = {};
    wblock(&b, 2, 3);
    wcol(&b, "a", "UInt8", NULL);
    w8(&b, 1); w8(&b, 2); w8(&b, 3);
    wcol(&b, "b", "String", "1");
    wvar(&b, 2); wvar(&b, END | 0); wstr(&b, "tail");
    chc_block *blk = NULL;
    chc_err err = {};
    int retained = 0;
    CHECK(decode_bytewise(&b, &blk, &retained, &err) == CHC_OK);
    CHECK(retained > 0);
    const chc_column *c = chc_block_column(blk, 1);
    CHECK(c && chc_column_string_offsets(c)[2] == 4);
    chc_block_destroy(blk, &al);
}

int
main(void)
{
    test_fixed();
    test_string();
    test_tuple();
    test_rejects();
    test_zero_rows();
    test_resume_retains();

    if (fail_count) {
        fprintf(stderr, "%d failure(s)\n", fail_count);
        return 1;
    }
    printf("all sparse tests passed\n");
    return 0;
}
