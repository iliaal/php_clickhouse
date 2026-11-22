/*
 * test_client_errors.c -- handshake, send & recv rejection paths of
 * clickhouse-client.h plus the async wrappers, driven over hand-seeded
 * chc_client state and memory transports. No server.
 */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHC_PROVIDE_STDLIB_ALLOC
#define CHC_IMPLEMENTATION
#define CHC_NO_ZSTD                 /* lz4 alone covers the compressed paths */
#include "clickhouse.h"
#include "clickhouse-compression.h"
#include "clickhouse-client.h"
#include "clickhouse-async.h"

static int fail_count = 0;
static const char *current_test = "";

#include "test_common.h"

#define REV_MODERN CHC_CLIENT_REVISION

/* ---------------- byte builder ------------------------------------------- */

typedef struct { uint8_t d[8192]; size_t n; } wbuf;

static void w8(wbuf *b, uint8_t v) { b->d[b->n++] = v; }

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

static void
wbytes(wbuf *b, const void *p, size_t n)
{
    memcpy(b->d + b->n, p, n);
    b->n += n;
}

/* Server-side exception frame: code, name, text, stack, has_nested. */
static void
wexception(wbuf *b)
{
    uint8_t code[4] = { 42, 0, 0, 0 };
    wbytes(b, code, 4);
    wstr(b, "DB::Exception");
    wstr(b, "boom");
    wstr(b, "0. boom()");
    w8(b, 0);
}

/* ---------------- hand-seeded client ------------------------------------- */

typedef struct {
    chc_client    c;
    test_mem_src  src;
    chc_io        rio;
    test_mem_sink sink;
    chc_io        wio;
} fake;

static int
fake_up(fake *f, const chc_alloc *al, const wbuf *b, uint64_t revision,
        chc_err *err)
{
    memset(f, 0, sizeof *f);
    test_mem_src_init(&f->src, &f->rio, b->d, b->n);
    test_mem_sink_init(&f->sink, &f->wio);
    f->c.al = al;
    f->c.io = &f->wio;
    f->c.compression = CHC_COMP_NONE;
    f->c.server.revision = revision;
    return chc_in_init(&f->c.in, &f->rio, al, 0, err);
}

static void
fake_down(fake *f)
{
    chc__client_recv_state_free(&f->c);
    chc_in_free(&f->c.in);
    test_mem_sink_free(&f->sink);
}

/* ---------------- handshake ---------------------------------------------- */

static void
test_handshake_rejects(void)
{
    current_test = "handshake_rejects";
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {};
    fake f;
    wbuf b;

    b = (wbuf) {}; wvar(&b, CHC_PKT_PONG);
    if (fake_up(&f, &al, &b, REV_MODERN, &err) == CHC_OK) {
        CHECK(chc__client_recv_hello(&f.c, NULL, &err) == CHC_ERR_PROTOCOL);
        CHECK(strstr(err.msg, "expected Hello") != NULL);
        fake_down(&f);
    } else
        fail_count++;

    b = (wbuf) {}; wvar(&b, CHC_PKT_HELLO);
    if (fake_up(&f, &al, &b, REV_MODERN, &err) == CHC_OK) {
        CHECK(chc__recv_pong(&f.c, NULL, &err) == CHC_ERR_PROTOCOL);
        CHECK(strstr(err.msg, "expected Pong") != NULL);
        fake_down(&f);
    } else
        fail_count++;

    /* Exception in place of Pong, once discarded and once handed back. */
    b = (wbuf) {}; wvar(&b, CHC_PKT_EXCEPTION); wexception(&b);
    if (fake_up(&f, &al, &b, REV_MODERN, &err) == CHC_OK) {
        CHECK(chc__recv_pong(&f.c, NULL, &err) == CHC_ERR_SERVER);
        fake_down(&f);
    } else
        fail_count++;

    if (fake_up(&f, &al, &b, REV_MODERN, &err) == CHC_OK) {
        chc_exception *e = NULL;
        CHECK(chc__recv_pong(&f.c, &e, &err) == CHC_ERR_SERVER);
        CHECK(e != NULL);
        if (e) CHECK_EQ_I64(e->code, 42);
        chc_exception_free(e, &al);
        fake_down(&f);
    } else
        fail_count++;

    /* Truncated exception body propagates the read error. */
    b = (wbuf) {}; wvar(&b, CHC_PKT_EXCEPTION); w8(&b, 1);
    if (fake_up(&f, &al, &b, REV_MODERN, &err) == CHC_OK) {
        CHECK(chc__recv_pong(&f.c, NULL, &err) == CHC_ERR_EOF);
        fake_down(&f);
    } else
        fail_count++;
}

/* ---------------- send side ---------------------------------------------- */

static void
test_send_paths(void)
{
    current_test = "send_paths";
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {};
    wbuf none = {};
    fake f;

    if (fake_up(&f, &al, &none, REV_MODERN, &err) == CHC_OK) {
        CHECK(chc_client_send_ping(&f.c, &err) == CHC_OK);
        CHECK(chc_client_send_cancel(&f.c, &err) == CHC_OK);
        CHECK_EQ_U64(f.sink.len, 2);

        /* Compression on without a codec is caller error, not a crash. */
        f.c.compression = CHC_COMP_LZ4;
        f.c.codec = NULL;
        CHECK(chc_client_send_data(&f.c, NULL, &err) == CHC_ERR_USAGE);
        f.c.compression = CHC_COMP_NONE;
        fake_down(&f);
    } else
        fail_count++;
}

/* ---------------- recv side ---------------------------------------------- */

static void
expect_packet(const char *what, const wbuf *b, uint64_t revision,
              int want_rc, chc_packet_kind want_kind)
{
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {};
    fake f;
    if (fake_up(&f, &al, b, revision, &err) != CHC_OK) { fail_count++; return; }

    chc_packet pkt = {};
    int rc = chc_client_recv_packet(&f.c, &pkt, &err);
    if (rc != want_rc || (rc == CHC_OK && pkt.kind != want_kind)) {
        fprintf(stderr, "%s: FAIL: %s rc=%d want %d kind=%d want %d err='%s'\n",
                current_test, what, rc, want_rc, (int) pkt.kind, (int) want_kind,
                err.msg);
        fail_count++;
    }
    chc_packet_clear(&f.c, &pkt);
    fake_down(&f);
}

static void
wempty_block(wbuf *b)
{
    wvar(b, 1); w8(b, 0); wvar(b, 2);
    for (int i = 0; i < 4; i++) w8(b, 0);
    wvar(b, 0);
    wvar(b, 0);                                 /* n_cols */
    wvar(b, 0);                                 /* n_rows */
}

static void
test_recv_packets(void)
{
    current_test = "recv_packets";
    wbuf b;

    b = (wbuf) {}; wvar(&b, 99);
    expect_packet("unknown kind", &b, REV_MODERN, CHC_ERR_PROTOCOL, CHC_PKT_HELLO);

    /* Block-bearing kinds each commit their own recv_kind. */
    static const chc_packet_kind blockish[] = {
        CHC_PKT_TOTALS, CHC_PKT_EXTREMES, CHC_PKT_LOG, CHC_PKT_PROFILE_EVENTS,
    };
    for (size_t i = 0; i < sizeof blockish / sizeof *blockish; i++) {
        b = (wbuf) {};
        wvar(&b, blockish[i]);
        wstr(&b, "");                           /* temp table name / log tag */
        wempty_block(&b);
        expect_packet("block-bearing", &b, REV_MODERN, CHC_OK, blockish[i]);
    }

    /* Truncated ProfileInfo. */
    b = (wbuf) {}; wvar(&b, CHC_PKT_PROFILE_INFO); wvar(&b, 1);
    expect_packet("short profile info", &b, REV_MODERN, CHC_ERR_EOF, CHC_PKT_HELLO);

    /* Block body that fails to parse: the partial is dropped, not returned. */
    b = (wbuf) {};
    wvar(&b, CHC_PKT_DATA);
    wstr(&b, "");
    wvar(&b, 1); w8(&b, 0); wvar(&b, 2);
    for (int i = 0; i < 4; i++) w8(&b, 0);
    wvar(&b, 0);
    wvar(&b, 1); wvar(&b, 1);                   /* one column, one row */
    wstr(&b, "c"); wstr(&b, "NoSuchType"); w8(&b, 0);
    expect_packet("bad column type", &b, REV_MODERN, CHC_ERR_TYPE, CHC_PKT_HELLO);
}

/* ---------------- compressed recv ---------------------------------------- */

static void
test_compressed_recv(void)
{
    current_test = "compressed_recv";
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {};
    wbuf b = {};
    wvar(&b, CHC_PKT_DATA);
    wstr(&b, "");
    /* Frame bytes never get read: both paths bail on the missing codec. */
    for (int i = 0; i < 32; i++) w8(&b, 0);

    fake f;
    if (fake_up(&f, &al, &b, REV_MODERN, &err) == CHC_OK) {
        f.c.compression = CHC_COMP_LZ4;
        f.c.codec = NULL;
        chc_packet pkt = {};
        CHECK(chc_client_recv_packet(&f.c, &pkt, &err) == CHC_ERR_USAGE);
        chc_packet_clear(&f.c, &pkt);
        fake_down(&f);
    } else
        fail_count++;

    /* Ioless variant routes through the resuming decompressor instead. */
    chc_client c;
    memset(&c, 0, sizeof c);
    c.al = &al;
    c.compression = CHC_COMP_LZ4;
    c.codec = NULL;
    c.server.revision = REV_MODERN;
    if (chc_in_init_ioless(&c.in, &al) == CHC_OK) {
        CHECK(chc_in_submit(&c.in, b.d, b.n, &err) == CHC_OK);
        chc_packet pkt = {};
        CHECK(chc_client_recv_packet(&c, &pkt, &err) == CHC_ERR_USAGE);
        chc_packet_clear(&c, &pkt);
        chc__client_recv_state_free(&c);
        chc_in_free(&c.in);
    } else
        fail_count++;
}

/* Wrap a block body in a Data packet with an LZ4-framed payload. */
static size_t
compress_packet(const chc_alloc *al, const uint8_t *body, size_t body_len,
                bool corrupt, uint8_t *out, size_t out_cap)
{
    test_mem_sink packet;
    chc_io packet_io;
    chc_err err = {};
    chc_codec codec;
    chc_lz4_codec_init(&codec);

    test_mem_sink_init(&packet, &packet_io);
    if (chc__write_varuint(&packet_io, CHC_PKT_DATA, &err)
        || chc__write_string(&packet_io, "", 0, &err)
        || chc__comp_emit_chunks(&packet_io, &codec, CHC_COMP_LZ4,
                                 body, body_len, al, &err)) {
        test_mem_sink_free(&packet);
        return 0;
    }
    size_t n = packet.len <= out_cap ? packet.len : 0;
    if (n) {
        memcpy(out, packet.data, n);
        if (corrupt) out[n - 1] ^= 0xff;        /* breaks the frame hash */
    }
    test_mem_sink_free(&packet);
    return n;
}

/* A one-column block body written with the library's own encoder. */
static size_t
good_body(const chc_alloc *al, uint8_t *out, size_t out_cap)
{
    test_mem_sink body;
    chc_io body_io;
    chc_err err = {};
    chc_block_opts opts = {
        .has_block_info = true, .has_custom_serialization = true,
    };
    chc_type *t = NULL;
    if (chc_type_parse("UInt32", 6, al, &t, &err)) return 0;

    uint32_t values[2] = { 1, 2 };
    chc_column col = chc_build_fixed(values, sizeof values[0], 2);
    chc_block_col bc = { .name = "c", .name_len = 1, .type = t, .col = &col };
    test_mem_sink_init(&body, &body_io);
    int rc = chc_block_write_cols(&body_io, &bc, 1, 2, &opts, &err);
    chc_type_destroy(t, al);

    size_t n = (rc == CHC_OK && body.len <= out_cap) ? body.len : 0;
    if (n) memcpy(out, body.data, n);
    test_mem_sink_free(&body);
    return n;
}

/* Same shape, but naming a type the parser rejects. */
static size_t
bad_type_body(uint8_t *out, size_t out_cap)
{
    wbuf b = {};
    wvar(&b, 1); w8(&b, 0); wvar(&b, 2);
    for (int i = 0; i < 4; i++) w8(&b, 0);
    wvar(&b, 0);
    wvar(&b, 1); wvar(&b, 2);
    wstr(&b, "c"); wstr(&b, "NoSuchType"); w8(&b, 0);
    if (b.n > out_cap) return 0;
    memcpy(out, b.d, b.n);
    return b.n;
}

static void
test_compressed_resume(void)
{
    current_test = "compressed_resume";
    chc_alloc al = chc_alloc_stdlib();
    chc_codec codec;
    chc_lz4_codec_init(&codec);
    uint8_t body[2048], bytes[4096];

    static const struct { bool bad_type, corrupt; int want; } cases[] = {
        { false, false, CHC_OK           },
        { false, true,  CHC_ERR_PROTOCOL },   /* frame hash mismatch */
        { true,  false, CHC_ERR_TYPE     },   /* block names an unknown type */
    };

    for (size_t i = 0; i < sizeof cases / sizeof *cases; i++) {
        size_t body_len = cases[i].bad_type ? bad_type_body(body, sizeof body)
                                            : good_body(&al, body, sizeof body);
        size_t n = body_len ? compress_packet(&al, body, body_len,
                                              cases[i].corrupt, bytes, sizeof bytes)
                            : 0;
        if (!n) { fail_count++; continue; }

        chc_client c;
        memset(&c, 0, sizeof c);
        c.al = &al;
        c.compression = CHC_COMP_LZ4;
        c.codec = &codec;
        c.server.revision = REV_MODERN;
        chc_err err = {};
        if (chc_in_init_ioless(&c.in, &al) != CHC_OK) { fail_count++; continue; }

        /* One byte at a time so the resume path takes every would-block
         * branch it has. */
        int rc = CHC_WOULD_BLOCK;
        chc_packet pkt = {};
        for (size_t fed = 0; fed <= n; fed++) {
            rc = chc_client_recv_packet(&c, &pkt, &err);
            if (rc != CHC_WOULD_BLOCK) break;
            if (fed == n) break;
            if (chc_in_submit(&c.in, bytes + fed, 1, &err) != CHC_OK) break;
        }
        if (rc != cases[i].want) {
            fprintf(stderr, "%s: FAIL: case %zu rc=%d want %d err='%s'\n",
                    current_test, i, rc, cases[i].want, err.msg);
            fail_count++;
        }
        chc_packet_clear(&c, &pkt);
        chc__client_recv_state_free(&c);
        chc_in_free(&c.in);
    }
}

/* Same stream, one failed allocation at a time: the resume path must unwind
 * its decompressor and partial block on every OOM. */
static void
test_compressed_resume_oom(void)
{
    current_test = "compressed_resume_oom";
    chc_alloc al = chc_alloc_stdlib();
    chc_codec codec;
    chc_lz4_codec_init(&codec);
    uint8_t body[2048], bytes[4096];

    size_t body_len = good_body(&al, body, sizeof body);
    size_t n = body_len ? compress_packet(&al, body, body_len, false,
                                          bytes, sizeof bytes) : 0;
    if (!n) { fail_count++; return; }

    for (size_t fail_at = 0; ; fail_at++) {
        test_fail_alloc fa;
        chc_alloc fal = test_fail_alloc_init(&fa, fail_at);
        chc_client c;
        memset(&c, 0, sizeof c);
        c.al = &fal;
        c.compression = CHC_COMP_LZ4;
        c.codec = &codec;
        c.server.revision = REV_MODERN;
        chc_err err = {};

        int rc = chc_in_init_ioless(&c.in, &fal);
        if (rc == CHC_OK) rc = chc_in_submit(&c.in, bytes, n, &err);
        chc_packet pkt = {};
        if (rc == CHC_OK) rc = chc_client_recv_packet(&c, &pkt, &err);
        chc_packet_clear(&c, &pkt);
        chc__client_recv_state_free(&c);
        chc_in_free(&c.in);

        if (rc != CHC_OK && rc != CHC_ERR_OOM) {
            fprintf(stderr, "%s: FAIL: fail_at=%zu rc=%d err='%s'\n",
                    current_test, fail_at, rc, err.msg);
            fail_count++;
            break;
        }
        CHECK_EQ_U64(fa.live, 0);
        CHECK_EQ_U64(fa.live_bytes, 0);
        if (rc == CHC_OK && fail_at >= fa.calls) break;
    }
}

/* ---------------- async wrappers ----------------------------------------- */

static void
test_async_wrappers(void)
{
    current_test = "async_wrappers";
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {};
    chc_client_opts opts = {
        .client_name = "test-client",
        .database    = "db",
        .user        = "u",
        .password    = "p",
    };

    chc_async_client *c = NULL;
    CHECK(chc_async_client_init(&c, &opts, &al, &err) == CHC_OK);
    if (!c) return;

    uint32_t values[2] = { 1, 2 };
    chc_type *t = NULL;
    if (chc_type_parse("UInt32", 6, &al, &t, &err) == CHC_OK) {
        chc_column col = chc_build_fixed(values, sizeof values[0], 2);
        chc_block_col storage[1];
        chc_block_builder bb;
        chc_block_builder_init(&bb, storage);
        chc_block_builder_append(&bb, "c", 1, t, &col);
        CHECK(chc_async_send_data(c, &bb, &err) == CHC_OK);
        chc_type_destroy(t, &al);
    } else
        fail_count++;
    CHECK(chc_async_send_data_end(c, &err) == CHC_OK);
    const uint8_t *pending = NULL;
    size_t pending_len = 0;
    chc_async_pending_out(c, &pending, &pending_len);
    CHECK(pending_len > 0);
    chc_async_client_free(c);

    /* Fail each allocation in turn: init must unwind to nothing. */
    for (size_t fail_at = 0; ; fail_at++) {
        test_fail_alloc fa;
        chc_alloc fal = test_fail_alloc_init(&fa, fail_at);
        chc_async_client *ac = NULL;
        chc_err oerr = {};
        int rc = chc_async_client_init(&ac, &opts, &fal, &oerr);
        if (rc == CHC_OK) {
            chc_async_client_free(ac);
            CHECK_EQ_U64(fa.live, 0);
            if (fail_at >= fa.calls) break;
            continue;
        }
        if (rc != CHC_ERR_OOM) {
            fprintf(stderr, "%s: FAIL: init fail_at=%zu rc=%d\n",
                    current_test, fail_at, rc);
            fail_count++;
            break;
        }
        CHECK_EQ_U64(fa.live, 0);
    }
}

/* ---------------- handshake & control packets --------------------------- */

/* Server Hello as 23.3+ sends it: password rules & nonce always present */
static void
whello(wbuf *b, uint64_t revision, uint64_t n_rules)
{
    wvar(b, CHC_PKT_HELLO);
    wstr(b, "ClickHouse");
    wvar(b, 99);                                /* version major */
    wvar(b, 1);                                 /* version minor */
    wvar(b, revision);
    wstr(b, "UTC");
    wstr(b, "host");
    wvar(b, 7);                                 /* version patch */
    wvar(b, n_rules);
    for (uint64_t i = 0; i < n_rules; i++) {
        wstr(b, i ? "" : "^.{8,}$");
        wstr(b, i ? "" : "8 chars");
    }
    for (int i = 0; i < 8; i++) w8(b, (uint8_t) (0xa0 + i));  /* nonce */
}

typedef struct { test_mem_src src; test_mem_sink sink; } duplex;

static int
duplex_read(void *ud, void *buf, size_t len, size_t *out_n, chc_err *err)
{
    return test_mem_read(&((duplex *) ud)->src, buf, len, out_n, err);
}

static int
duplex_write(void *ud, const void *buf, size_t n, chc_err *err)
{
    return test_mem_sink_write(&((duplex *) ud)->sink, buf, n, err);
}

/* Blocking handshake over b, returning negotiated revision or 0 on failure */
static int
sync_handshake(const wbuf *b, chc_server_info *si, chc_err *err)
{
    chc_alloc al = chc_alloc_stdlib();
    duplex d = { .src = { .data = b->d, .len = b->n } };
    chc_io io = { .ud = &d, .read = duplex_read, .write = duplex_write };
    chc_client_opts opts = {};
    chc_client *c = NULL;
    int rc = chc_client_init(&c, &opts, &al, &io, NULL, err);
    if (rc == CHC_OK) *si = *chc_client_server_info(c);
    chc_client_close(c);
    test_mem_sink_free(&d.sink);
    return rc;
}

/* Async handshake fed one byte at a time, server info must stay unpublished
 * until Hello completes */
static int
async_handshake(const wbuf *b, chc_server_info *si, chc_err *err)
{
    chc_alloc al = chc_alloc_stdlib();
    chc_client_opts opts = {};
    chc_async_client *c = NULL;
    int rc = chc_async_client_init(&c, &opts, &al, err);
    if (rc != CHC_OK) return rc;
    for (size_t fed = 0; ; fed++) {
        rc = chc_async_handshake(c, NULL, err);
        if (rc != CHC_WOULD_BLOCK || fed == b->n) break;
        CHECK(c->hs_phase != CHC__HS_RECV_HELLO || chc_async_server_info(c)->name[0] == '\0');
        if ((rc = chc_async_submit(c, b->d + fed, 1, err))) break;
    }
    *si = *chc_async_server_info(c);
    chc_async_client_free(c);
    return rc;
}

static void
test_handshake_revisions(void)
{
    current_test = "handshake_revisions";
    static const uint64_t revs[] = { 54462, 54463, 54464, 54465, 54489 };
    for (size_t i = 0; i < sizeof revs / sizeof *revs; i++) {
        wbuf b = {};
        whello(&b, revs[i], 1);
        wvar(&b, CHC_PKT_PONG);
        uint64_t want = revs[i] < CHC_CLIENT_REVISION ? revs[i] : CHC_CLIENT_REVISION;
        chc_server_info si = {};
        chc_err err = {};
        CHECK(sync_handshake(&b, &si, &err) == CHC_OK);
        CHECK_EQ_U64(si.revision, want);
        CHECK_EQ_U64(si.version_patch, 7);
        CHECK(strcmp(si.display_name, "host") == 0);
        si = (chc_server_info) {};
        CHECK(async_handshake(&b, &si, &err) == CHC_OK);
        CHECK_EQ_U64(si.revision, want);
        CHECK(strcmp(si.timezone, "UTC") == 0);
    }
}

static void
test_handshake_bounds(void)
{
    current_test = "handshake_bounds";
    chc_server_info si;
    chc_err err = {};
    wbuf b;

    b = (wbuf) {}; whello(&b, CHC_SERVER_MIN_REVISION - 1, 0); wvar(&b, CHC_PKT_PONG);
    CHECK(sync_handshake(&b, &si, &err) == CHC_ERR_PROTOCOL);
    CHECK(strstr(err.msg, "older than") != NULL);
    CHECK(async_handshake(&b, &si, &err) == CHC_ERR_PROTOCOL);

    b = (wbuf) {}; whello(&b, 54465, 256); wvar(&b, CHC_PKT_PONG);
    CHECK(sync_handshake(&b, &si, &err) == CHC_OK);
    CHECK(async_handshake(&b, &si, &err) == CHC_OK);

    b = (wbuf) {}; whello(&b, 54465, 257); wvar(&b, CHC_PKT_PONG);
    CHECK(sync_handshake(&b, &si, &err) == CHC_ERR_PROTOCOL);
    CHECK(strstr(err.msg, "password rules") != NULL);
    CHECK(async_handshake(&b, &si, &err) == CHC_ERR_PROTOCOL);

    /* Length checked before any payload read */
    b = (wbuf) {}; wvar(&b, CHC_PKT_HELLO); wvar(&b, 4097);
    CHECK(sync_handshake(&b, &si, &err) == CHC_ERR_PROTOCOL);
    CHECK(strstr(err.msg, "too long") != NULL);
    CHECK(async_handshake(&b, &si, &err) == CHC_ERR_PROTOCOL);

    /* Long server name truncates to buffer, 4096 accepted */
    b = (wbuf) {};
    wvar(&b, CHC_PKT_HELLO); wvar(&b, 4096);
    for (int i = 0; i < 4096; i++) w8(&b, 'n');
    wvar(&b, 1); wvar(&b, 1); wvar(&b, 54465);
    wstr(&b, "UTC"); wstr(&b, "h"); wvar(&b, 0); wvar(&b, 0);
    size_t before_nonce = b.n;
    for (int i = 0; i < 8; i++) w8(&b, 0);
    CHECK(b.n < sizeof b.d);
    wvar(&b, CHC_PKT_PONG);
    CHECK(sync_handshake(&b, &si, &err) == CHC_OK);
    CHECK_EQ_U64(strlen(si.name), sizeof si.name - 1);

    /* Truncated nonce */
    b.n = before_nonce + 7;
    CHECK(sync_handshake(&b, &si, &err) == CHC_ERR_EOF);
    CHECK(async_handshake(&b, &si, &err) == CHC_WOULD_BLOCK);
    CHECK(si.name[0] == '\0');
}

/* Ioless client past handshake at revision, bytes fed one at a time */
static int
recv_bytewise(chc_client *c, const wbuf *b, size_t *fed, chc_packet *pkt, chc_err *err)
{
    for (;;) {
        int rc = chc_client_recv_packet(c, pkt, err);
        if (rc != CHC_WOULD_BLOCK || *fed == b->n) return rc;
        if ((rc = chc_in_submit(&c->in, b->d + (*fed)++, 1, err))) return rc;
    }
}

static void
test_progress_timezone(void)
{
    current_test = "progress_timezone";
    chc_alloc al = chc_alloc_stdlib();
    static const uint64_t revs[] = { 54462, 54463, 54465 };
    for (size_t i = 0; i < sizeof revs / sizeof *revs; i++) {
        bool has_total_bytes = revs[i] >= 54463;
        wbuf b = {};
        wvar(&b, CHC_PKT_PROGRESS);
        wvar(&b, 1); wvar(&b, 2); wvar(&b, 3);
        if (has_total_bytes) wvar(&b, 4);
        wvar(&b, 5); wvar(&b, 6); wvar(&b, 7);
        wvar(&b, CHC_PKT_TIMEZONE_UPDATE); wstr(&b, "Asia/Tokyo");
        wvar(&b, CHC_PKT_TIMEZONE_UPDATE); wstr(&b, "");
        wvar(&b, CHC_PKT_PONG);
        wvar(&b, CHC_PKT_TIMEZONE_UPDATE); wstr(&b, "Europe/Paris");

        chc_client c;
        memset(&c, 0, sizeof c);
        c.al = &al;
        c.server.revision = revs[i];
        strcpy(c.server.timezone, "UTC");
        chc_err err = {};
        if (chc_in_init_ioless(&c.in, &al) != CHC_OK) { fail_count++; continue; }

        size_t fed = 0;
        chc_packet pkt = {};
        CHECK(recv_bytewise(&c, &b, &fed, &pkt, &err) == CHC_OK);
        CHECK(pkt.kind == CHC_PKT_PROGRESS);
        CHECK_EQ_U64(pkt.progress.rows, 1);
        CHECK_EQ_U64(pkt.progress.bytes, 2);
        CHECK_EQ_U64(pkt.progress.total_rows, 3);
        CHECK_EQ_U64(pkt.progress.total_bytes, has_total_bytes ? 4 : 0);
        CHECK_EQ_U64(pkt.progress.written_rows, 5);
        CHECK_EQ_U64(pkt.progress.written_bytes, 6);
        CHECK_EQ_U64(pkt.progress.elapsed_ns, 7);

        CHECK(recv_bytewise(&c, &b, &fed, &pkt, &err) == CHC_OK);
        CHECK(pkt.kind == CHC_PKT_TIMEZONE_UPDATE);
        CHECK(strcmp(c.server.timezone, "Asia/Tokyo") == 0);
        CHECK(recv_bytewise(&c, &b, &fed, &pkt, &err) == CHC_OK);
        CHECK(pkt.kind == CHC_PKT_TIMEZONE_UPDATE);
        CHECK(c.server.timezone[0] == '\0');
        CHECK(recv_bytewise(&c, &b, &fed, &pkt, &err) == CHC_OK);
        CHECK(pkt.kind == CHC_PKT_PONG);

        /* Incomplete update leaves timezone untouched */
        b.n--;
        CHECK(recv_bytewise(&c, &b, &fed, &pkt, &err) == CHC_WOULD_BLOCK);
        CHECK(c.server.timezone[0] == '\0');
        b.n++;
        CHECK(recv_bytewise(&c, &b, &fed, &pkt, &err) == CHC_OK);
        CHECK(strcmp(c.server.timezone, "Europe/Paris") == 0);

        chc__server_info_free(&c.server, &al);
        chc__client_recv_state_free(&c);
        chc_in_free(&c.in);
    }

    wbuf b = {};
    wvar(&b, CHC_PKT_TIMEZONE_UPDATE); wvar(&b, 4097);
    expect_packet("oversized timezone", &b, REV_MODERN, CHC_ERR_PROTOCOL, CHC_PKT_HELLO);
}

int
main(void)
{
    test_handshake_rejects();
    test_send_paths();
    test_recv_packets();
    test_compressed_recv();
    test_compressed_resume();
    test_compressed_resume_oom();
    test_async_wrappers();
    test_handshake_revisions();
    test_handshake_bounds();
    test_progress_timezone();

    if (fail_count) {
        fprintf(stderr, "%d failure(s)\n", fail_count);
        return 1;
    }
    printf("all client_errors tests passed\n");
    return 0;
}
