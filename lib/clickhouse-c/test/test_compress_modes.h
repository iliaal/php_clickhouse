/*
 * test_compress_modes.h -- compressed Data recv pinned to one reader mode, no
 * server. Shared by test_compress_no_sync.c and test_compress_no_async.c;
 * each wrapper #defines its CHC_NO_* before including.
 *
 *   - CHC_NO_ASYNC: io-backed chc_in -> chc__recv_block_compressed (baseline).
 *   - CHC_NO_SYNC:  ioless chc_in fed in chunks -> chc__recv_block_compressed_resume.
 * Two back-to-back multi-frame Data packets exercise the over-read guard, then
 * each decoded block is checked against an independently decoded uncompressed
 * reference.
 */

#ifndef CLICKHOUSE_TEST_COMPRESS_MODES_H
#define CLICKHOUSE_TEST_COMPRESS_MODES_H

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHC_PROVIDE_STDLIB_ALLOC
#define CHC_IMPLEMENTATION
#define CHC_NO_ZSTD                 /* lz4 alone covers the compressed path */
#include "clickhouse.h"
#include "clickhouse-compression.h"
#include "clickhouse-client.h"

static int        fail_count = 0;
static const char *current_test = "";

#include "test_common.h"
#include "test_block_compare.h"
#include "test_golden_blocks.h"

#define TEST_REVISION CHC_CLIENT_REVISION
#define BIG_ROWS 30000   /* > CHC_COMPRESS_MAX_CHUNK raw => multi-frame */
#define MED_ROWS 5000    /* second multi-frame block, back-to-back */
#define N_DATA   2

static uint8_t *
build_compressed_stream(const chc_alloc *al, const chc_block_opts *opts,
                        const chc_codec *codec, size_t *out_len)
{
    test_mem_sink s;
    chc_io io;
    test_mem_sink_init(&s, &io);
    chc_err err = {};
    if (test_write_compressed_packet(&io, al, opts, codec, BIG_ROWS, &err) ||
        test_write_compressed_packet(&io, al, opts, codec, MED_ROWS, &err) ||
        chc__write_varuint(&io, CHC_PKT_END_OF_STREAM, &err)) {
        fprintf(stderr, "build_compressed_stream: %s\n", err.msg);
        test_mem_sink_free(&s);
        return NULL;
    }
    *out_len = s.len;
    return s.data;
}

#ifdef CHC_NO_SYNC
#define MODE_NAME "no_sync_ioless"

/* Reference decoder must use the same pinned reader mode: under CHC_NO_SYNC
 * every chc_in is ioless (CHC__IOLESS == 1), so an io-backed reader never
 * refills. Submit the whole raw stream at once, then read N blocks. */
static int
decode_raw_blocks(const uint8_t *bytes, size_t len, const chc_alloc *al,
                  const chc_block_opts *opts, chc_block **out, size_t n_blocks,
                  chc_err *err)
{
    chc_in in;
    if (chc_in_init_ioless(&in, al)) return -1;
    if (chc_in_submit(&in, bytes, len, err)) { chc_in_free(&in); return -1; }
    for (size_t i = 0; i < n_blocks; i++) {
        int rc = chc_block_read(&in, al, opts, &out[i], err);
        if (rc != CHC_OK || !out[i]) { chc_in_free(&in); return -1; }
    }
    chc_in_free(&in);
    return 0;
}

/* Ioless: feed the stream in `chunk`-byte slices, submitting more on each
 * CHC_WOULD_BLOCK; resume keeps decomp state alive across the gaps. */
static int
decode_subject(const uint8_t *bytes, size_t len, size_t chunk,
               const chc_codec *codec, const chc_alloc *al,
               chc_block **out, size_t *out_n, chc_err *err)
{
    chc_client c;
    memset(&c, 0, sizeof c);
    c.al = al;
    c.compression = CHC_COMP_LZ4;
    c.codec = codec;
    c.server.revision = TEST_REVISION;
    if (chc_in_init_ioless(&c.in, al)) return -1;

    size_t fed = 0, n = 0;
    for (;;) {
        chc_packet pkt = {};
        chc_err e = {};
        int rc = chc_client_recv_packet(&c, &pkt, &e);
        if (rc == CHC_WOULD_BLOCK) {
            chc_packet_clear(&c, &pkt);
            if (fed >= len) {
                chc__err_set(err, CHC_ERR_EOF, "feed underrun");
                chc_in_free(&c.in); return -1;
            }
            size_t take = (len - fed) < chunk ? (len - fed) : chunk;
            if (chc_in_submit(&c.in, bytes + fed, take, &e)) {
                *err = e; chc_in_free(&c.in); return -1;
            }
            fed += take;
            continue;
        }
        if (rc != CHC_OK) {
            *err = e; chc_packet_clear(&c, &pkt); chc_in_free(&c.in); return -1;
        }
        chc_packet_kind k = pkt.kind;
        if (k == CHC_PKT_DATA && n < N_DATA) { out[n++] = pkt.block; pkt.block = NULL; }
        chc_packet_clear(&c, &pkt);
        if (k == CHC_PKT_END_OF_STREAM) break;
    }
    *out_n = n;
    chc_in_free(&c.in);
    return 0;
}

static const size_t g_chunks[] = { 1, 3, 7, 64, 4096, 1u << 20 };

#else  /* io-backed: default + CHC_NO_ASYNC */
#define MODE_NAME "no_async_io"

/* Reference decoder, io-backed to match the pinned mode. */
static int
decode_raw_blocks(const uint8_t *bytes, size_t len, const chc_alloc *al,
                  const chc_block_opts *opts, chc_block **out, size_t n_blocks,
                  chc_err *err)
{
    uint64_t consumed = 0;
    return test_decode_blocks_io(bytes, len, al, opts, out, n_blocks,
                                 &consumed, err);
}

/* Io-backed: whole stream behind a blocking mem source; recv completes each
 * packet in one call. `chunk` is unused. */
static int
decode_subject(const uint8_t *bytes, size_t len, size_t chunk,
               const chc_codec *codec, const chc_alloc *al,
               chc_block **out, size_t *out_n, chc_err *err)
{
    (void) chunk;
    test_mem_src m;
    chc_io io;
    test_mem_src_init(&m, &io, bytes, len);

    chc_client c;
    memset(&c, 0, sizeof c);
    c.al = al;
    c.io = &io;
    c.compression = CHC_COMP_LZ4;
    c.codec = codec;
    c.server.revision = TEST_REVISION;
    if (chc_in_init(&c.in, &io, al, 0, err)) return -1;

    size_t n = 0;
    for (;;) {
        chc_packet pkt = {};
        int rc = chc_client_recv_packet(&c, &pkt, err);
        if (rc != CHC_OK) { chc_packet_clear(&c, &pkt); chc_in_free(&c.in); return -1; }
        chc_packet_kind k = pkt.kind;
        if (k == CHC_PKT_DATA && n < N_DATA) { out[n++] = pkt.block; pkt.block = NULL; }
        chc_packet_clear(&c, &pkt);
        if (k == CHC_PKT_END_OF_STREAM) break;
    }
    *out_n = n;
    chc_in_free(&c.in);
    return 0;
}

static const size_t g_chunks[] = { 0 };  /* chunk ignored io-backed */

#endif

/* Independently decoded uncompressed reference: the same two block bodies,
 * concatenated raw and read back through the pinned reader mode. */
static int
build_reference(const chc_alloc *al, const chc_block_opts *opts,
                chc_block **ref, chc_err *err)
{
    test_mem_sink raw;
    chc_io io;
    test_mem_sink_init(&raw, &io);
    if (test_write_uint_string_block(&io, al, opts, BIG_ROWS, err) ||
        test_write_uint_string_block(&io, al, opts, MED_ROWS, err)) {
        test_mem_sink_free(&raw);
        return -1;
    }
    int rc = decode_raw_blocks(raw.data, raw.len, al, opts, ref, N_DATA, err);
    test_mem_sink_free(&raw);
    return rc;
}

static void
test_compressed_recv(void)
{
    current_test = "compressed_recv[" MODE_NAME "]";
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {};
    chc_block_opts opts = { .has_block_info = true, .has_custom_serialization = true };
    chc_codec codec;
    chc_lz4_codec_init(&codec);

    chc_block *ref[N_DATA] = {};
    if (build_reference(&al, &opts, ref, &err)) {
        fprintf(stderr, "%s: reference build failed: %s\n", current_test, err.msg);
        fail_count++;
        return;
    }

    size_t len = 0;
    uint8_t *stream = build_compressed_stream(&al, &opts, &codec, &len);
    CHECK(stream != NULL);
    if (!stream) { test_free_blocks(ref, N_DATA, &al); return; }

    for (size_t ci = 0; ci < sizeof g_chunks / sizeof *g_chunks; ci++) {
        chc_block *subj[N_DATA] = {};
        size_t sn = 0;
        chc_err e = {};
        if (decode_subject(stream, len, g_chunks[ci], &codec, &al,
                           subj, &sn, &e) != 0) {
            fprintf(stderr, "%s: chunk=%zu decode failed: %s\n",
                    current_test, g_chunks[ci], e.msg);
            fail_count++;
            test_free_blocks(subj, sn, &al);
            continue;
        }
        CHECK_EQ_U64(sn, N_DATA);
        for (size_t i = 0; i < sn && i < N_DATA; i++) {
            if (!test_block_eq(ref[i], subj[i])) {
                fprintf(stderr, "%s: chunk=%zu block %zu mismatch\n",
                        current_test, g_chunks[ci], i);
                fail_count++;
            }
        }
        test_free_blocks(subj, N_DATA, &al);
    }

    free(stream);
    test_free_blocks(ref, N_DATA, &al);
}

/* Hostile frame: LZ4 original size above INT_MAX must be rejected at the
 * frame header, before checksum verify, output allocation, or codec call.
 * Boundary companion at exactly INT_MAX passes the size domain and dies on
 * checksum instead. */
static long g_hostile_decomp_calls;

static int
hostile_lz4_decompress(void *ud, const void *src, size_t src_len,
                       void *dst, size_t original, chc_err *err)
{
    (void) ud; (void) src; (void) src_len; (void) dst; (void) original;
    g_hostile_decomp_calls++;
    return chc__err_set(err, CHC_ERR_USAGE, "codec must not be reached");
}

static int
hostile_frame_read(const uint8_t *frame, size_t len, const chc_alloc *al,
                   chc_err *err)
{
    chc_in in;
#ifdef CHC_NO_SYNC
    if (chc_in_init_ioless(&in, al)) return -1;
    if (chc_in_submit(&in, frame, len, err)) { chc_in_free(&in); return -1; }
#else
    test_mem_src m;
    chc_io io;
    test_mem_src_init(&m, &io, frame, len);
    if (chc_in_init(&in, &io, al, 0, err)) return -1;
#endif
    chc_codec codec = {};
    codec.lz4_decompress = hostile_lz4_decompress;

    chc__decomp_src ds;
    chc_io dio;
    chc__decomp_src_init(&ds, &in, &codec, al, &dio);
    uint8_t sink[8];
    size_t got = 0;
    int rc = dio.read(dio.ud, sink, sizeof sink, &got, err);
    chc__decomp_src_free(&ds);
    chc_in_free(&in);
    return rc;
}

static void
test_lz4_orig_over_int_max(void)
{
    current_test = "lz4_orig_over_int_max[" MODE_NAME "]";
    chc_alloc al = chc_alloc_stdlib();

    /* 16B checksum (zeros) + method + comp_with_hdr=9 + orig, no payload. */
    uint8_t frame[16 + CHC__COMP_HEADER_BYTES] = {};
    frame[16] = CHC__COMP_LZ4;
    frame[17] = CHC__COMP_HEADER_BYTES;
    frame[24] = 0x80;                     /* orig = 0x80000000 = INT_MAX+1 */

    chc_err err = {};
    g_hostile_decomp_calls = 0;
    int rc = hostile_frame_read(frame, sizeof frame, &al, &err);
    CHECK(rc == CHC_ERR_PROTOCOL);
    CHECK(strstr(err.msg, "original size") != NULL);
    CHECK_EQ_I64(g_hostile_decomp_calls, 0);

    frame[21] = frame[22] = frame[23] = 0xff;
    frame[24] = 0x7f;                     /* orig = INT_MAX: in domain */
    chc_err err2 = {};
    rc = hostile_frame_read(frame, sizeof frame, &al, &err2);
    CHECK(rc == CHC_ERR_PROTOCOL);
    CHECK(strstr(err2.msg, "hash mismatch") != NULL);
    CHECK_EQ_I64(g_hostile_decomp_calls, 0);
}

int
main(void)
{
    test_compressed_recv();
    test_lz4_orig_over_int_max();

    if (fail_count) {
        fprintf(stderr, "%d check(s) failed\n", fail_count);
        return 1;
    }
    printf("all compress_" MODE_NAME " tests passed\n");
    return 0;
}

#endif
