/*
 * test_async_compressed.c -- ioless async recv driver for the COMPRESSED Data
 * path, no server. The correctness gate for frame-granularity resumption
 * (chc__recv_block_compressed_resume, clickhouse-client.h).
 *
 * Mirrors test_async.c but with compression on:
 *   - Build a post-handshake server->client byte stream with the library's own
 *     writers: two compressed Data packets (the first genuinely multi-frame),
 *     then an uncompressed Progress + EndOfStream. Two back-to-back Data packets
 *     are the over-read guard: after a block's last frame, the next bytes are
 *     the next packet's uncompressed [tag][name], not a frame -- an eager pump
 *     would mis-read them and fail on a hash mismatch.
 *   - oracle:  a hand-seeded blocking chc_client (compression on) over an
 *              io-backed mem source; chc_client_recv_packet in a loop. io-backed
 *              => baseline rebuild-per-call decompressor.
 *   - subject: chc_async_client (compression on) fed the same blob in
 *              adversarial chunkings {1,2,3,7,64,4096,1<<20}, driving
 *              chc_async_recv_packet; on WOULD_BLOCK feed the next chunk.
 * The subject packet sequence must be byte-identical to the oracle for every
 * chunking, with identical raw `consumed`.
 *
 * Retention assertion: a counting lz4 codec records every decompress call.
 * Each frame must be decompressed exactly once across the whole chunked stream
 * (vs O(F^2) for a rewind-to-block-start baseline) -- the proof the re-work is
 * bounded to one frame per would-block.
 *
 * Run under ASan to catch partial frees across the would-block rewinds and the
 * persisted recv_dec_in / recv_decomp frees:
 *   CFLAGS='-fsanitize=address -g' LDFLAGS='-fsanitize=address' \
 *     ./test.sh async_compressed
 */

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
#include "clickhouse-async.h"

static int        fail_count = 0;
static const char *current_test = "";

#include "test_common.h"
#include "test_block_compare.h"
#include "test_golden_blocks.h"

/* Fixed revision: block_info + custom_serialization + temp tables all on. */
#define TEST_REVISION CHC_CLIENT_REVISION

/* ---------------- counting lz4 codec (retention assertion) --------------- */

static long      g_decomp_calls;
static chc_codec g_real_lz4;

static int
counting_lz4_decompress(void *ud, const void *src, size_t src_len,
                        void *dst, size_t original, chc_err *err)
{
    g_decomp_calls++;
    return g_real_lz4.lz4_decompress(ud, src, src_len, dst, original, err);
}

static chc_codec
make_counting_codec(void)
{
    chc_lz4_codec_init(&g_real_lz4);
    chc_codec c = g_real_lz4;
    c.lz4_decompress = counting_lz4_decompress;  /* wrap decompress, keep compress */
    return c;
}

#define SEQ_LEN 4  /* data(big) + data(med) + progress + eos */
#define BIG_ROWS 30000   /* > CHC_COMPRESS_MAX_CHUNK raw => multi-frame */
#define MED_ROWS 5000    /* second multi-frame block, back-to-back */

static uint8_t *
build_response_stream(const chc_alloc *al, const chc_block_opts *opts,
                      const chc_codec *codec, size_t *out_len)
{
    test_mem_sink s;
    chc_io io;
    test_mem_sink_init(&s, &io);
    chc_err err = {};
    if (test_write_compressed_packet(&io, al, opts, codec, BIG_ROWS, &err) ||
        test_write_compressed_packet(&io, al, opts, codec, MED_ROWS, &err) ||
        test_write_progress_packet(&io, &err) ||
        chc__write_varuint(&io, CHC_PKT_END_OF_STREAM, &err)) {
        fprintf(stderr, "build_response_stream: %s\n", err.msg);
        test_mem_sink_free(&s);
        return NULL;
    }
    *out_len = s.len;
    return s.data;
}

/* ---------------- recorded packet sequence ------------------------------- */

typedef struct {
    chc_packet_kind kind;
    chc_block      *block;          /* owned; for DATA kinds */
    uint64_t        prog[7];        /* Progress fields in wire order */
} rec_packet;

static void
rec_free(rec_packet *r, size_t n, const chc_alloc *al)
{
    for (size_t i = 0; i < n; i++)
        if (r[i].block) chc_block_destroy(r[i].block, al);
}

static void
rec_take(rec_packet *out, const chc_packet *pkt)
{
    out->kind = pkt->kind;
    switch (pkt->kind) {
    case CHC_PKT_DATA: case CHC_PKT_TOTALS: case CHC_PKT_EXTREMES:
    case CHC_PKT_LOG:  case CHC_PKT_PROFILE_EVENTS:
        out->block = pkt->block;    /* take ownership; aliases progress in union */
        break;
    case CHC_PKT_PROGRESS:
        out->prog[0] = pkt->progress.rows;
        out->prog[1] = pkt->progress.bytes;
        out->prog[2] = pkt->progress.total_rows;
        out->prog[3] = pkt->progress.total_bytes;
        out->prog[4] = pkt->progress.written_rows;
        out->prog[5] = pkt->progress.written_bytes;
        out->prog[6] = pkt->progress.elapsed_ns;
        break;
    default:
        break;
    }
}

/* ---------------- oracle: blocking client over io-backed source ---------- */

static int
oracle_decode(const uint8_t *bytes, size_t len, const chc_codec *codec,
              const chc_alloc *al, rec_packet *out, size_t *out_n,
              uint64_t *consumed, chc_err *err)
{
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
        rec_take(&out[n], &pkt);
        pkt.block = NULL;                /* moved into record */
        chc_packet_clear(&c, &pkt);
        n++;
        if (out[n - 1].kind == CHC_PKT_END_OF_STREAM) break;
        if (n >= SEQ_LEN + 1) break;     /* guard */
    }
    *out_n = n;
    *consumed = c.in.consumed;
    chc_in_free(&c.in);
    return 0;
}

/* ---------------- subject: async client fed in chunks -------------------- */

static int
subject_decode(const uint8_t *bytes, size_t len, size_t chunk,
               const chc_codec *codec, const chc_alloc *al,
               rec_packet *out, size_t *out_n, uint64_t *consumed, chc_err *err)
{
    chc_client_opts opts = {};
    opts.compression = CHC_COMP_LZ4;
    opts.codec = codec;

    chc_async_client *c = NULL;
    if (chc_async_client_init(&c, &opts, al, err)) return -1;
    /* Skip handshake: seed revision and pretend handshake is done. */
    c->cli.server.revision = TEST_REVISION;

    size_t fed = 0;
    size_t n = 0;
    for (;;) {
        chc_packet pkt = {};
        chc_err e = {};
        int rc = chc_async_recv_packet(c, &pkt, &e);
        if (rc == CHC_WOULD_BLOCK) {
            if (fed >= len) {
                chc__err_set(err, CHC_ERR_EOF, "feed underrun");
                chc_async_packet_clear(c, &pkt);
                chc_async_client_free(c);
                return -1;
            }
            size_t take = (len - fed) < chunk ? (len - fed) : chunk;
            if (chc_async_submit(c, bytes + fed, take, &e)) {
                *err = e; chc_async_packet_clear(c, &pkt);
                chc_async_client_free(c); return -1;
            }
            fed += take;
            continue;
        }
        if (rc != CHC_OK) {
            *err = e; chc_async_packet_clear(c, &pkt);
            chc_async_client_free(c); return -1;
        }
        rec_take(&out[n], &pkt);
        pkt.block = NULL;
        chc_async_packet_clear(c, &pkt);
        n++;
        if (out[n - 1].kind == CHC_PKT_END_OF_STREAM) break;
        if (n >= SEQ_LEN + 1) break;
    }
    *out_n = n;
    *consumed = c->cli.in.consumed;
    chc_async_client_free(c);
    return 0;
}

/* ---------------- tests -------------------------------------------------- */

static void
test_recv_golden_chunk_compressed(void)
{
    current_test = "recv_golden_chunk_compressed";
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {};
    chc_block_opts opts = { .has_block_info = true, .has_custom_serialization = true };
    chc_codec codec = make_counting_codec();

    size_t len = 0;
    uint8_t *stream = build_response_stream(&al, &opts, &codec, &len);
    CHECK(stream != NULL); if (!stream) return;

    rec_packet oracle[SEQ_LEN + 1] = {};
    size_t on = 0;
    uint64_t oracle_consumed = 0;
    g_decomp_calls = 0;
    if (oracle_decode(stream, len, &codec, &al, oracle, &on, &oracle_consumed, &err) != 0) {
        fprintf(stderr, "%s: oracle decode failed: %s\n", current_test, err.msg);
        fail_count++; free(stream); return;
    }
    long oracle_frames = g_decomp_calls;   /* each frame decompressed once */
    CHECK_EQ_U64(on, SEQ_LEN);
    CHECK_EQ_U64(oracle_consumed, len);
    CHECK(oracle[SEQ_LEN - 1].kind == CHC_PKT_END_OF_STREAM);
    CHECK(oracle[0].kind == CHC_PKT_DATA && oracle[0].block != NULL);
    CHECK(oracle[1].kind == CHC_PKT_DATA && oracle[1].block != NULL);
    CHECK(oracle_frames >= 2);              /* big block alone spans >1 frame */

    static const size_t chunks[] = { 1, 2, 3, 7, 64, 4096, 1u << 20 };
    for (size_t ci = 0; ci < sizeof chunks / sizeof *chunks; ci++) {
        rec_packet subj[SEQ_LEN + 1] = {};
        size_t sn = 0;
        uint64_t subj_consumed = 0;
        chc_err e = {};
        g_decomp_calls = 0;
        if (subject_decode(stream, len, chunks[ci], &codec, &al,
                           subj, &sn, &subj_consumed, &e) != 0) {
            fprintf(stderr, "%s: chunk=%zu decode failed: %s\n",
                    current_test, chunks[ci], e.msg);
            fail_count++;
            rec_free(subj, sn, &al);
            continue;
        }
        if (sn != on) {
            fprintf(stderr, "%s: chunk=%zu packet count %zu != oracle %zu\n",
                    current_test, chunks[ci], sn, on);
            fail_count++;
            rec_free(subj, sn, &al);
            continue;
        }
        if (subj_consumed != oracle_consumed) {
            fprintf(stderr, "%s: chunk=%zu consumed %llu != oracle %llu\n",
                    current_test, chunks[ci],
                    (unsigned long long) subj_consumed,
                    (unsigned long long) oracle_consumed);
            fail_count++;
        }
        /* Retention: at most one frame decompressed per would-block, so the
         * total across the whole chunked stream equals the frame count. */
        if (g_decomp_calls != oracle_frames) {
            fprintf(stderr, "%s: chunk=%zu decompressed %ld frames != %ld "
                    "(re-decompress regression)\n",
                    current_test, chunks[ci], g_decomp_calls, oracle_frames);
            fail_count++;
        }
        for (size_t i = 0; i < on; i++) {
            if (subj[i].kind != oracle[i].kind) {
                fprintf(stderr, "%s: chunk=%zu pkt %zu kind %d != %d\n",
                        current_test, chunks[ci], i,
                        (int) subj[i].kind, (int) oracle[i].kind);
                fail_count++;
                continue;
            }
            if (oracle[i].kind == CHC_PKT_PROGRESS) {
                for (int k = 0; k < 7; k++)
                    if (subj[i].prog[k] != oracle[i].prog[k]) {
                        fprintf(stderr, "%s: chunk=%zu pkt %zu prog[%d] %llu != %llu\n",
                                current_test, chunks[ci], i, k,
                                (unsigned long long) subj[i].prog[k],
                                (unsigned long long) oracle[i].prog[k]);
                        fail_count++;
                    }
            }
            if (oracle[i].block || subj[i].block) {
                if (!test_block_eq(oracle[i].block, subj[i].block)) {
                    fprintf(stderr, "%s: chunk=%zu pkt %zu block mismatch\n",
                            current_test, chunks[ci], i);
                    fail_count++;
                }
            }
        }
        rec_free(subj, sn, &al);
    }

    rec_free(oracle, on, &al);
    free(stream);
}

/* Free the client mid-compressed-block, while recv_dec_active holds a persisted
 * decompressor + ioless decompressed buffer. ASan/leak-check proves the
 * teardown path (chc__client_recv_comp_free via chc__client_recv_state_free)
 * releases recv_dec_in + recv_decomp + the retained partial block. */
static void
test_teardown_mid_block(void)
{
    current_test = "teardown_mid_block";
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {};
    chc_block_opts opts = { .has_block_info = true, .has_custom_serialization = true };
    chc_codec codec = make_counting_codec();

    /* A single multi-frame compressed Data packet. */
    test_mem_sink s;
    chc_io io;
    test_mem_sink_init(&s, &io);
    if (test_write_compressed_packet(&io, &al, &opts, &codec, BIG_ROWS, &err)) {
        fprintf(stderr, "%s: build failed: %s\n", current_test, err.msg);
        fail_count++; test_mem_sink_free(&s); return;
    }
    CHECK(s.len > 50);

    chc_client_opts copts = {};
    copts.compression = CHC_COMP_LZ4;
    copts.codec = &codec;
    chc_async_client *c = NULL;
    if (chc_async_client_init(&c, &copts, &al, &err)) {
        fprintf(stderr, "%s: init failed: %s\n", current_test, err.msg);
        fail_count++; test_mem_sink_free(&s); return;
    }
    c->cli.server.revision = TEST_REVISION;

    /* Feed all but the last 50 bytes: kind + name + several full frames get
     * decompressed and pushed (recv_dec_in + frame_buf allocated), but the
     * final frame is truncated, so recv returns WOULD_BLOCK with the block
     * still in flight. */
    if (chc_async_submit(c, s.data, s.len - 50, &err)) {
        fprintf(stderr, "%s: submit failed: %s\n", current_test, err.msg);
        fail_count++; chc_async_client_free(c); test_mem_sink_free(&s); return;
    }
    chc_packet pkt = {};
    int rc = chc_async_recv_packet(c, &pkt, &err);
    CHECK(rc == CHC_WOULD_BLOCK);
    CHECK(c->cli.recv_dec_active);          /* persisted decomp state is live */
    chc_async_packet_clear(c, &pkt);

    /* Free mid-block: must not leak the persisted dec_in/decomp or partial. */
    chc_async_client_free(c);
    test_mem_sink_free(&s);
}

int
main(void)
{
    test_recv_golden_chunk_compressed();
    test_teardown_mid_block();

    if (fail_count) {
        fprintf(stderr, "%d check(s) failed\n", fail_count);
        return 1;
    }
    printf("all async_compressed tests passed\n");
    return 0;
}
