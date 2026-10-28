/*
 * test_ioless.c -- ioless chc_in: would-block / checkpoint / rewind / reset,
 * plus the golden-chunk correctness gate. No server needed.
 *
 * Blocks are produced by the library's own writer into a memory sink, then
 * decoded two ways and compared:
 *   - oracle:  io-backed chc_in over a memory source (whole stream at once).
 *   - subject: ioless chc_in driven by a rewind-and-re-parse loop, fed in
 *              adversarial chunkings (1 byte, 2, 3, 7, 64, whole).
 * The subject must reconstruct a byte-identical block sequence and an
 * identical `consumed` total. Run under ASan/valgrind to catch any partial
 * frees missed on a would-block rewind.
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

static int        fail_count = 0;
static const char *current_test = "";

#include "test_common.h"
#include "test_block_compare.h"
#include "test_golden_blocks.h"

/* ---------------- decoders ----------------------------------------------- */

/* Ioless driver: checkpoint, parse, and on CHC_WOULD_BLOCK rewind and feed
 * the next chunk. Mirrors the baseline rewind-and-re-parse the phase-3 recv
 * driver will use, including the per-packet chc_in_reset compaction. */
static int
decode_ioless(const uint8_t *bytes, size_t len, size_t chunk, const chc_alloc *al,
            const chc_block_opts *opts, chc_block **out, uint64_t *consumed,
            chc_err *err)
{
    chc_in in;
    if (chc_in_init_ioless(&in, al)) return -1;
    size_t fed = 0;
    for (size_t bi = 0; bi < TEST_GOLDEN_BLOCKS; bi++) {
        chc_block *b = NULL;
        for (;;) {
            chc__in_checkpoint(&in);
            chc_err e = {};
            int rc = chc_block_read(&in, al, opts, &b, &e);
            if (rc == CHC_OK && b) break;
            if (rc == CHC_WOULD_BLOCK) {
                chc__in_rewind(&in);
                if (fed >= len) {                 /* stream exhausted mid-block */
                    chc__err_set(err, CHC_ERR_EOF, "feed underrun");
                    chc_in_free(&in);
                    return -1;
                }
                size_t take = (len - fed) < chunk ? (len - fed) : chunk;
                if (chc_in_submit(&in, bytes + fed, take, &e)) {
                    *err = e; chc_in_free(&in); return -1;
                }
                fed += take;
                continue;
            }
            *err = e;                             /* real error or unexpected NULL */
            chc_in_free(&in);
            return -1;
        }
        out[bi] = b;
        chc_in_reset(&in);                        /* drop the just-parsed packet */
    }
    *consumed = in.consumed;
    chc_in_free(&in);
    return 0;
}

/* ---------------- tests -------------------------------------------------- */

static void
test_ioless_basic(void)
{
    current_test = "ioless_basic";
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {};
    chc_in in;
    CHECK_OK(chc_in_init_ioless(&in, &al), err);

    uint8_t b = 0;
    int rc = chc__read_byte(&in, &b, &err);
    CHECK(rc == CHC_WOULD_BLOCK);
    CHECK_EQ_U64(chc_in_available(&in), 0);

    chc_err_reset(&err);
    uint8_t data[2] = { 0x11, 0x22 };
    CHECK_OK(chc_in_submit(&in, data, 2, &err), err);
    CHECK_EQ_U64(chc_in_available(&in), 2);

    rc = chc__read_byte(&in, &b, &err); CHECK(rc == CHC_OK); CHECK_EQ_U64(b, 0x11);
    rc = chc__read_byte(&in, &b, &err); CHECK(rc == CHC_OK); CHECK_EQ_U64(b, 0x22);
    CHECK_EQ_U64(in.consumed, 2);

    chc_err_reset(&err);
    rc = chc__read_byte(&in, &b, &err);
    CHECK(rc == CHC_WOULD_BLOCK);
out:
    chc_in_free(&in);
}

static void
test_ioless_checkpoint_rewind(void)
{
    current_test = "ioless_checkpoint_rewind";
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {};
    chc_in in;
    CHECK_OK(chc_in_init_ioless(&in, &al), err);

    uint8_t src[10];
    for (int i = 0; i < 10; i++) src[i] = (uint8_t) (0xA0 + i);

    /* Feed first 5, checkpoint, read 3, then attempt 4 (only 2 left). */
    CHECK_OK(chc_in_submit(&in, src, 5, &err), err);
    chc__in_checkpoint(&in);
    uint8_t got[3];
    CHECK_OK(chc__read_bytes(&in, got, 3, &err), err);
    CHECK(memcmp(got, src, 3) == 0);
    CHECK_EQ_U64(in.consumed, 3);

    chc_err_reset(&err);
    uint8_t got4[4];
    int rc = chc__read_bytes(&in, got4, 4, &err);   /* drains, then would-block */
    CHECK(rc == CHC_WOULD_BLOCK);

    /* Rewind un-counts the partial read so consumed matches the checkpoint. */
    CHECK_OK(chc__in_rewind(&in), err);
    CHECK_EQ_U64(in.consumed, 0);

    /* Feed the rest, re-read 7 from the checkpoint: identical bytes. */
    chc_err_reset(&err);
    CHECK_OK(chc_in_submit(&in, src + 5, 5, &err), err);
    uint8_t got7[7];
    CHECK_OK(chc__read_bytes(&in, got7, 7, &err), err);
    CHECK(memcmp(got7, src, 7) == 0);
    CHECK_EQ_U64(in.consumed, 7);
out:
    chc_in_free(&in);
}

static void
test_ioless_mark_growth(void)
{
    current_test = "ioless_mark_growth";
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {};
    chc_in in;
    CHECK_OK(chc_in_init_ioless(&in, &al), err);

    uint8_t src[16];
    for (int i = 0; i < 16; i++) src[i] = (uint8_t) i;

    /* Consume 4, set a checkpoint, then feed many more times: the growth must
     * preserve bytes from the mark onward even though [0,mark) is droppable. */
    CHECK_OK(chc_in_submit(&in, src, 6, &err), err);
    uint8_t tmp[4];
    CHECK_OK(chc__read_bytes(&in, tmp, 4, &err), err);   /* pos = 4 */
    chc__in_checkpoint(&in);                              /* mark = 4 */
    for (int i = 6; i < 16; i++)
        CHECK_OK(chc_in_submit(&in, src + i, 1, &err), err); /* forces compaction */

    /* From the mark we should see src[4..16). */
    uint8_t rest[12];
    CHECK_OK(chc__read_bytes(&in, rest, 12, &err), err);
    CHECK(memcmp(rest, src + 4, 12) == 0);

    /* Rewind to the mark and re-read: same 12 bytes, no loss. */
    CHECK_OK(chc__in_rewind(&in), err);
    uint8_t rest2[12];
    CHECK_OK(chc__read_bytes(&in, rest2, 12, &err), err);
    CHECK(memcmp(rest2, src + 4, 12) == 0);
out:
    chc_in_free(&in);
}

static void
test_ioless_reset_compacts(void)
{
    current_test = "ioless_reset_compacts";
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {};
    chc_in in;
    CHECK_OK(chc_in_init_ioless(&in, &al), err);

    uint8_t src[8] = { 0,1,2,3,4,5,6,7 };
    CHECK_OK(chc_in_submit(&in, src, 8, &err), err);
    uint8_t tmp[5];
    CHECK_OK(chc__read_bytes(&in, tmp, 5, &err), err);
    CHECK_EQ_U64(in.consumed, 5);
    CHECK_EQ_U64(chc_in_available(&in), 3);

    chc_in_reset(&in);                       /* drop [0,5), keep [5,8) */
    CHECK_EQ_U64(chc_in_available(&in), 3);
    CHECK_EQ_U64(in.consumed, 5);            /* consumed total preserved */

    uint8_t rest[3];
    CHECK_OK(chc__read_bytes(&in, rest, 3, &err), err);
    CHECK(memcmp(rest, src + 5, 3) == 0);
    CHECK_EQ_U64(in.consumed, 8);
out:
    chc_in_free(&in);
}

static void
test_golden_chunk(void)
{
    current_test = "golden_chunk";
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {};
    chc_block_opts opts = { .has_block_info = true, .has_custom_serialization = true };

    size_t len = 0;
    uint8_t *stream = test_build_golden_stream(&al, &opts, 3000, &len);
    CHECK(stream != NULL); if (!stream) return;

    chc_block *oracle[TEST_GOLDEN_BLOCKS] = {};
    uint64_t oracle_consumed = 0;
    int rc = test_decode_blocks_io(stream, len, &al, &opts, oracle,
                                   TEST_GOLDEN_BLOCKS, &oracle_consumed, &err);
    if (rc != CHC_OK) {
        fprintf(stderr, "%s: oracle decode failed: %s\n", current_test, err.msg);
        fail_count++;
        test_free_blocks(oracle, TEST_GOLDEN_BLOCKS, &al);
        free(stream);
        return;
    }
    CHECK_EQ_U64(oracle_consumed, len);     /* reader consumes exactly the stream */

    static const size_t chunks[] = { 1, 2, 3, 7, 64, 1u << 20 };
    for (size_t ci = 0; ci < sizeof chunks / sizeof *chunks; ci++) {
        chc_block *subj[TEST_GOLDEN_BLOCKS] = {};
        uint64_t subj_consumed = 0;
        chc_err e = {};
        int sr = decode_ioless(stream, len, chunks[ci], &al, &opts, subj,
                             &subj_consumed, &e);
        if (sr != 0) {
            fprintf(stderr, "%s: chunk=%zu decode failed: %s\n",
                    current_test, chunks[ci], e.msg);
            fail_count++;
            test_free_blocks(subj, TEST_GOLDEN_BLOCKS, &al);
            continue;
        }
        if (subj_consumed != oracle_consumed) {
            fprintf(stderr, "%s: chunk=%zu consumed %llu != oracle %llu\n",
                    current_test, chunks[ci],
                    (unsigned long long) subj_consumed,
                    (unsigned long long) oracle_consumed);
            fail_count++;
        }
        for (size_t i = 0; i < TEST_GOLDEN_BLOCKS; i++) {
            if (!test_block_eq(oracle[i], subj[i])) {
                fprintf(stderr, "%s: chunk=%zu block %zu mismatch\n",
                        current_test, chunks[ci], i);
                fail_count++;
            }
        }
        test_free_blocks(subj, TEST_GOLDEN_BLOCKS, &al);
    }

    test_free_blocks(oracle, TEST_GOLDEN_BLOCKS, &al);
    free(stream);
}

/* ---------------- compressed-path golden gate ---------------------------- */

/* The compressed Data path wraps the ioless raw chc_in in a nested
 * io-backed dec_in (over chc__decomp_io_read). This is the layer where a
 * CHC_WOULD_BLOCK could be reclassified as CHC_ERR_IO: dec_in's refill (and
 * its read_bytes bypass) return the underlying read's status verbatim, so
 * the CHC_WOULD_BLOCK the phase-1 feed source raises propagates intact
 * rather than being flattened. This test proves it end-to-end, multi-frame. */

static uint8_t *
build_compressed_block(const chc_alloc *al, const chc_block_opts *opts,
                       const chc_codec *codec, size_t *out_raw, size_t *out_comp)
{
    /* >64 KiB raw so it spans multiple compressed frames. */
    test_mem_sink raw;
    chc_io rio;
    test_mem_sink_init(&raw, &rio);
    chc_err err = {};
    if (test_write_uint_string_block(&rio, al, opts, 30000, &err)) {
        fprintf(stderr, "build_compressed_block: raw: %s\n", err.msg);
        test_mem_sink_free(&raw); return NULL;
    }
    test_mem_sink comp;
    chc_io cio;
    test_mem_sink_init(&comp, &cio);
    if (chc__comp_emit_chunks(&cio, codec, CHC_COMP_LZ4, raw.data, raw.len, al, &err)) {
        fprintf(stderr, "build_compressed_block: compress: %s\n", err.msg);
        test_mem_sink_free(&raw); test_mem_sink_free(&comp); return NULL;
    }
    *out_raw = raw.len;
    *out_comp = comp.len;
    test_mem_sink_free(&raw);
    return comp.data;
}

static int
decode_io_comp(const uint8_t *bytes, size_t len, const chc_codec *codec,
               const chc_alloc *al, const chc_block_opts *opts,
               chc_block **out, uint64_t *consumed, chc_err *err)
{
    test_mem_src m;
    chc_io rio;
    test_mem_src_init(&m, &rio, bytes, len);
    chc_in raw;
    if (chc_in_init(&raw, &rio, al, 0, err)) return -1;
    chc__decomp_src src;
    chc_io dio;
    chc__decomp_src_init(&src, &raw, codec, al, &dio);
    chc_in dec;
    if (chc_in_init(&dec, &dio, al, 0, err)) {
        chc__decomp_src_free(&src); chc_in_free(&raw); return -1;
    }
    int rc = chc_block_read(&dec, al, opts, out, err);
    *consumed = raw.consumed;
    chc_in_free(&dec);
    chc__decomp_src_free(&src);
    chc_in_free(&raw);
    return (rc == CHC_OK && *out) ? 0 : -1;
}

static int
decode_ioless_comp(const uint8_t *bytes, size_t len, size_t chunk,
                 const chc_codec *codec, const chc_alloc *al,
                 const chc_block_opts *opts, chc_block **out,
                 uint64_t *consumed, chc_err *err)
{
    chc_in raw;
    if (chc_in_init_ioless(&raw, al)) return -1;
    size_t fed = 0;
    chc_block *b = NULL;
    for (;;) {
        chc__in_checkpoint(&raw);
        /* Per-attempt decomp state, rebuilt each retry (baseline). */
        chc__decomp_src src;
        chc_io dio;
        chc__decomp_src_init(&src, &raw, codec, al, &dio);
        chc_in dec;
        chc_err e = {};
        if (chc_in_init(&dec, &dio, al, 0, &e)) {
            chc__decomp_src_free(&src); *err = e; chc_in_free(&raw); return -1;
        }
        int rc = chc_block_read(&dec, al, opts, &b, &e);
        chc_in_free(&dec);
        chc__decomp_src_free(&src);
        if (rc == CHC_OK && b) break;
        if (rc == CHC_WOULD_BLOCK) {
            chc__in_rewind(&raw);
            if (fed >= len) {
                chc__err_set(err, CHC_ERR_EOF, "feed underrun");
                chc_in_free(&raw); return -1;
            }
            size_t take = (len - fed) < chunk ? (len - fed) : chunk;
            if (chc_in_submit(&raw, bytes + fed, take, &e)) {
                *err = e; chc_in_free(&raw); return -1;
            }
            fed += take;
            continue;
        }
        *err = e; chc_in_free(&raw); return -1;
    }
    *out = b;
    *consumed = raw.consumed;
    chc_in_free(&raw);
    return 0;
}

static void
test_golden_chunk_compressed(void)
{
    current_test = "golden_chunk_compressed";
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {};
    chc_block_opts opts = { .has_block_info = true, .has_custom_serialization = true };
    chc_codec codec;
    chc_lz4_codec_init(&codec);

    size_t raw_len = 0, comp_len = 0;
    uint8_t *comp = build_compressed_block(&al, &opts, &codec, &raw_len, &comp_len);
    CHECK(comp != NULL); if (!comp) return;
    CHECK(raw_len > CHC_COMPRESS_MAX_CHUNK);   /* genuinely multi-frame */

    chc_block *oracle = NULL;
    uint64_t oracle_consumed = 0;
    int rc = decode_io_comp(comp, comp_len, &codec, &al, &opts,
                            &oracle, &oracle_consumed, &err);
    if (rc != 0) {
        fprintf(stderr, "%s: oracle decode failed: %s\n", current_test, err.msg);
        fail_count++; free(comp); return;
    }
    CHECK_EQ_U64(oracle_consumed, comp_len);

    static const size_t chunks[] = { 1, 2, 3, 7, 64, 4096, 1u << 20 };
    for (size_t ci = 0; ci < sizeof chunks / sizeof *chunks; ci++) {
        chc_block *subj = NULL;
        uint64_t subj_consumed = 0;
        chc_err e = {};
        int sr = decode_ioless_comp(comp, comp_len, chunks[ci], &codec, &al, &opts,
                                  &subj, &subj_consumed, &e);
        if (sr != 0) {
            fprintf(stderr, "%s: chunk=%zu decode failed: %s\n",
                    current_test, chunks[ci], e.msg);
            fail_count++;
        } else {
            if (subj_consumed != oracle_consumed) {
                fprintf(stderr, "%s: chunk=%zu consumed %llu != oracle %llu\n",
                        current_test, chunks[ci],
                        (unsigned long long) subj_consumed,
                        (unsigned long long) oracle_consumed);
                fail_count++;
            }
            if (!test_block_eq(oracle, subj)) {
                fprintf(stderr, "%s: chunk=%zu block mismatch\n",
                        current_test, chunks[ci]);
                fail_count++;
            }
        }
        chc_block_destroy(subj, &al);
    }

    chc_block_destroy(oracle, &al);
    free(comp);
}

int
main(void)
{
    test_ioless_basic();
    test_ioless_checkpoint_rewind();
    test_ioless_mark_growth();
    test_ioless_reset_compacts();
    test_golden_chunk();
    test_golden_chunk_compressed();

    if (fail_count) {
        fprintf(stderr, "%d check(s) failed\n", fail_count);
        return 1;
    }
    printf("all ioless tests passed\n");
    return 0;
}
