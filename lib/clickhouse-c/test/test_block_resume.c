/*
 * test_block_resume.c -- level-3 column resumption: chc__block_resume_in keeps
 * a partial block alive across CHC_WOULD_BLOCK, re-parsing only the in-progress
 * column. No server needed.
 *
 * Same fixture shape as test_ioless.c (wide UInt32+String, Nullable(String)+
 * Array(UInt32), LowCardinality(String)), decoded two ways and compared:
 *   - oracle:  io-backed chc_in over a memory source (whole stream at once).
 *   - subject: ioless chc_in driven by chc__block_resume_in with a persisted
 *              partial block + next_col across would-blocks, fed in adversarial
 *              chunkings. On would-block the driver does NOT free the partial
 *              and does NOT rewind (resume owns that): it only submits the next
 *              chunk and re-calls with the SAME partial/next_col.
 * The subject must reconstruct a byte-identical block sequence and an identical
 * `consumed` total, and must demonstrably retain a partial block mid-column
 * rather than restart from the header each chunk. Run under ASan/valgrind to
 * catch partial frees missed on a would-block rewind.
 */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHC_PROVIDE_STDLIB_ALLOC
#define CHC_IMPLEMENTATION
#define CHC_NO_ZSTD
#include "clickhouse.h"

static int        fail_count = 0;
static const char *current_test = "";

#include "test_common.h"
#include "test_block_compare.h"
#include "test_golden_blocks.h"

/* Counters proving retention: how often resume returned WOULD_BLOCK with a
 * partial block whose next_col was already > 0 (i.e. some columns retained),
 * vs returns where next_col was still 0 (header re-/parse, nothing retained). */
typedef struct {
    int wb_retained_midcol;   /* WOULD_BLOCK with partial != NULL && next_col > 0 */
    int wb_header;            /* WOULD_BLOCK with partial == NULL (mid-header) */
    size_t max_next_col;      /* highest next_col observed on a WOULD_BLOCK */
} resume_stats;

/* Resumption driver: persist a partial block + next_col across WOULD_BLOCK.
 * On would-block do NOT free partial and do NOT rewind; resume owns the in
 * checkpoint/rewind. Just submit the next chunk and re-call with the SAME
 * partial/next_col. chc_in_reset at each completed packet (compaction). */
static int
decode_resume(const uint8_t *bytes, size_t len, size_t chunk, const chc_alloc *al,
              const chc_block_opts *opts, chc_block **out, uint64_t *consumed,
              resume_stats *st, chc_err *err)
{
    chc_in in;
    if (chc_in_init_ioless(&in, al)) return -1;
    size_t fed = 0;
    for (size_t bi = 0; bi < TEST_GOLDEN_BLOCKS; bi++) {
        chc_block *partial = NULL;
        size_t next_col = 0;
        for (;;) {
            chc_err e = {};
            int rc = chc__block_resume_in(&in, al, opts, &partial, &next_col, &e);
            if (rc == CHC_OK && partial) break;
            if (rc == CHC_WOULD_BLOCK) {
                if (st) {
                    if (partial && next_col > 0) st->wb_retained_midcol++;
                    else                          st->wb_header++;
                    if (next_col > st->max_next_col) st->max_next_col = next_col;
                }
                if (fed >= len) {                 /* stream exhausted mid-block */
                    chc_block_destroy(partial, al);
                    chc__err_set(err, CHC_ERR_EOF, "feed underrun");
                    chc_in_free(&in);
                    return -1;
                }
                size_t take = (len - fed) < chunk ? (len - fed) : chunk;
                if (chc_in_submit(&in, bytes + fed, take, &e)) {
                    chc_block_destroy(partial, al);
                    *err = e; chc_in_free(&in); return -1;
                }
                fed += take;
                continue;
            }
            chc_block_destroy(partial, al);       /* real error or unexpected NULL */
            *err = e;
            chc_in_free(&in);
            return -1;
        }
        out[bi] = partial;
        chc_in_reset(&in);                         /* drop the just-parsed packet */
    }
    *consumed = in.consumed;
    chc_in_free(&in);
    return 0;
}

/* ---------------- tests -------------------------------------------------- */

static void
test_resume_golden(void)
{
    current_test = "resume_golden";
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {};
    chc_block_opts opts = { .has_block_info = true, .has_custom_serialization = true };

    size_t len = 0;
    uint8_t *stream = test_build_golden_stream(&al, &opts, 5000, &len);
    CHECK(stream != NULL); if (!stream) return;

    chc_block *oracle[TEST_GOLDEN_BLOCKS] = {};
    uint64_t oracle_consumed = 0;
    int rc = test_decode_blocks_io(stream, len, &al, &opts, oracle,
                                   TEST_GOLDEN_BLOCKS, &oracle_consumed, &err);
    if (rc != 0) {
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
        int sr = decode_resume(stream, len, chunks[ci], &al, &opts, subj,
                               &subj_consumed, NULL, &e);
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

/* Retention proof: with a tiny chunk size the wide block's second column (s,
 * the String column) cannot finish before the buffer drains, so resume must at
 * least once return WOULD_BLOCK holding a partial block with next_col > 0 (the
 * UInt32 column already retained). If resumption were broken (restart from
 * header each chunk) next_col would always be 0 and max_next_col would stay 0. */
static void
test_resume_retains(void)
{
    current_test = "resume_retains";
    chc_alloc al = chc_alloc_stdlib();
    chc_block_opts opts = { .has_block_info = true, .has_custom_serialization = true };

    size_t len = 0;
    uint8_t *stream = test_build_golden_stream(&al, &opts, 5000, &len);
    CHECK(stream != NULL); if (!stream) return;

    chc_block *subj[TEST_GOLDEN_BLOCKS] = {};
    uint64_t subj_consumed = 0;
    resume_stats st = {};
    chc_err e = {};
    int sr = decode_resume(stream, len, 7, &al, &opts, subj, &subj_consumed, &st, &e);
    if (sr != 0) {
        fprintf(stderr, "%s: decode failed: %s\n", current_test, e.msg);
        fail_count++;
        test_free_blocks(subj, TEST_GOLDEN_BLOCKS, &al);
        free(stream);
        return;
    }

    /* A partial block with at least one fully-decoded column was retained
     * across a would-block, not restarted from the header. */
    CHECK(st.wb_retained_midcol > 0);
    CHECK(st.max_next_col >= 1);
    /* Mid-header would-blocks happen too (the header doesn't fit in 7 bytes),
     * but retention must dominate for a 5000-row column over 7-byte chunks. */
    CHECK(st.wb_retained_midcol > st.wb_header);

    test_free_blocks(subj, TEST_GOLDEN_BLOCKS, &al);
    free(stream);
}

int
main(void)
{
    test_resume_golden();
    test_resume_retains();

    if (fail_count) {
        fprintf(stderr, "%d check(s) failed\n", fail_count);
        return 1;
    }
    printf("all block_resume tests passed\n");
    return 0;
}
