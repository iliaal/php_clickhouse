/*
 * test_golden_blocks.h -- shared multi-block fixtures for ioless tests
 */

#ifndef CLICKHOUSE_TEST_GOLDEN_BLOCKS_H
#define CLICKHOUSE_TEST_GOLDEN_BLOCKS_H

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "test_common.h"

#define TEST_GOLDEN_BLOCKS 3

CHC_MAYBE_UNUSED static int
test_write_uint_string_block(chc_io *io, const chc_alloc *al,
                             const chc_block_opts *opts, int n_rows,
                             chc_err *err)
{
    int rc = -1;
    chc_block_col bbcols[2];
    chc_block_builder bb;
    chc_type *tu = NULL, *ts = NULL;
    uint32_t *u = malloc((size_t) n_rows * sizeof *u);
    uint64_t *off = malloc((size_t) n_rows * sizeof *off);
    char *sdata = malloc((size_t) n_rows * 12);
    if (!u || !off || !sdata) goto done;

    size_t total = 0;
    for (int i = 0; i < n_rows; i++) {
        u[i] = (uint32_t) (i * 2654435761u);
        int k = snprintf(sdata + total, 12, "%d", i);
        total += (size_t) k;
        off[i] = total;
    }
    chc_block_builder_init(&bb, bbcols);
    if (chc_type_parse("UInt32", 6, al, &tu, err)) goto done;
    if (chc_type_parse("String", 6, al, &ts, err)) goto done;
    chc_column ucol = chc_build_fixed(u, sizeof u[0], (size_t) n_rows);
    chc_block_builder_append(&bb, "u", 1, tu, &ucol);
    chc_column scol = chc_build_string(off, (uint8_t *) sdata, (size_t) n_rows);
    chc_block_builder_append(&bb, "s", 1, ts, &scol);
    rc = chc_block_write(io, &bb, opts, err);
done:
    chc_type_destroy(tu, al);
    chc_type_destroy(ts, al);
    free(u);
    free(off);
    free(sdata);
    return rc;
}

CHC_MAYBE_UNUSED static int
test_write_nullable_array_block(chc_io *io, const chc_alloc *al,
                                const chc_block_opts *opts, chc_err *err)
{
    int rc = -1;
    chc_block_col bbcols[2];
    chc_block_builder bb;
    chc_type *tn = NULL, *ta = NULL;
    uint8_t  nulls[4] = { 0, 1, 0, 1 };
    uint64_t noff[4] = { 2, 2, 5, 5 };
    const uint8_t nbuf[] = "abcde";
    uint64_t aoff[4] = { 3, 3, 6, 10 };
    uint32_t aval[10] = { 1,2,3, 7,8,9, 10,11,12,13 };

    chc_block_builder_init(&bb, bbcols);
    if (chc_type_parse("Nullable(String)", 16, al, &tn, err)) goto done;
    if (chc_type_parse("Array(UInt32)", 13, al, &ta, err)) goto done;
    chc_column strings = chc_build_string(noff, nbuf, 4);
    chc_column nullable = chc_build_nullable(nulls, &strings);
    chc_block_builder_append(&bb, "n", 1, tn, &nullable);
    chc_column values = chc_build_fixed(aval, sizeof aval[0], 10);
    chc_column array = chc_build_array(aoff, 4, &values);
    chc_block_builder_append(&bb, "a", 1, ta, &array);
    rc = chc_block_write(io, &bb, opts, err);
done:
    chc_type_destroy(tn, al);
    chc_type_destroy(ta, al);
    return rc;
}

CHC_MAYBE_UNUSED static int
test_write_lc_string_block(chc_io *io, const chc_alloc *al,
                           const chc_block_opts *opts, chc_err *err)
{
    int rc = -1;
    chc_block_col bbcols[2];
    chc_block_builder bb;
    chc_type *t = NULL;
    uint64_t doff[3] = { 3, 8, 12 };
    const uint8_t ddata[] = "redgreenblue";
    uint8_t keys[5] = { 0, 2, 1, 0, 2 };

    chc_block_builder_init(&bb, bbcols);
    if (chc_type_parse("LowCardinality(String)", 22, al, &t, err)) goto done;
    chc_column dict = chc_build_string(doff, ddata, 3);
    chc_column lc = chc_build_lc(1, keys, 5, &dict);
    chc_block_builder_append(&bb, "lc", 2, t, &lc);
    rc = chc_block_write(io, &bb, opts, err);
done:
    chc_type_destroy(t, al);
    return rc;
}

#ifdef CLICKHOUSE_CLIENT_H

static int
test_write_data_header(chc_io *io, chc_err *err)
{
    int rc = chc__write_varuint(io, CHC_PKT_DATA, err);
    return rc ? rc : chc__write_string(io, "", 0, err);
}

CHC_MAYBE_UNUSED static int
test_write_uint_string_packet(chc_io *io, const chc_alloc *al,
                              const chc_block_opts *opts, int n_rows,
                              chc_err *err)
{
    int rc = test_write_data_header(io, err);
    return rc ? rc : test_write_uint_string_block(io, al, opts, n_rows, err);
}

CHC_MAYBE_UNUSED static int
test_write_nullable_array_packet(chc_io *io, const chc_alloc *al,
                                 const chc_block_opts *opts, chc_err *err)
{
    int rc = test_write_data_header(io, err);
    return rc ? rc : test_write_nullable_array_block(io, al, opts, err);
}

CHC_MAYBE_UNUSED static int
test_write_lc_string_packet(chc_io *io, const chc_alloc *al,
                            const chc_block_opts *opts, chc_err *err)
{
    int rc = test_write_data_header(io, err);
    return rc ? rc : test_write_lc_string_block(io, al, opts, err);
}

CHC_MAYBE_UNUSED static int
test_write_progress_packet(chc_io *io, chc_err *err)
{
    int rc;
    if ((rc = chc__write_varuint(io, CHC_PKT_PROGRESS, err))) return rc;
    if ((rc = chc__write_varuint(io, 1000, err))) return rc;
    if ((rc = chc__write_varuint(io, 8000, err))) return rc;
    if ((rc = chc__write_varuint(io, 5000, err))) return rc;
    if ((rc = chc__write_varuint(io, 40000, err))) return rc;
    if ((rc = chc__write_varuint(io, 7, err))) return rc;
    if ((rc = chc__write_varuint(io, 70, err))) return rc;
    return chc__write_varuint(io, 123456, err);
}

#ifdef CLICKHOUSE_COMPRESSION_H
CHC_MAYBE_UNUSED static int
test_write_compressed_packet(chc_io *io, const chc_alloc *al,
                             const chc_block_opts *opts, const chc_codec *codec,
                             int n_rows, chc_err *err)
{
    int rc = test_write_data_header(io, err);
    if (rc) return rc;
    test_mem_sink raw;
    chc_io raw_io;
    test_mem_sink_init(&raw, &raw_io);
    rc = test_write_uint_string_block(&raw_io, al, opts, n_rows, err);
    if (!rc)
        rc = chc__comp_emit_chunks(io, codec, CHC_COMP_LZ4,
                                   raw.data, raw.len, al, err);
    test_mem_sink_free(&raw);
    return rc;
}
#endif

#endif

CHC_MAYBE_UNUSED static uint8_t *
test_build_golden_stream(const chc_alloc *al, const chc_block_opts *opts,
                         int wide_rows, size_t *out_len)
{
    test_mem_sink s;
    chc_io io;
    test_mem_sink_init(&s, &io);
    chc_err err = {};
    if (test_write_uint_string_block(&io, al, opts, wide_rows, &err) ||
        test_write_nullable_array_block(&io, al, opts, &err) ||
        test_write_lc_string_block(&io, al, opts, &err)) {
        fprintf(stderr, "build_stream: %s\n", err.msg);
        test_mem_sink_free(&s);
        return NULL;
    }
    *out_len = s.len;
    return s.data;
}

CHC_MAYBE_UNUSED static int
test_decode_blocks_io(const uint8_t *bytes, size_t len, const chc_alloc *al,
                      const chc_block_opts *opts, chc_block **out,
                      size_t n_blocks, uint64_t *consumed, chc_err *err)
{
    test_mem_src m;
    chc_io io;
    test_mem_src_init(&m, &io, bytes, len);
    chc_in in;
    if (chc_in_init(&in, &io, al, 0, err)) return -1;
    for (size_t i = 0; i < n_blocks; i++) {
        int rc = chc_block_read(&in, al, opts, &out[i], err);
        if (rc != CHC_OK || !out[i]) {
            chc_in_free(&in);
            return -1;
        }
    }
    *consumed = in.consumed;
    chc_in_free(&in);
    return 0;
}

CHC_MAYBE_UNUSED static void
test_free_blocks(chc_block **b, size_t n_blocks, const chc_alloc *al)
{
    for (size_t i = 0; i < n_blocks; i++) {
        chc_block_destroy(b[i], al);
        b[i] = NULL;
    }
}

#endif
