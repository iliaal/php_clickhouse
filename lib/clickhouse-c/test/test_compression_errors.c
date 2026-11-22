/*
 * test_compression_errors.c -- frame header rejection, codec adapter failure
 * returns, and the sink growth paths of clickhouse-compression.h. No server.
 */

#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHC_PROVIDE_STDLIB_ALLOC
#define CHC_IMPLEMENTATION
#include "clickhouse.h"
#include "clickhouse-compression.h"

static int fail_count = 0;
static const char *current_test = "";

#include "test_common.h"

/* ---------------- frame construction ------------------------------------- */

typedef struct { uint8_t d[512]; size_t n; } fbuf;

static void
put_u32(uint8_t *p, uint32_t v)
{
    for (int i = 0; i < 4; i++) p[i] = (uint8_t) (v >> (8 * i));
}

/* One wire frame: cityhash128 of header+payload, then header, then payload.
 * `comp_with_hdr` and `orig` are written verbatim so malformed headers can
 * be exercised. */
static void
frame(fbuf *b, uint8_t method, uint32_t comp_with_hdr, uint32_t orig,
      const uint8_t *payload, size_t payload_len)
{
    uint8_t hdr[CHC__COMP_HEADER_BYTES];
    hdr[0] = method;
    put_u32(hdr + 1, comp_with_hdr);
    put_u32(hdr + 5, orig);

    uint8_t hashed[256];
    memcpy(hashed, hdr, sizeof hdr);
    memcpy(hashed + sizeof hdr, payload, payload_len);

    uint64_t lo, hi;
    chc_cityhash128(hashed, sizeof hdr + payload_len, &lo, &hi);

    b->n = 0;
    for (int i = 0; i < 8; i++) b->d[b->n++] = (uint8_t) (lo >> (8 * i));
    for (int i = 0; i < 8; i++) b->d[b->n++] = (uint8_t) (hi >> (8 * i));
    memcpy(b->d + b->n, hashed, sizeof hdr + payload_len);
    b->n += sizeof hdr + payload_len;
}

static void
expect_frame_error(const char *what, const fbuf *b, const chc_codec *codec,
                   int want_rc)
{
    chc_alloc al = chc_alloc_stdlib();
    test_mem_src src;
    chc_io io;
    chc_err err = {};
    chc_in raw;

    test_mem_src_init(&src, &io, b->d, b->n);
    if (chc_in_init(&raw, &io, &al, 0, &err) != CHC_OK) { fail_count++; return; }

    chc__decomp_src ds;
    chc_io dio;
    chc__decomp_src_init(&ds, &raw, codec, &al, &dio);
    int rc = chc__decomp_read_frame(&ds, &err);
    if (rc != want_rc) {
        fprintf(stderr, "%s: FAIL: %s rc=%d want %d err='%s'\n",
                current_test, what, rc, want_rc, err.msg);
        fail_count++;
    }
    chc__decomp_src_free(&ds);
    chc_in_free(&raw);
}

static void
test_frame_rejects(void)
{
    current_test = "frame_rejects";
    chc_codec lz4 = {}, zstd = {}, none = {};
    chc_lz4_codec_init(&lz4);
    chc_zstd_codec_init(&zstd);

    const uint8_t payload[4] = { 1, 2, 3, 4 };
    fbuf b;

    frame(&b, CHC__COMP_LZ4, 8, 4, payload, 0);
    expect_frame_error("short frame", &b, &lz4, CHC_ERR_PROTOCOL);

    frame(&b, CHC__COMP_LZ4, 0x40000001u, 4, payload, 0);
    expect_frame_error("oversized frame", &b, &lz4, CHC_ERR_PROTOCOL);

    frame(&b, 0x77, CHC__COMP_HEADER_BYTES + 4, 4, payload, 4);
    expect_frame_error("unknown method", &b, &lz4, CHC_ERR_PROTOCOL);

    frame(&b, CHC__COMP_LZ4, CHC__COMP_HEADER_BYTES + 4, 4, payload, 4);
    expect_frame_error("lz4 without codec", &b, &none, CHC_ERR_USAGE);
    expect_frame_error("lz4 payload garbage", &b, &lz4, CHC_ERR_PROTOCOL);

    frame(&b, CHC__COMP_ZSTD, CHC__COMP_HEADER_BYTES + 4, 4, payload, 4);
    expect_frame_error("zstd without codec", &b, &none, CHC_ERR_USAGE);
    expect_frame_error("zstd payload garbage", &b, &zstd, CHC_ERR_PROTOCOL);

    /* Hash covering the wrong bytes. */
    frame(&b, CHC__COMP_LZ4, CHC__COMP_HEADER_BYTES + 4, 4, payload, 4);
    b.d[0] ^= 0xff;
    expect_frame_error("hash mismatch", &b, &lz4, CHC_ERR_PROTOCOL);
}

/* ---------------- emit side ---------------------------------------------- */

static void
test_emit_frame(void)
{
    current_test = "emit_frame";
    chc_alloc al = chc_alloc_stdlib();
    chc_codec lz4 = {};
    chc_lz4_codec_init(&lz4);
    test_mem_sink sink;
    chc_io io;
    chc_err err = {};
    const char src[] = "compressible compressible compressible";

    test_mem_sink_init(&sink, &io);
    CHECK(chc__comp_emit_frame(&io, &lz4, CHC_COMP_NONE, src, sizeof src - 1,
                               &al, &err) == CHC_ERR_USAGE);
    test_mem_sink_free(&sink);

    /* Chunked emit splits past CHC_COMPRESS_MAX_CHUNK & stops at the first
     * frame the codec refuses. */
    static uint8_t bulk[CHC_COMPRESS_MAX_CHUNK + 1024];
    test_mem_sink_init(&sink, &io);
    CHECK(chc__comp_emit_chunks(&io, &lz4, CHC_COMP_LZ4, bulk, sizeof bulk,
                                &al, &err) == CHC_OK);
    CHECK(sink.len > 0);
    test_mem_sink_free(&sink);

    test_mem_sink_init(&sink, &io);
    CHECK(chc__comp_emit_chunks(&io, &lz4, CHC_COMP_NONE, bulk, sizeof bulk,
                                &al, &err) == CHC_ERR_USAGE);
    test_mem_sink_free(&sink);

    /* A codec without a bound hook falls back to the lz4-style estimate. */
    chc_codec no_bound = lz4;
    no_bound.lz4_bound = NULL;
    test_mem_sink_init(&sink, &io);
    CHECK(chc__comp_emit_frame(&io, &no_bound, CHC_COMP_LZ4, src, sizeof src - 1,
                               &al, &err) == CHC_OK);
    CHECK(sink.len > 0);
    test_mem_sink_free(&sink);
}

static void
test_mem_sink_growth(void)
{
    current_test = "mem_sink_growth";
    chc_alloc al = chc_alloc_stdlib();
    chc__mem_sink s;
    chc_io io;
    chc_err err = {};

    /* Past the 4 KiB seed so the doubling loop runs. */
    static uint8_t big[9000];
    chc__mem_sink_init(&s, &io, &al);
    CHECK(io.write(io.ud, big, sizeof big, &err) == CHC_OK);
    CHECK_EQ_U64(s.len, sizeof big);
    chc__mem_sink_free(&s);

    /* Length counter about to wrap. */
    chc__mem_sink_init(&s, &io, &al);
    s.len = SIZE_MAX - 1;
    CHECK(io.write(io.ud, big, 2, &err) == CHC_ERR_OOM);
    CHECK(io.write(io.ud, big, 1, &err) == CHC_ERR_OOM);   /* sticky */
    s.len = 0;
    chc__mem_sink_free(&s);
}

/* ---------------- codec adapters ----------------------------------------- */

static void
test_codec_adapters(void)
{
    current_test = "codec_adapters";
    chc_codec lz4 = {}, zstd = {};
    chc_lz4_codec_init(&lz4);
    chc_zstd_codec_init(&zstd);
    chc_err err = {};

    uint8_t src[128], dst[512];
    for (size_t i = 0; i < sizeof src; i++) src[i] = (uint8_t) i;
    size_t n = 0;

    /* Input past the LZ4 domain is rejected before the buffer is touched. */
    CHECK(lz4.lz4_compress(NULL, src, (size_t) LZ4_MAX_INPUT_SIZE + 1,
                           dst, sizeof dst, &n, &err) == CHC_ERR_USAGE);
    CHECK(lz4.lz4_compress(NULL, src, sizeof src, dst, 1, &n, &err) == CHC_ERR_OOM);
    CHECK(lz4.lz4_decompress(NULL, src, sizeof src, dst, sizeof dst, &err)
          == CHC_ERR_PROTOCOL);

    CHECK(zstd.zstd_compress(NULL, src, sizeof src, dst, 1, &n, &err) == CHC_ERR_OOM);
    CHECK(zstd.zstd_decompress(NULL, src, sizeof src, dst, sizeof dst, &err)
          == CHC_ERR_PROTOCOL);

    /* Well-formed zstd frame decoded against the wrong original size. */
    CHECK(zstd.zstd_compress(NULL, src, 16, dst, sizeof dst, &n, &err) == CHC_OK);
    uint8_t out[64];
    CHECK(zstd.zstd_decompress(NULL, dst, n, out, sizeof out, &err)
          == CHC_ERR_PROTOCOL);
}

int
main(void)
{
    test_frame_rejects();
    test_emit_frame();
    test_mem_sink_growth();
    test_codec_adapters();

    if (fail_count) {
        fprintf(stderr, "%d failure(s)\n", fail_count);
        return 1;
    }
    printf("all compression_errors tests passed\n");
    return 0;
}
