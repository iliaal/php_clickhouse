/*
 * test_async_uring.c -- liburing validation of clickhouse-async.h. Proves the
 * ioless API drives from a real single-threaded C event loop.
 *
 * Spawns a clickhouse server (mirrors test_client_tcp.c), connects via an
 * io_uring connect SQE, then loops submitting recv SQEs (feeding bytes into
 * chc_async_submit) and send SQEs (draining chc_async_pending_out /
 * chc_async_consume_out), calling chc_async_handshake then chc_async_recv_packet
 * as completions arrive. Runs SELECT number FROM numbers(N) (N large enough to
 * span multiple recvs, proving recv resumption) and an INSERT round-trip;
 * asserts decoded blocks match expected and EndOfStream is reached.
 *
 * Runs the whole suite twice on separate connections: once uncompressed, once
 * LZ4-compressed. The compressed pass drives chc__recv_block_compressed_resume
 * over a real socket -- a SELECT result spanning many recvs forces would-blocks
 * mid-frame and mid-column, exercising frame-granularity resumption live.
 *
 * Gating: real body compiles only when <liburing.h> available. test.sh probes
 * for liburing and, when present, builds this with -DCHC_ASYNC_URING_TEST and
 * -luring; otherwise it falls back to a trivial skip stub that needs no -luring.
 * The manual line below mirrors that real build. A runtime skip (exit 0) also
 * fires if no `clickhouse` binary is on PATH.
 *
 * Manual compile + run (real path):
 *   cc -std=c11 -D_POSIX_C_SOURCE=200809L -O2 -Wall -Wextra \
 *      -Wno-unused-parameter -DCHC_ASYNC_URING_TEST -I. \
 *      test/test_async_uring.c -o /tmp/chc_test_async_uring -luring
 *   /tmp/chc_test_async_uring
 */

#include <stdio.h>
#include <stdlib.h>

#if defined(CHC_ASYNC_URING_TEST) && defined(__has_include)
#  if __has_include(<liburing.h>)
#    define CHC__URING_AVAILABLE 1
#  endif
#endif

#ifndef CHC__URING_AVAILABLE

int
main(void)
{
    /* Built without -DCHC_ASYNC_URING_TEST (e.g. plain ./test.sh) or liburing
     * absent: skip cleanly so CI stays green. */
    printf("skip: build with -DCHC_ASYNC_URING_TEST and link -luring "
           "(liburing absent or test not enabled)\n");
    return 0;
}

#else  /* CHC__URING_AVAILABLE */

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <liburing.h>

#define CHC_PROVIDE_STDLIB_ALLOC
#define CHC_IMPLEMENTATION
#define CHC_NO_ZSTD                 /* lz4 alone covers the compressed suite */
#include "clickhouse.h"
#include "clickhouse-compression.h"
#include "clickhouse-client.h"
#include "clickhouse-async.h"

static int        fail_count = 0;
static const char *current_test = "";

#include "test_common.h"
#include "test_clickhouse_server.h"

#define TEMP_ROOT_DIR "/tmp/ch-test-srv-uring"
#define TEST_PORT     19011

static pid_t g_server_pid = -1;

static void
stop_server(void)
{
    test_clickhouse_server_stop(&g_server_pid);
}

/* ---- single-ring, single-fd event loop ---------------------------------- */

/* Completion tags. */
enum { OP_CONNECT = 1, OP_RECV = 2, OP_SEND = 3 };

typedef struct {
    struct io_uring  ring;
    int              fd;
    chc_async_client *cli;
    chc_alloc        al;
    uint8_t          rbuf[65536];   /* socket read buffer */
    bool             recv_inflight;
    bool             send_inflight;
    size_t           send_len;      /* bytes the in-flight send was issued for */
    bool             eof;
    const char      *suite;         /* "" or "_lz4"; appended to test labels */
} loop_ctx;

static char g_label[64];

static const char *
suite_label(loop_ctx *L, const char *base)
{
    snprintf(g_label, sizeof g_label, "%s%s", base, L->suite);
    return g_label;
}

static struct io_uring_sqe *
get_sqe(loop_ctx *L)
{
    struct io_uring_sqe *sqe = io_uring_get_sqe(&L->ring);
    if (!sqe) { io_uring_submit(&L->ring); sqe = io_uring_get_sqe(&L->ring); }
    return sqe;
}

/* Issue a recv if none in flight and not at EOF. */
static void
arm_recv(loop_ctx *L)
{
    if (L->recv_inflight || L->eof) return;
    struct io_uring_sqe *sqe = get_sqe(L);
    io_uring_prep_recv(sqe, L->fd, L->rbuf, sizeof L->rbuf, 0);
    io_uring_sqe_set_data(sqe, (void *) (uintptr_t) OP_RECV);
    L->recv_inflight = true;
}

/* Issue a send for any pending out bytes if none in flight. */
static void
arm_send(loop_ctx *L)
{
    if (L->send_inflight) return;
    const uint8_t *ob; size_t ol;
    chc_async_pending_out(L->cli, &ob, &ol);
    if (ol == 0) return;
    struct io_uring_sqe *sqe = get_sqe(L);
    io_uring_prep_send(sqe, L->fd, ob, ol, 0);
    io_uring_sqe_set_data(sqe, (void *) (uintptr_t) OP_SEND);
    L->send_inflight = true;
    L->send_len = ol;
}

/* Pump the ring once: submit pending SQEs, wait for one completion, apply it.
 * Returns 0 on success, -1 on fatal error. */
static int
pump(loop_ctx *L, chc_err *err)
{
    arm_send(L);
    arm_recv(L);
    io_uring_submit(&L->ring);

    struct io_uring_cqe *cqe = NULL;
    int rc = io_uring_wait_cqe(&L->ring, &cqe);
    if (rc < 0) return chc__err_set(err, CHC_ERR_IO, "wait_cqe: %d", rc);

    int op = (int) (uintptr_t) io_uring_cqe_get_data(cqe);
    int res = cqe->res;
    io_uring_cqe_seen(&L->ring, cqe);

    switch (op) {
    case OP_CONNECT:
        if (res < 0) return chc__err_set(err, CHC_ERR_IO, "connect: %d", res);
        break;
    case OP_SEND:
        L->send_inflight = false;
        if (res < 0) return chc__err_set(err, CHC_ERR_IO, "send: %d", res);
        chc_async_consume_out(L->cli, (size_t) res);
        break;
    case OP_RECV:
        L->recv_inflight = false;
        if (res < 0) return chc__err_set(err, CHC_ERR_IO, "recv: %d", res);
        if (res == 0) { L->eof = true; break; }
        if (chc_async_submit(L->cli, L->rbuf, (size_t) res, err)) return -1;
        break;
    default:
        return chc__err_set(err, CHC_ERR_PROTOCOL, "bad completion tag %d", op);
    }
    return 0;
}

static int
do_handshake(loop_ctx *L, chc_err *err)
{
    chc_exception *exc = NULL;
    for (int i = 0; i < 100000; i++) {
        int rc = chc_async_handshake(L->cli, &exc, err);
        if (rc == CHC_OK) return CHC_OK;
        if (rc != CHC_WOULD_BLOCK) {
            if (exc) {  /* Preserve test diagnostics */
                chc__err_set(err, CHC_ERR_SERVER, "%s",
                             exc->display_text ? exc->display_text : "");
                chc_exception_free(exc, &L->al);
            }
            return rc;
        }
        if (pump(L, err) != 0) return CHC_ERR_IO;
        if (L->eof) return chc__err_set(err, CHC_ERR_EOF, "eof during handshake");
    }
    return chc__err_set(err, CHC_ERR_PROTOCOL, "handshake stalled");
}

/* Drive recv until EndOfStream / Exception, dispatching to cb(block). */
typedef void (*block_cb)(const chc_block *b, void *ud);

static int
recv_until_eos(loop_ctx *L, block_cb cb, void *ud, chc_err *err)
{
    for (int i = 0; i < 1000000; i++) {
        chc_packet pkt = {};
        int rc = chc_async_recv_packet(L->cli, &pkt, err);
        if (rc == CHC_WOULD_BLOCK) {
            if (pump(L, err) != 0) return CHC_ERR_IO;
            if (L->eof) return chc__err_set(err, CHC_ERR_EOF, "eof mid-stream");
            continue;
        }
        if (rc != CHC_OK) { chc_async_packet_clear(L->cli, &pkt); return rc; }
        if (pkt.kind == CHC_PKT_EXCEPTION) {
            chc__err_set(err, CHC_ERR_SERVER, "%s",
                pkt.exception ? pkt.exception->display_text : "");
            chc_async_packet_clear(L->cli, &pkt);
            return CHC_ERR_SERVER;
        }
        if (pkt.kind == CHC_PKT_DATA && pkt.block && cb)
            cb(pkt.block, ud);
        bool eos = pkt.kind == CHC_PKT_END_OF_STREAM;
        chc_async_packet_clear(L->cli, &pkt);
        if (eos) return CHC_OK;
    }
    return chc__err_set(err, CHC_ERR_PROTOCOL, "no end-of-stream");
}

/* ---- SELECT: sum & count number FROM numbers(N) ------------------------- */

typedef struct { uint64_t rows; uint64_t sum; } select_acc;

static void
select_cb(const chc_block *b, void *ud)
{
    select_acc *a = ud;
    size_t n = chc_block_n_rows(b);
    if (n == 0 || chc_block_n_columns(b) < 1) return;
    const chc_column *c0 = chc_block_column(b, 0);
    size_t es = 0;
    const uint64_t *vals = chc_column_fixed_data(c0, &es);
    if (es != 8 || !vals) return;
    for (size_t r = 0; r < n; r++) { a->sum += vals[r]; a->rows++; }
}

static void
test_uring_select(loop_ctx *L)
{
    current_test = suite_label(L, "uring_select");
    chc_err err = {};
    /* N big enough that the result spans several 64 KiB recvs, proving recv
     * resumption across CHC_WOULD_BLOCK. */
    const uint64_t N = 200000;
    char sql[64];
    int sn = snprintf(sql, sizeof sql, "SELECT number FROM numbers(%llu)",
                      (unsigned long long) N);
    int rc = chc_async_send_query(L->cli, sql, (size_t) sn, "", 0, &err);
    CHECK_OK(rc, err);

    select_acc acc = {};
    rc = recv_until_eos(L, select_cb, &acc, &err);
    CHECK_OK(rc, err);
    CHECK_EQ_U64(acc.rows, N);
    CHECK_EQ_U64(acc.sum, N * (N - 1) / 2);
out:
    return;
}

/* ---- INSERT round-trip -------------------------------------------------- */

static void
test_uring_insert(loop_ctx *L)
{
    current_test = suite_label(L, "uring_insert");
    chc_err err = {};
    chc_block_col bbcols[1];
    chc_block_builder bb;
    chc_type *tu = NULL;

    int rc = chc_async_send_query(L->cli,
        "DROP TABLE IF EXISTS test_uring SYNC",
        sizeof "DROP TABLE IF EXISTS test_uring SYNC" - 1, "", 0, &err);
    CHECK_OK(rc, err);
    rc = recv_until_eos(L, NULL, NULL, &err); CHECK_OK(rc, err);

    rc = chc_async_send_query(L->cli,
        "CREATE TABLE test_uring (x UInt32) ENGINE = Memory",
        sizeof "CREATE TABLE test_uring (x UInt32) ENGINE = Memory" - 1, "", 0, &err);
    CHECK_OK(rc, err);
    rc = recv_until_eos(L, NULL, NULL, &err); CHECK_OK(rc, err);

    /* INSERT: send query, server echoes an empty schema Data block, then we
     * send the data block + terminator and drain to EndOfStream. */
    rc = chc_async_send_query(L->cli,
        "INSERT INTO test_uring (x) VALUES",
        sizeof "INSERT INTO test_uring (x) VALUES" - 1, "", 0, &err);
    CHECK_OK(rc, err);

    /* Wait for the schema-echo Data packet before sending rows. */
    bool got_echo = false;
    for (int i = 0; i < 100000 && !got_echo; i++) {
        chc_packet pkt = {};
        rc = chc_async_recv_packet(L->cli, &pkt, &err);
        if (rc == CHC_WOULD_BLOCK) {
            if (pump(L, &err) != 0) { fail_count++; goto out; }
            continue;
        }
        CHECK_OK(rc, err);
        if (pkt.kind == CHC_PKT_DATA) got_echo = true;
        if (pkt.kind == CHC_PKT_EXCEPTION) {
            fprintf(stderr, "%s: insert echo exception: %s\n", current_test,
                    pkt.exception ? pkt.exception->display_text : "");
            fail_count++;
            chc_async_packet_clear(L->cli, &pkt);
            goto out;
        }
        chc_async_packet_clear(L->cli, &pkt);
    }
    CHECK(got_echo);

    uint32_t xs[4] = { 10, 20, 30, 40 };
    chc_block_builder_init(&bb, bbcols);
    rc = chc_type_parse("UInt32", 6, &L->al, &tu, &err); CHECK_OK(rc, err);
    chc_column cx = chc_build_fixed(xs, sizeof xs[0], 4);
    chc_block_builder_append(&bb, "x", 1, tu, &cx);
    rc = chc_async_send_data(L->cli, &bb, &err); CHECK_OK(rc, err);
    rc = chc_async_send_data_end(L->cli, &err); CHECK_OK(rc, err);
    /* Flush outbound + drain to EndOfStream. */
    rc = recv_until_eos(L, NULL, NULL, &err); CHECK_OK(rc, err);

    /* Read back: sum must be 100, 4 rows. */
    rc = chc_async_send_query(L->cli,
        "SELECT x FROM test_uring ORDER BY x",
        sizeof "SELECT x FROM test_uring ORDER BY x" - 1, "", 0, &err);
    CHECK_OK(rc, err);
    /* readback column is UInt32 (4-byte); tally inline. */
    bool eos = false;
    uint64_t sum = 0, rows = 0;
    for (int i = 0; i < 100000 && !eos; i++) {
        chc_packet pkt = {};
        rc = chc_async_recv_packet(L->cli, &pkt, &err);
        if (rc == CHC_WOULD_BLOCK) {
            if (pump(L, &err) != 0) { fail_count++; goto out; }
            continue;
        }
        CHECK_OK(rc, err);
        if (pkt.kind == CHC_PKT_DATA && pkt.block && chc_block_n_rows(pkt.block)) {
            const chc_column *c0 = chc_block_column(pkt.block, 0);
            size_t es = 0;
            const void *d = chc_column_fixed_data(c0, &es);
            for (size_t r = 0; r < chc_block_n_rows(pkt.block); r++) {
                uint32_t v;
                memcpy(&v, (const uint8_t *) d + r * es, sizeof v);
                sum += v; rows++;
            }
        }
        if (pkt.kind == CHC_PKT_END_OF_STREAM) eos = true;
        chc_async_packet_clear(L->cli, &pkt);
    }
    CHECK(eos);
    CHECK_EQ_U64(rows, 4);
    CHECK_EQ_U64(sum, 100);
out:
    chc_type_destroy(tu, &L->al);
}

/* One connection's worth: fresh ring + fd + client, handshake, run the SELECT
 * and INSERT tests, tear everything down. opts carries compression (NULL =
 * uncompressed). Each suite owns its ring so no SQE outlives its connection. */
static void
run_suite(const chc_client_opts *opts, const char *suite)
{
    chc_err err = {};
    loop_ctx L;
    memset(&L, 0, sizeof L);
    L.al = chc_alloc_stdlib();
    L.suite = suite;

    if (io_uring_queue_init(64, &L.ring, 0) < 0) {
        fprintf(stderr, "%s: io_uring_queue_init failed\n", suite);
        fail_count++; return;
    }

    L.fd = socket(AF_INET, SOCK_STREAM, 0);
    if (L.fd < 0) {
        fprintf(stderr, "%s: socket failed\n", suite);
        fail_count++; io_uring_queue_exit(&L.ring); return;
    }
    int one = 1;
    setsockopt(L.fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);

    struct sockaddr_in sa;
    memset(&sa, 0, sizeof sa);
    sa.sin_family = AF_INET;
    sa.sin_port   = htons((uint16_t) TEST_PORT);
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    struct io_uring_sqe *sqe = io_uring_get_sqe(&L.ring);
    io_uring_prep_connect(sqe, L.fd, (struct sockaddr *) &sa, sizeof sa);
    io_uring_sqe_set_data(sqe, (void *) (uintptr_t) OP_CONNECT);
    io_uring_submit(&L.ring);
    {
        struct io_uring_cqe *cqe = NULL;
        if (io_uring_wait_cqe(&L.ring, &cqe) < 0 || cqe->res < 0) {
            fprintf(stderr, "%s: connect failed: %d\n", suite, cqe ? cqe->res : -1);
            if (cqe) io_uring_cqe_seen(&L.ring, cqe);
            fail_count++; close(L.fd); io_uring_queue_exit(&L.ring); return;
        }
        io_uring_cqe_seen(&L.ring, cqe);
    }

    if (chc_async_client_init(&L.cli, opts, &L.al, &err)) {
        fprintf(stderr, "%s: client init: %s\n", suite, err.msg);
        fail_count++; close(L.fd); io_uring_queue_exit(&L.ring); return;
    }

    if (do_handshake(&L, &err) != CHC_OK) {
        fprintf(stderr, "%s: handshake: %s\n", suite, err.msg);
        fail_count++;
        goto done;
    }
    {
        const chc_server_info *si = chc_async_server_info(L.cli);
        CHECK(si != NULL);
        CHECK(strncmp(si->name, "ClickHouse", 10) == 0);
    }
    if (opts) {
        /* ClickHouse 26.9 defaults network compression to ZSTD */
        static const char set[] = "SET network_compression_method='lz4'";
        if (chc_async_send_query(L.cli, set, sizeof set - 1, "", 0, &err) != CHC_OK
            || recv_until_eos(&L, NULL, NULL, &err) != CHC_OK) {
            fprintf(stderr, "%s: set lz4: %s\n", suite, err.msg);
            fail_count++;
            goto done;
        }
    }

    test_uring_select(&L);
    test_uring_insert(&L);

done:
    chc_async_client_free(L.cli);
    close(L.fd);
    io_uring_queue_exit(&L.ring);
}

int
main(void)
{
    if (!test_clickhouse_on_path()) {
        printf("skip: clickhouse binary not on PATH\n");
        return 0;
    }
    g_server_pid = test_clickhouse_server_start(TEMP_ROOT_DIR, TEST_PORT);
    if (g_server_pid < 0) {
        printf("skip: could not start clickhouse server\n");
        return 0;
    }
    atexit(stop_server);

    /* Uncompressed pass, then an LZ4 pass over a fresh connection so the
     * compressed recv path (chc__recv_block_compressed_resume) runs live. */
    run_suite(NULL, "");

    chc_codec lz4 = {};
    chc_lz4_codec_init(&lz4);
    chc_client_opts comp = {};
    comp.compression = CHC_COMP_LZ4;
    comp.codec = &lz4;
    run_suite(&comp, "_lz4");

    if (fail_count) {
        fprintf(stderr, "%d check(s) failed\n", fail_count);
        return 1;
    }
    printf("all async uring tests passed\n");
    return 0;
}

#endif /* CHC__URING_AVAILABLE */
