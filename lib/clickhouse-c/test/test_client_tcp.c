/*
 * test_client_tcp.c -- end-to-end TCP client tests against a live
 * clickhouse-server. Spawns the server on a fixed loopback port,
 * exercises handshake / Ping / SELECT / INSERT / exception paths, then
 * tears down.
 *
 * Compile:
 *   cc -std=c11 -O2 -I. test/test_client_tcp.c -o /tmp/test_client_tcp
 *
 * Run requires:
 *   - clickhouse binary on PATH (provides both `server` and `local` modes)
 *   - port 19000 free on 127.0.0.1
 */

#include <arpa/inet.h>
#include <assert.h>
#include <fcntl.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define CHC_PROVIDE_STDLIB_ALLOC
#define CHC_IMPLEMENTATION
#include "clickhouse.h"
#include "clickhouse-posix-io.h"
#include "clickhouse-compression.h"
#include "clickhouse-client.h"

static int fail_count = 0;
static const char *current_test = "";

#include "test_common.h"
#include "test_clickhouse_server.h"

#define TEMP_ROOT_DIR "/tmp/ch-test-srv"
#define TEST_PORT     19000

static pid_t g_server_pid = -1;
static uint16_t g_port = TEST_PORT;

static int
connect_to_server(void)
{
    return test_clickhouse_connect(g_port);
}

static void
stop_server(void)
{
    test_clickhouse_server_stop(&g_server_pid);
}

/* Wraps connect_to_server + chc_posix_io_init + chc_client_init. */
typedef struct {
    int fd;
    chc_posix_io io_state;
    chc_io io;
    chc_alloc al;
    chc_client *c;
    chc_codec codec;
} test_conn;

static int
open_conn_options(test_conn *t, chc_compression comp,
                   const chc_client_opts *overrides, chc_err *err)
{
    memset(t, 0, sizeof *t);
    t->al = chc_alloc_stdlib();
    t->fd = connect_to_server();
    if (t->fd < 0) {
        chc__err_set(err, CHC_ERR_IO, "connect failed");
        return CHC_ERR_IO;
    }
    chc_posix_io_init(&t->io_state, &t->io, t->fd, NULL, NULL);
    chc_client_opts opts = {};
    if (overrides) opts = *overrides;
    if (comp == CHC_COMP_LZ4) {
        chc_lz4_codec_init(&t->codec);
        opts.compression = CHC_COMP_LZ4;
        opts.codec = &t->codec;
    } else if (comp == CHC_COMP_ZSTD) {
        chc_zstd_codec_init(&t->codec);
        opts.compression = CHC_COMP_ZSTD;
        opts.codec = &t->codec;
    }
    int rc = chc_client_init(&t->c, &opts, &t->al, &t->io, NULL, err);
    if (rc != CHC_OK) {
        close(t->fd);
        t->fd = -1;
    }
    return rc;
}

static int
open_conn_compressed(test_conn *t, chc_compression comp, chc_err *err)
{
    return open_conn_options(t, comp, NULL, err);
}

static int
open_conn(test_conn *t, chc_err *err)
{
    return open_conn_compressed(t, CHC_COMP_NONE, err);
}

static void
close_conn(test_conn *t)
{
    if (t->c) chc_client_close(t->c);
    if (t->fd >= 0) close(t->fd);
    memset(t, 0, sizeof *t);
    t->fd = -1;
}

/* ------------------------------------------------------------------ */

typedef void (*test_block_cb)(const chc_block *b, void *ud);

static int
send_query(test_conn *t, const char *sql, chc_err *err)
{
    return chc_client_send_query(t->c, sql, strlen(sql), "", 0, err);
}

static int
recv_until_eos(test_conn *t, int max_packets, test_block_cb cb, void *ud,
               chc_err *err)
{
    for (int i = 0; i < max_packets; i++) {
        chc_packet pkt = {};
        int rc = chc_client_recv_packet(t->c, &pkt, err);
        if (rc != CHC_OK) {
            chc_packet_clear(t->c, &pkt);
            return rc;
        }

        bool done = pkt.kind == CHC_PKT_END_OF_STREAM;
        bool exc = pkt.kind == CHC_PKT_EXCEPTION;
        if (exc && pkt.exception) {
            chc__err_set(err, CHC_ERR_SERVER, "%s", pkt.exception->display_text);
        }
        if (pkt.kind == CHC_PKT_DATA && pkt.block &&
            chc_block_n_rows(pkt.block) > 0 && cb) {
            cb(pkt.block, ud);
        }
        chc_packet_clear(t->c, &pkt);
        if (done) return CHC_OK;
        if (exc) return CHC_ERR_SERVER;
    }
    return chc__err_set(err, CHC_ERR_PROTOCOL, "too many packets");
}

static int
recv_insert_header(test_conn *t, int max_packets, chc_err *err)
{
    for (int i = 0; i < max_packets; i++) {
        chc_packet pkt = {};
        int rc = chc_client_recv_packet(t->c, &pkt, err);
        if (rc != CHC_OK) {
            chc_packet_clear(t->c, &pkt);
            return rc;
        }

        bool got_data = pkt.kind == CHC_PKT_DATA;
        bool exc = pkt.kind == CHC_PKT_EXCEPTION;
        if (exc && pkt.exception) {
            chc__err_set(err, CHC_ERR_SERVER, "%s", pkt.exception->display_text);
        }
        chc_packet_clear(t->c, &pkt);
        if (got_data) return CHC_OK;
        if (exc) return CHC_ERR_SERVER;
    }
    return chc__err_set(err, CHC_ERR_PROTOCOL, "missing insert header");
}

typedef struct {
    bool saw_data;
} select_simple_acc;

static void
select_simple_cb(const chc_block *b, void *ud)
{
    select_simple_acc *a = ud;
    a->saw_data = true;
    CHECK(chc_block_n_columns(b) == 1);
    const chc_column *col = chc_block_column(b, 0);
    CHECK(chc_column_layout(col) == CHC_COL_FIXED);
    size_t elem_size = 0;
    const void *data = chc_column_fixed_data(col, &elem_size);
    CHECK(data != NULL);
    CHECK(elem_size >= 1);
    if (data && elem_size >= 1)
        CHECK(((const uint8_t *) data)[0] == 42);
}

typedef struct {
    size_t total_rows;
    uint64_t sum;
    size_t expect_columns;
    bool check_string_col;
} select_many_acc;

static void
select_many_cb(const chc_block *b, void *ud)
{
    select_many_acc *a = ud;
    if (a->expect_columns) CHECK(chc_block_n_columns(b) == a->expect_columns);
    size_t nrows = chc_block_n_rows(b);
    a->total_rows += nrows;
    const chc_column *c0 = chc_block_column(b, 0);
    size_t es = 0;
    const uint64_t *vals = chc_column_fixed_data(c0, &es);
    CHECK(es == 8);
    CHECK(vals != NULL);
    if (!vals) return;
    for (size_t r = 0; r < nrows; r++) a->sum += vals[r];
    if (a->check_string_col) {
        const chc_column *c1 = chc_block_column(b, 1);
        CHECK(chc_column_layout(c1) == CHC_COL_STRING);
        CHECK(chc_column_string_offsets(c1) != NULL);
        CHECK(chc_column_string_data(c1) != NULL);
    }
}

typedef struct {
    bool saw_value;
} setting_acc;

static void
setting_cb(const chc_block *b, void *ud)
{
    setting_acc *a = ud;
    const chc_column *c0 = chc_block_column(b, 0);
    size_t es = 0;
    const uint8_t *v = chc_column_fixed_data(c0, &es);
    CHECK(v != NULL);
    if (!v) return;
    uint64_t got = 0;
    for (size_t k = 0; k < es; k++)
        got |= ((uint64_t) v[k]) << (8 * k);
    if (got == 12345ULL) a->saw_value = true;
}

static void
string_param_cb(const chc_block *b, void *ud)
{
    setting_acc *a = ud;
    const chc_column *c0 = chc_block_column(b, 0);
    const uint64_t *off = chc_column_string_offsets(c0);
    const uint8_t *by = chc_column_string_data(c0);
    if (off && by && off[0] == 11 && memcmp(by, "hello world", 11) == 0)
        a->saw_value = true;
}

static void
array_param_cb(const chc_block *b, void *ud)
{
    setting_acc *a = ud;
    size_t es = 0;
    const uint64_t *lens = chc_column_fixed_data(chc_block_column(b, 0), &es);
    const chc_column *cs = chc_block_column(b, 1);
    const uint64_t *off = chc_column_string_offsets(cs);
    const uint8_t *by = chc_column_string_data(cs);
    if (lens && lens[0] == 2 && off && by &&
        off[0] == 3 && memcmp(by, "foo", 3) == 0)
        a->saw_value = true;
}

typedef struct {
    uint64_t x_sum;
    size_t n_rows;
    int seen_s;
} insert_readback_acc;

static void
insert_readback_cb(const chc_block *b, void *ud)
{
    insert_readback_acc *a = ud;
    size_t nrows = chc_block_n_rows(b);
    a->n_rows += nrows;
    const chc_column *xc = chc_block_column(b, 0);
    size_t es = 0;
    const uint32_t *xv = chc_column_fixed_data(xc, &es);
    CHECK(es == 4);
    CHECK(xv != NULL);
    if (xv) {
        for (size_t r = 0; r < nrows; r++) a->x_sum += xv[r];
    }

    const chc_column *sc = chc_block_column(b, 1);
    const uint64_t *off = chc_column_string_offsets(sc);
    const uint8_t *by = chc_column_string_data(sc);
    CHECK(off != NULL && by != NULL);
    if (!off || !by) return;
    uint64_t prev = 0;
    for (size_t r = 0; r < nrows; r++) {
        uint64_t end = off[r], len = end - prev;
        if (len == 2 && memcmp(by + prev, "ab",  2) == 0) a->seen_s |= 1;
        if (len == 3 && memcmp(by + prev, "cde", 3) == 0) a->seen_s |= 2;
        if (len == 1 && by[prev] == 'z')                  a->seen_s |= 4;
        prev = end;
    }
}

typedef struct {
    uint64_t count;
    uint64_t sum;
} count_sum_acc;

static void
count_sum_cb(const chc_block *b, void *ud)
{
    count_sum_acc *a = ud;
    const chc_column *c0 = chc_block_column(b, 0);
    const chc_column *c1 = chc_block_column(b, 1);
    size_t es = 0;
    const uint64_t *cv = chc_column_fixed_data(c0, &es);
    CHECK(cv != NULL);
    if (cv) a->count = cv[0];
    cv = chc_column_fixed_data(c1, &es);
    CHECK(cv != NULL);
    if (cv) a->sum = cv[0];
}

typedef struct {
    bool saw;
} qbit_acc;

#define QBIT_DIM 16   /* >8 so planes span ceil(16/8)=2 bytes; multiple of 8 so
                       * an all-equal vector fills each set plane's bytes fully */

/*
 * QBit(Float32, 16) on the wire is a Tuple of 32 FixedString(2) bit-planes,
 * MSB plane first; plane p carries word bit (31 - p). All sixteen dimensions
 * are 1.0f (0x3F800000), whose set bits 23..29 land in planes 2..8; identical
 * rows fill every bit position, so each selected plane's two bytes are both
 * 0xFF and every other plane (sign, exponent MSBs, mantissa) stays 0x00.
 * Unlike test_block_decode.c's crafted bytes, this pins the client's QBit
 * decode against output a real server actually emits.
 */
static void
qbit_cb(const chc_block *b, void *ud)
{
    qbit_acc *a = ud;
    a->saw = true;
    CHECK(chc_block_n_columns(b) == 1);
    if (chc_block_n_columns(b) < 1) return;

    const chc_type *t = chc_block_column_type(b, 0);
    CHECK(chc_type_kind(t) == CHC_QBIT);
    CHECK_EQ_U64(chc_type_qbit_dimension(t), QBIT_DIM);
    CHECK_EQ_U64(chc_type_qbit_element_size(t), 32);

    const chc_column *q = chc_block_column(b, 0);
    CHECK(chc_column_layout(q) == CHC_COL_TUPLE);
    CHECK_EQ_U64(chc_column_tuple_arity(q), 32);
    CHECK_EQ_U64(chc_column_n_rows(q), 1);
    for (size_t p = 0; p < 32; p++) {
        const chc_column *plane = chc_column_tuple_child(q, p);
        CHECK(chc_column_layout(plane) == CHC_COL_FIXED);
        size_t es = 0;
        const uint8_t *d = chc_column_fixed_data(plane, &es);
        CHECK_EQ_U64(es, (QBIT_DIM + 7) / 8);
        if (!d || es < 1) continue;
        uint8_t want = (p >= 2 && p <= 8) ? 0xFF : 0x00;
        for (size_t k = 0; k < es; k++) CHECK_EQ_I64(d[k], want);
    }
}

typedef struct {
    bool saw;
} saf_acc;

static void
check_type_name(const chc_type *t, const char *want)
{
    size_t len = 0;
    const char *nm = chc_type_name(t, &len);
    CHECK(nm != NULL);
    if (nm) CHECK_STR_EQ(nm, len, want);
}

/* Include LowCardinality to check that its metadata is read before values */
static void
saf_cb(const chc_block *b, void *ud)
{
    saf_acc *a = ud;
    a->saw = true;
    CHECK(chc_block_n_columns(b) == 2);
    if (chc_block_n_columns(b) < 2) return;

    const chc_type *t = chc_block_column_type(b, 0);
    CHECK(chc_type_kind(t) == CHC_SIMPLE_AGGREGATE_FUNCTION);
    check_type_name(t, "SimpleAggregateFunction(sum, UInt64)");
    CHECK_EQ_U64(chc_type_n_children(t), 1);
    CHECK(chc_type_kind(chc_type_child(t, 0)) == CHC_UINT64);

    const chc_column *c = chc_block_column(b, 0);
    CHECK(chc_column_layout(c) == CHC_COL_FIXED);
    CHECK_EQ_U64(chc_column_n_rows(c), 1);
    size_t elem_size = 0;
    const uint64_t *v = chc_column_fixed_data(c, &elem_size);
    CHECK_EQ_U64(elem_size, 8);
    if (v) CHECK_EQ_U64(v[0], 6);

    t = chc_block_column_type(b, 1);
    check_type_name(t,
        "SimpleAggregateFunction(anyLast, LowCardinality(String))");
    CHECK(chc_type_kind(chc_type_child(t, 0)) == CHC_LOW_CARDINALITY);

    c = chc_block_column(b, 1);
    CHECK(chc_column_layout(c) == CHC_COL_LOW_CARDINALITY);
    const chc_column *dict = chc_column_lc_dict(c);
    const uint8_t *keys = chc_column_lc_keys(c);
    CHECK(dict != NULL && keys != NULL);
    if (!dict || !keys) return;
    CHECK(chc_column_layout(dict) == CHC_COL_STRING);
    const uint8_t  *dd   = chc_column_string_data(dict);
    const uint64_t *doff = chc_column_string_offsets(dict);
    CHECK(dd != NULL && doff != NULL);
    if (!dd || !doff) return;
    uint8_t k = keys[0];
    uint64_t from = k ? doff[k - 1] : 0;
    CHECK_STR_EQ((const char *) dd + from, (size_t) (doff[k] - from), "x");
}

static void
test_handshake(void)
{
    current_test = "handshake";
    test_conn t; chc_err err = {};
    int rc = open_conn(&t, &err); CHECK_OK(rc, err);
    const chc_server_info *si = chc_client_server_info(t.c);
    CHECK(si != NULL);
    CHECK(strncmp(si->name, "ClickHouse", 10) == 0);
    CHECK(si->revision >= CHC_SERVER_MIN_REVISION);
    CHECK(si->version_major >= 1);
out:
    close_conn(&t);
}

/* chc_client_init must surface a bad default_database here, not race
 * the caller's first query. Older CH versions defer the database check
 * until after the Addendum read, so the probe Ping at init tail is the
 * point where the server-side exception lands. */
static void
test_bad_database(void)
{
    current_test = "bad_database";
    test_conn t; chc_err err = {};
    memset(&t, 0, sizeof t);
    t.fd = -1;
    t.al = chc_alloc_stdlib();
    t.fd = connect_to_server();
    CHECK(t.fd >= 0);
    if (t.fd < 0) goto out;
    chc_posix_io_init(&t.io_state, &t.io, t.fd, NULL, NULL);
    chc_client_opts opts = { .database = "no such database" };
    chc_exception *exc = NULL;
    int rc = chc_client_init(&t.c, &opts, &t.al, &t.io, &exc, &err);
    CHECK(rc == CHC_ERR_SERVER);
    CHECK(err.msg[0] == '\0');
    CHECK(exc != NULL);
    if (exc) {
        CHECK(exc->code != 0);
        CHECK(exc->display_text != NULL);
        CHECK(strstr(exc->display_text, "Database") != NULL);
        CHECK(strstr(exc->display_text, "no such database") != NULL);
        chc_exception_free(exc, &t.al);
    }
out:
    close_conn(&t);
}

static void
test_ping(void)
{
    current_test = "ping";
    test_conn t; chc_err err = {};
    int rc = open_conn(&t, &err); CHECK_OK(rc, err);
    rc = chc_client_send_ping(t.c, &err); CHECK_OK(rc, err);
    chc_packet pkt = {};
    rc = chc_client_recv_packet(t.c, &pkt, &err); CHECK_OK(rc, err);
    CHECK(pkt.kind == CHC_PKT_PONG);
    chc_packet_clear(t.c, &pkt);
out:
    close_conn(&t);
}

static void
test_select_simple(void)
{
    current_test = "select_simple";
    test_conn t; chc_err err = {};
    int rc = open_conn(&t, &err); CHECK_OK(rc, err);

    const char *sql = "SELECT 42 AS x";
    rc = send_query(&t, sql, &err);
    CHECK_OK(rc, err);

    select_simple_acc acc = {};
    rc = recv_until_eos(&t, 32, select_simple_cb, &acc, &err);
    CHECK_OK(rc, err);
    CHECK(acc.saw_data);
out:
    close_conn(&t);
}

static void
test_select_many(void)
{
    current_test = "select_many";
    test_conn t; chc_err err = {};
    int rc = open_conn(&t, &err); CHECK_OK(rc, err);

    const char *sql = "SELECT number, toString(number) FROM system.numbers LIMIT 100";
    rc = send_query(&t, sql, &err);
    CHECK_OK(rc, err);

    select_many_acc acc = {
        .expect_columns = 2,
        .check_string_col = true,
    };
    rc = recv_until_eos(&t, 64, select_many_cb, &acc, &err);
    CHECK_OK(rc, err);
    CHECK(acc.total_rows == 100);
    CHECK(acc.sum == 4950);
out:
    close_conn(&t);
}

static int
run_simple_query(test_conn *t, const char *sql, chc_err *err)
{
    int rc = send_query(t, sql, err);
    if (rc != CHC_OK) return rc;
    return recv_until_eos(t, 64, NULL, NULL, err);
}

static void
test_exception(void)
{
    current_test = "exception";
    test_conn t; chc_err err = {};
    int rc = open_conn(&t, &err); CHECK_OK(rc, err);

    const char *sql = "SELECT * FROM nonexistent_table_xyz";
    rc = send_query(&t, sql, &err);
    CHECK_OK(rc, err);

    bool saw_exc = false;
    for (int i = 0; i < 32; i++) {
        chc_packet pkt = {};
        rc = chc_client_recv_packet(t.c, &pkt, &err); CHECK_OK(rc, err);
        bool eos = pkt.kind == CHC_PKT_END_OF_STREAM;
        if (pkt.kind == CHC_PKT_EXCEPTION) {
            saw_exc = true;
            CHECK(pkt.exception != NULL);
            /* Error code 60 = UNKNOWN_TABLE */
            CHECK(pkt.exception->code == 60 || pkt.exception->code == 81
                  || pkt.exception->code == 47);
            CHECK(pkt.exception->name && strstr(pkt.exception->name, "DB::") != NULL);
            chc_packet_clear(t.c, &pkt);
            break;
        }
        chc_packet_clear(t.c, &pkt);
        if (eos) break;
    }
    CHECK(saw_exc);
out:
    close_conn(&t);
}

static void
test_insert_roundtrip(void)
{
    current_test = "insert_roundtrip";
    test_conn t; chc_err err = {};
    chc_block_col bbcols[2];
    chc_block_builder bb;
    chc_type *u32 = NULL, *str = NULL;
    int rc = open_conn(&t, &err); CHECK_OK(rc, err);

    /* Recreate Memory table, valid only until server restart */
    rc = run_simple_query(&t, "DROP TABLE IF EXISTS test_t SYNC", &err);
    CHECK_OK(rc, err);
    rc = run_simple_query(&t,
        "CREATE TABLE test_t (x UInt32, s String) ENGINE = Memory", &err);
    CHECK_OK(rc, err);

    rc = send_query(&t, "INSERT INTO test_t (x, s) VALUES", &err);
    CHECK_OK(rc, err);

    rc = recv_insert_header(&t, 8, &err);
    CHECK_OK(rc, err);

    /* Build 3-row block */
    chc_block_builder_init(&bb, bbcols);

    uint32_t xs[3] = { 100, 200, 300 };
    rc = chc_type_parse("UInt32", 6, &t.al, &u32, &err); CHECK_OK(rc, err);
    chc_column cx = chc_build_fixed(xs, sizeof xs[0], 3);
    chc_block_builder_append(&bb, "x", 1, u32, &cx);

    uint64_t offs[3] = { 2, 5, 6 };
    const uint8_t bytes[] = "abcdez";   /* "ab", "cde", "z" */
    rc = chc_type_parse("String", 6, &t.al, &str, &err); CHECK_OK(rc, err);
    chc_column cs = chc_build_string(offs, bytes, 3);
    chc_block_builder_append(&bb, "s", 1, str, &cs);

    rc = chc_client_send_data(t.c, &bb, &err); CHECK_OK(rc, err);
    rc = chc_client_send_data(t.c, NULL, &err); CHECK_OK(rc, err);

    rc = recv_until_eos(&t, 32, NULL, NULL, &err);
    CHECK_OK(rc, err);

    chc_type_destroy(u32, &t.al);
    u32 = NULL;
    chc_type_destroy(str, &t.al);
    str = NULL;

    /* Read it back. */
    rc = send_query(&t, "SELECT x, s FROM test_t ORDER BY x", &err);
    CHECK_OK(rc, err);

    insert_readback_acc acc = {};
    rc = recv_until_eos(&t, 64, insert_readback_cb, &acc, &err);
    CHECK_OK(rc, err);
    CHECK(acc.n_rows == 3);
    CHECK(acc.x_sum == 600);
    CHECK(acc.seen_s == 7);

    rc = run_simple_query(&t, "DROP TABLE IF EXISTS test_t SYNC", &err);
    CHECK_OK(rc, err);

out:
    chc_type_destroy(u32, &t.al);
    chc_type_destroy(str, &t.al);
    close_conn(&t);
}

static void
test_select_with_setting(void)
{
    current_test = "select_with_setting";
    test_conn t; chc_err err = {};
    int rc = open_conn(&t, &err); CHECK_OK(rc, err);

    chc_query_setting settings[1] = {
        { .name = "max_block_size", .value = "12345", .important = true },
    };
    chc_query_opts opts = { .settings = settings, .n_settings = 1 };

    const char *sql = "SELECT getSetting('max_block_size')";
    rc = chc_client_send_query_ex(t.c, sql, strlen(sql), &opts, &err);
    CHECK_OK(rc, err);

    setting_acc acc = {};
    rc = recv_until_eos(&t, 32, setting_cb, &acc, &err);
    CHECK_OK(rc, err);
    CHECK(acc.saw_value);
out:
    close_conn(&t);
}

static void
test_select_with_param(void)
{
    current_test = "select_with_param";
    test_conn t; chc_err err = {};
    int rc = open_conn(&t, &err); CHECK_OK(rc, err);

    /* Param values pass through Field::restoreFromDump on the server, so
     * a String must arrive single-quoted. */
    chc_query_param params[1] = {
        { .name = "p1", .value = "'hello world'" },
    };
    chc_query_opts opts = { .params = params, .n_params = 1 };

    const char *sql = "SELECT {p1:String} AS s";
    rc = chc_client_send_query_ex(t.c, sql, strlen(sql), &opts, &err);
    CHECK_OK(rc, err);

    setting_acc acc = {};
    rc = recv_until_eos(&t, 32, string_param_cb, &acc, &err);
    CHECK_OK(rc, err);
    CHECK(acc.saw_value);
out:
    close_conn(&t);
}

/*
 * Array(String) parameter with inner quotes. The value must be a fully
 * formed Field literal: wrapped in single quotes, inner `'` escaped as
 * `\x27`; chc passes the value verbatim per the doc on
 * chc_query_param. A caller that does NOT escape inner quotes ends up
 * with the server stopping at the first `'` inside the array literal,
 * surfacing as "value [ cannot be parsed as Array(String) for query
 * parameter 'p1'". This test pins both halves: the verbatim
 * pass-through, AND the fact that the server actually parses the
 * escaped form back to a real Array(String).
 */
static void
test_select_with_array_param(void)
{
    current_test = "select_with_array_param";
    test_conn t; chc_err err = {};
    int rc = open_conn(&t, &err); CHECK_OK(rc, err);

    /* Pre-escaped Array(String) literal: outer quote, inner quotes as
     * \x27, comma between elements -- the form the server's Field parser
     * reads back as a real Array(String). */
    chc_query_param params[1] = {
        { .name = "p1",
          .value = "'[\\x27foo\\x27,\\x27bar\\x27]'" },
    };
    chc_query_opts opts = { .params = params, .n_params = 1 };

    const char *sql =
        "SELECT length({p1:Array(String)}) AS n, {p1:Array(String)}[1] AS s";
    rc = chc_client_send_query_ex(t.c, sql, strlen(sql), &opts, &err);
    CHECK_OK(rc, err);

    setting_acc acc = {};
    rc = recv_until_eos(&t, 32, array_param_cb, &acc, &err);
    CHECK_OK(rc, err);
    CHECK(acc.saw_value);
out:
    close_conn(&t);
}

static void
test_select_many_lz4(void)
{
    current_test = "select_many_lz4";
    test_conn t; chc_err err = {};
    int rc = open_conn_compressed(&t, CHC_COMP_LZ4, &err); CHECK_OK(rc, err);

    /* ClickHouse 26.9 defaults network compression to ZSTD */
    rc = run_simple_query(&t, "SET network_compression_method='lz4'", &err);
    CHECK_OK(rc, err);

    /* 10 000 rows pushes the body past one LZ4 frame. */
    const char *sql =
        "SELECT number, toString(number) FROM system.numbers LIMIT 10000";
    rc = send_query(&t, sql, &err);
    CHECK_OK(rc, err);

    select_many_acc acc = {};
    rc = recv_until_eos(&t, 256, select_many_cb, &acc, &err);
    CHECK_OK(rc, err);
    CHECK(acc.total_rows == 10000);
    CHECK(acc.sum == 49995000ULL);
out:
    close_conn(&t);
}

static void
run_insert_compressed_roundtrip(chc_compression comp, const char *table)
{
    test_conn t;
    chc_err err = {};
    uint32_t *xs = NULL;
    chc_block_col bbcols[2];
    chc_block_builder bb;
    chc_type *u32 = NULL;
    const size_t N = 500;
    char sql[160];

    int rc = open_conn_compressed(&t, comp, &err); CHECK_OK(rc, err);

    rc = run_simple_query(&t, comp == CHC_COMP_ZSTD
                                  ? "SET network_compression_method='zstd'"
                                  : "SET network_compression_method='lz4'", &err);
    CHECK_OK(rc, err);

    int sn = snprintf(sql, sizeof sql, "DROP TABLE IF EXISTS %s SYNC", table);
    CHECK(sn > 0 && (size_t) sn < sizeof sql);
    if (sn <= 0 || (size_t) sn >= sizeof sql) goto out;
    rc = run_simple_query(&t, sql, &err);
    CHECK_OK(rc, err);

    sn = snprintf(sql, sizeof sql,
                  "CREATE TABLE %s (x UInt32) ENGINE = Memory", table);
    CHECK(sn > 0 && (size_t) sn < sizeof sql);
    if (sn <= 0 || (size_t) sn >= sizeof sql) goto out;
    rc = run_simple_query(&t, sql, &err);
    CHECK_OK(rc, err);

    sn = snprintf(sql, sizeof sql, "INSERT INTO %s (x) VALUES", table);
    CHECK(sn > 0 && (size_t) sn < sizeof sql);
    if (sn <= 0 || (size_t) sn >= sizeof sql) goto out;
    rc = send_query(&t, sql, &err);
    CHECK_OK(rc, err);

    rc = recv_insert_header(&t, 8, &err);
    CHECK_OK(rc, err);

    xs = malloc(N * sizeof *xs);
    CHECK(xs != NULL);
    if (!xs) goto out;
    for (size_t i = 0; i < N; i++) xs[i] = (uint32_t) i;

    chc_block_builder_init(&bb, bbcols);
    rc = chc_type_parse("UInt32", 6, &t.al, &u32, &err); CHECK_OK(rc, err);
    chc_column cx = chc_build_fixed(xs, sizeof xs[0], N);
    chc_block_builder_append(&bb, "x", 1, u32, &cx);

    rc = chc_client_send_data(t.c, &bb, &err); CHECK_OK(rc, err);
    rc = chc_client_send_data(t.c, NULL, &err); CHECK_OK(rc, err);

    rc = recv_until_eos(&t, 32, NULL, NULL, &err);
    CHECK_OK(rc, err);

    chc_type_destroy(u32, &t.al);
    u32 = NULL;
    free(xs);
    xs = NULL;

    sn = snprintf(sql, sizeof sql, "SELECT count(), sum(x) FROM %s", table);
    CHECK(sn > 0 && (size_t) sn < sizeof sql);
    if (sn <= 0 || (size_t) sn >= sizeof sql) goto out;
    rc = send_query(&t, sql, &err);
    CHECK_OK(rc, err);

    count_sum_acc acc = {};
    rc = recv_until_eos(&t, 32, count_sum_cb, &acc, &err);
    CHECK_OK(rc, err);
    CHECK(acc.count == N);
    CHECK(acc.sum == 124750ULL);

    sn = snprintf(sql, sizeof sql, "DROP TABLE IF EXISTS %s SYNC", table);
    CHECK(sn > 0 && (size_t) sn < sizeof sql);
    if (sn <= 0 || (size_t) sn >= sizeof sql) goto out;
    rc = run_simple_query(&t, sql, &err);
    CHECK_OK(rc, err);

out:
    chc_type_destroy(u32, &t.al);
    free(xs);
    close_conn(&t);
}

static void
test_insert_lz4_roundtrip(void)
{
    current_test = "insert_lz4_roundtrip";
    run_insert_compressed_roundtrip(CHC_COMP_LZ4, "test_lz4");
}

static void
test_select_many_zstd(void)
{
    current_test = "select_many_zstd";
    test_conn t; chc_err err = {};
    int rc = open_conn_compressed(&t, CHC_COMP_ZSTD, &err); CHECK_OK(rc, err);

    /* Server defaults to LZ4 before 26.9; pin ZSTD so codec's
     * zstd_decompress path is the one exercised. */
    rc = run_simple_query(&t,
        "SET network_compression_method='zstd'", &err);
    CHECK_OK(rc, err);

    /* 10 000 rows: mirrors test_select_many_lz4 so the zstd path is
     * exercised against the same shape of payload. */
    const char *sql =
        "SELECT number, toString(number) FROM system.numbers LIMIT 10000";
    rc = send_query(&t, sql, &err);
    CHECK_OK(rc, err);

    select_many_acc acc = {};
    rc = recv_until_eos(&t, 256, select_many_cb, &acc, &err);
    CHECK_OK(rc, err);
    CHECK(acc.total_rows == 10000);
    CHECK(acc.sum == 49995000ULL);
out:
    close_conn(&t);
}

static void
test_insert_zstd_roundtrip(void)
{
    current_test = "insert_zstd_roundtrip";
    run_insert_compressed_roundtrip(CHC_COMP_ZSTD, "test_zstd");
}

/*
 * QBit is experimental from ClickHouse 25.10 and Beta (on by default) from
 * 26.1; older servers lack the type, so gate on the handshake version and
 * skip below 25.10. allow_experimental_qbit_type enables it on 25.10..26.0
 * and is a harmless obsolete alias afterwards; sent non-important so an
 * eventual removal of the setting can't fail the query.
 */
static void
test_qbit_live(void)
{
    current_test = "qbit_live";
    test_conn t; chc_err err = {};
    int rc = open_conn(&t, &err); CHECK_OK(rc, err);

    const chc_server_info *si = chc_client_server_info(t.c);
    CHECK(si != NULL);
    if (!si || si->version_major < 25 ||
        (si->version_major == 25 && si->version_minor < 10)) {
        fprintf(stderr, "qbit_live: server %llu.%llu < 25.10, skipping QBit\n",
                (unsigned long long) (si ? si->version_major : 0),
                (unsigned long long) (si ? si->version_minor : 0));
        goto out;
    }

    chc_query_setting settings[1] = {
        { .name = "allow_experimental_qbit_type", .value = "1",
          .important = false },
    };
    chc_query_opts opts = { .settings = settings, .n_settings = 1 };

    const char *sql =
        "SELECT CAST(arrayWithConstant(16, toFloat32(1)) AS QBit(Float32, 16)) AS q";
    rc = chc_client_send_query_ex(t.c, sql, strlen(sql), &opts, &err);
    CHECK_OK(rc, err);

    qbit_acc acc = {};
    rc = recv_until_eos(&t, 32, qbit_cb, &acc, &err);
    CHECK_OK(rc, err);
    CHECK(acc.saw);
out:
    close_conn(&t);
}

static void
test_simple_aggregate_live(void)
{
    current_test = "simple_aggregate_live";
    test_conn t; chc_err err = {};
    int rc = open_conn(&t, &err); CHECK_OK(rc, err);

    const char *sql =
        "SELECT CAST(6, 'SimpleAggregateFunction(sum, UInt64)') AS a,"
        " CAST('x', 'SimpleAggregateFunction(anyLast, LowCardinality(String))') AS b";
    rc = send_query(&t, sql, &err);
    CHECK_OK(rc, err);

    saf_acc acc = {};
    rc = recv_until_eos(&t, 32, saf_cb, &acc, &err);
    CHECK_OK(rc, err);
    CHECK(acc.saw);
out:
    close_conn(&t);
}

/* Records server bytes so tests can see serialization kinds on the wire */
typedef struct {
    chc_io        inner;
    test_mem_sink bytes;
} rec_io;

static int
rec_read(void *ud, void *buf, size_t len, size_t *out_n, chc_err *err)
{
    rec_io *r = ud;
    int rc = r->inner.read(r->inner.ud, buf, len, out_n, err);
    if (rc == CHC_OK) rc = test_mem_sink_write(&r->bytes, buf, *out_n, err);
    return rc;
}

static int
rec_write(void *ud, const void *buf, size_t len, chc_err *err)
{
    rec_io *r = ud;
    return r->inner.write(r->inner.ud, buf, len, err);
}

/* Column header naming name & type followed by has_custom=1, kinds after */
static const uint8_t *
find_custom_col(const test_mem_sink *m, const char *name, const char *type)
{
    uint8_t pat[128];
    size_t nl = strlen(name), tl = strlen(type), n = 0;
    pat[n++] = (uint8_t) nl; memcpy(pat + n, name, nl); n += nl;
    pat[n++] = (uint8_t) tl; memcpy(pat + n, type, tl); n += tl;
    pat[n++] = 1;
    for (size_t i = 0; i + n <= m->len; i++)
        if (memcmp(m->data + i, pat, n) == 0) return m->data + i + n;
    return NULL;
}

typedef struct { size_t rows, bad; } sparse_acc;

/* Rows with id % 97 == 0 hold values, others defaults */
static void
sparse_cb(const chc_block *b, void *ud)
{
    sparse_acc *a = ud;
    CHECK(chc_block_n_columns(b) == 4);
    if (chc_block_n_columns(b) != 4) return;
    const uint32_t *id = chc_column_fixed_data(chc_block_column(b, 0), NULL);
    const uint32_t *u = chc_column_fixed_data(chc_block_column(b, 1), NULL);
    const chc_column *s = chc_block_column(b, 2);
    const chc_column *t = chc_block_column(b, 3);
    const uint64_t *ta = chc_column_fixed_data(chc_column_tuple_child(t, 0), NULL);
    const chc_column *tb = chc_column_tuple_child(t, 1);
    const uint64_t *so = chc_column_string_offsets(s);
    const uint8_t *sd = chc_column_string_data(s);
    const uint64_t *tbo = chc_column_string_offsets(tb);
    CHECK(id && u && so && ta && tbo);
    if (!id || !u || !so || !ta || !tbo) return;
    for (size_t r = 0; r < chc_block_n_rows(b); r++, a->rows++) {
        bool nd = id[r] % 97 == 0;
        char want[16];
        int wl = nd ? snprintf(want, sizeof want, "v%u", id[r]) : 0;
        uint64_t from = r ? so[r - 1] : 0;
        if (u[r] != (nd ? id[r] : 0) || ta[r] != (nd ? id[r] : 0)
            || so[r] - from != (uint64_t) wl
            || (wl && memcmp(sd + from, want, (size_t) wl) != 0)
            || tbo[r] != 0)
            a->bad++;
    }
}

/* Read default-heavy sparse_t over io, report whether sparse kinds arrived
 * for scalar & Tuple child. Return negotiated revision */
static uint64_t
sparse_select(chc_io io, bool *scalar, bool *tuple_child)
{
    test_conn t = { .fd = -1 }; chc_err err = {};
    rec_io rec = { .inner = io };
    uint64_t revision = 0;
    t.al = chc_alloc_stdlib();
    t.io = (chc_io) { .ud = &rec, .read = rec_read, .write = rec_write };
    chc_client_opts opts = {};
    int rc = chc_client_init(&t.c, &opts, &t.al, &t.io, NULL, &err);
    CHECK_OK(rc, err);
    revision = chc_client_server_info(t.c)->revision;

    rc = send_query(&t, "SELECT id, u, s, t FROM sparse_t", &err);
    CHECK_OK(rc, err);
    sparse_acc acc = {};
    rc = recv_until_eos(&t, 256, sparse_cb, &acc, &err);
    CHECK_OK(rc, err);
    CHECK_EQ_U64(acc.rows, 1000);
    CHECK_EQ_U64(acc.bad, 0);

    const uint8_t *k = find_custom_col(&rec.bytes, "u", "UInt32");
    *scalar = k && k[0] == 1 && find_custom_col(&rec.bytes, "s", "String");
    k = find_custom_col(&rec.bytes, "t", "Tuple(a UInt64, b String)");
    *tuple_child = k && (k[1] == 1 || k[2] == 1);
out:
    close_conn(&t);
    test_mem_sink_free(&rec.bytes);
    return revision;
}

static int
discard_write(void *ud, const void *buf, size_t n, chc_err *err)
{
    return CHC_OK;
}

static int
file_read(void *ud, void *buf, size_t len, size_t *out_n, chc_err *err)
{
    *out_n = fread(buf, 1, len, ud);
    return ferror(ud) ? chc__err_set(err, CHC_ERR_IO, "fread failed") : CHC_OK;
}

/* Regenerate: run clickhouse/clickhouse-server:23.3.22.3, proxy it through
 * tools/capture.py --match 'FROM sparse_t', run this test with CHC_TEST_PORT
 * 23.8+ strips sparse below revision 54465, older servers send it from 54454 */
static void
test_sparse_ch23_3_replay(void)
{
    current_test = "sparse_ch23_3_replay";
    FILE *f = fopen("test/ch23_3_sparse.bin", "rb");
    CHECK(f);
    if (!f) return;
    chc_io io = { .ud = f, .read = file_read, .write = discard_write };
    bool scalar = false, tuple_child = false;
    CHECK_EQ_U64(sparse_select(io, &scalar, &tuple_child), CHC_SERVER_MIN_REVISION);
    fclose(f);
    CHECK(scalar);
    CHECK(tuple_child);
}

static void
test_sparse_live(void)
{
    current_test = "sparse_live";
    test_conn t; chc_err err = {};
    int rc = open_conn(&t, &err); CHECK_OK(rc, err);
    rc = run_simple_query(&t, "DROP TABLE IF EXISTS sparse_t SYNC", &err);
    CHECK_OK(rc, err);
    rc = run_simple_query(&t,
        "CREATE TABLE sparse_t (id UInt32, u UInt32, s String,"
        " t Tuple(a UInt64, b String)) ENGINE = MergeTree ORDER BY id"
        " SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5,"
        " min_bytes_for_wide_part = 0", &err);
    CHECK_OK(rc, err);
    rc = run_simple_query(&t,
        "INSERT INTO sparse_t SELECT number, if(number % 97 = 0, number, 0),"
        " if(number % 97 = 0, concat('v', toString(number)), ''),"
        " tuple(if(number % 97 = 0, number, 0), '') FROM numbers(1000)", &err);
    CHECK_OK(rc, err);

    chc_posix_io sel_state;
    chc_io sel_io;
    int fd = connect_to_server();
    CHECK(fd >= 0);
    chc_posix_io_init(&sel_state, &sel_io, fd, NULL, NULL);
    bool scalar = false, tuple_child = false;
    sparse_select(sel_io, &scalar, &tuple_child);
    close(fd);
    CHECK(scalar);
    CHECK(tuple_child);

    rc = run_simple_query(&t, "DROP TABLE IF EXISTS sparse_t SYNC", &err);
    CHECK_OK(rc, err);
out:
    close_conn(&t);
}

static void
test_cityhash_known_vector(void)
{
    current_test = "cityhash_known_vector";
    /* Spot-check CityHash128 against known vectors for an empty string
     * and a short fixed input, using the frozen CityHash128 variant the
     * compression frame mandates. */
    uint64_t lo, hi;
    chc_cityhash128("", 0, &lo, &hi);
    CHECK(lo == 0x3df09dfc64c09a2bULL);
    CHECK(hi == 0x3cb540c392e51e29ULL);

    chc_cityhash128("Hello, World!", 13, &lo, &hi);
    CHECK(lo == 0x703dabf8d081ec00ULL);
    CHECK(hi == 0xa196e28f28c3ee09ULL);

    const char *fox = "The quick brown fox jumps over the lazy dog";
    chc_cityhash128(fox, strlen(fox), &lo, &hi);
    CHECK(lo == 0x69102202d326a2fdULL);
    CHECK(hi == 0xe4b1346bbee531a1ULL);

    /* 200 bytes of 0xab -- exercises the len>=128 unrolled path. */
    char big[200]; memset(big, 0xab, sizeof big);
    chc_cityhash128(big, sizeof big, &lo, &hi);
    CHECK(lo == 0xb29e1d196fe650dfULL);
    CHECK(hi == 0x3dae10d6a77e0432ULL);
}

static void
test_external_data_compat(chc_compression compression, size_t chunk_size)
{
    current_test = "external_data_compat";
    test_conn t;
    chc_err err = {};
    chc_type *type = NULL;
    chc_client_opts opts = {
        .protocol_revision = 54459,
        .allow_legacy_server = true,
        .skip_init_ping = true,
        .has_max_compression_chunk_size = true,
        .max_compression_chunk_size = chunk_size,
    };
    int rc = open_conn_options(&t, compression, &opts, &err);
    CHECK_OK(rc, err);
    CHECK(chc_client_server_info(t.c)->revision == 54459);
    CHECK(chc_client_server_info(t.c)->server_revision >= 54459);
    if (compression != CHC_COMP_NONE) {
        rc = run_simple_query(&t, compression == CHC_COMP_ZSTD
            ? "SET network_compression_method='zstd'"
            : "SET network_compression_method='lz4'", &err);
        CHECK_OK(rc, err);
    }

    char type_name[2048] = "Enum16(";
    size_t type_len = strlen(type_name);
    for (unsigned i = 1; i <= 40; ++i) {
        int n = snprintf(type_name + type_len, sizeof type_name - type_len,
                         "%s'value_%u'=%u", i == 1 ? "" : ",", i, i);
        CHECK(n > 0 && (size_t) n < sizeof type_name - type_len);
        if (n <= 0 || (size_t) n >= sizeof type_name - type_len) goto out;
        type_len += (size_t) n;
    }
    type_name[type_len++] = ')';
    CHECK(type_len > 255);
    rc = chc_type_parse(type_name, type_len, &t.al, &type, &err);
    CHECK_OK(rc, err);

    const char *sql = "SELECT count(), sum(toInt64(x)) FROM external_values";
    chc_query_opts query_opts = { .defer_data_terminator = true };
    rc = chc_client_send_query_ex(t.c, sql, strlen(sql), &query_opts, &err);
    CHECK_OK(rc, err);
    uint8_t values[] = {1, 0, 2, 0, 3, 0};
    chc_column column = chc_build_fixed(values, 2, 3);
    chc_block_col columns[1];
    chc_block_builder block;
    chc_block_builder_init(&block, columns);
    chc_block_builder_append(&block, "x", 1, type, &column);
    rc = chc_client_send_data_named(t.c, &block, "external_values", 15, &err);
    CHECK_OK(rc, err);
    rc = chc_client_send_data(t.c, NULL, &err);
    CHECK_OK(rc, err);
    count_sum_acc acc = {};
    rc = recv_until_eos(&t, 32, count_sum_cb, &acc, &err);
    CHECK_OK(rc, err);
    CHECK(acc.count == 3);
    CHECK(acc.sum == 6);
out:
    chc_type_destroy(type, &t.al);
    close_conn(&t);
}

int main(void)
{
    signal(SIGPIPE, SIG_IGN);

    test_cityhash_known_vector();
    test_sparse_ch23_3_replay();

    /* CHC_TEST_PORT targets running server, eg docker image of older release */
    const char *port = getenv("CHC_TEST_PORT");
    if (port) {
        g_port = (uint16_t) atoi(port);
    } else {
        g_server_pid = test_clickhouse_server_start(TEMP_ROOT_DIR, TEST_PORT);
        if (g_server_pid < 0) {
            fprintf(stderr, "failed to start clickhouse-server; skipping tests\n");
            return fail_count ? 1 : 77;   /* automake-style "skipped" */
        }
        atexit(stop_server);
    }

    test_handshake();
    test_bad_database();
    test_ping();
    test_select_simple();
    test_select_many();
    test_exception();
    test_insert_roundtrip();
    test_select_with_setting();
    test_select_with_param();
    test_select_with_array_param();
    test_select_many_lz4();
    test_insert_lz4_roundtrip();
    test_select_many_zstd();
    test_insert_zstd_roundtrip();
    test_qbit_live();
    test_simple_aggregate_live();
    test_sparse_live();
    test_external_data_compat(CHC_COMP_NONE, 0);
    test_external_data_compat(CHC_COMP_LZ4, 64);
    test_external_data_compat(CHC_COMP_LZ4, 0);
    test_external_data_compat(CHC_COMP_ZSTD, 64);
    test_external_data_compat(CHC_COMP_ZSTD, 0);

    stop_server();

    if (fail_count) {
        fprintf(stderr, "FAIL: %d check(s)\n", fail_count);
        return 1;
    }
    fprintf(stderr, "ok\n");
    return 0;
}
