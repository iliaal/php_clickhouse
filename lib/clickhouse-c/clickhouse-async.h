/*
 * clickhouse-async.h
 *
 * Never touches a socket; caller manages bytes:
 *   - inbound socket bytes  -> chc_async_submit
 *   - outbound bytes        -> chc_async_pending_out / chc_async_consume_out
 *
 * handshake / recv_packet return CHC_WOULD_BLOCK when the input buffer
 * drains mid-parse; the caller submit more and retries. Sends only ever
 * append to *out buffer.
 *
 * No write backpressure: sends never block, never return CHC_WOULD_BLOCK, and
 * grow *out unbounded (only OOM fails). Outbound flow control is the caller's.
 * Use chc_async_pending_out: stop sending when enough pending, resume when
 * chc_async_consume_out has drained it enough.
 *
 * recv keeps parser state alive across CHC_WOULD_BLOCK (in cli.recv_*): a Data
 * block streamed over many reads resumes at the in-progress column rather than
 * re-parsing from scratch (uncompressed path, via chc__block_resume_in).
 * Compressed Data resumes at frame granularity: at most one frame is
 * re-decompressed and one column re-parsed on retry.
 */

#ifndef CLICKHOUSE_ASYNC_H
#define CLICKHOUSE_ASYNC_H

#ifdef CHC_NO_ASYNC
#  error "clickhouse-async.h needs the ioless reader; remove CHC_NO_ASYNC"
#endif

#include "clickhouse-client.h"      /* pulls clickhouse.h + clickhouse-compression.h */

#ifdef __cplusplus
extern "C" {
#endif

typedef struct chc_async_client chc_async_client;

/* No I/O, cannot block; only OOM fails. opts->codec carries compression. */
int  chc_async_client_init(chc_async_client **out, const chc_client_opts *opts,
                           const chc_alloc *al, chc_err *err);
void chc_async_client_free(chc_async_client *c);

/* byte shuttle */
int  chc_async_submit(chc_async_client *c, const void *buf, size_t len,
                    chc_err *err);
void chc_async_pending_out(chc_async_client *c, const uint8_t **buf, size_t *len);
void chc_async_consume_out(chc_async_client *c, size_t n);

/* Drive calls never block. CHC_WOULD_BLOCK requests more input or output
 * draining
 *
 * Handshake rejection returns CHC_ERR_SERVER and leaves err->msg empty. When
 * exc is not NULL, caller owns *exc and frees it with chc_exception_free.
 * Other results leave *exc unchanged. Query exceptions still arrive as
 * packets, but handshake rejection ends connection setup */
int  chc_async_handshake(chc_async_client *c, chc_exception **exc,
                         chc_err *err);
int  chc_async_send_query(chc_async_client *c, const char *sql, size_t sql_len,
                          const char *query_id, size_t query_id_len, chc_err *err);
int  chc_async_send_data(chc_async_client *c, const chc_block_builder *bb,
                         chc_err *err);
int  chc_async_send_data_end(chc_async_client *c, chc_err *err);
int  chc_async_recv_packet(chc_async_client *c, chc_packet *out, chc_err *err);
const chc_server_info *chc_async_server_info(const chc_async_client *c);

/* Mirror chc_packet_clear: free p->block / p->exception via the client's
 * allocator. */
void chc_async_packet_clear(chc_async_client *c, chc_packet *p);

#ifdef __cplusplus
}
#endif

#ifdef CHC_IMPLEMENTATION

/* This block compiles in the same TU as clickhouse-client.h's implementation,
 * so struct chc_client and the static chc__client_* / chc__recv_packet_resumable /
 * chc__recv_pong / chc__client_recv_state_free / chc__read_* / chc__mem_sink_*
 * internals are visible. */

enum {
    CHC__HS_SEND_HELLO = 0,  /* buffer Hello, advance */
    CHC__HS_RECV_HELLO,      /* parse server Hello (resumable) */
    CHC__HS_POST_HELLO,      /* buffer addendum quota_key + Ping */
    CHC__HS_RECV_PONG,       /* parse Pong (resumable) */
    CHC__HS_DONE
};

struct chc_async_client {
    chc_client    cli;       /* driven directly; not via chc_client_init */

    chc__mem_sink out;       /* outbound write sink; cli.io points at out_io */
    chc_io        out_io;
    size_t        out_consumed;  /* bytes of out.buf the caller has drained */

    int           hs_phase;  /* CHC__HS_* */

    /* recv resumption state lives on cli (chc_client.recv_*), shared with the
     * blocking decoder; persisted across CHC_WOULD_BLOCK. */

    /* Owned copies of the Hello string fields (opts may not outlive init). */
    char *client_name;
    char *database;
    char *user;
    char *password;
    size_t client_name_len, database_len, user_len, password_len;
};

static char *
chc__async_dup(const chc_alloc *al, const char *s, size_t len, chc_err *err)
{
    return s ? chc__strdup(al, s, len, err) : NULL;
}

static void
chc__async_free_str(const chc_alloc *al, char *s, size_t len)
{
    if (s) al->free(al->ud, s, len + 1);
}

int
chc_async_client_init(chc_async_client **out, const chc_client_opts *opts,
                      const chc_alloc *al, chc_err *err)
{
    chc_client_opts def_opts = {0};
    if (!opts) opts = &def_opts;
    if (opts->protocol_revision > CHC_CLIENT_REVISION)
        return chc__err_set(err, CHC_ERR_USAGE, "unsupported client protocol revision");

    chc_async_client *c = chc__calloc(al, sizeof *c, err);
    if (!c) return CHC_ERR_OOM;

    c->cli.al = al;
    chc__client_setup(&c->cli, opts);

    chc__mem_sink_init(&c->out, &c->out_io, al);
    c->cli.io = &c->out_io;

    int rc = chc_in_init_ioless(&c->cli.in, al);
    if (rc != CHC_OK) { al->free(al->ud, c, sizeof *c); return rc; }

    /* Own strings used after init. */
    c->client_name_len = opts->strings_have_lengths ? opts->client_name_len : (opts->client_name ? strlen(opts->client_name) : 0);
    c->database_len = opts->strings_have_lengths ? opts->database_len : (opts->database ? strlen(opts->database) : 0);
    c->user_len = opts->strings_have_lengths ? opts->user_len : (opts->user ? strlen(opts->user) : 0);
    c->password_len = opts->strings_have_lengths ? opts->password_len : (opts->password ? strlen(opts->password) : 0);
    if ((opts->client_name && !(c->client_name = chc__async_dup(al, opts->client_name, c->client_name_len, err))) ||
        (opts->database    && !(c->database    = chc__async_dup(al, opts->database, c->database_len, err))) ||
        (opts->user        && !(c->user        = chc__async_dup(al, opts->user, c->user_len, err))) ||
        (opts->password    && !(c->password    = chc__async_dup(al, opts->password, c->password_len, err)))) {
        chc_async_client_free(c);
        return CHC_ERR_OOM;
    }

    c->hs_phase = CHC__HS_SEND_HELLO;
    *out = c;
    return CHC_OK;
}

void
chc_async_client_free(chc_async_client *c)
{
    if (!c) return;
    const chc_alloc *al = c->cli.al;
    chc__client_recv_state_free(&c->cli);
    al->free(al->ud, c->cli.client_name, c->cli.client_name_len + 1);
    chc__server_info_free(&c->cli.server, al);
    chc_in_free(&c->cli.in);
    chc__mem_sink_free(&c->out);
    chc__async_free_str(al, c->client_name, c->client_name_len);
    chc__async_free_str(al, c->database, c->database_len);
    chc__async_free_str(al, c->user, c->user_len);
    chc__async_free_str(al, c->password, c->password_len);
    al->free(al->ud, c, sizeof *c);
}

int
chc_async_submit(chc_async_client *c, const void *buf, size_t len, chc_err *err)
{
    return chc_in_submit(&c->cli.in, buf, len, err);
}

void
chc_async_pending_out(chc_async_client *c, const uint8_t **buf, size_t *len)
{
    *buf = c->out.buf + c->out_consumed;
    *len = c->out.len - c->out_consumed;
}

void
chc_async_consume_out(chc_async_client *c, size_t n)
{
    size_t avail = c->out.len - c->out_consumed;
    if (n > avail) n = avail;
    c->out_consumed += n;
    /* Fully drained: reset length so the sink doesn't grow unbounded. Keep the
     * allocation (no realloc-shrink). */
    if (c->out_consumed == c->out.len) {
        c->out.len = 0;
        c->out_consumed = 0;
    }
}

int
chc_async_handshake(chc_async_client *c, chc_exception **exc, chc_err *err)
{
    int rc;
    chc_client *cli = &c->cli;

    for (;;) {
        switch (c->hs_phase) {
        case CHC__HS_SEND_HELLO: {
            chc_client_opts o = {
                .client_name = c->client_name,
                .client_version_major = cli->client_version_major,
                .client_version_minor = cli->client_version_minor,
                .client_version_patch = cli->client_version_patch,
                .database = c->database,
                .user = c->user,
                .password = c->password,
                .strings_have_lengths = true,
                .client_name_len = c->client_name_len,
                .database_len = c->database_len,
                .user_len = c->user_len,
                .password_len = c->password_len,
            };
            rc = chc__client_send_hello(cli, &o, err);
            if (rc != CHC_OK) return rc;
            c->hs_phase = CHC__HS_RECV_HELLO;
            continue;
        }

        case CHC__HS_RECV_HELLO:
            chc__in_checkpoint(&cli->in);
            rc = chc__client_recv_hello(cli, exc, err);
            if (rc == CHC_WOULD_BLOCK) { chc__in_rewind(&cli->in); return rc; }
            if (rc != CHC_OK) return rc;  /* server exception / protocol */
            chc_in_reset(&cli->in);
            c->hs_phase = CHC__HS_POST_HELLO;
            continue;

        case CHC__HS_POST_HELLO:
            if (cli->server.revision >= CHC__REV_ADDENDUM) {
                rc = chc__write_string(cli->io, "", 0, err);  /* quota_key */
                if (rc != CHC_OK) return rc;
            }
            if (cli->skip_init_ping) {
                c->hs_phase = CHC__HS_DONE;
                continue;
            }
            rc = chc_client_send_ping(cli, err);
            if (rc != CHC_OK) return rc;
            c->hs_phase = CHC__HS_RECV_PONG;
            continue;

        case CHC__HS_RECV_PONG:
            rc = chc__recv_pong(cli, exc, err);  /* owns checkpoint/rewind */
            if (rc != CHC_OK) return rc;
            chc_in_reset(&cli->in);
            c->hs_phase = CHC__HS_DONE;
            continue;

        case CHC__HS_DONE:
        default:
            return CHC_OK;
        }
    }
}

int
chc_async_send_query(chc_async_client *c, const char *sql, size_t sql_len,
                     const char *query_id, size_t query_id_len, chc_err *err)
{
    return chc_client_send_query(&c->cli, sql, sql_len, query_id, query_id_len, err);
}

int
chc_async_send_data(chc_async_client *c, const chc_block_builder *bb, chc_err *err)
{
    return chc_client_send_data(&c->cli, bb, err);
}

int
chc_async_send_data_end(chc_async_client *c, chc_err *err)
{
    return chc_client_send_data(&c->cli, NULL, err);
}

const chc_server_info *
chc_async_server_info(const chc_async_client *c)
{
    return &c->cli.server;
}

void
chc_async_packet_clear(chc_async_client *c, chc_packet *p)
{
    chc_packet_clear(&c->cli, p);
}

/* The recv packet decoder is shared with the blocking client
 * (chc__recv_packet_resumable, clickhouse-client.h): the ioless `cli.in`
 * makes it checkpoint at packet/column boundaries and yield CHC_WOULD_BLOCK
 * on underrun, with c->cli.recv_* carrying parse state across calls. */
int
chc_async_recv_packet(chc_async_client *c, chc_packet *out, chc_err *err)
{
    return chc__recv_packet_resumable(&c->cli, out, err);
}

#endif /* CHC_IMPLEMENTATION */

#endif /* CLICKHOUSE_ASYNC_H */
