/*
 * clickhouse-client.h -- TCP packet loop over chc_io.
 *
 * Exactly one TU must `#define CHC_IMPLEMENTATION` before including;
 * the implementation uses internal varint / io / block helpers from
 * clickhouse.h.
 *
 * Caller owns the chc_io (socket setup, TLS, timeouts, cancel polling).
 * One chc_client wraps one connection. No reconnect / endpoint failover
 * / DNS — caller-side concerns.
 */

#ifndef CLICKHOUSE_CLIENT_H
#define CLICKHOUSE_CLIENT_H

#include <stdbool.h>
#include <stdint.h>

#include "clickhouse.h"
#include "clickhouse-compression.h"  /* chc_compression enum, chc_codec struct */

#ifdef __cplusplus
extern "C" {
#endif

/* Protocol revisions & feature gates are documented in the Native Protocol
 * spec: clickhouse.com/docs/reference/interfaces/specs/NativeProtocol
 *
 * The default minimum is ClickHouse 23.3. Compatibility callers can opt into
 * legacy revision gates. The default advertised revision is 54465. */
#define CHC_SERVER_MIN_REVISION 54462u
#define CHC_CLIENT_REVISION     54465u

typedef struct chc_client_opts {
    /* Identity. Defaults applied when fields are zero/NULL. */
    const char *client_name;        /* default "clickhouse-c" */
    uint64_t client_version_major;  /* default 0 */
    uint64_t client_version_minor;  /* default 0 */
    uint64_t client_version_patch;  /* default 0 */

    /* Hello body. */
    const char *database;           /* default "default" */
    const char *user;               /* default "default" */
    const char *password;           /* default "" */

    /* Compression. CHC_COMP_NONE if codec is NULL. */
    chc_compression compression;
    const chc_codec *codec;

    /* Internal read buffer size. 0 = use CHC_READ_BUFFER (8 KiB). */
    size_t read_buffer_bytes;

    /* Compatibility overrides. Zero-initialized options retain upstream defaults. */
    uint64_t protocol_revision;     /* 0 = CHC_CLIENT_REVISION */
    bool skip_init_ping;
    bool allow_legacy_server;
    bool strings_have_lengths;
    size_t client_name_len, database_len, user_len, password_len;
    bool has_max_compression_chunk_size;
    size_t max_compression_chunk_size; /* explicit 0 = no configured cap */
} chc_client_opts;

typedef struct chc_server_info {
    char     name[64];
    char     timezone[64];          /* updated by CHC_PKT_TIMEZONE_UPDATE */
    char     display_name[128];
    uint64_t version_major;
    uint64_t version_minor;
    uint64_t version_patch;
    uint64_t revision;              /* min(advertised revision, server revision) */
    uint64_t server_revision;       /* actual server revision, before negotiation */
    /* Untruncated strings owned by the client until close or timezone update. */
    char *full_name, *full_timezone, *full_display_name;
    size_t full_name_len, full_timezone_len, full_display_name_len;
} chc_server_info;

typedef struct chc_client chc_client;
typedef struct chc_exception chc_exception;

/* Performs Hello / HelloAck handshake immediately. On failure caller may
 * call chc_client_close to free any partially-allocated state.
 *
 * Handshake rejection returns CHC_ERR_SERVER and leaves err->msg empty. When
 * exc is not NULL, caller owns *exc and frees it with chc_exception_free.
 * Other results leave *exc unchanged. Query exceptions still arrive as
 * packets, but handshake rejection produces no client */
int  chc_client_init(chc_client **out, const chc_client_opts *opts,
                     const chc_alloc *al, chc_io *io, chc_exception **exc,
                     chc_err *err);

void chc_client_close(chc_client *c);

const chc_server_info *chc_client_server_info(const chc_client *c);

int  chc_client_send_query(chc_client *c,
                           const char *sql, size_t sql_len,
                           const char *query_id, size_t query_id_len,
                           chc_err *err);

/* Per-query setting. name / value are NUL-terminated unless strings_have_lengths.
 * Flags layout per the
 * Native Protocol spec (Query packet, settings entry); only the low two
 * bits are assigned. */
typedef struct chc_query_setting {
    const char *name;
    const char *value;
    bool        important;          /* flag bit 0 */
    bool        custom;             /* flag bit 1 (user-defined "SET custom_*=...") */
    bool        strings_have_lengths;
    size_t      name_len, value_len;
} chc_query_setting;

/* Per-query parameter (substituted into `{name:Type}` placeholders in the
 * SQL text). name / value are NUL-terminated unless strings_have_lengths.
 * The wire-level flags byte is
 * always CUSTOM. The server parses value via Field::restoreFromDump, so
 * callers must format value as a typed Field literal: e.g. `'hello'` for a
 * String, `42` for an integer, `[1,2,3]` for an array. NULL is `'\\N'`.
 * The value goes on the wire verbatim -- no auto-quoting -- so callers
 * keep full control of typed and non-string values.) */
typedef struct chc_query_param {
    const char *name;
    const char *value;
    bool        strings_have_lengths;
    size_t      name_len, value_len;
} chc_query_param;

typedef struct chc_query_opts {
    const char *query_id;
    size_t      query_id_len;
    const chc_query_setting *settings;
    size_t                   n_settings;
    const chc_query_param   *params;
    size_t                   n_params;
    bool                     defer_data_terminator;
} chc_query_opts;

int  chc_client_send_query_ex(chc_client *c,
                              const char *sql, size_t sql_len,
                              const chc_query_opts *opts, chc_err *err);

typedef enum chc_packet_kind {
    CHC_PKT_HELLO           = 0,
    CHC_PKT_DATA            = 1,
    CHC_PKT_EXCEPTION       = 2,
    CHC_PKT_PROGRESS        = 3,
    CHC_PKT_PONG            = 4,
    CHC_PKT_END_OF_STREAM   = 5,
    CHC_PKT_PROFILE_INFO    = 6,
    CHC_PKT_TOTALS          = 7,
    CHC_PKT_EXTREMES        = 8,
    CHC_PKT_LOG             = 10,
    CHC_PKT_TABLE_COLUMNS   = 11,
    CHC_PKT_PROFILE_EVENTS  = 14,
    CHC_PKT_TIMEZONE_UPDATE = 17,   /* no payload, see chc_server_info.timezone */
} chc_packet_kind;

/* CHC_PKT_EXCEPTION payload. Caller frees with chc_exception_free
 * if produced. */
struct chc_exception {
    int32_t        code;
    char          *name;         /* allocated in chc_alloc */
    size_t         name_len;
    char          *display_text;
    size_t         display_text_len;
    char          *stack_trace;
    size_t         stack_trace_len;
};

void chc_exception_free(chc_exception *e, const chc_alloc *al);

typedef struct chc_packet {
    chc_packet_kind kind;

    /* Payload selected by kind; exactly one member is live, none for
     * PONG / END_OF_STREAM / TABLE_COLUMNS / TIMEZONE_UPDATE. */
    union {
        /* CHC_PKT_DATA / TOTALS / EXTREMES / LOG / PROFILE_EVENTS:
         * caller-owned chc_block, freed with chc_block_destroy. */
        chc_block *block;

        /* CHC_PKT_EXCEPTION: caller-owned, freed with chc_exception_free. */
        chc_exception *exception;

        /* CHC_PKT_PROGRESS. */
        struct {
            uint64_t rows, bytes, total_rows;
            uint64_t total_bytes;               /* 0 below rev 54463 */
            uint64_t written_rows, written_bytes;
            uint64_t elapsed_ns;
        } progress;

        /* CHC_PKT_PROFILE_INFO. */
        struct {
            uint64_t rows, blocks, bytes, rows_before_limit;
            uint8_t  applied_limit, calculated_rows_before_limit;
        } profile;
    };
} chc_packet;

/* Read the next packet. On exception packets the caller MUST inspect
 * out->kind == CHC_PKT_EXCEPTION; the function returns CHC_OK with the
 * exception attached, not CHC_ERR_SERVER. */
int  chc_client_recv_packet(chc_client *c, chc_packet *out, chc_err *err);

/* Free anything chc_client_recv_packet allocated for this packet:
 * the block (if any) and the exception chain (if any). Safe to call
 * with packet->{block,exception} already NULLed by the caller (after
 * the caller takes ownership). */
void chc_packet_clear(chc_client *c, chc_packet *p);

/* Send a Data block. bb == NULL emits an empty block which the server
 * interprets as "no more INSERT rows" or as the query-text terminator
 * sent at the end of every SendQuery. */
int  chc_client_send_data(chc_client *c, const chc_block_builder *bb,
                          chc_err *err);
int  chc_client_send_data_named(chc_client *c, const chc_block_builder *bb,
                                const char *table_name, size_t table_name_len,
                                chc_err *err);

int  chc_client_send_cancel(chc_client *c, chc_err *err);
int  chc_client_send_ping(chc_client *c, chc_err *err);

#ifdef CHC_IMPLEMENTATION

#include <stdlib.h>
#include <string.h>

#define CHC__REV_TEMPORARY_TABLES 50264u
#define CHC__REV_TOTAL_ROWS_IN_PROGRESS 51554u
#define CHC__REV_BLOCK_INFO 51903u
#define CHC__REV_CLIENT_INFO 54032u
#define CHC__REV_SERVER_TIMEZONE 54058u
#define CHC__REV_QUOTA_KEY 54060u
#define CHC__REV_SERVER_DISPLAY_NAME 54372u
#define CHC__REV_VERSION_PATCH 54401u
#define CHC__REV_CLIENT_WRITE_INFO 54420u
#define CHC__REV_STRING_SETTINGS 54429u
#define CHC__REV_INTERSERVER_SECRET 54441u
#define CHC__REV_OPENTELEMETRY 54442u
#define CHC__REV_DISTRIBUTED_DEPTH 54448u
#define CHC__REV_INITIAL_QUERY_TIME 54449u
#define CHC__REV_PARALLEL_REPLICAS 54453u
#define CHC__REV_CUSTOM_SERIALIZATION 54454u
#define CHC__REV_ADDENDUM 54458u
#define CHC__REV_PARAMETERS 54459u
#define CHC__REV_PASSWORD_RULES 54461u
#define CHC__REV_NONCE 54462u
#define CHC__REV_TOTAL_BYTES_IN_PROGRESS 54463u
#define CHC__REV_ELAPSED_NS_IN_PROGRESS 54460u

/* Mirror ClickHouse DBMS_MAX_HELLO_STRING_SIZE & DBMS_MAX_PASSWORD_COMPLEXITY_RULES */
#ifndef CHC_MAX_HELLO_STRING_SIZE
#define CHC_MAX_HELLO_STRING_SIZE 4096u
#endif
#define CHC__MAX_HELLO_STRING CHC_MAX_HELLO_STRING_SIZE
#define CHC__MAX_PASSWORD_RULES 256u

/* Client → server packet kinds. */
#define CHC__CLIENT_HELLO  0u
#define CHC__CLIENT_QUERY  1u
#define CHC__CLIENT_DATA   2u
#define CHC__CLIENT_CANCEL 3u
#define CHC__CLIENT_PING   4u

struct chc_client {
    const chc_alloc *al;
    chc_io          *io;
    chc_in           in;            /* persistent buffered input */

    chc_server_info  server;
    uint64_t         client_version_major;
    uint64_t         client_version_minor;
    uint64_t         client_version_patch;
    uint64_t         protocol_revision;
    bool             skip_init_ping;
    bool             allow_legacy_server;
    size_t           max_compression_chunk_size;
    char            *client_name;
    size_t           client_name_len;
    chc_compression  compression;
    const chc_codec *codec;

    /* Meaningful only for an ioless `chc_in`.
     * io-backed refill blocks inside reader, so packet always completes,
     * leaving these at reset values across calls. */
    int              recv_in_block;  /* 0 = at packet boundary; 1 = mid block-bearing packet */
    chc_packet_kind  recv_kind;      /* committed kind for the in-progress block */
    chc_block       *recv_partial;   /* retained partial block */
    size_t           recv_next_col;  /* resume column index */

    /* Compressed-block frame-granularity resume; lifecycle = one compressed block.
     * recv_dec_in accumulates decompressed bytes the column reader has not yet consumed;
     * recv_decomp decompresses one frame at a time from `in`.
     * recv_partial / recv_next_col carry per-column progress over recv_dec_in. */
    bool             recv_dec_active;
    chc__decomp_src  recv_decomp;
    chc_in           recv_dec_in;
};

/* Free the persisted compressed-resume decompressor + decompressed buffer.
 * Idempotent; no-op when no compressed block is in flight. */
static void
chc__client_recv_comp_free(chc_client *c)
{
    if (!c->recv_dec_active) return;
    chc__decomp_src_free(&c->recv_decomp);
    chc_in_free(&c->recv_dec_in);
    c->recv_dec_active = false;
}

/* Free retained recv-resume state. Safe at any packet boundary or teardown. */
static void
chc__client_recv_state_free(chc_client *c)
{
    if (c->recv_partial) { chc_block_destroy(c->recv_partial, c->al); c->recv_partial = NULL; }
    chc__client_recv_comp_free(c);
    c->recv_in_block = 0;
    c->recv_next_col = 0;
}

void
chc_exception_free(chc_exception *e, const chc_alloc *al)
{
    if (!e) return;
    al->free(al->ud, e->name,         e->name_len         + 1);
    al->free(al->ud, e->display_text, e->display_text_len + 1);
    al->free(al->ud, e->stack_trace,  e->stack_trace_len  + 1);
    al->free(al->ud, e, sizeof *e);
}

static int
chc__read_i32_le(chc_in *in, int32_t *out, chc_err *err)
{
    uint32_t u;
    int rc = chc__read_u32_le(in, &u, err);
    if (rc != CHC_OK) return rc;
    *out = (int32_t) u;
    return CHC_OK;
}

static int
chc__client_send_hello(chc_client *c, const chc_client_opts *opts, chc_err *err)
{
    int rc;
    const char *name = opts->client_name ? opts->client_name : "clickhouse-c";
    size_t name_len = opts->strings_have_lengths && opts->client_name
        ? opts->client_name_len : strlen(name);
    const char *db  = opts->database ? opts->database : "default";
    const char *us  = opts->user     ? opts->user     : "default";
    const char *pw  = opts->password ? opts->password : "";
    size_t db_len = opts->strings_have_lengths && opts->database ? opts->database_len : strlen(db);
    size_t us_len = opts->strings_have_lengths && opts->user ? opts->user_len : strlen(us);
    size_t pw_len = opts->strings_have_lengths && opts->password ? opts->password_len : strlen(pw);
    if (CHC_PHP_COMPAT && !c->client_name) {
        c->client_name = chc__strdup(c->al, name, name_len, err);
        if (!c->client_name) return CHC_ERR_OOM;
        c->client_name_len = name_len;
    }

    if ((rc = chc__write_varuint(c->io, CHC__CLIENT_HELLO, err))) return rc;
    if ((rc = chc__write_string (c->io, name, name_len, err)))   return rc;
    if ((rc = chc__write_varuint(c->io, opts->client_version_major, err))) return rc;
    if ((rc = chc__write_varuint(c->io, opts->client_version_minor, err))) return rc;
    if ((rc = chc__write_varuint(c->io, c->protocol_revision, err))) return rc;
    if ((rc = chc__write_string (c->io, db, db_len, err))) return rc;
    if ((rc = chc__write_string (c->io, us, us_len, err))) return rc;
    if ((rc = chc__write_string (c->io, pw, pw_len, err))) return rc;
    return CHC_OK;
}

/* Read Hello-bounded server string into dst, truncating to cap - 1 bytes */
static int
chc__read_server_string(chc_in *in, char *dst, size_t cap, chc_err *err)
{
    uint64_t len;
    int rc = chc__read_varuint(in, &len, err);
    if (rc != CHC_OK) return rc;
    if (len > CHC__MAX_HELLO_STRING || len >= SIZE_MAX)
        return chc__err_set(err, CHC_ERR_PROTOCOL, "server string too long: %llu",
                            (unsigned long long) len);
    size_t n = len < cap ? (size_t) len : cap - 1;
    if ((rc = chc__read_bytes(in, dst, n, err))) return rc;
    dst[n] = '\0';
    return chc__skip_bytes(in, (size_t) len - n, err);
}

static void
chc__server_info_free(chc_server_info *s, const chc_alloc *al)
{
    al->free(al->ud, s->full_name, s->full_name_len + 1);
    al->free(al->ud, s->full_timezone, s->full_timezone_len + 1);
    al->free(al->ud, s->full_display_name, s->full_display_name_len + 1);
    s->full_name = s->full_timezone = s->full_display_name = NULL;
    s->full_name_len = s->full_timezone_len = s->full_display_name_len = 0;
}

/* Publish neither the full value nor its compatibility prefix until complete. */
static int
chc__read_server_string_full(chc_in *in, char *dst, size_t cap,
                             char **full, size_t *full_len, chc_err *err)
{
    uint64_t len;
    int rc = chc__read_varuint(in, &len, err);
    if (rc != CHC_OK) return rc;
    if (len > CHC__MAX_HELLO_STRING || len >= SIZE_MAX)
        return chc__err_set(err, CHC_ERR_PROTOCOL, "server string too long: %llu",
                            (unsigned long long) len);
    char *value = NULL;
    rc = chc__read_string_body(in, (size_t) len, &value, err);
    if (rc != CHC_OK) return rc;
    size_t n = len < cap ? (size_t) len : cap - 1;
    memcpy(dst, value, n);
    dst[n] = '\0';
    *full = value;
    *full_len = (size_t) len;
    return CHC_OK;
}

static void
chc__client_setup(chc_client *c, const chc_client_opts *opts)
{
    c->client_version_major = opts->client_version_major;
    c->client_version_minor = opts->client_version_minor;
    c->client_version_patch = opts->client_version_patch;
    c->protocol_revision = opts->protocol_revision ? opts->protocol_revision : CHC_CLIENT_REVISION;
    c->server.revision = c->protocol_revision;
    c->skip_init_ping = opts->skip_init_ping;
    c->allow_legacy_server = opts->allow_legacy_server;
    c->max_compression_chunk_size = opts->has_max_compression_chunk_size
        ? opts->max_compression_chunk_size : CHC_COMPRESS_MAX_CHUNK;
    c->compression = opts->codec ? opts->compression : CHC_COMP_NONE;
    c->codec       = opts->codec;
}

/* Reads chained exception. Caller frees via chc_exception_free. */
static int
chc__read_exception(chc_client *c, chc_exception **out, chc_err *err)
{
    chc_exception *e = chc__calloc(c->al, sizeof *e, err);
    if (!e) return CHC_ERR_OOM;
    uint8_t has_nested;
    int rc;
    if ((rc = chc__read_i32_le (&c->in, &e->code, err)) ||
        (rc = chc__read_string (&c->in, &e->name,         &e->name_len,         err)) ||
        (rc = chc__read_string (&c->in, &e->display_text, &e->display_text_len, err)) ||
        (rc = chc__read_string (&c->in, &e->stack_trace,  &e->stack_trace_len,  err)) ||
        (rc = chc__read_byte   (&c->in, &has_nested, err))) {
        chc_exception_free(e, c->al);
        return rc;
    }
    *out = e;
    return CHC_OK;
}

static int
chc__client_recv_hello(chc_client *c, chc_exception **exc, chc_err *err)
{
    uint64_t kind;
    int rc = chc__read_varuint(&c->in, &kind, err);
    if (rc != CHC_OK) return rc;
    if (kind == CHC_PKT_EXCEPTION) {
        chc_exception *e = NULL;
        rc = chc__read_exception(c, &e, err);
        if (rc != CHC_OK) return rc;
        if (exc) *exc = e;
        else chc_exception_free(e, c->al);
        chc_err_reset(err);
        return CHC_ERR_SERVER;
    }
    if (kind != CHC_PKT_HELLO)
        return chc__err_set(err, CHC_ERR_PROTOCOL,
                            "expected Hello, got %llu",
                            (unsigned long long) kind);

    /* Commit only complete Hello, async handshake retries on WOULD_BLOCK */
    chc_server_info s = {0};
    uint64_t n_rules, nonce;
    char rule[1];
    if ((rc = chc__read_server_string_full(&c->in, s.name, sizeof s.name,
                                           &s.full_name, &s.full_name_len, err)) ||
        (rc = chc__read_varuint(&c->in, &s.version_major, err)) ||
        (rc = chc__read_varuint(&c->in, &s.version_minor, err)) ||
        (rc = chc__read_varuint(&c->in, &s.revision, err)))
        goto fail;
    s.server_revision = s.revision;
    if (!c->allow_legacy_server && s.revision < CHC_SERVER_MIN_REVISION) {
        rc = chc__err_set(err, CHC_ERR_PROTOCOL,
                            "server revision %llu older than %u",
                            (unsigned long long) s.revision, CHC_SERVER_MIN_REVISION);
        goto fail;
    }
    if (s.revision > c->protocol_revision) s.revision = c->protocol_revision;

    if (s.revision >= CHC__REV_SERVER_TIMEZONE &&
        (rc = chc__read_server_string_full(&c->in, s.timezone, sizeof s.timezone,
                                           &s.full_timezone, &s.full_timezone_len, err))) goto fail;
    if (s.revision >= CHC__REV_SERVER_DISPLAY_NAME &&
        (rc = chc__read_server_string_full(&c->in, s.display_name, sizeof s.display_name,
                                           &s.full_display_name, &s.full_display_name_len, err))) goto fail;
    if (s.revision >= CHC__REV_VERSION_PATCH &&
        (rc = chc__read_varuint(&c->in, &s.version_patch, err))) goto fail;
    if (s.revision >= CHC__REV_PASSWORD_RULES) {
        if ((rc = chc__read_varuint(&c->in, &n_rules, err))) goto fail;
        if (n_rules > CHC__MAX_PASSWORD_RULES) {
            rc = chc__err_set(err, CHC_ERR_PROTOCOL, "too many password rules: %llu",
                            (unsigned long long) n_rules);
            goto fail;
        }
        /* Pattern & message pairs, discarded */
        for (uint64_t i = 0; i < 2 * n_rules; i++)
            if ((rc = chc__read_server_string(&c->in, rule, sizeof rule, err))) goto fail;
    }
    /* Nonce only serves interserver secret */
    if (s.revision >= CHC__REV_NONCE &&
        (rc = chc__read_u64_le(&c->in, &nonce, err))) goto fail;
    chc__server_info_free(&c->server, c->al);
    c->server = s;
    return CHC_OK;
fail:
    chc__server_info_free(&s, c->al);
    return rc;
}

static int
chc__recv_pong(chc_client *c, chc_exception **exc, chc_err *err)
{
    bool ioless = c->in.io == NULL;
    if (ioless) chc__in_checkpoint(&c->in);
    uint64_t kind;
    int rc = chc__read_varuint(&c->in, &kind, err);
    if (rc != CHC_OK) goto maybe_rewind;
    if (kind == CHC_PKT_EXCEPTION) {
        chc_exception *e = NULL;
        rc = chc__read_exception(c, &e, err);  /* frees its partial on non-OK */
        if (rc != CHC_OK) goto maybe_rewind;
        if (exc) *exc = e;
        else chc_exception_free(e, c->al);
        chc_err_reset(err);
        return CHC_ERR_SERVER;
    }
    if (kind != CHC_PKT_PONG)
        return chc__err_set(err, CHC_ERR_PROTOCOL, "expected Pong, got %llu",
                            (unsigned long long) kind);
    return CHC_OK;
maybe_rewind:
    if (ioless && rc == CHC_WOULD_BLOCK) chc__in_rewind(&c->in);
    return rc;
}

int
chc_client_init(chc_client **out, const chc_client_opts *opts,
                const chc_alloc *al, chc_io *io, chc_exception **exc,
                chc_err *err)
{
    chc_client_opts def_opts = {0};
    if (!opts) opts = &def_opts;
    if (opts->protocol_revision > CHC_CLIENT_REVISION)
        return chc__err_set(err, CHC_ERR_USAGE, "unsupported client protocol revision");

    chc_client *c = chc__calloc(al, sizeof *c, err);
    if (!c) return CHC_ERR_OOM;
    c->al = al;
    c->io = io;

    chc__client_setup(c, opts);

    int rc = chc_in_init(&c->in, io, al, opts->read_buffer_bytes, err);
    if (rc != CHC_OK) { al->free(al->ud, c, sizeof *c); return rc; }

    rc = chc__client_send_hello(c, opts, err);
    if (rc != CHC_OK) goto fail;
    rc = chc__client_recv_hello(c, exc, err);
    if (rc != CHC_OK) goto fail;

    /* Addendum: send empty quota_key. */
    if (c->server.revision >= CHC__REV_ADDENDUM) {
        rc = chc__write_string(c->io, "", 0, err);
        if (rc != CHC_OK) goto fail;
    }

    /* Probe Ping. Server-side late-stage rejections (eg invalid
     * default_database in 24.x) only surface after the Addendum is read,
     * not in the Hello reply. Without a probe, the rejection races the
     * caller's first query: caller's writes may hit ECONNRESET before the
     * exception packet is read. The Ping forces a round-trip here so the
     * exception is delivered at init time instead. */
    if (!c->skip_init_ping) {
        rc = chc_client_send_ping(c, err);
        if (rc != CHC_OK) goto fail;
        rc = chc__recv_pong(c, exc, err);
        if (rc != CHC_OK) goto fail;
    }

    *out = c;
    return CHC_OK;

fail:
    al->free(al->ud, c->client_name, c->client_name_len + 1);
    chc__server_info_free(&c->server, al);
    chc_in_free(&c->in);
    al->free(al->ud, c, sizeof *c);
    *out = NULL;
    return rc;
}

void
chc_client_close(chc_client *c)
{
    if (!c) return;
    chc__client_recv_state_free(c);
    c->al->free(c->al->ud, c->client_name, c->client_name_len + 1);
    chc__server_info_free(&c->server, c->al);
    chc_in_free(&c->in);
    c->al->free(c->al->ud, c, sizeof *c);
}

const chc_server_info *
chc_client_server_info(const chc_client *c)
{
    return c ? &c->server : NULL;
}

int
chc_client_send_ping(chc_client *c, chc_err *err)
{
    return chc__write_varuint(c->io, CHC__CLIENT_PING, err);
}

int
chc_client_send_cancel(chc_client *c, chc_err *err)
{
    return chc__write_varuint(c->io, CHC__CLIENT_CANCEL, err);
}

/* Write a block body (BlockInfo + cols + rows) to a chc_io. Used for
 * both the uncompressed direct path and the compressed buffer-then-emit
 * path. */
static int
chc__client_write_block_body(chc_client *c, chc_io *sink, const chc_block_builder *bb,
                             chc_err *err)
{
    int rc;
    chc_block_opts opts = {
        .has_block_info = c->server.revision >= CHC__REV_BLOCK_INFO,
        .has_custom_serialization = c->server.revision >= CHC__REV_CUSTOM_SERIALIZATION,
    };
    if (bb) return chc_block_write(sink, bb, &opts, err);

    if (opts.has_block_info && (rc = chc__write_block_info(sink, err))) return rc;
    if ((rc = chc__write_varuint(sink, 0, err))) return rc;  /* n_cols */
    if ((rc = chc__write_varuint(sink, 0, err))) return rc;  /* n_rows */
    return CHC_OK;
}

typedef struct {
    chc_io *destination;
    size_t used;
    uint8_t data[8192];
} chc__data_sink;

static int
chc__data_sink_flush(chc__data_sink *sink, chc_err *err)
{
    if (!sink->used) return CHC_OK;
    int rc = chc__write_bytes(sink->destination, sink->data, sink->used, err);
    if (rc == CHC_OK) sink->used = 0;
    return rc;
}

static int
chc__data_sink_write(void *ud, const void *data, size_t len, chc_err *err)
{
    chc__data_sink *sink = ud;
    if (len > sizeof sink->data - sink->used) {
        int rc = chc__data_sink_flush(sink, err);
        if (rc != CHC_OK) return rc;
        if (len > sizeof sink->data / 2)
            return chc__write_bytes(sink->destination, data, len, err);
    }
    if (len) memcpy(sink->data + sink->used, data, len);
    sink->used += len;
    return CHC_OK;
}

/* Write a Data packet. bb may be NULL for an empty (0 columns, 0 rows)
 * block — the terminator the server uses to detect end-of-INSERT and
 * end-of-query-text. */
static int
chc__client_write_data(chc_client *c, const chc_block_builder *bb,
                        const char *table_name, size_t table_name_len, chc_err *err)
{
    int rc;
    chc__data_sink buffered;
    buffered.destination = c->io;
    buffered.used = 0;
    chc_io buffered_io = {.ud = &buffered, .write = chc__data_sink_write};
    chc_io *sink = CHC_PHP_COMPAT && c->compression == CHC_COMP_NONE
        ? &buffered_io : c->io;
    if ((rc = chc__write_varuint(sink, CHC__CLIENT_DATA, err))) return rc;
    if (c->server.revision >= CHC__REV_TEMPORARY_TABLES &&
        (rc = chc__write_string(sink, table_name, table_name_len, err))) return rc;

    if (c->compression == CHC_COMP_NONE) {
        rc = chc__client_write_block_body(c, sink, bb, err);
        if (rc == CHC_OK && CHC_PHP_COMPAT) rc = chc__data_sink_flush(&buffered, err);
        return rc;
    }

    if (!c->codec)
        return chc__err_set(err, CHC_ERR_USAGE,
                            "compression enabled but codec is NULL");

    chc__mem_sink ms;
    chc_io sink_io;
    chc__mem_sink_init(&ms, &sink_io, c->al);
    rc = chc__client_write_block_body(c, &sink_io, bb, err);
    if (rc != CHC_OK) { chc__mem_sink_free(&ms); return rc; }
    rc = chc__comp_emit_chunks_sized(c->io, c->codec, c->compression,
                                     ms.buf, ms.len, c->max_compression_chunk_size,
                                     c->al, err);
    chc__mem_sink_free(&ms);
    return rc;
}

int
chc_client_send_data(chc_client *c, const chc_block_builder *bb, chc_err *err)
{
    return chc__client_write_data(c, bb, "", 0, err);
}

int
chc_client_send_data_named(chc_client *c, const chc_block_builder *bb,
                           const char *table_name, size_t table_name_len, chc_err *err)
{
    if (!c || (!table_name && table_name_len))
        return chc__err_set(err, CHC_ERR_USAGE, "invalid named Data arguments");
    if (table_name_len && c->server.revision < CHC__REV_TEMPORARY_TABLES)
        return chc__err_set(err, CHC_ERR_USAGE, "server does not support external tables");
    return chc__client_write_data(c, bb, table_name, table_name_len, err);
}

int
chc_client_send_query_ex(chc_client *c, const char *sql, size_t sql_len,
                         const chc_query_opts *opts, chc_err *err)
{
    chc_query_opts def = {0};
    if (!opts) opts = &def;
    if (opts->n_settings && c->server.revision < CHC__REV_STRING_SETTINGS)
        return chc__err_set(err, CHC_ERR_USAGE, "server does not support string query settings");

    int rc;
    if ((rc = chc__write_varuint(c->io, CHC__CLIENT_QUERY, err))) return rc;
    if ((rc = chc__write_string (c->io, opts->query_id, opts->query_id_len, err))) return rc;

    /* ClientInfo. Body layout per the Native Protocol spec (Query packet).
     * We send the minimum the server tolerates (initial fields blank,
     * iface=TCP); only client_name identifies us. */
    uint8_t query_kind = 1;       /* INITIAL_QUERY */
    uint8_t z8[8] = {0};
    uint8_t iface_type = 1;       /* TCP */
    uint8_t no_otel = 0;
    if (c->server.revision >= CHC__REV_CLIENT_INFO) {
    if ((rc = chc__write_bytes (c->io, &query_kind, 1, err))) return rc;
    if ((rc = chc__write_string(c->io, "", 0, err))) return rc;  /* initial_user */
    if ((rc = chc__write_string(c->io, "", 0, err))) return rc;  /* initial_query_id */
    if ((rc = chc__write_string(c->io, "[::ffff:127.0.0.1]:0", 20, err))) return rc;
    if (c->server.revision >= CHC__REV_INITIAL_QUERY_TIME &&
        (rc = chc__write_bytes (c->io, z8, 8, err))) return rc;
    if ((rc = chc__write_bytes (c->io, &iface_type, 1, err))) return rc;
    if ((rc = chc__write_string(c->io, "", 0, err))) return rc;  /* os_user */
    if ((rc = chc__write_string(c->io, "", 0, err))) return rc;  /* client_hostname */
    if ((rc = chc__write_string(c->io,
        c->client_name ? c->client_name : "clickhouse-c client",
        c->client_name ? c->client_name_len : 19, err))) return rc;
    if ((rc = chc__write_varuint(c->io, c->client_version_major, err))) return rc;
    if ((rc = chc__write_varuint(c->io, c->client_version_minor, err))) return rc;
    if ((rc = chc__write_varuint(c->io, c->protocol_revision, err))) return rc;
    if (c->server.revision >= CHC__REV_QUOTA_KEY &&
        (rc = chc__write_string(c->io, "", 0, err))) return rc;
    if (c->server.revision >= CHC__REV_DISTRIBUTED_DEPTH &&
        (rc = chc__write_varuint(c->io, 0, err))) return rc;
    if (c->server.revision >= CHC__REV_VERSION_PATCH &&
        (rc = chc__write_varuint(c->io, c->client_version_patch, err))) return rc;
    if (c->server.revision >= CHC__REV_OPENTELEMETRY &&
        (rc = chc__write_bytes (c->io, &no_otel, 1, err))) return rc;
    /* Parallel replicas: collaborate_with_initiator, count, number */
    if (c->server.revision >= CHC__REV_PARALLEL_REPLICAS) {
        if ((rc = chc__write_varuint(c->io, 0, err))) return rc;
        if ((rc = chc__write_varuint(c->io, 0, err))) return rc;
        if ((rc = chc__write_varuint(c->io, 0, err))) return rc;
    }
    }

    /* Per-query settings: name + varuint(flags) + value, repeated, then
     * empty-string terminator. */
    for (size_t i = 0; i < opts->n_settings; i++) {
        const chc_query_setting *s = &opts->settings[i];
        size_t nlen = s->strings_have_lengths ? s->name_len : (s->name ? strlen(s->name) : 0);
        size_t vlen = s->strings_have_lengths ? s->value_len : (s->value ? strlen(s->value) : 0);
        uint64_t flags = (s->important ? 1u : 0u) | (s->custom ? 2u : 0u);
        if ((rc = chc__write_string (c->io, s->name,  nlen, err))) return rc;
        if ((rc = chc__write_varuint(c->io, flags, err))) return rc;
        if ((rc = chc__write_string (c->io, s->value, vlen, err))) return rc;
    }
    if ((rc = chc__write_string(c->io, "", 0, err))) return rc;

    if (c->server.revision >= CHC__REV_INTERSERVER_SECRET &&
        (rc = chc__write_string(c->io, "", 0, err))) return rc;

    /* Stages::Complete = 2. */
    if ((rc = chc__write_varuint(c->io, 2, err))) return rc;
    /* Compression state: 1 if enabled, 0 otherwise. */
    if ((rc = chc__write_varuint(c->io,
        c->compression != CHC_COMP_NONE ? 1u : 0u, err))) return rc;
    /* Query text. */
    if ((rc = chc__write_string(c->io, sql, sql_len, err))) return rc;

    /* Parameters: same shape as settings; flags always CUSTOM (bit 1). */
    if (c->server.revision >= CHC__REV_PARAMETERS) {
    for (size_t i = 0; i < opts->n_params; i++) {
        const chc_query_param *p = &opts->params[i];
        size_t nlen = p->strings_have_lengths ? p->name_len : (p->name ? strlen(p->name) : 0);
        size_t vlen = p->strings_have_lengths ? p->value_len : (p->value ? strlen(p->value) : 0);
        if ((rc = chc__write_string (c->io, p->name,  nlen, err))) return rc;
        if ((rc = chc__write_varuint(c->io, 2u, err))) return rc;
        if ((rc = chc__write_string (c->io, p->value, vlen, err))) return rc;
    }
    if ((rc = chc__write_string(c->io, "", 0, err))) return rc;
    }

    /* Finalize: send an empty Data block as the query-text terminator. */
    return opts->defer_data_terminator ? CHC_OK : chc__client_write_data(c, NULL, "", 0, err);
}

int
chc_client_send_query(chc_client *c, const char *sql, size_t sql_len,
                      const char *query_id, size_t query_id_len, chc_err *err)
{
    chc_query_opts opts = {
        .query_id = query_id,
        .query_id_len = query_id_len,
    };
    return chc_client_send_query_ex(c, sql, sql_len, &opts, err);
}

void
chc_packet_clear(chc_client *c, chc_packet *p)
{
    if (!p) return;
    switch (p->kind) {
    case CHC_PKT_DATA: case CHC_PKT_TOTALS: case CHC_PKT_EXTREMES:
    case CHC_PKT_LOG:  case CHC_PKT_PROFILE_EVENTS:
        if (p->block) { chc_block_destroy(p->block, c->al); p->block = NULL; }
        break;
    case CHC_PKT_EXCEPTION:
        if (p->exception) { chc_exception_free(p->exception, c->al); p->exception = NULL; }
        break;
    default:
        break;
    }
}

/* Skip temp-table name or log tag block-bearing packets carry before block */
static int
chc__recv_skip_lead_string(chc_client *c, chc_err *err)
{
    char *s; size_t slen;
    int rc = chc__read_string(&c->in, &s, &slen, err);
    if (rc != CHC_OK) return rc;
    c->al->free(c->al->ud, s, slen + 1);
    return CHC_OK;
}

/* Read one compressed block body from c->in into *out, io-backed
 * path only. Build a per-call decompressor + io-backed dec_in over the raw in
 * and run the block reader; io-backed refill blocks, so the block always
 * completes in one call. The ioless path is handled by
 * chc__recv_block_compressed_resume, which keeps decomp state alive across
 * CHC_WOULD_BLOCK for frame-granularity resumption.
 *
 * The ioless checkpoint/rewind below is dead for the blocking caller; it stays
 * to keep this function correct if ever driven over an ioless raw in (baseline
 * rewind-to-block-start). */
static int
chc__recv_block_compressed(chc_client *c, const chc_block_opts *opts,
                           chc_block **out, chc_err *err)
{
    if (!c->codec)
        return chc__err_set(err, CHC_ERR_USAGE,
                            "compression enabled but codec is NULL");
    bool ioless = c->in.io == NULL;
    if (ioless) chc__in_checkpoint(&c->in);   /* raw in, at compressed block start */
    chc__decomp_src src;
    chc_io decomp_io;
    chc__decomp_src_init(&src, &c->in, c->codec, c->al, &decomp_io);
    chc_in dec_in;
    int rc = chc_in_init(&dec_in, &decomp_io, c->al, 0, err);
    if (rc != CHC_OK) { chc__decomp_src_free(&src); return rc; }
    rc = chc_block_read(&dec_in, c->al, opts, out, err);
    chc_in_free(&dec_in);
    chc__decomp_src_free(&src);
    if (ioless && rc == CHC_WOULD_BLOCK) chc__in_rewind(&c->in);
    return rc;
}

/* Frame-granularity compressed resume for an ioless `in`. Keeps a
 * persisted decompressor + ioless decompressed buffer (c->recv_decomp /
 * c->recv_dec_in) alive across CHC_WOULD_BLOCK so a compressed Data block
 * streamed over many reads re-decompresses at most one frame and re-parses at
 * most one column on retry, instead of re-decompressing from frame 0.
 *
 * Demand-driven: pumps exactly one frame whenever the column reader underruns
 * the decompressed buffer. A block's frames sum to exactly its serialized size
 * and the next packet starts with an uncompressed tag, so chc__block_resume_in
 * reports CHC_OK before it would ever demand a frame from the next packet --
 * eager pumping would mis-read that tag as a frame and fail on a hash mismatch.
 *
 * Contract mirrors the uncompressed branch's chc__block_resume_in use:
 *   CHC_OK          -- block complete in c->recv_partial (caller takes it);
 *                      decomp state torn down.
 *   CHC_WOULD_BLOCK -- raw `in` drained mid-frame; partial block + decomp state
 *                      retained, raw rewound to the incomplete frame's start.
 *   other           -- error; recv_partial freed/NULL, decomp state torn down. */
static int
chc__recv_block_compressed_resume(chc_client *c, const chc_block_opts *opts,
                                  chc_err *err)
{
    if (!c->codec)
        return chc__err_set(err, CHC_ERR_USAGE,
                            "compression enabled but codec is NULL");

    if (!c->recv_dec_active) {
        int rc = chc_in_init_ioless(&c->recv_dec_in, c->al);
        if (rc != CHC_OK) return rc;
        chc_io scratch_io;  /* push mode: the decomp io adapter goes unused */
        chc__decomp_src_init(&c->recv_decomp, &c->in, c->codec, c->al, &scratch_io);
        c->recv_dec_active = true;
    }

    for (;;) {
        /* Parse as far as the decompressed bytes allow (per-column retain is
         * active: recv_dec_in is ioless). */
        int rc = chc__block_resume_in(&c->recv_dec_in, c->al, opts,
                                      &c->recv_partial, &c->recv_next_col, err);
        if (rc == CHC_OK) {
            chc__client_recv_comp_free(c);
            return CHC_OK;
        }
        if (rc != CHC_WOULD_BLOCK) {            /* real error: resume freed the block */
            chc__client_recv_comp_free(c);
            c->recv_partial = NULL;
            c->recv_next_col = 0;
            return rc;
        }

        /* recv_dec_in drained mid-block: decompress exactly ONE more frame from
         * the raw compressed `in` and push it into recv_dec_in. */
        chc__in_checkpoint(&c->in);             /* frame start on raw */
        int frc = chc__decomp_read_frame(&c->recv_decomp, err);
        if (frc == CHC_WOULD_BLOCK) {           /* frame incomplete: need socket bytes */
            chc__in_rewind(&c->in);             /* keep partial frame buffered */
            return CHC_WOULD_BLOCK;
        }
        if (frc != CHC_OK) {                     /* hash mismatch / codec / oom */
            chc__client_recv_state_free(c);
            return frc;
        }
        /* Complete frame committed (no rewind); push its decompressed bytes.
         * chc_in_submit copies, so the next frame may reuse frame_buf. */
        if (!c->recv_decomp.frame_fill) continue;
        rc = chc_in_submit(&c->recv_dec_in, c->recv_decomp.frame_buf,
                           c->recv_decomp.frame_fill, err);
        if (rc != CHC_OK) {
            chc__client_recv_state_free(c);
            return rc;
        }
    }
}

/* Resumable packet decoder shared by chc_client_recv_packet and
 * chc_async_recv_packet. For io-backed `in` refill blocks inside the reader.
 * Ioless-gated checkpoints/resets are skipped, and a packet always completes
 * in one call. For an ioless `in` it checkpoints at packet start and at each
 * resumable boundary so a mid-parse drain returns CHC_WOULD_BLOCK with
 * c->recv_* state retained for the next call.
 *
 * recv keeps parse state alive across CHC_WOULD_BLOCK: a Data block streamed
 * over many reads resumes at the in-progress column (uncompressed, via
 * chc__block_resume_in). Compressed Data resumes at frame granularity over an
 * ioless `in` (chc__recv_block_compressed_resume); the blocking io-backed path
 * rebuilds the decompressor per call (chc__recv_block_compressed). */
static int
chc__recv_packet_resumable(chc_client *c, chc_packet *out, chc_err *err)
{
    bool ioless = c->in.io == NULL;
    chc_block_opts block_opts = {
        .has_block_info = c->server.revision >= CHC__REV_BLOCK_INFO,
        .has_custom_serialization = c->server.revision >= CHC__REV_CUSTOM_SERIALIZATION,
    };
    const chc_block_opts *opts = &block_opts;
    int rc;
    int is_log = 0;  /* LOG/PROFILE_EVENTS: never compressed */

    memset(out, 0, sizeof *out);

    if (!c->recv_in_block) {
        if (ioless) chc__in_checkpoint(&c->in);   /* packet start */
        uint64_t kind;
        rc = chc__read_varuint(&c->in, &kind, err);
        if (rc != CHC_OK) goto maybe_rewind;
        if (CHC_PHP_COMPAT && (kind == CHC_PKT_TOTALS || kind == CHC_PKT_EXTREMES))
            return chc__err_set(err, CHC_ERR_PROTOCOL, "unimplemented %llu", (unsigned long long) kind);

        switch (kind) {
        /* ---- control / non-block packets: parse inline ---- */
        case CHC_PKT_EXCEPTION:
            out->kind = CHC_PKT_EXCEPTION;
            rc = chc__read_exception(c, &out->exception, err);  /* frees its partial on non-OK */
            if (CHC_PHP_COMPAT && rc != CHC_OK && rc != CHC_WOULD_BLOCK && rc != CHC_ERR_OOM)
                rc = chc__err_set(err, CHC_ERR_PROTOCOL,
                    "server reported an error, but the exception packet could not be decoded (error details lost)");
            if (rc != CHC_OK) goto maybe_rewind;
            goto control_done;

        case CHC_PKT_PROGRESS:
            out->kind = CHC_PKT_PROGRESS;
            if ((rc = chc__read_varuint(&c->in, &out->progress.rows,       err)) ||
                (rc = chc__read_varuint(&c->in, &out->progress.bytes,      err)))
                goto maybe_rewind;
            if (c->server.revision >= CHC__REV_TOTAL_ROWS_IN_PROGRESS &&
                (rc = chc__read_varuint(&c->in, &out->progress.total_rows, err))) goto maybe_rewind;
            if (c->server.revision >= CHC__REV_TOTAL_BYTES_IN_PROGRESS)
                if ((rc = chc__read_varuint(&c->in, &out->progress.total_bytes, err)))
                    goto maybe_rewind;
            if (c->server.revision >= CHC__REV_CLIENT_WRITE_INFO &&
                ((rc = chc__read_varuint(&c->in, &out->progress.written_rows,  err)) ||
                 (rc = chc__read_varuint(&c->in, &out->progress.written_bytes, err))))
                goto maybe_rewind;
            if (c->server.revision >= CHC__REV_ELAPSED_NS_IN_PROGRESS &&
                (rc = chc__read_varuint(&c->in, &out->progress.elapsed_ns, err))) goto maybe_rewind;
            goto control_done;

        case CHC_PKT_PONG:
            out->kind = CHC_PKT_PONG;
            goto control_done;

        case CHC_PKT_END_OF_STREAM:
            out->kind = CHC_PKT_END_OF_STREAM;
            goto control_done;

        case CHC_PKT_PROFILE_INFO:
            out->kind = CHC_PKT_PROFILE_INFO;
            if ((rc = chc__read_varuint(&c->in, &out->profile.rows,   err)) ||
                (rc = chc__read_varuint(&c->in, &out->profile.blocks, err)) ||
                (rc = chc__read_varuint(&c->in, &out->profile.bytes,  err)) ||
                (rc = chc__read_byte   (&c->in, &out->profile.applied_limit, err)) ||
                (rc = chc__read_varuint(&c->in, &out->profile.rows_before_limit, err)) ||
                (rc = chc__read_byte   (&c->in, &out->profile.calculated_rows_before_limit, err)))
                goto maybe_rewind;
            goto control_done;

        case CHC_PKT_TABLE_COLUMNS: {
            out->kind = CHC_PKT_TABLE_COLUMNS;
            /* table name + columns metadata; both varstrings, both ignored. */
            char *s; size_t slen;
            if ((rc = chc__read_string(&c->in, &s, &slen, err))) goto maybe_rewind;
            c->al->free(c->al->ud, s, slen + 1);
            if ((rc = chc__read_string(&c->in, &s, &slen, err))) goto maybe_rewind;
            c->al->free(c->al->ud, s, slen + 1);
            goto control_done;
        }

        /* Empty timezone means server default, as clickhouse-client treats it */
        case CHC_PKT_TIMEZONE_UPDATE: {
            out->kind = CHC_PKT_TIMEZONE_UPDATE;
            char tz[sizeof c->server.timezone] = {0};
            char *full = NULL;
            size_t full_len = 0;
            if ((rc = chc__read_server_string_full(&c->in, tz, sizeof tz,
                                                   &full, &full_len, err))) goto maybe_rewind;
            c->al->free(c->al->ud, c->server.full_timezone, c->server.full_timezone_len + 1);
            c->server.full_timezone = full;
            c->server.full_timezone_len = full_len;
            memcpy(c->server.timezone, tz, sizeof tz);
            goto control_done;
        }

        /* ---- block-bearing: commit kind + leading string, then resume ---- */
        case CHC_PKT_DATA:    out->kind = c->recv_kind = CHC_PKT_DATA;     break;
        case CHC_PKT_TOTALS:  out->kind = c->recv_kind = CHC_PKT_TOTALS;   break;
        case CHC_PKT_EXTREMES:out->kind = c->recv_kind = CHC_PKT_EXTREMES; break;
        case CHC_PKT_LOG:     out->kind = c->recv_kind = CHC_PKT_LOG;      is_log = 1; break;
        case CHC_PKT_PROFILE_EVENTS:
            out->kind = c->recv_kind = CHC_PKT_PROFILE_EVENTS; is_log = 1; break;

        default:
            return chc__err_set(err, CHC_ERR_PROTOCOL,
                                "unknown server packet %llu",
                                (unsigned long long) kind);
        }

        if (c->server.revision >= CHC__REV_TEMPORARY_TABLES) {
            rc = chc__recv_skip_lead_string(c, err);
            if (rc != CHC_OK) goto maybe_rewind;
        }

        /* Kind + leading string committed; resume owns the cursor from here.
         * Do NOT rewind/reset on a mid-block would-block: resume re-parses only
         * from the in-progress column. */
        c->recv_in_block = 1;
        c->recv_partial = NULL;
        c->recv_next_col = 0;
    } else {
        out->kind = c->recv_kind;
        is_log = (c->recv_kind == CHC_PKT_LOG ||
                  c->recv_kind == CHC_PKT_PROFILE_EVENTS);
    }

    /* LOG/PROFILE_EVENTS blocks are never compressed; route through the
     * uncompressed resume path regardless of c->compression. */
    if (c->compression == CHC_COMP_NONE || is_log) {
        rc = chc__block_resume_in(&c->in, c->al, opts,
                                  &c->recv_partial, &c->recv_next_col, err);
        if (rc == CHC_WOULD_BLOCK) return rc;  /* resume owns rewind; partial retained */
        c->recv_in_block = 0;
        if (rc != CHC_OK) {
            c->recv_partial = NULL;  /* resume already freed it */
            c->recv_next_col = 0;
            return rc;
        }
        out->block = c->recv_partial;
        c->recv_partial = NULL;
        c->recv_next_col = 0;
        if (ioless) chc_in_reset(&c->in);
        return CHC_OK;
    }

    /* Compressed Data. ioless: frame-granularity resume over a persisted
     * decompressor + ioless decompressed buffer. io-backed:
     * baseline rebuild-per-call (chc__recv_block_compressed). */
    if (ioless) {
        rc = chc__recv_block_compressed_resume(c, opts, err);
        if (rc == CHC_WOULD_BLOCK) return rc;  /* partial block + decomp state retained */
        c->recv_in_block = 0;
        if (rc != CHC_OK) return rc;           /* resume tore down its state + recv_partial */
        out->block = c->recv_partial;
        c->recv_partial = NULL;
        c->recv_next_col = 0;
        chc_in_reset(&c->in);
        return CHC_OK;
    }

    rc = chc__recv_block_compressed(c, opts, &out->block, err);
    if (rc == CHC_WOULD_BLOCK) return rc;
    c->recv_in_block = 0;
    if (rc != CHC_OK) return rc;
    return CHC_OK;

control_done:
    if (ioless) chc_in_reset(&c->in);
    return CHC_OK;

maybe_rewind:
    if (ioless && rc == CHC_WOULD_BLOCK) chc__in_rewind(&c->in);
    return rc;
}

int
chc_client_recv_packet(chc_client *c, chc_packet *out, chc_err *err)
{
    return chc__recv_packet_resumable(c, out, err);
}

#endif /* CHC_IMPLEMENTATION */

#ifdef __cplusplus
}
#endif

#endif /* CLICKHOUSE_CLIENT_H */
