/*
 * clickhouse-openssl.h -- chc_io backend over OpenSSL SSL_read/SSL_write.
 *
 * Exactly one TU must `#define CHC_IMPLEMENTATION` before including;
 * other TUs include for declarations only. Depends on clickhouse.h.
 *
 * The caller-supplied SSL* must already be connected: SSL_CTX setup,
 * certificate verification, SNI, BIO wiring and SSL_connect/SSL_accept
 * are caller-side concerns. Mirrors the posture of clickhouse-posix-io.h,
 * where socket()/connect()/option-setting belong to the caller.
 *
 * Caller links -lssl -lcrypto.
 */

#ifndef CLICKHOUSE_OPENSSL_H
#define CLICKHOUSE_OPENSSL_H

#include <stdbool.h>

#include "clickhouse.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Forward-declared so this header is includable without dragging in
 * <openssl/ssl.h>; struct tag matches OpenSSL's own typedef. C11 allows
 * redundant typedefs of the same type, so callers may include in either
 * order. */
typedef struct ssl_st SSL;

typedef struct chc_openssl_io {
    SSL  *ssl;
    bool (*check_cancel)(void *ud);
    void *cancel_ud;
    /* Monotonic-us deadline applied to each blocking read. 0 disables. */
    int64_t deadline_us;
} chc_openssl_io;

void chc_openssl_io_init(chc_openssl_io *state, chc_io *out_io, SSL *ssl,
                         bool (*check_cancel)(void *), void *cancel_ud);

/* Bound subsequent reads by absolute CLOCK_MONOTONIC microseconds; 0 = none. */
void chc_openssl_io_set_deadline(chc_openssl_io *state, int64_t deadline_us);

#ifdef CHC_IMPLEMENTATION

#include <errno.h>
#include <limits.h>
#include <poll.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include <openssl/err.h>
#include <openssl/ssl.h>

static int
chc__openssl_fail(chc_err *err, int ret, int ssl_err, const char *op)
{
    const char *what = "I/O error";
    char detail[160] = "";
    switch (ssl_err) {
    case SSL_ERROR_ZERO_RETURN:
        what = "peer closed TLS connection";
        break;
    case SSL_ERROR_SYSCALL:
        what = (ret == 0) ? "EOF before close_notify"
                          : (errno ? strerror(errno) : "syscall failed");
        break;
    case SSL_ERROR_SSL:
        what = "TLS protocol error";
        break;
    default:
        break;
    }
    unsigned long e = ERR_peek_last_error();
    if (e) {
        char buf[128];
        ERR_error_string_n(e, buf, sizeof buf);
        snprintf(detail, sizeof detail, " (%s)", buf);
    }
    return chc__err_set(err, CHC_ERR_IO, "%s: %s%s", op, what, detail);
}

static int64_t
chc__openssl_now_us(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t) ts.tv_sec * 1000000 + ts.tv_nsec / 1000;
}

/* Poll underlying fd until readable (or deadline). SSL is on a blocking
 * fd, so we only poll when no record bytes are already decrypted; once
 * data is available SSL_read can still block briefly waiting for the
 * remainder of a record, but record size is bounded so slip is small. */
static int
chc__openssl_wait_readable(SSL *ssl, int64_t deadline_us, chc_err *err)
{
    if (deadline_us == 0) return CHC_OK;
    if (SSL_pending(ssl) > 0) return CHC_OK;
    int fd = SSL_get_fd(ssl);
    if (fd < 0)
        return chc__err_set(err, CHC_ERR_IO, "SSL_get_fd: no fd");
    for (;;) {
        int64_t now = chc__openssl_now_us();
        if (now >= deadline_us)
            return chc__err_set(err, CHC_ERR_IO, "read timeout");
        int64_t rem_ms = (deadline_us - now + 999) / 1000;
        if (rem_ms > INT_MAX) rem_ms = INT_MAX;
        struct pollfd pfd = { .fd = fd, .events = POLLIN, .revents = 0 };
        int pr = poll(&pfd, 1, (int) rem_ms);
        if (pr > 0) return CHC_OK;
        if (pr == 0)
            return chc__err_set(err, CHC_ERR_IO, "read timeout");
        if (errno == EINTR) continue;
        return chc__err_set(err, CHC_ERR_IO, "poll(fd=%d): %s",
                            fd, strerror(errno));
    }
}

static int
chc__openssl_read(void *ud, void *buf, size_t len, size_t *out_n, chc_err *err)
{
    chc_openssl_io *s = ud;
    if (len == 0) { *out_n = 0; return CHC_OK; }
    int want = (len > (size_t) INT_MAX) ? INT_MAX : (int) len;
    for (;;) {
        if (s->check_cancel && s->check_cancel(s->cancel_ud))
            return chc__err_set(err, CHC_ERR_CANCELLED, "canceled");
        int rc = chc__openssl_wait_readable(s->ssl, s->deadline_us, err);
        if (rc != CHC_OK) return rc;
        ERR_clear_error();
        errno = 0;
        int n = SSL_read(s->ssl, buf, want);
        if (n > 0) { *out_n = (size_t) n; return CHC_OK; }
        int e = SSL_get_error(s->ssl, n);
        if (e == SSL_ERROR_ZERO_RETURN) { *out_n = 0; return CHC_OK; }
        if (e == SSL_ERROR_WANT_READ || e == SSL_ERROR_WANT_WRITE) continue;
        if (e == SSL_ERROR_SYSCALL && errno == EINTR) continue;
        return chc__openssl_fail(err, n, e, "SSL_read");
    }
}

static int
chc__openssl_write(void *ud, const void *buf, size_t len, chc_err *err)
{
    chc_openssl_io *s = ud;
    const unsigned char *p = buf;
    while (len) {
        if (s->check_cancel && s->check_cancel(s->cancel_ud))
            return chc__err_set(err, CHC_ERR_CANCELLED, "canceled");
        ERR_clear_error();
        errno = 0;
        int chunk = (len > (size_t) INT_MAX) ? INT_MAX : (int) len;
        int n = SSL_write(s->ssl, p, chunk);
        if (n > 0) { p += n; len -= (size_t) n; continue; }
        int e = SSL_get_error(s->ssl, n);
        if (e == SSL_ERROR_WANT_READ || e == SSL_ERROR_WANT_WRITE) continue;
        if (e == SSL_ERROR_SYSCALL && errno == EINTR) continue;
        return chc__openssl_fail(err, n, e, "SSL_write");
    }
    return CHC_OK;
}

static int
chc__openssl_cancel(void *ud)
{
    chc_openssl_io *s = ud;
    return s->check_cancel && s->check_cancel(s->cancel_ud);
}

void
chc_openssl_io_init(chc_openssl_io *state, chc_io *out_io, SSL *ssl,
                    bool (*check_cancel)(void *), void *cancel_ud)
{
    *state = (chc_openssl_io) {
        .ssl = ssl, .check_cancel = check_cancel, .cancel_ud = cancel_ud,
    };
    *out_io = (chc_io) {
        .ud = state, .read = chc__openssl_read, .write = chc__openssl_write,
        .check_cancel = check_cancel ? chc__openssl_cancel : NULL,
    };
}

void
chc_openssl_io_set_deadline(chc_openssl_io *state, int64_t deadline_us)
{
    state->deadline_us = deadline_us;
}

#endif /* CHC_IMPLEMENTATION */

#ifdef __cplusplus
}
#endif

#endif /* CLICKHOUSE_OPENSSL_H */
