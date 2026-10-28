/*
 * clickhouse-posix-io.h -- blocking POSIX fd backend for chc_io.
 *
 * Exactly one TU must `#define CHC_IMPLEMENTATION` before including;
 * other TUs include for declarations only. Depends on clickhouse.h
 * (for chc_io / chc_err declarations).
 */

#ifndef CLICKHOUSE_POSIX_IO_H
#define CLICKHOUSE_POSIX_IO_H

#include <stdbool.h>

#include "clickhouse.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct chc_posix_io {
    int   fd;
    bool (*check_cancel)(void *ud);
    void *cancel_ud;
    /* Monotonic-us deadline applied to each blocking read. 0 disables. */
    int64_t deadline_us;
} chc_posix_io;

void chc_posix_io_init(chc_posix_io *state, chc_io *out_io, int fd,
                       bool (*check_cancel)(void *), void *cancel_ud);

/* Bound subsequent reads by absolute CLOCK_MONOTONIC microseconds; 0 = none. */
void chc_posix_io_set_deadline(chc_posix_io *state, int64_t deadline_us);

#ifdef CHC_IMPLEMENTATION

#include <errno.h>
#include <limits.h>
#include <poll.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static int64_t
chc__posix_now_us(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t) ts.tv_sec * 1000000 + ts.tv_nsec / 1000;
}

static int
chc__posix_wait_readable(int fd, int64_t deadline_us, chc_err *err)
{
    if (deadline_us == 0) return CHC_OK;
    for (;;) {
        int64_t now = chc__posix_now_us();
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
chc__posix_read(void *ud, void *buf, size_t len, size_t *out_n, chc_err *err)
{
    chc_posix_io *s = ud;
    for (;;) {
        int rc = chc__posix_wait_readable(s->fd, s->deadline_us, err);
        if (rc != CHC_OK) return rc;
        ssize_t n = read(s->fd, buf, len);
        if (n >= 0) { *out_n = (size_t) n; return CHC_OK; }
        if (errno == EINTR) continue;
        return chc__err_set(err, CHC_ERR_IO, "read(fd=%d): %s",
                            s->fd, strerror(errno));
    }
}

static int
chc__posix_write(void *ud, const void *buf, size_t len, chc_err *err)
{
    chc_posix_io *s = ud;
    const unsigned char *p = buf;
    while (len) {
        ssize_t n = write(s->fd, p, len);
        if (n > 0) { p += n; len -= (size_t) n; continue; }
        if (n < 0 && errno == EINTR) continue;
        return chc__err_set(err, CHC_ERR_IO, "write(fd=%d): %s",
                            s->fd, strerror(errno));
    }
    return CHC_OK;
}

static int
chc__posix_cancel(void *ud)
{
    chc_posix_io *s = ud;
    return s->check_cancel && s->check_cancel(s->cancel_ud);
}

void
chc_posix_io_init(chc_posix_io *state, chc_io *out_io, int fd,
                  bool (*check_cancel)(void *), void *cancel_ud)
{
    *state = (chc_posix_io) {
        .fd = fd, .check_cancel = check_cancel, .cancel_ud = cancel_ud,
    };
    *out_io = (chc_io) {
        .ud = state, .read = chc__posix_read, .write = chc__posix_write,
        .check_cancel = check_cancel ? chc__posix_cancel : NULL,
    };
}

void
chc_posix_io_set_deadline(chc_posix_io *state, int64_t deadline_us)
{
    state->deadline_us = deadline_us;
}

#endif /* CHC_IMPLEMENTATION */

#ifdef __cplusplus
}
#endif

#endif /* CLICKHOUSE_POSIX_IO_H */
