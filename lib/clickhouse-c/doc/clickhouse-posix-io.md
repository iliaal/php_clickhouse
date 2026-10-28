# clickhouse-posix-io.h

[`chc_io`](clickhouse.md#io) backend over blocking `read(2)` / `write(2)`.
EINTR is looped internally. `write` loops until all bytes are flushed;
`read` may short-read & lets `chc_block_read` / the TCP loop's buffered
input layer handle assembly.

Exactly one TU defines `CHC_IMPLEMENTATION` before including. Depends on
[clickhouse.h](clickhouse.md).

```c
typedef struct chc_posix_io {
    int      fd;
    bool   (*check_cancel)(void *ud);
    void    *cancel_ud;
    int64_t  deadline_us;   /* absolute CLOCK_MONOTONIC us; 0 disables */
} chc_posix_io;

void chc_posix_io_init(chc_posix_io *state, chc_io *out_io, int fd,
                       bool (*check_cancel)(void *), void *cancel_ud);

void chc_posix_io_set_deadline(chc_posix_io *state, int64_t deadline_us);
```

`state` is caller-owned (typically stack); the filled `chc_io` references
it via `ud`, so `state` must outlive `out_io`. Library never closes `fd`.

`check_cancel` is optional. When non-NULL, it's wrapped into the `chc_io`
`check_cancel` slot — polled between reads, non-zero aborts the current
operation with `CHC_ERR_CANCELLED`. PG callers wire this to
`QueryCancelPending || ProcDiePending`.

Caller is responsible for `socket()`, `connect()`, `setsockopt`, timeouts,
non-blocking mode, etc. — same posture as `clickhouse-openssl.h`. For
non-blocking sockets, callers wanting `WaitLatchOrSocket`/`epoll` should
roll their own `chc_io` rather than use this header.

## Read deadline

`chc_posix_io_set_deadline` bounds later reads by an absolute
`CLOCK_MONOTONIC` microsecond timestamp; `0` (the init default) disables it.
Before each blocking `read(2)` the backend `poll(2)`s the fd until readable
or the deadline passes; on expiry the read fails with `CHC_ERR_IO` & a
`read timeout` message. `EINTR` during the wait is looped; `write` is not
bounded.

Deadline is absolute & shared across reads until changed. Set before each
logical operation (e.g. a packet read).
