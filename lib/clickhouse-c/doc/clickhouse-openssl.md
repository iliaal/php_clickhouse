# clickhouse-openssl.h

[`chc_io`](clickhouse.md#io) backend over OpenSSL `SSL_read` / `SSL_write`.

Link `-lssl -lcrypto`. Exactly one TU defines `CHC_IMPLEMENTATION` before
including. Depends on [clickhouse.h](clickhouse.md). The header forward-
declares `SSL` so it's includable without dragging in `<openssl/ssl.h>`;
the implementation TU pulls OpenSSL in.

```c
typedef struct ssl_st SSL;

typedef struct chc_openssl_io {
    SSL     *ssl;
    bool   (*check_cancel)(void *ud);
    void    *cancel_ud;
    int64_t  deadline_us;   /* absolute CLOCK_MONOTONIC us; 0 disables */
} chc_openssl_io;

void chc_openssl_io_init(chc_openssl_io *state, chc_io *out_io, SSL *ssl,
                         bool (*check_cancel)(void *), void *cancel_ud);

void chc_openssl_io_set_deadline(chc_openssl_io *state, int64_t deadline_us);
```

`state` is caller-owned (typically stack); the filled `chc_io` keeps a
pointer into it. Library never frees `ssl`, never calls `SSL_shutdown`.

Caller-side concerns mirror [clickhouse-posix-io.md](clickhouse-posix-io.md):
`SSL_CTX` setup, certificate verification, SNI, BIO wiring, and
`SSL_connect`/`SSL_accept` all run before init. By the time `chc_io.read`
fires, the handshake must be complete.

## Loop semantics

* `SSL_ERROR_WANT_READ` / `SSL_ERROR_WANT_WRITE` retry in place. Backend
  assumes a blocking BIO; with a non-blocking BIO these become a busy
  loop & the caller should roll their own `chc_io`.
* `SSL_ERROR_SYSCALL` with `errno == EINTR` retries.
* `SSL_ERROR_ZERO_RETURN` (clean TLS shutdown) is reported as `read`
  returning 0 bytes — same shape as POSIX EOF.
* Cancel is polled at the top of every read & every write chunk; non-zero
  returns `CHC_ERR_CANCELLED`.

Errors emerge as `CHC_ERR_IO` with an `op: what (detail)` message — `op`
is `SSL_read` or `SSL_write`, `what` summarizes the `SSL_get_error` code,
detail is `ERR_peek_last_error`'s human-readable text when present.

## Read deadline

`chc_openssl_io_set_deadline` bounds later reads by an absolute
`CLOCK_MONOTONIC` microsecond timestamp; `0` (the init default) disables it.
When no decrypted bytes are already buffered (`SSL_pending() == 0`) the
backend `poll(2)`s the underlying `SSL_get_fd` until readable or the
deadline; on expiry the read fails with `CHC_ERR_IO` / `read timeout`.
`EINTR` loops.

Because the wait is on the socket, `SSL_read` may still block briefly after
poll returns while it assembles the remainder of a TLS record (bounded by
the max record size). As with the POSIX backend the deadline is absolute and
shared across reads; set it before each logical operation.
