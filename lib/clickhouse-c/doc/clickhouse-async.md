# clickhouse-async.h

Ioless variant of client. Never touches a socket: caller submits bytes and
drives event loop (epoll, io_uring, `WaitLatchOrSocket`) however it likes.
Same packet loop as [clickhouse-client.h](clickhouse-client.md)
— Hello / Query / Data / EOS / Exception / Progress / Pong, with compression —
built on the ioless [buffered reader](clickhouse.md#buffered-reader).

## Submission

The client never reads or writes a socket. The caller moves bytes both ways:

* **inbound** — socket bytes the caller received go in via `chc_async_submit`.
* **outbound** — bytes the client wants to send accumulate in an internal
  buffer; the caller drains them with `chc_async_pending_out` /
  `chc_async_consume_out` and writes them to the socket.

```c
int  chc_async_submit      (chc_async_client *c, const void *buf, size_t len,
                            chc_err *err);
void chc_async_pending_out (chc_async_client *c, const uint8_t **buf, size_t *len);
void chc_async_consume_out (chc_async_client *c, size_t n);
```

`pending_out` hands back a pointer/length into the pending bytes (zero length
when nothing is queued); after the socket accepts `n` of them, call
`consume_out(n)`. A partial write is fine — report only what the socket took.
When fully drained the buffer resets to length 0, keeping its allocation.

No write backpressure: sends never block, never return `CHC_WOULD_BLOCK`, and
grow the out buffer unbounded (only OOM fails). Outbound flow control is the
caller's job — watch `pending_out`'s length, stop issuing sends when it grows
too large, resume after `consume_out` drains it.

## Lifecycle & opts

```c
typedef struct chc_async_client chc_async_client;

int  chc_async_client_init(chc_async_client **out, const chc_client_opts *opts,
                           const chc_alloc *al, chc_err *err);
void chc_async_client_free(chc_async_client *c);
const chc_server_info *chc_async_server_info(const chc_async_client *c);
```

`opts` is the same [`chc_client_opts`](clickhouse-client.md#opts--handshake) as
the blocking client (`NULL` for defaults). Unlike `chc_client_init`, init does
**no** I/O — it cannot block and only OOM fails; the handshake runs later via
`chc_async_handshake`. The Hello string fields (`client_name`, `database`,
`user`, `password`) are copied internally, so `opts` and its strings need not
outlive the call.

`opts->codec` carries compression: pass a filled `codec` plus
`compression = CHC_COMP_LZ4`/`CHC_COMP_ZSTD` to (de)compress Data packets. See
[clickhouse-compression.md](clickhouse-compression.md).

`chc_async_server_info` returns the negotiated server info (revision is
`min(client, server)`); meaningful only after the handshake completes, and
left untouched by a partial Hello. Server revision floor & Hello limits match
[clickhouse-client.md](clickhouse-client.md#opts--handshake).

## Drive

```c
int  chc_async_handshake    (chc_async_client *c, chc_exception **exc,
                             chc_err *err);
int  chc_async_send_query   (chc_async_client *c,
                             const char *sql,      size_t sql_len,
                             const char *query_id, size_t query_id_len,
                             chc_err *err);
int  chc_async_send_data    (chc_async_client *c, const chc_block_builder *bb,
                             chc_err *err);
int  chc_async_send_data_end(chc_async_client *c, chc_err *err);
int  chc_async_recv_packet  (chc_async_client *c, chc_packet *out, chc_err *err);
void chc_async_packet_clear (chc_async_client *c, chc_packet *p);
```

Every call returns `CHC_OK`, `CHC_WOULD_BLOCK`, or a hard `CHC_ERR_*`.
`CHC_WOULD_BLOCK` from `handshake` / `recv_packet` means the input buffer
drained mid-parse: submit more inbound bytes and call again. Parser state is
preserved across the retry — a Data block streamed over many reads resumes at
the in-progress column rather than re-parsing. Compressed Data resumes at frame
granularity: at most one frame is re-decompressed and one column re-parsed.

* `chc_async_handshake` runs the Hello / Pong exchange as a resumable state
  machine. Call it after init, re-driving on `CHC_WOULD_BLOCK`, until `CHC_OK`.
  Handshake rejection returns `CHC_ERR_SERVER` and leaves `err->msg` empty.
  When `exc` is not NULL, caller owns `*exc` and frees it with
  [`chc_exception_free`](clickhouse-client.md#receive). Other results leave
  `*exc` unchanged.
* `send_query` / `send_data` only append to the out buffer; they never block.
  `send_data(bb)` appends a Data block; `send_data_end` appends the empty
  terminating block that ends an INSERT stream. Semantics mirror
  [the blocking client](clickhouse-client.md#send).
* `recv_packet` decodes one packet. Server exceptions arrive as
  `CHC_PKT_EXCEPTION` with `CHC_OK` (not `CHC_ERR_SERVER`); only transport-level
  failures return non-OK. `chc_async_packet_clear` frees `block` / `exception`
  via the client's allocator, exactly like `chc_packet_clear`. Packet kinds and
  `chc_packet` layout are [in the client doc](clickhouse-client.md#receive).

## Driving loop

Sketch of a single-threaded event loop (see `test/test_async_uring.c` for a
working liburing driver):

```c
chc_async_client *c;
chc_async_client_init(&c, &opts, &al, &err);

/* handshake */
chc_exception *exc = NULL;
for (;;) {
    int rc = chc_async_handshake(c, &exc, &err);
    if (rc == CHC_OK) break;
    if (rc == CHC_ERR_SERVER) {
        fprintf(stderr, "handshake: %s\n", exc->display_text);
        chc_exception_free(exc, &al);
        goto fail;
    }
    if (rc != CHC_WOULD_BLOCK) goto fail;
    pump(c);              /* drain pending_out -> socket; recv -> submit */
}

chc_async_send_query(c, sql, sql_len, "", 0, &err);

for (;;) {
    chc_packet pkt = {};
    int rc = chc_async_recv_packet(c, &pkt, &err);
    if (rc == CHC_WOULD_BLOCK) { pump(c); continue; }
    if (rc != CHC_OK) goto fail;

    if (pkt.kind == CHC_PKT_END_OF_STREAM) { chc_async_packet_clear(c, &pkt); break; }
    if (pkt.kind == CHC_PKT_DATA) { /* consume pkt.block */ }
    chc_async_packet_clear(c, &pkt);
}
```

`pump` writes `chc_async_pending_out` bytes to the socket (then
`chc_async_consume_out`) and feeds received bytes into `chc_async_submit` — the
shape of that loop is whatever the caller's reactor dictates.

## Threading

Single-threaded, like `chc_client`. There are no `chc_io` callbacks at all; the
caller's event loop is the only thing that touches the socket.
