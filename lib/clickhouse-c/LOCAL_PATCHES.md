# Local compatibility patch

This directory contains ClickHouse/clickhouse-c commit
`084498e63a8e814d8f8ab9a32b923d7d6c3c7d9f`, including its license,
documentation, examples, and tests. The migration retains the revision examined
in the original analysis. No newer upstream implementation is mixed into it.

Apply `patches/clickhouse-c/0001-php-compatibility.patch` with `git apply` from
a checkout of that commit to reproduce the local changes. Paths in the patch
are relative to this directory. Reapply or explicitly retire the patch when
updating upstream.

## 0001-php-compatibility.patch

The patch adds the protocol behavior required by the existing PHP API:

- `chc_client_opts.protocol_revision` selects an advertised revision no greater
  than 54465. Zero preserves upstream's default. The PHP adapter uses 54459,
  matching its previous client. `allow_legacy_server` enables the Hello,
  ClientInfo, settings, parameter, progress, and block feature gates needed for
  older peers. It does not change the default 54462 minimum.
- `skip_init_ping` allows the adapter to preserve the constructor's existing
  handshake. Zero retains upstream's Ping probe. Both synchronous and async
  clients apply these options.
- The Hello identity and credentials, settings, and parameters accept explicit
  lengths when `strings_have_lengths` is true. Default callers retain their
  NUL-terminated string behavior.
- `chc_server_info.server_revision` preserves the server's actual revision.
  `revision` remains the negotiated revision. The client owns the `full_name`,
  `full_timezone`, and `full_display_name` strings and their length fields;
  callers borrow them until close, or until a timezone update replaces that
  field. Existing fixed arrays remain truncated compatibility views. The
  default 4096-byte Hello string limit remains available. The PHP translation
  unit sets `CHC_MAX_HELLO_STRING_SIZE` and `CHC_MAX_PROTOCOL_STRING_SIZE` to
  `0x00FFFFFF`, preserving its previous metadata limit. Metadata strings grow
  only as bytes arrive, preserving the previous client's allocation behavior.
- `defer_data_terminator` and `chc_client_send_data_named()` allow callers to
  send external tables before the final unnamed empty Data block. Ordinary
  queries retain automatic finalization.
- Block writers use the type parser's complete stored name. The former
  256-byte staging buffer is removed. Its rejection test now checks a valid
  long type's complete roundtrip.
- `has_max_compression_chunk_size` selects a runtime compression chunk limit.
  When present, zero means no configured cap; wire and codec size limits still
  apply. When absent, upstream's 65535-byte default remains. Size calculations
  are checked before frame allocation and wire narrowing.
- `chc_column_destroy()` exposes the existing owned-column destructor. Columns
  borrowed from blocks remain owned by their block; types remain separately
  owned. Empty initializers in implementation headers use C11 syntax.
- Fixed, String, nullable, array-offset, LowCardinality-index, and compressed
  input buffers grow in steps as preceding bytes arrive. Ioless readers defer
  allocation until the submitted payload is available. String buffers retain
  geometric capacity across rows. Valid empty compressed frames are consumed
  before reading the next data-bearing frame. Block metadata arrays grow as
  columns arrive, and cleanup visits only allocated slots. Type-child and Tuple
  field arrays grow geometrically instead of reallocating for every element.
  In PHP compatibility mode, parsed nodes borrow length-delimited names from
  one immutable, reference-counted spelling buffer. Partial parsing and
  SimpleAggregateFunction unwrapping preserve ownership without copying every
  subtree's spelling. Generic upstream mode retains separate terminated names.
- `CHC_PHP_COMPAT=1` selects the previous column factory's supported types,
  required temporal parameters, diagnostics, and 1 GiB decompressed-frame
  limit. It validates offsets before consuming their nested values and checks
  dictionary indexes before returning a block. SimpleAggregateFunction with a
  supported terminal argument becomes that argument's type, matching the old
  factory. LowCardinality dictionaries retain the previous String/FixedString
  restriction beneath Nullable wrappers. Incoming DateTime timezones and named
  Tuple fields remain intact. Empty Tuple factory errors retain their original
  diagnostic, including inside LowCardinality dictionaries.
  Empty DateTime, DateTime64, Time64, and fixed-width Decimal argument lists
  retain the previous zero-precision defaults and canonical wire names. Required
  argument failures are deferred until the factory constructs the affected node,
  preserving both their diagnostics and outer-type rejection precedence.
  Outgoing PHP-specific normalization belongs to the binding's column builder.
- In PHP compatibility mode, ClientInfo uses the same owned identity bytes as
  Hello. `CHC_ZSTD_COMPRESSION_LEVEL=1` restores the previous encoder level;
  without an override, the upstream ZSTD default remains available.
- Uncompressed PHP Data packets coalesce small writes in an 8 KiB stack buffer,
  matching the previous output buffer. Large payloads can write directly to the
  transport. A successful send flushes all bytes before returning; errors retain
  no pending buffer across calls. Compression framing and chunk limits are
  unchanged.
- PHP compatibility mode rejects Totals and Extremes packets with the previous
  native errors before reading their bodies. Generic upstream clients retain
  support for both packet types.

Revision constants follow the existing vendored C++ client's feature gates and
[ClickHouse's protocol definitions](https://github.com/ClickHouse/ClickHouse/blob/master/src/Core/ProtocolDefines.h).
The default decoder limits remain available; PHP overrides preserve its prior
limits and column validation remains mandatory.

`test/test_php_compat.c` checks compatibility against wire fixtures, including
legacy revisions, full metadata, explicit credential lengths, and no implicit
Ping. These are unit tests, not live interoperability evidence.
`test/test_client_tcp.c` adds live external-table queries with a long Enum16 type
at revision 54459, using uncompressed, LZ4, and ZSTD connections and both capped
and uncapped chunks. `test/test_php_live.c` repeats the compatible operations
with `CHC_PHP_COMPAT=1` against a separate disposable server. The standard
`bash test.sh` runner includes these tests.
