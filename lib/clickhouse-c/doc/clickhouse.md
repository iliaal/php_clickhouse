# clickhouse.h

Core header. Types, errors, allocator, I/O vtable, type-name parser & AST,
varint codec, block reader/writer, column accessors. No I/O backend, no TCP
loop, no compression — sibling headers cover those.

stb-style single-header. Exactly one TU defines `CHC_IMPLEMENTATION`; others
include for declarations only.

## Errors

```c
enum {
    CHC_OK            = 0,
    CHC_ERR_IO        = 1,
    CHC_ERR_EOF       = 2,
    CHC_ERR_PROTOCOL  = 3,
    CHC_ERR_TYPE      = 4,
    CHC_ERR_OOM       = 5,
    CHC_ERR_CANCELLED = 6,
    CHC_ERR_SERVER    = 7,
    CHC_ERR_USAGE     = 8,
    CHC_WOULD_BLOCK   = 9,
};

typedef struct chc_err {
    char msg[CHC_ERR_MSG_LEN];       /* default 256, NUL-terminated, snprintf-truncated */
} chc_err;

static inline void chc_err_reset(chc_err *e);
```

Caller-stack-allocated; library never heap-allocates an error. Override
`CHC_ERR_MSG_LEN` before including to resize `msg`.

`chc_err` is for errors. Server exceptions use
[`chc_exception`](clickhouse-client.md#receive).

`CHC_WOULD_BLOCK` is not a failure: an ioless [buffered reader](#buffered-reader)
returns it when a parse runs past the submitted bytes. Submit more and retry.
The blocking io-backed path never produces it.

## Allocator

```c
typedef struct chc_alloc {
    void *ud;
    void *(*alloc)  (void *ud, size_t bytes);
    void *(*realloc)(void *ud, void *p, size_t old_bytes, size_t new_bytes);
    void  (*free)   (void *ud, void *p, size_t bytes);
} chc_alloc;
```

`alloc` may return NULL, but arenas can safely `longjmp`.
`old_bytes` lets a `new`/`delete`-backed allocator emulate `realloc` via
copy-then-free.

stdlib.h backend, gated on a define so it doesn't drag in `<stdlib.h>` for
freestanding builds:

```c
#define CHC_PROVIDE_STDLIB_ALLOC
#include "clickhouse.h"
chc_alloc al = chc_alloc_stdlib();
```

## I/O

```c
typedef struct chc_io {
    void *ud;
    int (*read)        (void *ud, void *buf, size_t len, size_t *out_n, chc_err *err);
    int (*write)       (void *ud, const void *buf, size_t len, chc_err *err);
    int (*check_cancel)(void *ud);   /* optional, NULL = never */
} chc_io;
```

`read` & `write` return `CHC_OK` (0) on success or a nonzero `CHC_ERR_*` on
hard error (also recorded in `err`); a `read` returning `CHC_OK` with
`*out_n == 0` signals clean EOF. `read` may short-read; library loops.
`EAGAIN`/`EINTR` are the callback's problem. `check_cancel` is polled between
reads; non-zero aborts with `CHC_ERR_CANCELLED`. Ready-made backends:
[clickhouse-posix-io.md](clickhouse-posix-io.md),
[clickhouse-openssl.md](clickhouse-openssl.md).

## Buffered reader

```c
typedef struct chc_in chc_in;

int    chc_in_init       (chc_in *in, chc_io *io, const chc_alloc *al,
                          size_t cap, chc_err *err);
int    chc_in_init_ioless(chc_in *in, const chc_alloc *al);
int    chc_in_submit     (chc_in *in, const void *buf, size_t len, chc_err *err);
size_t chc_in_available  (const chc_in *in);   /* unconsumed bytes */
void   chc_in_reset      (chc_in *in);         /* drop consumed, compact */
void   chc_in_free       (chc_in *in);
```

`chc_in` is the staging buffer the block & packet parsers pull from. The block
reader and [client](clickhouse-client.md) own one internally, so most callers
never touch it; it is public for ioless drivers that submit bytes. Two modes:

* **io-backed** — `chc_in_init` with a `chc_io`. The reader refills by calling
  `io->read` on underrun, so a parse blocks in that callback until bytes arrive.
  `cap` is the refill buffer size; `0` selects `CHC_READ_BUFFER` (8 KiB,
  overridable via `#define`). `chc_in_submit` is a `CHC_ERR_USAGE` here.
* **ioless** — `chc_in_init_ioless`, no `chc_io`. The caller pushes received
  bytes with `chc_in_submit`; the buffer grows on demand (doubling). A parse
  that runs past the submitted bytes returns `CHC_WOULD_BLOCK` instead of
  blocking, leaving in-progress parser state intact so the same call can be
  retried once more bytes are submitted.

`chc_in_reset` drops the consumed prefix and compacts, keeping the unconsumed
tail; call it only at a packet boundary, never mid-parse. `chc_in_available`
reports bytes submitted but not yet consumed. `chc_in_free` releases the
buffer.

Mode is chosen at runtime per `in` (presence of a `chc_io`). To pin it at
compile time and let the dead branch fold away, define exactly one of
`CHC_NO_ASYNC` (io-backed only; drops `chc_in_init_ioless`) or `CHC_NO_SYNC`
(ioless only; drops `chc_in_init`) before the implementation include. Defining
both is a compile error; `clickhouse-async.h` requires the ioless reader and
rejects `CHC_NO_ASYNC`.

The ioless mode is the foundation of [clickhouse-async.h](clickhouse-async.md):
the parser checkpoints at packet boundaries and rewinds on `CHC_WOULD_BLOCK`, so
a Data block streamed over many reads resumes rather than re-parsing. That
checkpoint/rewind machinery is internal; the public surface is the six calls
above.

## Type AST

```c
typedef enum chc_kind {
    CHC_VOID = 0,
    CHC_INT8, CHC_INT16, CHC_INT32, CHC_INT64, CHC_INT128, CHC_INT256,
    CHC_UINT8, CHC_UINT16, CHC_UINT32, CHC_UINT64, CHC_UINT128, CHC_UINT256,
    CHC_FLOAT32, CHC_FLOAT64, CHC_BFLOAT16,
    CHC_BOOL,
    CHC_DATE, CHC_DATE32,
    CHC_DATETIME, CHC_DATETIME64,
    CHC_TIME, CHC_TIME64,
    CHC_STRING, CHC_FIXED_STRING,
    CHC_DECIMAL32, CHC_DECIMAL64, CHC_DECIMAL128, CHC_DECIMAL256,
    CHC_UUID, CHC_IPV4, CHC_IPV6,
    CHC_ENUM8, CHC_ENUM16,
    CHC_NULLABLE, CHC_ARRAY, CHC_TUPLE, CHC_MAP, CHC_NESTED,
    CHC_LOW_CARDINALITY,
    CHC_INTERVAL,
    CHC_POINT, CHC_RING, CHC_POLYGON, CHC_MULTI_POLYGON,
    CHC_VARIANT, CHC_DYNAMIC, CHC_JSON, CHC_OBJECT,
    CHC_AGGREGATE_FUNCTION, CHC_SIMPLE_AGGREGATE_FUNCTION,
    CHC_QBIT,
    CHC_NOTHING,
    CHC_KIND_COUNT
} chc_kind;

typedef struct chc_type chc_type;

int  chc_type_parse  (const char *name, size_t name_len,
                      const chc_alloc *al, chc_type **out, chc_err *err);
void chc_type_destroy(chc_type *t, const chc_alloc *al);
```

Parses printable type names like `Array(Nullable(DateTime64(3, 'UTC')))`
into an AST. Block reader uses this internally; the AST also reaches the
caller via `chc_block_column_type`.

Accessors:

```c
chc_kind         chc_type_kind        (const chc_type *t);
size_t           chc_type_n_children  (const chc_type *t);
const chc_type  *chc_type_child       (const chc_type *t, size_t i);

int              chc_type_fixed_size        (const chc_type *t);
int              chc_type_decimal_precision (const chc_type *t);
int              chc_type_decimal_scale     (const chc_type *t);
int              chc_type_datetime64_scale  (const chc_type *t);
chc_interval_unit chc_type_interval_unit    (const chc_type *t);
size_t           chc_type_qbit_dimension    (const chc_type *t);
size_t           chc_type_qbit_element_size (const chc_type *t);
const char      *chc_type_timezone          (const chc_type *t, size_t *out_len);
const char      *chc_type_name              (const chc_type *t, size_t *out_len);

size_t           chc_type_enum_count        (const chc_type *t);
void             chc_type_enum_at           (const chc_type *t, size_t i,
                                             const char **name, size_t *name_len,
                                             int64_t *value);

const char      *chc_type_tuple_field_name  (const chc_type *t, size_t i,
                                             size_t *out_len);
const char      *chc_type_agg_function      (const chc_type *t,
                                             size_t *out_len);

size_t           chc_type_format            (const chc_type *t,
                                             char *buf, size_t buf_len);
```

`chc_type_format` is snprintf-style: pass `NULL`/0 to query length, then
allocate & call again.

`chc_type_tuple_field_name` covers `Nested` as well as `Tuple`; both label
their children, `Nested(a UInt32, b String)` reading back `a` & `b`.

A server only sends `Nested` whole under `flatten_nested=0`; the default
flattens it into one `name.field Array(T)` column per field, which decode as
plain Arrays.

`Decimal*` precision is implicit in the kind: 9 / 18 / 38 / 76 digits for
32/64/128/256-bit. `chc_type_decimal_scale` returns the explicit `S`.

`DateTime64` timezone is metadata only; ticks on the wire are UTC.

Every `Interval*` type maps to `CHC_INTERVAL`; `chc_type_interval_unit` tells
which unit, from `CHC_INTERVAL_NANOSECOND` to `CHC_INTERVAL_YEAR`.

`QBit(T, N)` is a bit-sliced vector type. `chc_type_qbit_dimension` is `N`;
`chc_type_qbit_element_size` is the element width in bits (16 / 32 / 64 for
`BFloat16` / `Float32` / `Float64`), and the element type itself is
`chc_type_child(t, 0)`. On the wire QBit serializes as
`Tuple(FixedString(ceil(N/8)) × element_size)` — one bit-plane column per
element bit, MSB plane first — so it decodes to `CHC_COL_TUPLE`; see the
column table.

## Columns

```c
typedef enum chc_col_kind {
    CHC_COL_FIXED = 1,
    CHC_COL_STRING,
    CHC_COL_NULLABLE,
    CHC_COL_ARRAY,
    CHC_COL_TUPLE,
    CHC_COL_LOW_CARDINALITY,
    CHC_COL_NOTHING,
} chc_col_kind;

typedef struct chc_column chc_column;

struct chc_column {
    chc_col_kind layout;
    size_t       n_rows;
    union {
        struct { void *data; size_t elem_size; } fixed;
        struct { uint8_t *data; uint64_t *offsets; size_t bytes; } str;
        struct { uint8_t *null_map; chc_column *inner; } nullable;
        struct { uint64_t *offsets; chc_column *values; } array;
        struct { chc_column **children; size_t arity; } tuple;
        struct {
            int key_size; void *keys; chc_column *dict; size_t dict_n;
        } lc;
    };
};

chc_col_kind chc_column_layout (const chc_column *c);
size_t       chc_column_n_rows (const chc_column *c);

int          chc_column_validate(const chc_column *c, chc_err *err);
```

`chc_column` is a public tree node. `chc_block_read` allocates and owns its
trees; writer callers can build compatible caller-owned trees or reuse reader
output directly. Access through fields or helpers below.

Caller dispatches on `chc_column_layout`:

| Layout | Accessors |
|---|---|
| `CHC_COL_FIXED` | `chc_column_fixed_data(c, &elem_size)` — `n_rows * elem_size` LE bytes |
| `CHC_COL_STRING` | `chc_column_string_data(c)`, `chc_column_string_offsets(c)` — `offsets[i]` is row i's exclusive end, host byte order; row 0 starts at 0 |
| `CHC_COL_NULLABLE` | `chc_column_null_map(c)` (1 byte per row, 1 = NULL), `chc_column_nullable_inner(c)` |
| `CHC_COL_ARRAY` | `chc_column_array_offsets(c)` (cumulative ends, host byte order), `chc_column_array_values(c)`; Map decodes as Array(Tuple(K,V)), Nested as Array(Tuple(fields)) |
| `CHC_COL_TUPLE` | `chc_column_tuple_arity(c)`, `chc_column_tuple_child(c, i)` — each child has the same row count; QBit decodes as a tuple of `element_size` FixedString bit-plane columns |
| `CHC_COL_LOW_CARDINALITY` | `chc_column_lc_key_size(c)` (1/2/4/8), `chc_column_lc_keys(c)` (host byte order), `chc_column_lc_dict(c)`; dict slot 0 is the default value; NULLs ride at slot 0 of inner Nullable |

`chc_column_validate(c, &err)` walks a decoded column tree & enforces the
cross-field invariants the server enforces on its own deserialization path —
array offsets non-decreasing, LowCardinality keys within the dictionary.
`chc_block_read` does **not** call it; a peer that forges offsets or keys can
make these accessors read past inner-column bounds. Consumers ingesting from
untrusted senders should call it on each column before traversing. Returns
`CHC_OK`, or `CHC_ERR_PROTOCOL` with a reason on the first violation; `NULL`
is treated as OK.

## Block reader

```c
typedef struct chc_block chc_block;

typedef struct chc_block_opts {
    bool   has_block_info;            /* TCP */
    bool   has_custom_serialization;  /* TCP, decodes sparse columns */
    size_t read_buffer_bytes;         /* 0 = default 8 KiB */
} chc_block_opts;

int  chc_block_read   (chc_in *in, const chc_alloc *al,
                       const chc_block_opts *opts,
                       chc_block **out, chc_err *err);
void chc_block_destroy(chc_block *b, const chc_alloc *al);

size_t            chc_block_n_rows       (const chc_block *b);
size_t            chc_block_n_columns    (const chc_block *b);
const char       *chc_block_column_name  (const chc_block *b, size_t i, size_t *out_len);
const chc_type   *chc_block_column_type  (const chc_block *b, size_t i);
const chc_column *chc_block_column       (const chc_block *b, size_t i);

bool    chc_block_is_overflows (const chc_block *b);
int32_t chc_block_bucket_num   (const chc_block *b);
```

Clean EOF at a block boundary returns `CHC_OK` with `*out == NULL`. Any
short read mid-block is `CHC_ERR_EOF`. The TCP packet loop in
[clickhouse-client.md](clickhouse-client.md) drives this with the right
`opts` flags; for `clickhouse local` over a pipe both flags are false.

`chc_block_read` takes a caller-owned `chc_in`. Init one `chc_in` (`chc_in_init`
/ `chc_in_free`) and loop `chc_block_read` over it to stream successive blocks —
bytes read past a block boundary stay buffered for the next call. A fresh
`chc_in` per block would drop that over-read tail and mis-frame the stream.

BlockInfo accessors return zero when `has_block_info == false`.

`JSON` decodes only under string serialization: set
`output_format_native_write_json_as_string=1` on the SELECT. Hints & typed
paths in the type name, as in ``JSON(max_dynamic_paths=16, `a.b` UInt32,
SKIP `a.e`)``, steer the server's dynamic storage rather than that
serialization, so the parser skips them & the type keeps no children.
Modern JSON uses an 8-byte serialization version. Legacy `Object('json')`
uses a distinct 1-byte serialization kind and decodes only STRING kind 1;
TUPLE kind 0 returns `CHC_ERR_TYPE`. Each supported row arrives as one JSON document in a
`CHC_COL_STRING` column, so string accessors apply. Prefix rides the column's
prefix sub-stream, ahead of null map / offsets, so composite nesting preserves
stream order.

`QBit(T, N)` decodes to `CHC_COL_TUPLE`: `element_size` `FixedString(ceil(N/8))`
bit-plane columns, MSB plane first. See [Type AST](#type-ast) for the dimension
& element accessors.

Remaining Tier 4 types (`Variant`, `Dynamic`, `AggregateFunction`) return
`CHC_ERR_TYPE`. Wire format still shifting in 25.x / 26.x. Consumer falls
back to `CAST(... AS String)`.

`SimpleAggregateFunction(f, T)` decodes as `T` — wire is just T's stream.
Both aggregate types parse the function name, its literal parameters &
`AggregateFunction`'s optional leading serialization version. Children are
the argument types alone, the first being the storage type;
`chc_type_agg_function` returns `f`.

## Decode limits

`chc_block_read` rejects structurally implausible input up front. These
sanity bounds mirror what ClickHouse enforces server-side; override any with
`#define` before the implementation include.

| Macro | Default | Bounds |
|---|---|---|
| `CHC_MAX_STRING_SIZE` | `1 << 30` | one String / FixedString / JSON value |
| `CHC_MAX_NUM_COLUMNS` | `1000000` | columns per block |
| `CHC_MAX_NUM_ROWS` | `1000000000000` | rows per block, & each per-column nested element count (Array / Map / Geo length, LowCardinality dictionary) |
| `CHC_MAX_FIXEDSTRING_SIZE` | `0xFFFFFF` | `FixedString(N)` width |
| `CHC_MAX_TYPE_DEPTH` | `300` | nested type-parser recursion, e.g. `Array(Array(…))` |

Over-limit input returns `CHC_ERR_PROTOCOL`, or `CHC_ERR_TYPE` for the type
bounds. Bounding the element counts & FixedString width also keeps
`n_rows * elem_size` buffer sizing from overflowing `size_t`.

## Block writer

```c
typedef struct {
    const char       *name;
    size_t            name_len;
    const chc_type   *type;
    const chc_column *col;
} chc_block_col;

typedef struct chc_block_builder {
    chc_block_col *cols;
    size_t         n_cols;
    size_t         n_rows;
} chc_block_builder;

void chc_block_builder_init(chc_block_builder *bb, chc_block_col *cols);

chc_column chc_build_fixed  (const void *data, size_t elem_size,
                             size_t n_rows);
chc_column chc_build_string (const uint64_t *offsets, const uint8_t *data,
                             size_t n_rows);
chc_column chc_build_nullable(const uint8_t *null_map, chc_column *inner);
chc_column chc_build_array  (const uint64_t *offsets, size_t n_rows,
                             chc_column *values);
chc_column chc_build_tuple  (chc_column **children, size_t arity);
chc_column chc_build_lc     (int key_size, const void *keys, size_t n_rows,
                             chc_column *dict);

void chc_block_builder_append(chc_block_builder *bb,
                              const char *name, size_t name_len,
                              const chc_type *t, const chc_column *col);

int chc_block_write_cols(chc_io *io, const chc_block_col *cols,
                         size_t n_cols, size_t n_rows,
                         const chc_block_opts *opts, chc_err *err);

int chc_block_write(chc_io *io, const chc_block_builder *bb,
                    const chc_block_opts *opts, chc_err *err);
```

Builder uses caller-provided `chc_block_col` storage and allocates nothing, so
no destroy call is needed. Given parsed `chc_type *array_nullable_u32`, init
storage, construct column tree by value, then append root:

```c
chc_block_col storage[1];
chc_block_builder bb;
chc_block_builder_init(&bb, storage);

uint32_t values[] = { 10, 0, 30 };
uint8_t nulls[] = { 0, 1, 0 };
uint64_t offsets[] = { 2, 3 };

chc_column fixed = chc_build_fixed(values, sizeof values[0], 3);
chc_column nullable = chc_build_nullable(nulls, &fixed);
chc_column array = chc_build_array(offsets, 2, &nullable);

chc_block_builder_append(&bb, "v", 1, array_nullable_u32, &array);
```

This is `Array(Nullable(UInt32))`. Compose Array, Tuple, Map, LowCardinality,
and deeper combinations from same nodes. Row counts must be consistent across
tree. For non-empty Array columns last offset equals rows in its values column

Map has no node; build it as `Array(Tuple(K, V))`. Nested has none either;
build it as `Array(Tuple(fields))`.
```c
/* Map(String, Int32) */
chc_column keys = chc_build_string(key_offsets, key_data, n_pairs);
chc_column vals = chc_build_fixed(int_vals, sizeof int_vals[0], n_pairs);
chc_column *kv[2] = { &keys, &vals };
chc_column entries = chc_build_tuple(kv, 2);
chc_column map = chc_build_array(map_offsets, n_rows, &entries);
```

Geo types are nested Array / Tuple aliases, also nodeless: `Point` is a 2-arity
tuple of two `Float64` columns, `Ring` and `LineString` = `Array(Point)`,
`Polygon` = `Array(Ring)`, `MultiLineString` = `Array(LineString)`,
`MultiPolygon` = `Array(Polygon)`.

`LowCardinality(Nullable(T))` uses dict slot 0 as the null sentinel; keys of 0
decode NULL. Supply the dict inner-typed with a slot-0 placeholder, or a
`chc_build_nullable` wrapping it with `null_map[0] = 1`; writer unwraps and emits
the same bytes. `chc_build_lc` row count is the key count, dict length comes from
the dict column.

Reader and builder share one tree, so a decoded `chc_column` from
`chc_block_column` appends directly to re-encode a block.

Builder records pointers without copying. Column storage, nodes, types, names,
offsets, keys, null maps, and data slabs must outlive `chc_block_write` or
`chc_client_send_data`. Offsets and LowCardinality keys use host byte order;
fixed data contains `n_rows * elem_size` little-endian bytes. String offsets
are cumulative exclusive ends. `chc_build_string` also supplies JSON and Object
STRING serialization. Writer emits JSON's 8-byte version-1 prefix or Object's
1-byte STRING-kind prefix based on type.

`chc_block_builder_append` only records the column; caller must size the storage
array. Writer checks tree layout and top-level row count against the parsed
ClickHouse type. Call `chc_block_write_cols` with a caller-built `chc_block_col`
array to skip builder.

For INSERT over TCP, hand builder to `chc_client_send_data` rather than calling
`chc_block_write`; client sets `opts` based on revision and handles compression.

## Pitfalls

* **FIXED slabs are LE on the wire.** `chc_column_fixed_data` returns
  raw bytes; on BE hosts the caller swaps when reading multi-byte ints.
  Offsets & LowCardinality keys are byte-swapped at decode, safe to
  dereference as host ints on either endianness.
* **UUID byte order.** CH stores UUID as two LE `UInt64` halves.
* **IPv4 byte order.** 4-byte LE int. IPv6 is NBO — no swap.
* **DateTime64 timezone is metadata only.** Ticks are UTC.
* **Decimal precision is implicit.** `Decimal32` = 9 digits, `Decimal64`
  = 18, `Decimal128` = 38, `Decimal256` = 76.

## Required server settings

```
output_format_native_encode_types_in_binary_format = 0
```

Set this in the Query packet's settings list (TCP) or on the
`clickhouse local` command line. If the server emits binary type tags
anyway, `chc_block_read` returns `CHC_ERR_TYPE` with a message naming
the column. With `has_custom_serialization`, sparse columns decode into
dense layouts, see [clickhouse-client.md](clickhouse-client.md#sparse-columns);
no caller setting required.
