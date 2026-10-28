/*
 * clickhouse.h -- core C client for ClickHouse Native wire format.
 *
 * Single-header library, stb style: exactly one translation unit in
 * the consumer's build must `#define CHC_IMPLEMENTATION` before including
 * this header. Other TUs include for declarations only.
 *
 * Scope here: type-name parser + AST, varint codec, block reader,
 * column accessors, block writer. No I/O backend, no TCP loop, no
 * compression -- those live in sibling headers (clickhouse-posix-io.h,
 * clickhouse-client.h, clickhouse-compression.h, ...).
 *
 * License: Apache-2.0. See LICENSE.
 */

#ifndef CLICKHOUSE_H
#define CLICKHOUSE_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* -------------------------------------------------------------------------- */
/* C23 portability shim                                                       */
/* -------------------------------------------------------------------------- */
/* Macros use C23 features when available, degrading to GNU/builtin equivalent,
 * then to harmless nop, so consumers may build under any -std >= c11 */

#if defined(__has_c_attribute) && defined(__STDC_VERSION__) && __STDC_VERSION__ >= 202311L
#  define CHC__HAS_ATTR(x) __has_c_attribute(x)
#else
#  define CHC__HAS_ATTR(x) 0
#endif

#if CHC__HAS_ATTR(nodiscard)
#  define CHC_NODISCARD [[nodiscard]]
#elif defined(__GNUC__) || defined(__clang__)
#  define CHC_NODISCARD __attribute__((warn_unused_result))
#else
#  define CHC_NODISCARD
#endif

#if CHC__HAS_ATTR(maybe_unused)
#  define CHC_MAYBE_UNUSED [[maybe_unused]]
#elif defined(__GNUC__) || defined(__clang__)
#  define CHC_MAYBE_UNUSED __attribute__((unused))
#else
#  define CHC_MAYBE_UNUSED
#endif

#if CHC__HAS_ATTR(deprecated)
#  define CHC_DEPRECATED(msg) [[deprecated(msg)]]
#elif defined(__GNUC__) || defined(__clang__)
#  define CHC_DEPRECATED(msg) __attribute__((deprecated(msg)))
#else
#  define CHC_DEPRECATED(msg)
#endif

#if CHC__HAS_ATTR(unsequenced)
#  define CHC_UNSEQUENCED [[unsequenced]]
#else
#  define CHC_UNSEQUENCED
#endif

#if CHC__HAS_ATTR(reproducible)
#  define CHC_REPRODUCIBLE [[reproducible]]
#else
#  define CHC_REPRODUCIBLE
#endif

#if CHC__HAS_ATTR(fallthrough)
#  define CHC_FALLTHROUGH [[fallthrough]]
#else
#  define CHC_FALLTHROUGH
#endif

/* unreachable() is C23 <stddef.h>, already included above. */
#if defined(__STDC_VERSION__) && __STDC_VERSION__ >= 202311L
#  define CHC_UNREACHABLE() unreachable()
#elif defined(__GNUC__) || defined(__clang__)
#  define CHC_UNREACHABLE() __builtin_unreachable()
#else
#  define CHC_UNREACHABLE() ((void) 0)
#endif

/* ckd_mul (C23 <stdckdint.h>) backs chc__mul_size; see CHC__HAVE_CKD_MUL. */
#if defined(__has_include)
#  if __has_include(<stdckdint.h>)
#    include <stdckdint.h>
#    define CHC__HAVE_CKD_MUL 1
#  endif
#endif

#ifdef __cplusplus
extern "C" {
#endif

/* -------------------------------------------------------------------------- */
/* Errors                                                                     */
/* -------------------------------------------------------------------------- */

enum {
    CHC_OK,
    CHC_ERR_IO,
    CHC_ERR_EOF,
    CHC_ERR_PROTOCOL,
    CHC_ERR_TYPE,
    CHC_ERR_OOM,
    CHC_ERR_CANCELLED,
    CHC_ERR_SERVER,
    CHC_ERR_USAGE,
    CHC_WOULD_BLOCK
};

#ifndef CHC_ERR_MSG_LEN
#define CHC_ERR_MSG_LEN 256
#endif
#ifndef CHC_PHP_COMPAT
#define CHC_PHP_COMPAT 0
#endif

typedef struct chc_err {
    char msg[CHC_ERR_MSG_LEN];
} chc_err;

static inline void chc_err_reset(chc_err *e) {
    if (e) e->msg[0] = '\0';
}

/* -------------------------------------------------------------------------- */
/* Allocator                                                                  */
/* -------------------------------------------------------------------------- */

typedef struct chc_alloc {
    void *ud;
    void *(*alloc)(void *ud, size_t bytes);
    void *(*realloc)(void *ud, void *p, size_t old_bytes, size_t new_bytes);
    void  (*free)(void *ud, void *p, size_t bytes);
} chc_alloc;

#ifdef CHC_PROVIDE_STDLIB_ALLOC
chc_alloc chc_alloc_stdlib(void);
#endif

/* -------------------------------------------------------------------------- */
/* I/O                                                                        */
/* -------------------------------------------------------------------------- */

typedef struct chc_io {
    void *ud;
    int (*read)(void *ud, void *buf, size_t len, size_t *out_n, chc_err *err);
    int (*write)(void *ud, const void *buf, size_t len, chc_err *err);
    int (*check_cancel)(void *ud);
} chc_io;

/* -------------------------------------------------------------------------- */
/* Buffered reader                                                            */
/* -------------------------------------------------------------------------- */

/* Buffered input parser pulls from. Two modes:
 *  - io-backed (chc_in_init): refills from a chc_io, for blocking io.
 *  - ioless (chc_in_init_ioless): bytes received via chc_in_submit;
 *    reading past submitted bytes returns CHC_WOULD_BLOCK, leaving
 *    in-progress parse to be retried once more bytes arrive. */
typedef struct chc_in chc_in;

CHC_NODISCARD int    chc_in_init(chc_in *in, chc_io *io, const chc_alloc *al,
                                 size_t cap, chc_err *err);
CHC_NODISCARD int    chc_in_init_ioless(chc_in *in, const chc_alloc *al);
CHC_NODISCARD int    chc_in_submit(chc_in *in, const void *buf, size_t len,
                                 chc_err *err);
size_t               chc_in_available(const chc_in *in); /* unconsumed bytes */
void                 chc_in_reset(chc_in *in);   /* drop consumed, compact */
void                 chc_in_free(chc_in *in);

/* -------------------------------------------------------------------------- */
/* Type AST                                                                   */
/* -------------------------------------------------------------------------- */

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
    CHC_POINT, CHC_RING, CHC_LINE_STRING,
    CHC_POLYGON, CHC_MULTI_POLYGON, CHC_MULTI_LINE_STRING,
    CHC_VARIANT, CHC_DYNAMIC, CHC_JSON, CHC_OBJECT,
    CHC_AGGREGATE_FUNCTION, CHC_SIMPLE_AGGREGATE_FUNCTION,
    CHC_QBIT,
    CHC_NOTHING,
    CHC_KIND_COUNT
} chc_kind;

/* Interval unit, ordered as ClickHouse IntervalKind */
typedef enum chc_interval_unit {
    CHC_INTERVAL_NONE = 0,
    CHC_INTERVAL_NANOSECOND,
    CHC_INTERVAL_MICROSECOND,
    CHC_INTERVAL_MILLISECOND,
    CHC_INTERVAL_SECOND,
    CHC_INTERVAL_MINUTE,
    CHC_INTERVAL_HOUR,
    CHC_INTERVAL_DAY,
    CHC_INTERVAL_WEEK,
    CHC_INTERVAL_MONTH,
    CHC_INTERVAL_QUARTER,
    CHC_INTERVAL_YEAR,
} chc_interval_unit;

typedef struct chc_type chc_type;

CHC_NODISCARD int  chc_type_parse(const char *name, size_t name_len,
                    const chc_alloc *al, chc_type **out, chc_err *err);
void chc_type_destroy(chc_type *t, const chc_alloc *al);

chc_kind     chc_type_kind(const chc_type *t);
size_t       chc_type_n_children(const chc_type *t);
const chc_type *chc_type_child(const chc_type *t, size_t i);

int          chc_type_fixed_size(const chc_type *t);
size_t       chc_type_elem_size(const chc_type *t);
int          chc_type_decimal_precision(const chc_type *t);
int          chc_type_decimal_scale(const chc_type *t);
int          chc_type_datetime64_scale(const chc_type *t);

/* Values arrive as Int64 ticks of unit. */
chc_interval_unit chc_type_interval_unit(const chc_type *t);

/* QBit(T, N): N (vector dimension). 0 on non-QBit types. The element type
 * (BFloat16/Float32/Float64) is children[0], reached via chc_type_child(t, 0). */
size_t       chc_type_qbit_dimension(const chc_type *t);
/* QBit element width in bits: 16/32/64. 0 on non-QBit types. Equals the
 * number of FixedString bit-plane columns the column decodes into. */
size_t       chc_type_qbit_element_size(const chc_type *t);
const char  *chc_type_timezone(const chc_type *t, size_t *out_len);
/* Borrowed spelling; use out_len. PHP compatibility nodes may share storage
 * and are not individually NUL-terminated. */
const char  *chc_type_name(const chc_type *t, size_t *out_len);

size_t       chc_type_enum_count(const chc_type *t);
void         chc_type_enum_at(const chc_type *t, size_t i,
                              const char **name, size_t *name_len,
                              int64_t *value);

/* Return a Tuple or Nested field name, or NULL for unnamed fields,
 * invalid indexes, or other types */
const char  *chc_type_tuple_field_name(const chc_type *t, size_t i,
                                       size_t *out_len);

/* Return an aggregate function name without parameters, or NULL when absent */
const char  *chc_type_agg_function(const chc_type *t, size_t *out_len);

/* Reproduce the printable type name into buf. Returns the number of bytes
 * that would have been written (snprintf-style); use to size buf on a
 * second pass when the return value >= buf_len. buf may be NULL when
 * buf_len == 0 (length query). */
size_t       chc_type_format(const chc_type *t, char *buf, size_t buf_len);

/* -------------------------------------------------------------------------- */
/* Columns                                                                    */
/* -------------------------------------------------------------------------- */

typedef enum chc_col_kind {
    CHC_COL_FIXED = 1,
    CHC_COL_STRING,
    CHC_COL_NULLABLE,
    CHC_COL_ARRAY,
    CHC_COL_TUPLE,
    CHC_COL_LOW_CARDINALITY,
    CHC_COL_NOTHING
} chc_col_kind;

typedef struct chc_column chc_column;

/* Column tree. Reader allocates & owns instances, freed by chc_block_destroy.
 * chc_build_* initializes caller-owned instances, usually on stack, over
 * caller slabs. Pointers use host byte order; writer converts to LE on BE
 * hosts. Access through helpers below or fields directly */
struct chc_column {
    chc_col_kind layout;
    size_t       n_rows;
    union {
        struct { void *data; size_t elem_size; }                              fixed;
        struct { uint8_t *data; uint64_t *offsets; size_t bytes; }            str;
        struct { uint8_t *null_map; chc_column *inner; }                      nullable;
        struct { uint64_t *offsets; chc_column *values; }                     array;
        struct { chc_column **children; size_t arity; }                       tuple;
        struct { int key_size; void *keys; chc_column *dict; size_t dict_n; } lc;
    };
};

chc_col_kind chc_column_layout(const chc_column *c);
size_t       chc_column_n_rows(const chc_column *c);

/* FIXED. Contiguous n_rows * (*elem_size) bytes, little-endian on the wire.
 * Caller responsible to fix endianness if necessary. */
const void     *chc_column_fixed_data(const chc_column *c, size_t *elem_size);

/* STRING. Row i's bytes are at data + (i == 0 ? 0 : offsets[i-1]) ..
 *                              data + offsets[i].
 * Offsets are exclusive ends, host-byte-order. */
const uint8_t  *chc_column_string_data(const chc_column *c);
const uint64_t *chc_column_string_offsets(const chc_column *c);

/* NULLABLE. null_map[i] == 1 means row i is NULL; inner column always has
 * a value at row i regardless (placeholder zero/empty for nulls). */
const uint8_t    *chc_column_null_map(const chc_column *c);
const chc_column *chc_column_nullable_inner(const chc_column *c);

/* ARRAY. offsets[i] is the cumulative end of row i in the values column.
 * Map decodes as ARRAY whose values column is TUPLE(K, V). Offsets in
 * host byte order. */
const uint64_t   *chc_column_array_offsets(const chc_column *c);
const chc_column *chc_column_array_values(const chc_column *c);

/* TUPLE. All children share the same row count as the tuple itself. */
size_t            chc_column_tuple_arity(const chc_column *c);
const chc_column *chc_column_tuple_child(const chc_column *c, size_t i);

/* LOW_CARDINALITY. key_size is 1/2/4/8. keys is n_rows * key_size, host
 * byte order (swapped at decode time on BE). Dict is a column of the inner
 * type; dict slot 0 is the default value, NULLs in LC(Nullable(T)) ride at
 * dict slot 0 of the inner Nullable. */
int               chc_column_lc_key_size(const chc_column *c);
const void       *chc_column_lc_keys(const chc_column *c);
const chc_column *chc_column_lc_dict(const chc_column *c);

/* Free a caller-owned column tree, not a borrowed column from a block. */
void chc_column_destroy(chc_column *c, const chc_alloc *al);

/* Walk a column tree & enforce cross-field invariants the server itself
 * enforces on its native deserialization path:
 *   - Array offsets non-decreasing
 *     ("Arrays offsets are not monotonically increasing")
 *   - LowCardinality keys < dict size
 *     ("Index for LowCardinality is out of range")
 * chc_block_read does NOT call this automatically — a peer that forges
 * offsets or LC keys can cause callers to read past inner-column bounds.
 * Consumers ingesting from untrusted senders should call this on each
 * block column before traversing it. Returns CHC_OK on success, or
 * CHC_ERR_PROTOCOL with a reason in err on the first violation. NULL c
 * is treated as OK. */
CHC_NODISCARD int chc_column_validate(const chc_column *c, chc_err *err);

/* -------------------------------------------------------------------------- */
/* Block reader                                                               */
/* -------------------------------------------------------------------------- */

typedef struct chc_block chc_block;

typedef struct chc_block_opts {
    /* TCP path: an 8-byte BlockInfo prefix is on the wire before
     * num_columns. clickhouse-local does not emit it. */
    bool has_block_info;

    /* TCP path: a has_custom_serialization byte follows each column's type
     * name, with serialization kinds when set. Reader decodes sparse
     * columns into dense layouts, writer always emits 0. clickhouse-local
     * does not emit it. */
    bool has_custom_serialization;

    /* Internal read-buffer size. 0 = default (8 KiB). */
    size_t read_buffer_bytes;
} chc_block_opts;

/* Read one block from a caller-owned chc_in. Reuse one chc_in (chc_in_init /
 * chc_in_free) across calls to stream successive blocks: bytes read past the
 * block boundary stay buffered for the next call. NULL opts means empty opts.
 * Return 0 with *out = NULL at clean EOF, or CHC_ERR_* on error. */
CHC_NODISCARD int  chc_block_read(chc_in *in, const chc_alloc *al,
                    const chc_block_opts *opts,
                    chc_block **out, chc_err *err);

void chc_block_destroy(chc_block *b, const chc_alloc *al);

size_t            chc_block_n_rows(const chc_block *b);
size_t            chc_block_n_columns(const chc_block *b);
const char       *chc_block_column_name(const chc_block *b, size_t i, size_t *out_len);
const chc_type   *chc_block_column_type(const chc_block *b, size_t i);
const chc_column *chc_block_column(const chc_block *b, size_t i);

/* BlockInfo accessors. Defined-but-zero when opts.has_block_info == false. */
bool    chc_block_is_overflows(const chc_block *b);
int32_t chc_block_bucket_num(const chc_block *b);

/* -------------------------------------------------------------------------- */
/* Block writer                                                               */
/* -------------------------------------------------------------------------- */

/* Block column: name, full CH type & column tree. Build tree with chc_build_*
 * or reuse reader output for round-trip. Tree stays caller-owned & must
 * outlive write */
typedef struct {
    const char       *name;
    size_t            name_len;
    const chc_type   *type;
    const chc_column *col;
} chc_block_col;

/* Stack builder over caller-provided chc_block_col storage. Initialize with the
 * array, append columns, then call chc_block_write. Caller sizes the array;
 * appending more columns than it holds is undefined. Skip builder by passing an
 * array directly to chc_block_write_cols */
typedef struct chc_block_builder {
    chc_block_col *cols;      /* caller-owned storage */
    size_t         n_cols;
    size_t         n_rows;    /* shared across columns */
} chc_block_builder;

void chc_block_builder_init(chc_block_builder *bb, chc_block_col *cols);

/* Build column trees over caller-owned slabs without copying. Children must
 * outlive write. Nest constructors to match any composite reader emits, e.g.
 * Array(LowCardinality(Nullable(String))):
 *
 *   chc_column d = chc_build_string(dict_offs, dict_data, dict_n);
 *   chc_column n = chc_build_nullable(dict_null_map, &d);
 *   chc_column l = chc_build_lc(key_size, keys, leaf_rows, &n);
 *   chc_column a = chc_build_array(offsets, n_rows, &l);
 *   chc_block_builder_append(bb, "c", 1, t, &a);
 *
 * Each node stores row count for its level. Fixed, string, array & LC builders
 * receive it explicitly; nullable & tuple builders derive it from children.
 * Array leaf count equals offsets[n_rows - 1]; LC leaf count equals key count.
 * Fixed data contains n_rows * elem_size LE bytes; string offsets contain
 * cumulative exclusive ends. For LC(Nullable(T)), supply inner-typed dict
 * with null sentinel at slot 0 or Nullable wrapper. Plain String, FixedString,
 * JSON & Object need one matching chc_build_fixed or chc_build_string */
chc_column chc_build_fixed(const void *data, size_t elem_size, size_t n_rows);
chc_column chc_build_string(const uint64_t *offsets, const uint8_t *data,
                            size_t n_rows);
chc_column chc_build_nullable(const uint8_t *null_map, chc_column *inner);
chc_column chc_build_array(const uint64_t *offsets, size_t n_rows,
                           chc_column *values);
chc_column chc_build_tuple(chc_column **children, size_t arity);
chc_column chc_build_lc(int key_size, const void *keys, size_t n_rows,
                        chc_column *dict);

/* Append column. `t` gives full CH type; `col` must match structurally & share
 * the block row count, both checked during write. Caller must not exceed the
 * storage passed to chc_block_builder_init */
void chc_block_builder_append(chc_block_builder *bb,
                     const char *name, size_t name_len,
                     const chc_type *t, const chc_column *col);

/* Write directly from caller-built columns. Require cols[i].col->n_rows ==
 * n_rows for every column */
CHC_NODISCARD int  chc_block_write_cols(chc_io *io, const chc_block_col *cols,
                     size_t n_cols, size_t n_rows,
                     const chc_block_opts *opts, chc_err *err);

CHC_NODISCARD int  chc_block_write(chc_io *io, const chc_block_builder *bb,
                     const chc_block_opts *opts, chc_err *err);

/* ========================================================================== */
/* Implementation                                                             */
/* ========================================================================== */

#ifdef CHC_IMPLEMENTATION

#include <ctype.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>

/* Endianness detection.  CH wire format is little-endian. On BE hosts the
 * library byte-swaps the offsets/keys arrays it exposes through host-typed
 * pointers; FIXED slabs stay LE. */
#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
#  define CHC_BIG_ENDIAN 1
#else
#  define CHC_BIG_ENDIAN 0
#endif

#if CHC_BIG_ENDIAN
static inline uint16_t chc__bswap16(uint16_t v) {
    return (uint16_t) ((v >> 8) | (v << 8));
}
static inline uint32_t chc__bswap32(uint32_t v) {
    return  ((v & 0xff000000u) >> 24) | ((v & 0x00ff0000u) >>  8)
          | ((v & 0x0000ff00u) <<  8) | ((v & 0x000000ffu) << 24);
}
static inline uint64_t chc__bswap64(uint64_t v) {
    return  ((v & 0xff00000000000000ull) >> 56) | ((v & 0x00ff000000000000ull) >> 40)
          | ((v & 0x0000ff0000000000ull) >> 24) | ((v & 0x000000ff00000000ull) >>  8)
          | ((v & 0x00000000ff000000ull) <<  8) | ((v & 0x0000000000ff0000ull) << 24)
          | ((v & 0x000000000000ff00ull) << 40) | ((v & 0x00000000000000ffull) << 56);
}
#else
#  define chc__bswap16(v) (v)
#  define chc__bswap32(v) (v)
#  define chc__bswap64(v) (v)
#endif

/* -------- CityHash short-string helpers ---------- */

/* Frozen v1.0.3 variant of CityHash, ported from city.cc.
 * Original: Copyright (c) 2011 Google, Inc. (MIT license).
 * Short-string path lives here so chc__name_lookup can reuse it; 128-bit
 * compressed-frame checksum driver in clickhouse-compression.h builds on
 * these helpers */

static uint64_t chc__city_fetch64(const char *p) CHC_REPRODUCIBLE
{
    uint64_t v;
    memcpy(&v, p, 8);
#if CHC_BIG_ENDIAN
    v = chc__bswap64(v);
#endif
    return v;
}

static uint32_t chc__city_fetch32(const char *p) CHC_REPRODUCIBLE
{
    uint32_t v;
    memcpy(&v, p, 4);
#if CHC_BIG_ENDIAN
    v = chc__bswap32(v);
#endif
    return v;
}

static const uint64_t chc__city_k0 = 0xc3a5c85c97cb3127ULL;
static const uint64_t chc__city_k1 = 0xb492b66fbe98f273ULL;
static const uint64_t chc__city_k2 = 0x9ae16a3b2f90404fULL;
static const uint64_t chc__city_k3 = 0xc949d7c7509e6557ULL;

static uint64_t chc__city_rotate_at_least_1(uint64_t v, int s) CHC_UNSEQUENCED
{
    return (v >> s) | (v << (64 - s));
}

static uint64_t chc__city_shift_mix(uint64_t v) CHC_UNSEQUENCED { return v ^ (v >> 47); }

static uint64_t chc__city_hash128_to_64(uint64_t lo, uint64_t hi) CHC_UNSEQUENCED
{
    const uint64_t kMul = 0x9ddfea08eb382d69ULL;
    uint64_t a = (lo ^ hi) * kMul;
    a ^= (a >> 47);
    uint64_t b = (hi ^ a) * kMul;
    b ^= (b >> 47);
    b *= kMul;
    return b;
}

static uint64_t chc__city_hash_len_16(uint64_t u, uint64_t v) CHC_UNSEQUENCED
{
    return chc__city_hash128_to_64(u, v);
}

static uint64_t chc__city_hash_len_0_to_16(const char *s, size_t len) CHC_REPRODUCIBLE
{
    if (len > 8) {
        uint64_t a = chc__city_fetch64(s);
        uint64_t b = chc__city_fetch64(s + len - 8);
        return chc__city_hash_len_16(a,
            chc__city_rotate_at_least_1(b + len, (int) len)) ^ b;
    }
    if (len >= 4) {
        uint64_t a = chc__city_fetch32(s);
        return chc__city_hash_len_16(len + (a << 3),
            chc__city_fetch32(s + len - 4));
    }
    if (len > 0) {
        uint8_t a = (uint8_t) s[0];
        uint8_t b = (uint8_t) s[len >> 1];
        uint8_t c = (uint8_t) s[len - 1];
        uint32_t y = (uint32_t) a + ((uint32_t) b << 8);
        uint32_t z = (uint32_t) len + ((uint32_t) c << 2);
        return chc__city_shift_mix(
                   (uint64_t) y * chc__city_k2 ^
                   (uint64_t) z * chc__city_k3) * chc__city_k2;
    }
    return chc__city_k2;
}

/* -------- error helpers ---------- */

#if defined(__GNUC__) || defined(__clang__)
#  define CHC__PRINTF_FMT(fmt_idx, va_idx) \
    __attribute__((format(printf, fmt_idx, va_idx)))
#else
#  define CHC__PRINTF_FMT(fmt_idx, va_idx)
#endif

static int CHC__PRINTF_FMT(3, 4)
chc__err_set(chc_err *e, int code, const char *fmt, ...)
{
    if (!e) return code;
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(e->msg, sizeof e->msg, fmt, ap);
    va_end(ap);
    return code;
}

/* -------- alloc helpers ---------- */

static void *
chc__alloc(const chc_alloc *al, size_t n, chc_err *err)
{
    void *p = al->alloc(al->ud, n);
    if (!p) {
        chc__err_set(err, CHC_ERR_OOM, "alloc(%zu) failed", n);
        return NULL;
    }
    return p;
}

static void *
chc__calloc(const chc_alloc *al, size_t n, chc_err *err)
{
    void *p = chc__alloc(al, n, err);
    if (p) memset(p, 0, n);
    return p;
}

static void *
chc__realloc(const chc_alloc *al, void *p, size_t old_n, size_t new_n,
             chc_err *err)
{
    void *q = al->realloc(al->ud, p, old_n, new_n);
    if (!q && new_n) {
        chc__err_set(err, CHC_ERR_OOM, "realloc(%zu->%zu) failed", old_n, new_n);
        return NULL;
    }
    return q;
}

/* Overflow-checked size multiply for count*elem allocation sizing. Matches ckd_mul. */
#if defined(__has_builtin)
#  if __has_builtin(__builtin_mul_overflow)
#    define CHC__HAVE_MUL_OVERFLOW 1
#  endif
#elif defined(__GNUC__) && (__GNUC__ >= 5)
#  define CHC__HAVE_MUL_OVERFLOW 1
#endif

static bool
chc__mul_size(size_t a, size_t b, size_t *out)
{
#if defined(CHC__HAVE_CKD_MUL)
    return ckd_mul(out, a, b);
#elif defined(CHC__HAVE_MUL_OVERFLOW)
    return __builtin_mul_overflow(a, b, out);
#else
    *out = a * b;
    return a != 0 && b > (size_t) SIZE_MAX / a;
#endif
}

static char *
chc__strdup(const chc_alloc *al, const char *s, size_t n, chc_err *err)
{
    char *p = chc__alloc(al, n + 1, err);
    if (!p) return NULL;
    if (n) memcpy(p, s, n);
    p[n] = '\0';
    return p;
}

/* Copy a quoted identifier body (between the outer quote chars), resolving
 * the two escape forms ClickHouse's lexer accepts: a doubled quote stands
 * for one literal quote (`` `` `` -> `` ` ``, `""` -> `"`), & `\X` keeps X
 * verbatim. n is an upper bound; the resolved length is returned via
 * *out_len. */
static char *
chc__strdup_unquote(const chc_alloc *al, const char *s, size_t n, char quote,
                    size_t *out_len, chc_err *err)
{
    /* n is an upper bound; escapes shrink it */
    size_t o = 0;
    for (size_t i = 0; i < n; i++) {
        if (s[i] == '\\' && i + 1 < n) { i++; o++; continue; }
        if (s[i] == quote && i + 1 < n && s[i + 1] == quote) { i++; o++; continue; }
        o++;
    }
    char *p = chc__alloc(al, o + 1, err);
    if (!p) return NULL;
    size_t j = 0;
    for (size_t i = 0; i < n; i++) {
        char c = s[i];
        if (c == '\\' && i + 1 < n) { p[j++] = s[++i]; continue; }
        if (c == quote && i + 1 < n && s[i + 1] == quote) { p[j++] = quote; i++; continue; }
        p[j++] = c;
    }
    p[o] = '\0';
    *out_len = o;
    return p;
}

#ifdef CHC_PROVIDE_STDLIB_ALLOC
#include <stdlib.h>
static void *chc__std_alloc(CHC_MAYBE_UNUSED void *ud, size_t n)
    { return malloc(n); }
static void *chc__std_realloc(CHC_MAYBE_UNUSED void *ud, void *p,
                              CHC_MAYBE_UNUSED size_t o, size_t n)
    { return realloc(p, n); }
static void  chc__std_free(CHC_MAYBE_UNUSED void *ud, void *p,
                           CHC_MAYBE_UNUSED size_t b)
{
#if defined(__STDC_VERSION_STDLIB_H__) \
    && __STDC_VERSION_STDLIB_H__ >= 202311L
    free_sized(p, b);
#else
    free(p);
#endif
}
chc_alloc chc_alloc_stdlib(void) {
    chc_alloc a = { NULL, chc__std_alloc, chc__std_realloc, chc__std_free };
    return a;
}
#endif

/* -------- buffered reader ---------- */

#ifndef CHC_READ_BUFFER
#define CHC_READ_BUFFER 8192
#endif

/* Mirrors the limits ClickHouse enforces server-side. */
#ifndef CHC_MAX_STRING_SIZE
#define CHC_MAX_STRING_SIZE       (1ULL << 30)
#endif
#ifndef CHC_MAX_PROTOCOL_STRING_SIZE
#define CHC_MAX_PROTOCOL_STRING_SIZE CHC_MAX_STRING_SIZE
#endif
#ifndef CHC_MAX_NUM_COLUMNS
#define CHC_MAX_NUM_COLUMNS       1000000ULL
#endif
#ifndef CHC_MAX_NUM_ROWS
#define CHC_MAX_NUM_ROWS          1000000000000ULL
#endif
#ifndef CHC_MAX_FIXEDSTRING_SIZE
#define CHC_MAX_FIXEDSTRING_SIZE  0xFFFFFFULL
#endif
#ifndef CHC_MAX_TYPE_DEPTH
#define CHC_MAX_TYPE_DEPTH        300
#endif

/* Reader mode is normally chosen at runtime per `in` (io-backed vs ioless).
 * Two opt-in macros fix it at compile time to optimize out other branches: */
#if defined(CHC_NO_ASYNC) && defined(CHC_NO_SYNC)
#  error "CHC_NO_ASYNC and CHC_NO_SYNC are mutually exclusive"
#elif defined(CHC_NO_ASYNC)
#  define CHC__IOLESS(in)    0
#elif defined(CHC_NO_SYNC)
#  define CHC__IOLESS(in)    1
#else
#  define CHC__IOLESS(in)    ((in)->io == NULL)
#endif

/* Public typedef in the declarations section; struct body is internal. */
struct chc_in {
    chc_io          *io;        /* NULL => ioless  */
    const chc_alloc *al;
    uint8_t         *buf;
    size_t           cap;
    size_t           pos;       /* read cursor */
    size_t           fill;      /* bytes valid in buf */
    bool             eof;
    uint64_t         consumed;  /* total bytes returned to caller */
    size_t           mark;      /* rewind target; SIZE_MAX when unset */
};

int
chc_in_init(chc_in *in, chc_io *io, const chc_alloc *al,
             size_t cap, chc_err *err)
{
    if (cap == 0) cap = CHC_READ_BUFFER;
    *in = (chc_in) { .io = io, .al = al, .cap = cap, .mark = SIZE_MAX };
    in->buf = chc__alloc(al, cap, err);
    if (!in->buf) return CHC_ERR_OOM;
    return CHC_OK;
}

int
chc_in_init_ioless(chc_in *in, const chc_alloc *al)
{
    *in = (chc_in) { .al = al, .mark = SIZE_MAX };
    return CHC_OK;
}

/* Drop prefix [0, keep), using mark while checkpoint is live, otherwise pos.
 * consumed tracks returned bytes rather than offsets, so compaction leaves it
 * unchanged; shifting mark & pos preserves pos - mark for rewind */
static void
chc__in_compact(chc_in *in)
{
    size_t keep = in->mark == SIZE_MAX ? in->pos : in->mark;
    if (keep == 0) return;
    size_t live = in->fill - keep;
    if (live) memmove(in->buf, in->buf + keep, live);
    in->fill -= keep;
    in->pos  -= keep;
    if (in->mark != SIZE_MAX) in->mark -= keep;
}

int
chc_in_submit(chc_in *in, const void *buf, size_t len, chc_err *err)
{
    if (in->io)
        return chc__err_set(err, CHC_ERR_USAGE, "chc_in_submit on io-backed reader");
    if (len == 0) return CHC_OK;
    chc__in_compact(in);
    if (in->fill + len > in->cap) {
        size_t ncap = in->cap ? in->cap : CHC_READ_BUFFER;
        while (ncap < in->fill + len) {
            if (ncap > SIZE_MAX / 2) { ncap = in->fill + len; break; }
            ncap *= 2;
        }
        uint8_t *nb = chc__realloc(in->al, in->buf, in->cap, ncap, err);
        if (!nb) return CHC_ERR_OOM;
        in->buf = nb;
        in->cap = ncap;
    }
    memcpy(in->buf + in->fill, buf, len);
    in->fill += len;
    return CHC_OK;
}

size_t
chc_in_available(const chc_in *in)
{
    return in->fill - in->pos;
}

void
chc_in_reset(chc_in *in)
{
    in->mark = SIZE_MAX;
    chc__in_compact(in);
}

void
chc_in_free(chc_in *in)
{
    in->al->free(in->al->ud, in->buf, in->cap);
    in->buf = NULL;
}

/* Mark read cursor for rewind. Ioless mode checkpoints at packet boundaries,
 * allowing mid-parse CHC_WOULD_BLOCK to rewind & retry after more bytes arrive */
CHC_MAYBE_UNUSED static void
chc__in_checkpoint(chc_in *in)
{
    in->mark = in->pos;
}

/* Restore the cursor to the last checkpoint, un-counting bytes consumed
 * since it (so consumed stays equal to an io-backed read of the same
 * stream). Clears the mark. */
CHC_MAYBE_UNUSED static int
chc__in_rewind(chc_in *in)
{
    if (in->mark == SIZE_MAX)
        return CHC_ERR_USAGE;
    in->consumed -= in->pos - in->mark;
    in->pos = in->mark;
    in->mark = SIZE_MAX;
    return CHC_OK;
}

/* Refill buf with at least one byte. Returns 0 on success, CHC_ERR_EOF on
 * clean EOF, CHC_ERR_IO/CANCELLED on failure.
 * Returns CHC_WOULD_BLOCK when not CHC_OK in ioless. */
static int
chc__in_refill(chc_in *in, chc_err *err)
{
    if (in->pos < in->fill) return CHC_OK;
    if (CHC__IOLESS(in))
        return chc__err_set(err, CHC_WOULD_BLOCK, "ioless buffer drained");
    if (in->eof) return chc__err_set(err, CHC_ERR_EOF, "unexpected eof");

    if (in->io->check_cancel && in->io->check_cancel(in->io->ud))
        return chc__err_set(err, CHC_ERR_CANCELLED, "canceled");

    in->pos = 0;
    in->fill = 0;
    size_t got = 0;
    int rc = in->io->read(in->io->ud, in->buf, in->cap, &got, err);
    if (rc != CHC_OK) {
        if (err && err->msg[0] == '\0') chc__err_set(err, rc, "read failed");
        return rc;
    }
    if (got == 0) { in->eof = true; return chc__err_set(err, CHC_ERR_EOF, "unexpected eof"); }
    in->fill = got;
    return CHC_OK;
}

static int
chc__read_byte(chc_in *in, uint8_t *out, chc_err *err)
{
    if (in->pos >= in->fill) {
        int rc = chc__in_refill(in, err);
        if (rc != CHC_OK) return rc;
    }
    *out = in->buf[in->pos++];
    in->consumed++;
    return CHC_OK;
}

static int
chc__read_bytes(chc_in *in, void *dst, size_t n, chc_err *err)
{
    uint8_t *p = dst;

    if (in->pos < in->fill) {
        size_t avail = in->fill - in->pos;
        size_t take = n < avail ? n : avail;
        memcpy(p, in->buf + in->pos, take);
        in->pos += take;
        in->consumed += take;
        p += take;
        n -= take;
    }

    /* Bypass staging buf when request spans more than one refill, read
     * straight into caller's dst to skip the staging memcpy. Only fires
     * after the staging buf is drained, so buffered-reader invariants
     * (pos, fill, consumed) stay consistent. Disabled in ioless: bypassed
     * bytes bypass in->buf & cannot be rewound; ioless routes all reads
     * through growable staging buf. */
    while (!CHC__IOLESS(in) && n > in->cap) {
        if (in->eof)
            return chc__err_set(err, CHC_ERR_EOF, "short read");
        if (in->io->check_cancel && in->io->check_cancel(in->io->ud))
            return chc__err_set(err, CHC_ERR_CANCELLED, "canceled");
        size_t got = 0;
        int rc = in->io->read(in->io->ud, p, n, &got, err);
        if (rc != CHC_OK) {
            if (err && err->msg[0] == '\0') chc__err_set(err, rc, "read failed");
            return rc;
        }
        if (got == 0) {
            in->eof = true;
            return chc__err_set(err, CHC_ERR_EOF, "short read");
        }
        p += got;
        in->consumed += got;
        n -= got;
    }

    while (n) {
        if (in->pos >= in->fill) {
            int rc = chc__in_refill(in, err);
            if (rc == CHC_ERR_EOF)
                return chc__err_set(err, CHC_ERR_EOF, "short read");
            if (rc != CHC_OK) return rc;
        }
        size_t avail = in->fill - in->pos;
        size_t take = n < avail ? n : avail;
        memcpy(p, in->buf + in->pos, take);
        in->pos += take;
        in->consumed += take;
        p += take;
        n -= take;
    }
    return CHC_OK;
}

static int
chc__skip_bytes(chc_in *in, size_t n, chc_err *err)
{
    uint8_t sink[256];
    while (n) {
        size_t take = n < sizeof sink ? n : sizeof sink;
        int rc = chc__read_bytes(in, sink, take, err);
        if (rc != CHC_OK) return rc;
        n -= take;
    }
    return CHC_OK;
}

static int
chc__read_varuint(chc_in *in, uint64_t *out, chc_err *err)
{
    uint64_t v = 0;
    for (int i = 0; i < 10; i++) {
        uint8_t b;
        int rc = chc__read_byte(in, &b, err);
        if (rc != CHC_OK) return rc;
        v |= ((uint64_t)(b & 0x7f)) << (7 * i);
        if (!(b & 0x80)) { *out = v; return CHC_OK; }
    }
    return chc__err_set(err, CHC_ERR_PROTOCOL, "varint too long");
}

static int
chc__read_u32_le(chc_in *in, uint32_t *out, chc_err *err)
{
    uint32_t v;
    int rc = chc__read_bytes(in, &v, sizeof v, err);
    if (rc != CHC_OK) return rc;
    *out = chc__bswap32(v);
    return CHC_OK;
}

static int
chc__read_u64_le(chc_in *in, uint64_t *out, chc_err *err)
{
    uint64_t v;
    int rc = chc__read_bytes(in, &v, sizeof v, err);
    if (rc != CHC_OK) return rc;
    *out = chc__bswap64(v);
    return CHC_OK;
}

static int
chc__read_string_body(chc_in *in, size_t len, char **out, chc_err *err)
{
    const chc_alloc *al = in->al;
    char *buf = NULL;
    size_t cap = 0, used = 0;
    uint8_t chunk[8192];
    while (used < len) {
        size_t take = len - used < sizeof chunk ? len - used : sizeof chunk;
        int rc = chc__read_bytes(in, chunk, take, err);
        if (rc != CHC_OK) { al->free(al->ud, buf, cap); return rc; }
        size_t need = used + take + 1;
        if (need > cap) {
            size_t next = cap && cap <= SIZE_MAX / 2 ? cap * 2 : need;
            if (next < need) next = need;
            if (next > len + 1) next = len + 1;
            char *grown = chc__realloc(al, buf, cap, next, err);
            if (!grown) { al->free(al->ud, buf, cap); return CHC_ERR_OOM; }
            buf = grown;
            cap = next;
        }
        memcpy(buf + used, chunk, take);
        used += take;
    }
    if (!buf) {
        buf = chc__alloc(al, 1, err);
        if (!buf) return CHC_ERR_OOM;
    }
    buf[len] = '\0';
    *out = buf;
    return CHC_OK;
}

/* Grow only as preceding bytes arrive, retaining the allocation on failure. */
static int
chc__read_growing(chc_in *in, uint8_t **buf, size_t *cap,
                  size_t loaded, size_t total, size_t first, bool reserve_ahead, chc_err *err)
{
    if (!in->io && total - loaded > in->fill - in->pos) return CHC_WOULD_BLOCK;
    while (loaded < total) {
        if (*cap <= loaded) {
            size_t limit = reserve_ahead ? SIZE_MAX : total;
            size_t next = loaded > limit / 8 ? limit : loaded * 8;
            if (next < first) next = first;
            if (next > total && !reserve_ahead) next = total;
            uint8_t *grown = chc__realloc(in->al, *buf, *cap, next, err);
            if (!grown) return CHC_ERR_OOM;
            *buf = grown;
            *cap = next;
        }
        size_t take = loaded > first ? loaded : first;
        if (take > total - loaded) take = total - loaded;
        if (take > *cap - loaded) take = *cap - loaded;
        int rc = chc__read_bytes(in, *buf + loaded, take, err);
        if (rc != CHC_OK) return rc;
        loaded += take;
    }
    return CHC_OK;
}

/* The caller receives an exact-sized allocation only after the read succeeds. */
static int
chc__read_allocated(chc_in *in, size_t bytes, void **out, chc_err *err)
{
    uint8_t *buf = NULL;
    size_t cap = 0;
    int rc = chc__read_growing(in, &buf, &cap, 0, bytes, 1u << 20, false, err);
    if (rc != CHC_OK) {
        in->al->free(in->al->ud, buf, cap);
        return rc;
    }
    *out = buf;
    return CHC_OK;
}

static int
chc__read_string(chc_in *in, char **out, size_t *out_len, chc_err *err)
{
    uint64_t len;
    int rc = chc__read_varuint(in, &len, err);
    if (rc != CHC_OK) return rc;
    if (len > CHC_MAX_PROTOCOL_STRING_SIZE || len >= SIZE_MAX)
        return chc__err_set(err, CHC_ERR_PROTOCOL, "string too long: %llu",
                            (unsigned long long) len);
    rc = chc__read_string_body(in, (size_t) len, out, err);
    if (rc != CHC_OK) return rc;
    *out_len = (size_t) len;
    return CHC_OK;
}

/* -------- type AST internals ---------- */

typedef struct {
    size_t refs;
    size_t bytes;
} chc__type_spelling;

enum {
    CHC__PHP_FACTORY_OK,
    CHC__PHP_FACTORY_CHILD_ZERO,
    CHC__PHP_FACTORY_CHILD_LAST,
    CHC__PHP_FACTORY_FIXEDSTRING_ZERO,
    CHC__PHP_FACTORY_CONTENTS
};

static void
chc__type_spelling_release(chc__type_spelling *spelling, const chc_alloc *al)
{
    if (spelling && !--spelling->refs)
        al->free(al->ud, spelling, spelling->bytes);
}

struct chc_type {
    chc_kind  kind;
    char     *name;
    size_t    name_len;
    chc__type_spelling *spelling;
    unsigned php_factory_error;
    size_t    n_children;
    size_t    children_capacity;
    chc_type **children;
    /* For Tuple: parallel to children[]. NULL when tuple has no field names.
     * Individual slots may be NULL for mixed-anonymity tuples (rare; CH allows it). */
    char    **field_names;
    size_t   *field_name_lens;
    size_t    fields_capacity;
    union {
        struct { int n; }                              fixed_string;  /* FixedString(N) */
        struct { int precision, scale; }               decimal;       /* Decimal(P, S) */
        struct { int scale; char *tz; size_t tz_len; } temporal;      /* DateTime / DateTime64 / Time64 */
        struct { size_t dimension; }                   qbit;          /* QBit(T, N): N; element type in children[0] */
        struct { char *func; size_t func_len; }        agg;
        chc_interval_unit                              interval;      /* IntervalNanosecond .. IntervalYear */
        struct {
            size_t n;
            struct { char *name; uint32_t name_len; int16_t value; } *items;
        } enum_;
    };
};

static bool chc__kind_is_decimal(chc_kind k)
{ return k == CHC_DECIMAL32 || k == CHC_DECIMAL64 || k == CHC_DECIMAL128 || k == CHC_DECIMAL256; }
static bool chc__kind_is_enum(chc_kind k)
{ return k == CHC_ENUM8 || k == CHC_ENUM16; }
static bool chc__kind_has_tz(chc_kind k)
{ return k == CHC_DATETIME || k == CHC_DATETIME64 || k == CHC_TIME64; }
static bool chc__kind_is_agg(chc_kind k)
{ return k == CHC_AGGREGATE_FUNCTION || k == CHC_SIMPLE_AGGREGATE_FUNCTION; }

void
chc_type_destroy(chc_type *t, const chc_alloc *al)
{
    if (!t) return;
    if (t->field_names) {
        for (size_t i = 0; i < t->n_children; i++)
            al->free(al->ud, t->field_names[i], t->field_name_lens[i] + 1);
        al->free(al->ud, t->field_names, t->fields_capacity * sizeof *t->field_names);
    }
    al->free(al->ud, t->field_name_lens, t->fields_capacity * sizeof *t->field_name_lens);
    for (size_t i = 0; i < t->n_children; i++)
        chc_type_destroy(t->children[i], al);
    al->free(al->ud, t->children, t->children_capacity * sizeof *t->children);
    if (chc__kind_is_enum(t->kind) && t->enum_.items) {
        for (size_t i = 0; i < t->enum_.n; i++)
            al->free(al->ud, t->enum_.items[i].name,
                     t->enum_.items[i].name_len + 1);
        al->free(al->ud, t->enum_.items,
                 t->enum_.n * sizeof *t->enum_.items);
    } else if (chc__kind_has_tz(t->kind))
        al->free(al->ud, t->temporal.tz, t->temporal.tz_len + 1);
    else if (chc__kind_is_agg(t->kind))
        al->free(al->ud, t->agg.func, t->agg.func_len + 1);
    if (t->spelling) chc__type_spelling_release(t->spelling, al);
    else al->free(al->ud, t->name, t->name_len + 1);
    al->free(al->ud, t, sizeof *t);
}

chc_kind         chc_type_kind(const chc_type *t)               { return t ? t->kind : CHC_VOID; }
size_t           chc_type_n_children(const chc_type *t)         { return t ? t->n_children : 0; }
const chc_type  *chc_type_child(const chc_type *t, size_t i)    { return (t && i < t->n_children) ? t->children[i] : NULL; }
int              chc_type_fixed_size(const chc_type *t)         { return t && t->kind == CHC_FIXED_STRING ? t->fixed_string.n : 0; }
int              chc_type_decimal_scale(const chc_type *t)      { return (t && chc__kind_is_decimal(t->kind)) ? t->decimal.scale : 0; }
int              chc_type_datetime64_scale(const chc_type *t)   { return (t && (t->kind == CHC_DATETIME64 || t->kind == CHC_TIME64)) ? t->temporal.scale : 0; }
chc_interval_unit chc_type_interval_unit(const chc_type *t)     { return (t && t->kind == CHC_INTERVAL) ? t->interval : CHC_INTERVAL_NONE; }
size_t           chc_type_qbit_dimension(const chc_type *t)     { return (t && t->kind == CHC_QBIT) ? t->qbit.dimension : 0; }
size_t           chc_type_qbit_element_size(const chc_type *t)  { return (t && t->kind == CHC_QBIT && t->n_children == 1) ? chc_type_elem_size(t->children[0]) * 8 : 0; }
const char      *chc_type_name(const chc_type *t, size_t *out_len) {
    if (out_len) *out_len = t ? t->name_len : 0;
    return t ? t->name : NULL;
}
const char      *chc_type_timezone(const chc_type *t, size_t *out_len) {
    bool has = t && chc__kind_has_tz(t->kind);
    if (out_len) *out_len = has ? t->temporal.tz_len : 0;
    return has ? t->temporal.tz : NULL;
}
size_t           chc_type_enum_count(const chc_type *t)         { return (t && chc__kind_is_enum(t->kind)) ? t->enum_.n : 0; }
void             chc_type_enum_at(const chc_type *t, size_t i,
                                  const char **name, size_t *name_len,
                                  int64_t *value) {
    if (!t || !chc__kind_is_enum(t->kind) || i >= t->enum_.n) {
        if (name) *name = NULL;
        if (name_len) *name_len = 0;
        if (value) *value = 0;
        return;
    }
    if (name) *name = t->enum_.items[i].name;
    if (name_len) *name_len = t->enum_.items[i].name_len;
    if (value) *value = t->enum_.items[i].value;
}

const char *
chc_type_tuple_field_name(const chc_type *t, size_t i, size_t *out_len)
{
    bool named = t && (t->kind == CHC_TUPLE || t->kind == CHC_NESTED);
    if (!named || !t->field_names || i >= t->n_children) {
        if (out_len) *out_len = 0;
        return NULL;
    }
    if (out_len) *out_len = t->field_name_lens[i];
    return t->field_names[i];
}

const char *
chc_type_agg_function(const chc_type *t, size_t *out_len)
{
    bool has = t && chc__kind_is_agg(t->kind);
    if (out_len) *out_len = has ? t->agg.func_len : 0;
    return has ? t->agg.func : NULL;
}

int
chc_type_decimal_precision(const chc_type *t)
{
    if (!t || !chc__kind_is_decimal(t->kind)) return 0;
    if (t->decimal.precision) return t->decimal.precision;
    switch (t->kind) {
    case CHC_DECIMAL32:  return 9;
    case CHC_DECIMAL64:  return 18;
    case CHC_DECIMAL128: return 38;
    case CHC_DECIMAL256: return 76;
    default: CHC_UNREACHABLE();
    }
}

/* -------- type parser ---------- */

/* Tokens & lexer for printable ClickHouse type names. The parser is
 * recursive on '(' / ')' / ','; single-quoted parameter strings carry no
 * escapes. */
typedef enum {
    CHC__TOK_EOS = 0, CHC__TOK_NAME, CHC__TOK_NUMBER, CHC__TOK_STRING,
    CHC__TOK_LPAREN, CHC__TOK_RPAREN, CHC__TOK_COMMA, CHC__TOK_EQ,
    CHC__TOK_INVALID
} chc__tok_kind;

typedef struct {
    chc__tok_kind kind;
    const char   *start;
    size_t        len;
    /* For CHC__TOK_NAME: 0 = bare identifier; '`' or '"' = quoted, & start/len
     * span the body between the outer quotes (still raw -- doubled-quote &
     * backslash escapes are resolved when copied out). */
    char          quote;
} chc__tok;

typedef struct {
    const char *cur, *end;
    chc__tok    peeked;
    bool        has_peek;
    chc__type_spelling *spelling;
} chc__lex;

static chc__tok
chc__next_tok(chc__lex *lx)
{
    while (lx->cur < lx->end) {
        char c = *lx->cur;
        if (c == ' ' || c == '\t' || c == '\n' || c == '\r') { lx->cur++; continue; }
        const char *st = lx->cur;
        if (c == '(') { lx->cur++; return (chc__tok){CHC__TOK_LPAREN, st, 1, 0}; }
        if (c == ')') { lx->cur++; return (chc__tok){CHC__TOK_RPAREN, st, 1, 0}; }
        if (c == ',') { lx->cur++; return (chc__tok){CHC__TOK_COMMA, st, 1, 0}; }
        if (c == '=') { lx->cur++; return (chc__tok){CHC__TOK_EQ, st, 1, 0}; }
        if (c == '\'') {
            /* single-quoted string; type names carry no escapes, so accept
             * anything up to the next quote. */
            lx->cur++;
            const char *body = lx->cur;
            while (lx->cur < lx->end && *lx->cur != '\'') lx->cur++;
            if (lx->cur >= lx->end) return (chc__tok){CHC__TOK_INVALID, st, 0, 0};
            size_t blen = (size_t) (lx->cur - body);
            lx->cur++;                                     /* eat closing ' */
            return (chc__tok){CHC__TOK_STRING, body, blen, 0};
        }
        if (c == '`' || c == '"') {
            /* Quoted identifier: doubled quote (`` `` `` or `""`) &
             * backslash-escapes are skipped during scanning, resolved at
             * copy time -- same rule as the ClickHouse SQL lexer. */
            char q = c;
            lx->cur++;
            const char *body = lx->cur;
            while (lx->cur < lx->end) {
                char d = *lx->cur;
                if (d == '\\') {
                    lx->cur++;
                    if (lx->cur < lx->end) lx->cur++;
                    continue;
                }
                if (d == q) {
                    if (lx->cur + 1 < lx->end && lx->cur[1] == q) {
                        lx->cur += 2;
                        continue;
                    }
                    break;
                }
                lx->cur++;
            }
            if (lx->cur >= lx->end) return (chc__tok){CHC__TOK_INVALID, st, 0, 0};
            size_t blen = (size_t) (lx->cur - body);
            lx->cur++;                                     /* eat closing quote */
            return (chc__tok){CHC__TOK_NAME, body, blen, q};
        }
        if (isalpha((unsigned char) c) || c == '_') {
            while (lx->cur < lx->end) {
                char d = *lx->cur;
                if (!(isalnum((unsigned char) d) || d == '_')) break;
                lx->cur++;
            }
            return (chc__tok){CHC__TOK_NAME, st, (size_t) (lx->cur - st), 0};
        }
        if (isdigit((unsigned char) c) || c == '-') {
            lx->cur++;
            while (lx->cur < lx->end && isdigit((unsigned char) *lx->cur)) lx->cur++;
            return (chc__tok){CHC__TOK_NUMBER, st, (size_t) (lx->cur - st), 0};
        }
        return (chc__tok){CHC__TOK_INVALID, st, 0, 0};
    }
    return (chc__tok){CHC__TOK_EOS, lx->end, 0, 0};
}

static chc__tok
chc__peek_tok(chc__lex *lx)
{
    if (!lx->has_peek) {
        lx->peeked = chc__next_tok(lx);
        lx->has_peek = true;
    }
    return lx->peeked;
}

static chc__tok
chc__eat_tok(chc__lex *lx)
{
    chc__tok t = chc__peek_tok(lx);
    lx->has_peek = false;
    return t;
}

static bool
chc__atoi64(const char *s, size_t n, int64_t *out)
{
    bool neg = n && *s == '-';
    if (neg) { s++; n--; }
    if (!n) return false;
    uint64_t v = 0, lim = (uint64_t) INT64_MAX + neg;
    while (n--) {
        uint64_t d = (uint64_t) (*s++ - '0');
        if (v > (lim - d) / 10) return false;
        v = v * 10 + d;
    }
    *out = neg ? (v == lim ? INT64_MIN : -(int64_t) v) : (int64_t) v;
    return true;
}

/* AUTO-GENERATED-NAME-TABLE-BEGIN -- regenerate via tools/regen_name_table.sh */
#define CHC__NAME_TABLE_M 256u
#define CHC__NAME_TABLE_SEED 5935ull
#define CHC__NAME_TABLE_MAXLEN 23u
struct chc__name_row { uint16_t off; uint8_t len; uint8_t kind; uint8_t unit; };
static const char chc__name_blob[] =
    "SimpleAggregateFunctionIntervalMicrosecondIntervalMillisecondInt"
    "ervalNanosecondMultiLineStringIntervalQuarterLowCardinalityInter"
    "valSecondIntervalMinuteIntervalMonthMultiPolygonIntervalHourInte"
    "rvalWeekIntervalYearFixedStringIntervalDayDateTime64Decimal128De"
    "cimal256Decimal32Decimal64BFloat16NullableUInt128UInt256Float32F"
    "loat64NothingVariantDynamicUInt16UInt32UInt64Date32Enum16NestedO"
    "bjectUInt8Enum8ArrayTuplePointBoolUUIDIPv4IPv6VoidRingJSONQBitMa"
    "p";
static const struct chc__name_row chc__name_rows[] = {
    {390,  4, CHC_INT8, CHC_INTERVAL_NONE},
    {348,  5, CHC_INT16, CHC_INTERVAL_NONE},
    {354,  5, CHC_INT32, CHC_INTERVAL_NONE},
    {360,  5, CHC_INT64, CHC_INTERVAL_NONE},
    {299,  6, CHC_INT128, CHC_INTERVAL_NONE},
    {306,  6, CHC_INT256, CHC_INTERVAL_NONE},
    {389,  5, CHC_UINT8, CHC_INTERVAL_NONE},
    {347,  6, CHC_UINT16, CHC_INTERVAL_NONE},
    {353,  6, CHC_UINT32, CHC_INTERVAL_NONE},
    {359,  6, CHC_UINT64, CHC_INTERVAL_NONE},
    {298,  7, CHC_UINT128, CHC_INTERVAL_NONE},
    {305,  7, CHC_UINT256, CHC_INTERVAL_NONE},
    {312,  7, CHC_FLOAT32, CHC_INTERVAL_NONE},
    {319,  7, CHC_FLOAT64, CHC_INTERVAL_NONE},
    {282,  8, CHC_BFLOAT16, CHC_INTERVAL_NONE},
    {414,  4, CHC_BOOL, CHC_INTERVAL_NONE},
    { 88,  6, CHC_STRING, CHC_INTERVAL_NONE},
    {212, 11, CHC_FIXED_STRING, CHC_INTERVAL_NONE},
    {234,  4, CHC_DATE, CHC_INTERVAL_NONE},
    {365,  6, CHC_DATE32, CHC_INTERVAL_NONE},
    {234,  8, CHC_DATETIME, CHC_INTERVAL_NONE},
    {234, 10, CHC_DATETIME64, CHC_INTERVAL_NONE},
    {238,  4, CHC_TIME, CHC_INTERVAL_NONE},
    {238,  6, CHC_TIME64, CHC_INTERVAL_NONE},
    {418,  4, CHC_UUID, CHC_INTERVAL_NONE},
    {422,  4, CHC_IPV4, CHC_INTERVAL_NONE},
    {426,  4, CHC_IPV6, CHC_INTERVAL_NONE},
    {394,  5, CHC_ENUM8, CHC_INTERVAL_NONE},
    {371,  6, CHC_ENUM16, CHC_INTERVAL_NONE},
    {264,  9, CHC_DECIMAL32, CHC_INTERVAL_NONE},
    {273,  9, CHC_DECIMAL64, CHC_INTERVAL_NONE},
    {244, 10, CHC_DECIMAL128, CHC_INTERVAL_NONE},
    {254, 10, CHC_DECIMAL256, CHC_INTERVAL_NONE},
    {290,  8, CHC_NULLABLE, CHC_INTERVAL_NONE},
    {399,  5, CHC_ARRAY, CHC_INTERVAL_NONE},
    {404,  5, CHC_TUPLE, CHC_INTERVAL_NONE},
    {446,  3, CHC_MAP, CHC_INTERVAL_NONE},
    {377,  6, CHC_NESTED, CHC_INTERVAL_NONE},
    {109, 14, CHC_LOW_CARDINALITY, CHC_INTERVAL_NONE},
    {326,  7, CHC_NOTHING, CHC_INTERVAL_NONE},
    {430,  4, CHC_VOID, CHC_INTERVAL_NONE},
    {409,  5, CHC_POINT, CHC_INTERVAL_NONE},
    {434,  4, CHC_RING, CHC_INTERVAL_NONE},
    {169,  7, CHC_POLYGON, CHC_INTERVAL_NONE},
    {164, 12, CHC_MULTI_POLYGON, CHC_INTERVAL_NONE},
    { 84, 10, CHC_LINE_STRING, CHC_INTERVAL_NONE},
    { 79, 15, CHC_MULTI_LINE_STRING, CHC_INTERVAL_NONE},
    {  0, 23, CHC_SIMPLE_AGGREGATE_FUNCTION, CHC_INTERVAL_NONE},
    {  6, 17, CHC_AGGREGATE_FUNCTION, CHC_INTERVAL_NONE},
    {333,  7, CHC_VARIANT, CHC_INTERVAL_NONE},
    {340,  7, CHC_DYNAMIC, CHC_INTERVAL_NONE},
    {438,  4, CHC_JSON, CHC_INTERVAL_NONE},
    {383,  6, CHC_OBJECT, CHC_INTERVAL_NONE},
    {442,  4, CHC_QBIT, CHC_INTERVAL_NONE},
    { 61, 18, CHC_INTERVAL, CHC_INTERVAL_NANOSECOND},
    { 23, 19, CHC_INTERVAL, CHC_INTERVAL_MICROSECOND},
    { 42, 19, CHC_INTERVAL, CHC_INTERVAL_MILLISECOND},
    {123, 14, CHC_INTERVAL, CHC_INTERVAL_SECOND},
    {137, 14, CHC_INTERVAL, CHC_INTERVAL_MINUTE},
    {176, 12, CHC_INTERVAL, CHC_INTERVAL_HOUR},
    {223, 11, CHC_INTERVAL, CHC_INTERVAL_DAY},
    {188, 12, CHC_INTERVAL, CHC_INTERVAL_WEEK},
    {151, 13, CHC_INTERVAL, CHC_INTERVAL_MONTH},
    { 94, 15, CHC_INTERVAL, CHC_INTERVAL_QUARTER},
    {200, 12, CHC_INTERVAL, CHC_INTERVAL_YEAR},
};
static const uint8_t chc__name_slot[CHC__NAME_TABLE_M] = {
    [  2] = 43, [  6] = 36, [  9] = 60, [ 27] =  7, [ 29] = 40, [ 36] = 53,
    [ 39] = 41, [ 42] = 65, [ 45] = 34, [ 51] = 28, [ 52] = 46, [ 59] = 16,
    [ 67] = 18, [ 68] = 29, [ 71] =  2, [ 72] = 61, [ 73] = 25, [ 80] = 49,
    [ 81] = 22, [ 85] = 45, [ 86] = 64, [ 97] = 31, [ 98] = 35, [107] = 24,
    [109] = 39, [111] = 54, [112] = 55, [113] =  4, [118] =  9, [123] = 57,
    [126] = 47, [127] = 11, [129] = 21, [133] = 59, [134] = 19, [137] = 58,
    [138] = 17, [139] = 20, [141] = 27, [142] = 42, [151] = 63, [153] = 23,
    [154] = 13, [159] =  1, [160] = 15, [168] = 50, [170] = 33, [173] = 48,
    [174] = 37, [175] = 30, [176] = 62, [180] = 52, [192] = 14, [202] =  5,
    [204] =  6, [205] = 38, [206] =  3, [215] = 44, [223] = 12, [226] =  8,
    [238] = 10, [240] = 56, [247] = 26, [251] = 51, [252] = 32,
};
/* AUTO-GENERATED-NAME-TABLE-END */

/* Plain "Decimal" is intentionally absent from the table; the parser's
 * decimal_alias branch resolves it from precision. */
static const struct chc__name_row *
chc__name_lookup(const char *s, size_t n) CHC_REPRODUCIBLE
{
    if (n == 0 || n > CHC__NAME_TABLE_MAXLEN) return NULL;
    size_t h_len = n < 16 ? n : 16;
    uint64_t h = chc__city_hash_len_16(
        chc__city_hash_len_0_to_16(s, h_len) + (uint64_t) n,
        CHC__NAME_TABLE_SEED);
    uint8_t i = chc__name_slot[h & (CHC__NAME_TABLE_M - 1)];
    if (!i) return NULL;
    const struct chc__name_row *r = &chc__name_rows[i - 1];
    return (r->len == n && memcmp(chc__name_blob + r->off, s, n) == 0) ? r : NULL;
}

/* Skip aggregate and JSON parameters, which can contain more than type names */
static bool
chc__skip_params(chc__lex *lx)
{
    size_t depth = 1;
    while (lx->cur < lx->end) {
        char c = *lx->cur++;
        if (c == '\'' || c == '`' || c == '"') {
            while (lx->cur < lx->end && *lx->cur != c) lx->cur++;
            if (lx->cur == lx->end) return false;
            lx->cur++;
        } else if (c == '(')
            depth++;
        else if (c == ')' && !--depth)
            return true;
    }
    return false;
}

static int chc__parse_type(chc__lex *lx, const chc_alloc *al,
                           const char *whole_start, const char *whole_end,
                           size_t depth, chc_type **out, chc_err *err);

/* Append a child pointer to parent's children array. */
static int
chc__type_push_child(const chc_alloc *al, chc_type *parent, chc_type *child,
                     chc_err *err)
{
    size_t n = parent->n_children;
    if (n == parent->children_capacity) {
        size_t limit = SIZE_MAX / sizeof *parent->children;
        if (n == limit) return chc__err_set(err, CHC_ERR_OOM, "too many type children");
        size_t next = n > limit / 2 ? limit : n * 2;
        if (next < 4) next = 4;
        chc_type **arr = chc__realloc(al, parent->children,
            n * sizeof *arr, next * sizeof *arr, err);
        if (!arr) return CHC_ERR_OOM;
        parent->children = arr;
        parent->children_capacity = next;
    }
    parent->children[n] = child;
    parent->n_children = n + 1;
    return CHC_OK;
}

static int
chc__type_push_enum(const chc_alloc *al, chc_type *parent,
                    const char *name, size_t name_len, int64_t value,
                    chc_err *err)
{
    char *nm = chc__strdup(al, name, name_len, err);
    if (!nm) return CHC_ERR_OOM;
    size_t n = parent->enum_.n;
    void *arr = chc__realloc(al, parent->enum_.items,
                             n * sizeof *parent->enum_.items,
                             (n + 1) * sizeof *parent->enum_.items, err);
    if (!arr) { al->free(al->ud, nm, name_len + 1); return CHC_ERR_OOM; }
    parent->enum_.items = arr;
    parent->enum_.items[n].name = nm;
    parent->enum_.items[n].name_len = name_len;
    parent->enum_.items[n].value = value;
    parent->enum_.n = n + 1;
    return CHC_OK;
}

/* Tuple field-name scratch: parallel arrays handed to the type on success,
 * freed exactly as far as each grew when a mid-list allocation fails. */
typedef struct {
    char   **names;
    size_t  *lens;
    size_t   n_names;
    size_t   n_lens;
} chc__fields;

static void
chc__fields_free(const chc_alloc *al, chc__fields *f)
{
    for (size_t i = 0; i < f->n_names && i < f->n_lens; i++)
        al->free(al->ud, f->names[i], f->lens[i] + 1);
    al->free(al->ud, f->names, f->n_names * sizeof *f->names);
    al->free(al->ud, f->lens,  f->n_lens * sizeof *f->lens);
    *f = (chc__fields) {0};
}

static int
chc__fields_grow(const chc_alloc *al, chc__fields *f, size_t n, chc_err *err)
{
    if (n <= f->n_names && n <= f->n_lens) return CHC_OK;
    size_t limit_names = SIZE_MAX / sizeof *f->names;
    size_t limit_lens = SIZE_MAX / sizeof *f->lens;
    size_t limit = limit_names < limit_lens ? limit_names : limit_lens;
    if (n > limit) return chc__err_set(err, CHC_ERR_OOM, "too many tuple fields");
    size_t next = f->n_names > limit / 2 ? limit : f->n_names * 2;
    if (next < 4) next = 4;
    if (next < n) next = n;
    char **nn = chc__realloc(al, f->names, f->n_names * sizeof *f->names,
                             next * sizeof *f->names, err);
    if (!nn) return CHC_ERR_OOM;
    f->names = nn;
    memset(f->names + f->n_names, 0, (next - f->n_names) * sizeof *f->names);
    f->n_names = next;
    size_t *nl = chc__realloc(al, f->lens, f->n_lens * sizeof *f->lens,
                              next * sizeof *f->lens, err);
    if (!nl) return CHC_ERR_OOM;
    f->lens = nl;
    memset(f->lens + f->n_lens, 0, (next - f->n_lens) * sizeof *f->lens);
    f->n_lens = next;
    return CHC_OK;
}

static int
chc__parse_type(chc__lex *lx, const chc_alloc *al,
                const char *whole_start, const char *whole_end,
                size_t depth, chc_type **out, chc_err *err)
{
    if (depth > CHC_MAX_TYPE_DEPTH)
        return chc__err_set(err, CHC_ERR_TYPE,
            "type nested too deeply (max %llu)",
            (unsigned long long) CHC_MAX_TYPE_DEPTH);

    chc__tok head = chc__eat_tok(lx);
    int64_t literal_number;
    bool literal = CHC_PHP_COMPAT &&
        (head.kind == CHC__TOK_STRING ||
         (head.kind == CHC__TOK_NUMBER && chc__atoi64(head.start, head.len, &literal_number)));
    if (!literal && (head.kind != CHC__TOK_NAME || head.quote))
        return chc__err_set(err, CHC_ERR_TYPE, "expected type name");

    chc_type *t = chc__calloc(al, sizeof *t, err);
    if (!t) return CHC_ERR_OOM;
    bool decimal_alias = (head.len == 7 && memcmp(head.start, "Decimal", 7) == 0);
    if (literal) {
        /* The old AST retained literal arguments for the column factory to reject. */
        t->kind = CHC_KIND_COUNT;
    } else if (decimal_alias) {
        t->kind = CHC_DECIMAL128;       /* placeholder; refined from precision */
    } else {
        const struct chc__name_row *row = chc__name_lookup(head.start, head.len);
        if (!row && !CHC_PHP_COMPAT) {
            chc_type_destroy(t, al);
            return chc__err_set(err, CHC_ERR_TYPE, "unknown type: %.*s",
                                (int) head.len, head.start);
        }
        t->kind = row ? (chc_kind) row->kind : CHC_KIND_COUNT;
        if (t->kind == CHC_INTERVAL) t->interval = (chc_interval_unit) row->unit;
    }

    const char *name_start = head.start;
    const char *name_end   = head.start + head.len;
    const char *canonical_name = NULL;

    /* ClickHouse defaults to milliseconds. */
    if (t->kind == CHC_DATETIME64 || t->kind == CHC_TIME64)
        t->temporal.scale = 3;

    /* Optional parameter list. */
    if (chc__peek_tok(lx).kind == CHC__TOK_LPAREN) {
        chc__eat_tok(lx);

        bool empty_arguments = CHC_PHP_COMPAT && chc__peek_tok(lx).kind == CHC__TOK_RPAREN;
        if (empty_arguments) {
            if (decimal_alias || t->kind == CHC_SIMPLE_AGGREGATE_FUNCTION)
                t->php_factory_error = CHC__PHP_FACTORY_CHILD_LAST;
            else if (t->kind == CHC_ARRAY || t->kind == CHC_NULLABLE)
                t->php_factory_error = CHC__PHP_FACTORY_CHILD_ZERO;
            else if (t->kind == CHC_FIXED_STRING)
                t->php_factory_error = CHC__PHP_FACTORY_FIXEDSTRING_ZERO;
            else if (t->kind == CHC_ENUM8 || t->kind == CHC_ENUM16 || t->kind == CHC_MAP)
                t->php_factory_error = CHC__PHP_FACTORY_CONTENTS;
        }

        if (empty_arguments && (t->php_factory_error || t->kind == CHC_DATETIME || t->kind == CHC_DATETIME64 ||
                t->kind == CHC_TIME64 || t->kind == CHC_DECIMAL32 ||
                t->kind == CHC_DECIMAL64 || t->kind == CHC_DECIMAL128)) {
            switch (t->kind) {
            case CHC_DATETIME: canonical_name = "DateTime"; break;
            case CHC_DATETIME64: canonical_name = "DateTime64(0)"; t->temporal.scale = 0; break;
            case CHC_TIME64: canonical_name = "Time64(0)"; t->temporal.scale = 0; break;
            case CHC_DECIMAL32: canonical_name = "Decimal(9,0)"; break;
            case CHC_DECIMAL64: canonical_name = "Decimal(18,0)"; break;
            case CHC_DECIMAL128:
                if (!decimal_alias) canonical_name = "Decimal(38,0)";
                break;
            default: break;
            }
        } else if (t->kind == CHC_ENUM8 || t->kind == CHC_ENUM16) {
            /* 'name' = value, 'name' = value, ... */
            while (chc__peek_tok(lx).kind != CHC__TOK_RPAREN) {
                chc__tok s = chc__eat_tok(lx);
                if (s.kind != CHC__TOK_STRING) {
                    chc_type_destroy(t, al);
                    return chc__err_set(err, CHC_ERR_TYPE, "Enum: expected quoted name");
                }
                chc__tok eq = chc__eat_tok(lx);
                if (eq.kind != CHC__TOK_EQ) {
                    chc_type_destroy(t, al);
                    return chc__err_set(err, CHC_ERR_TYPE, "Enum: expected '='");
                }
                chc__tok num = chc__eat_tok(lx);
                if (num.kind != CHC__TOK_NUMBER) {
                    chc_type_destroy(t, al);
                    return chc__err_set(err, CHC_ERR_TYPE, "Enum: expected value");
                }
                int64_t ev;
                int64_t lo = t->kind == CHC_ENUM8 ? INT8_MIN : INT16_MIN;
                int64_t hi = t->kind == CHC_ENUM8 ? INT8_MAX : INT16_MAX;
                if (!chc__atoi64(num.start, num.len, &ev) || ev < lo || ev > hi) {
                    chc_type_destroy(t, al);
                    return chc__err_set(err, CHC_ERR_TYPE,
                        "Enum: value out of range: %.*s",
                        (int) num.len, num.start);
                }
                int rc = chc__type_push_enum(al, t, s.start, s.len, ev, err);
                if (rc != CHC_OK) { chc_type_destroy(t, al); return rc; }
                if (chc__peek_tok(lx).kind == CHC__TOK_COMMA) chc__eat_tok(lx);
            }
        } else if (t->kind == CHC_FIXED_STRING) {
            chc__tok num = chc__eat_tok(lx);
            if (num.kind != CHC__TOK_NUMBER) {
                chc_type_destroy(t, al);
                return chc__err_set(err, CHC_ERR_TYPE, "FixedString: expected N");
            }
            int64_t n;
            bool parsed = chc__atoi64(num.start, num.len, &n);
            if (!parsed
                || n <= 0 || (uint64_t) n > CHC_MAX_FIXEDSTRING_SIZE) {
                chc_type_destroy(t, al);
                if (CHC_PHP_COMPAT && parsed)
                    return chc__err_set(err, CHC_ERR_USAGE,
                        "FixedString size must be between 1 and %llu, got %lld",
                        (unsigned long long) CHC_MAX_FIXEDSTRING_SIZE, (long long) n);
                return chc__err_set(err, CHC_ERR_TYPE,
                    "FixedString: N out of range: %.*s",
                    (int) num.len, num.start);
            }
            t->fixed_string.n = (int) n;
        } else if (decimal_alias) {
            chc__tok np = chc__eat_tok(lx);
            if (np.kind != CHC__TOK_NUMBER) {
                chc_type_destroy(t, al);
                return chc__err_set(err, CHC_ERR_TYPE, "Decimal: expected precision");
            }
            chc__tok cm = chc__eat_tok(lx);
            if (cm.kind != CHC__TOK_COMMA) {
                chc_type_destroy(t, al);
                return chc__err_set(err, CHC_ERR_TYPE, "Decimal: expected ','");
            }
            chc__tok ns = chc__eat_tok(lx);
            if (ns.kind != CHC__TOK_NUMBER) {
                chc_type_destroy(t, al);
                return chc__err_set(err, CHC_ERR_TYPE, "Decimal: expected scale");
            }
            int64_t prec, scale;
            if (!chc__atoi64(np.start, np.len, &prec)
                || !chc__atoi64(ns.start, ns.len, &scale)
                || prec < 1 || prec > 76 || scale < 0 || scale > prec) {
                chc_type_destroy(t, al);
                return chc__err_set(err, CHC_ERR_TYPE,
                    "Decimal: precision or scale out of range: %.*s, %.*s",
                    (int) np.len, np.start, (int) ns.len, ns.start);
            }
            t->decimal.precision = (int) prec;
            t->decimal.scale = (int) scale;
            if      (prec <=  9) t->kind = CHC_DECIMAL32;
            else if (prec <= 18) t->kind = CHC_DECIMAL64;
            else if (prec <= 38) t->kind = CHC_DECIMAL128;
            else                 t->kind = CHC_DECIMAL256;
        } else if (t->kind == CHC_DECIMAL32 || t->kind == CHC_DECIMAL64
                || t->kind == CHC_DECIMAL128 || t->kind == CHC_DECIMAL256) {
            chc__tok num = chc__eat_tok(lx);
            if (num.kind != CHC__TOK_NUMBER) {
                chc_type_destroy(t, al);
                return chc__err_set(err, CHC_ERR_TYPE, "Decimal: expected scale");
            }
            int64_t scale;
            if (!chc__atoi64(num.start, num.len, &scale)
                || scale < 0 || scale > chc_type_decimal_precision(t)) {
                chc_type_destroy(t, al);
                return chc__err_set(err, CHC_ERR_TYPE,
                    "Decimal: scale out of range: %.*s",
                    (int) num.len, num.start);
            }
            t->decimal.scale = (int) scale;
        } else if (t->kind == CHC_DATETIME64 || t->kind == CHC_TIME64) {
            if (chc__peek_tok(lx).kind != CHC__TOK_RPAREN) {
                chc__tok num = chc__eat_tok(lx);
                if (num.kind != CHC__TOK_NUMBER) {
                    chc_type_destroy(t, al);
                    return chc__err_set(err, CHC_ERR_TYPE, "DateTime64: expected precision");
                }
                int64_t scale;
                if (!chc__atoi64(num.start, num.len, &scale)
                    || scale < 0 || scale > 9) {
                    chc_type_destroy(t, al);
                    return chc__err_set(err, CHC_ERR_TYPE,
                        "DateTime64: precision out of range: %.*s",
                        (int) num.len, num.start);
                }
                t->temporal.scale = (int) scale;
            }
            if (chc__peek_tok(lx).kind == CHC__TOK_COMMA) {
                chc__eat_tok(lx);
                chc__tok s = chc__eat_tok(lx);
                if (s.kind != CHC__TOK_STRING) {
                    chc_type_destroy(t, al);
                    return chc__err_set(err, CHC_ERR_TYPE, "DateTime64: expected tz");
                }
                t->temporal.tz = chc__strdup(al, s.start, s.len, err);
                if (!t->temporal.tz) { chc_type_destroy(t, al); return CHC_ERR_OOM; }
                t->temporal.tz_len = s.len;
            }
        } else if (t->kind == CHC_DATETIME) {
            chc__tok s = chc__eat_tok(lx);
            if (s.kind != CHC__TOK_STRING) {
                chc_type_destroy(t, al);
                return chc__err_set(err, CHC_ERR_TYPE, "DateTime: expected tz");
            }
            t->temporal.tz = chc__strdup(al, s.start, s.len, err);
            if (!t->temporal.tz) { chc_type_destroy(t, al); return CHC_ERR_OOM; }
            t->temporal.tz_len = s.len;
        } else if (t->kind == CHC_JSON) {
            /* JSON options affect server storage but not JSON sent as strings */
            if (!chc__skip_params(lx)) {
                chc_type_destroy(t, al);
                return chc__err_set(err, CHC_ERR_TYPE,
                    "JSON: unterminated parameters");
            }
            lx->cur--;
        } else if (t->kind == CHC_OBJECT) {
            /* Object('name'), legacy JSON object syntax. Argument is schema
             * identifier (eg 'json'); any quoted string is accepted.
             * Discard argument & retain full source text in t->name
             * for round-trip & errors */
            chc__tok s = chc__eat_tok(lx);
            if (s.kind != CHC__TOK_STRING) {
                chc_type_destroy(t, al);
                return chc__err_set(err, CHC_ERR_TYPE, "Object: expected name");
            }
        } else if (t->kind == CHC_QBIT) {
            /* QBit(ElementType, N): element float type then positive dimension.
             * Wire form is Tuple(FixedString(ceil(N/8)) x element_size); keep
             * the element type as the sole child & stash N for the decoder. */
            chc_type *elem = NULL;
            int rc = chc__parse_type(lx, al, whole_start, whole_end, depth + 1, &elem, err);
            if (rc != CHC_OK) { chc_type_destroy(t, al); return rc; }
            rc = chc__type_push_child(al, t, elem, err);
            if (rc != CHC_OK) { chc_type_destroy(elem, al); chc_type_destroy(t, al); return rc; }
            if (elem->kind != CHC_BFLOAT16 && elem->kind != CHC_FLOAT32 && elem->kind != CHC_FLOAT64) {
                chc_type_destroy(t, al);
                return chc__err_set(err, CHC_ERR_TYPE,
                    "QBit: element type must be BFloat16, Float32, or Float64");
            }
            chc__tok cm = chc__eat_tok(lx);
            if (cm.kind != CHC__TOK_COMMA) {
                chc_type_destroy(t, al);
                return chc__err_set(err, CHC_ERR_TYPE, "QBit: expected ','");
            }
            chc__tok num = chc__eat_tok(lx);
            if (num.kind != CHC__TOK_NUMBER) {
                chc_type_destroy(t, al);
                return chc__err_set(err, CHC_ERR_TYPE, "QBit: expected dimension");
            }
            int64_t n;
            /* Nested FixedString width is ceil(N/8); bound it as FixedString is. */
            if (!chc__atoi64(num.start, num.len, &n)
                || n <= 0 || ((uint64_t) n + 7) / 8 > CHC_MAX_FIXEDSTRING_SIZE) {
                chc_type_destroy(t, al);
                return chc__err_set(err, CHC_ERR_TYPE,
                    "QBit: dimension out of range: %.*s",
                    (int) num.len, num.start);
            }
            t->qbit.dimension = (size_t) n;
        } else if (chc__kind_is_agg(t->kind)) {
            /* ClickHouse may include a version before an aggregate function name */
            if (chc__peek_tok(lx).kind == CHC__TOK_NUMBER) {
                chc__eat_tok(lx);
                if (chc__eat_tok(lx).kind != CHC__TOK_COMMA) {
                    chc_type_destroy(t, al);
                    return chc__err_set(err, CHC_ERR_TYPE,
                        "%.*s: expected ',' after version",
                        (int) head.len, head.start);
                }
            }
            chc__tok fn = chc__eat_tok(lx);
            if (fn.kind != CHC__TOK_NAME || fn.quote) {
                chc_type_destroy(t, al);
                return chc__err_set(err, CHC_ERR_TYPE,
                    "%.*s: expected function name", (int) head.len, head.start);
            }
            t->agg.func = chc__strdup(al, fn.start, fn.len, err);
            if (!t->agg.func) { chc_type_destroy(t, al); return CHC_ERR_OOM; }
            t->agg.func_len = fn.len;
            if (chc__peek_tok(lx).kind == CHC__TOK_LPAREN) {
                chc__eat_tok(lx);
                if (!chc__skip_params(lx)) {
                    chc_type_destroy(t, al);
                    return chc__err_set(err, CHC_ERR_TYPE,
                        "%.*s: %.*s parameters unterminated",
                        (int) head.len, head.start, (int) fn.len, fn.start);
                }
            }
            while (chc__peek_tok(lx).kind == CHC__TOK_COMMA) {
                chc__eat_tok(lx);
                chc_type *arg = NULL;
                int rc = chc__parse_type(lx, al, whole_start, whole_end,
                                         depth + 1, &arg, err);
                if (rc == CHC_OK) rc = chc__type_push_child(al, t, arg, err);
                if (rc != CHC_OK) {
                    chc_type_destroy(arg, al);
                    chc_type_destroy(t, al);
                    return rc;
                }
            }
            if (!t->n_children) {
                chc_type_destroy(t, al);
                return chc__err_set(err, CHC_ERR_TYPE,
                    "%.*s: %.*s has no argument type",
                    (int) head.len, head.start, (int) fn.len, fn.start);
            }
        } else {
            /* Tuple and Nested fields may have names before their types */
            bool        named     = (t->kind == CHC_TUPLE
                                     || t->kind == CHC_NESTED);
            bool        any_named = false;
            chc__fields fields    = {0};
            for (;;) {
                chc__tok la = chc__peek_tok(lx);
                if (la.kind == CHC__TOK_RPAREN) break;

                chc__tok field = {0};
                bool has_field = false;
                if (named && la.kind == CHC__TOK_NAME) {
                    chc__eat_tok(lx);
                    if (la.quote) {
                        /* `\`x\`` or `"x"` is never a type head, so it must be
                         * the field label. */
                        field = la;
                        has_field = true;
                    } else {
                        chc__tok la2 = chc__peek_tok(lx);
                        if (la2.kind == CHC__TOK_NAME) {
                            /* `la` is a field-name; `la2` starts the type. */
                            field = la;
                            has_field = true;
                        } else {
                            /* `la` was the type's leading NAME (terminal or
                             * parametric like `Tuple(LowCardinality(...))`).
                             * Put it back & rewind cur to la2's start so the
                             * next peek re-lexes la2. */
                            lx->peeked   = la;
                            lx->has_peek = true;
                            lx->cur      = la2.start;
                        }
                    }
                }

                chc_type *child = NULL;
                int rc = chc__parse_type(lx, al, whole_start, whole_end, depth + 1, &child, err);
                if (rc == CHC_OK) {
                    rc = chc__type_push_child(al, t, child, err);
                    if (rc == CHC_OK) child = NULL;     /* t owns it now */
                } else
                    child = NULL;
                if (rc == CHC_OK && named)
                    rc = chc__fields_grow(al, &fields, t->n_children, err);
                if (rc == CHC_OK && named && has_field) {
                    size_t flen = field.len;
                    char  *nm   = field.quote
                        ? chc__strdup_unquote(al, field.start, field.len,
                                              field.quote, &flen, err)
                        : chc__strdup(al, field.start, field.len, err);
                    if (nm) {
                        fields.names[t->n_children - 1] = nm;
                        fields.lens[t->n_children - 1]  = flen;
                        any_named = true;
                    } else
                        rc = CHC_ERR_OOM;
                }
                if (rc != CHC_OK) {
                    if (child) chc_type_destroy(child, al);
                    chc__fields_free(al, &fields);
                    chc_type_destroy(t, al);
                    return rc;
                }

                chc__tok c = chc__peek_tok(lx);
                if (c.kind == CHC__TOK_COMMA) { chc__eat_tok(lx); continue; }
                if (c.kind == CHC__TOK_RPAREN) break;
                chc__fields_free(al, &fields);
                chc_type_destroy(t, al);
                return chc__err_set(err, CHC_ERR_TYPE, "expected ',' or ')'");
            }
            if (any_named) {
                t->field_names     = fields.names;
                t->field_name_lens = fields.lens;
                t->fields_capacity = fields.n_names;
            } else
                chc__fields_free(al, &fields);
        }

        chc__tok rp = chc__eat_tok(lx);
        if (rp.kind != CHC__TOK_RPAREN) {
            chc_type_destroy(t, al);
            return chc__err_set(err, CHC_ERR_TYPE, "expected ')'");
        }
        name_end = rp.start + 1;
    }

    /* Decimal(P, S) compatibility: width selected by precision. */
    if (t->kind == CHC_DECIMAL128 && head.len == 7
        && memcmp(head.start, "Decimal", 7) == 0 && t->n_children == 0) {
        /* Treat Decimal without parameters as Decimal128 */
    }

    size_t stored_len = canonical_name ? strlen(canonical_name)
        : literal ? 0 : (size_t) (name_end - name_start);
    if (lx->spelling) {
        t->name = (char *) (canonical_name ? canonical_name : name_start);
        t->spelling = lx->spelling;
        ++t->spelling->refs;
    } else {
        t->name = chc__strdup(al, name_start, stored_len, err);
        if (!t->name) { chc_type_destroy(t, al); return CHC_ERR_OOM; }
    }
    t->name_len = stored_len;

    *out = t;
    return CHC_OK;
}

static int
chc__php_low_cardinality_error(const chc_type *nested, chc_err *err)
{
    const chc_type *inner = nested;
    bool nullable = nested->kind == CHC_NULLABLE;
    if (nullable && nested->n_children) inner = nested->children[0];
    size_t len = 0;
    while (len < inner->name_len && inner->name[len] != '(') ++len;
    return chc__err_set(err, CHC_ERR_USAGE,
        nullable ? "LowCardinality(Nullable(%.*s)) is not supported"
                 : "LowCardinality(%.*s) is not supported", (int) len, inner->name);
}

static bool
chc__php_terminal_kind(chc_kind kind)
{
    switch (kind) {
    case CHC_INT8: case CHC_INT16: case CHC_INT32: case CHC_INT64: case CHC_INT128:
    case CHC_UINT8: case CHC_UINT16: case CHC_UINT32: case CHC_UINT64: case CHC_UINT128:
    case CHC_FLOAT32: case CHC_FLOAT64: case CHC_BOOL: case CHC_NOTHING: case CHC_VOID:
    case CHC_STRING: case CHC_FIXED_STRING: case CHC_DATE: case CHC_DATE32:
    case CHC_DATETIME: case CHC_DATETIME64: case CHC_TIME: case CHC_TIME64:
    case CHC_DECIMAL32: case CHC_DECIMAL64: case CHC_DECIMAL128:
    case CHC_UUID: case CHC_IPV4: case CHC_IPV6: case CHC_JSON:
    case CHC_POINT: case CHC_RING: case CHC_POLYGON: case CHC_MULTI_POLYGON:
        return true;
    default:
        return false;
    }
}

/* Match the previous column factory before any column payload is consumed. */
static int
chc__php_factory_type(chc_type **slot, const chc_alloc *al, chc_err *err)
{
    chc_type *t = *slot;
    switch (t->php_factory_error) {
    case CHC__PHP_FACTORY_CHILD_ZERO:
        return chc__err_set(err, CHC_ERR_USAGE, "AST child element index out of bounds: 0");
    case CHC__PHP_FACTORY_CHILD_LAST:
        return chc__err_set(err, CHC_ERR_USAGE, "AST child element index out of bounds: -1");
    case CHC__PHP_FACTORY_FIXEDSTRING_ZERO:
        return chc__err_set(err, CHC_ERR_USAGE,
            "FixedString size must be between 1 and %llu, got 0",
            (unsigned long long) CHC_MAX_FIXEDSTRING_SIZE);
    case CHC__PHP_FACTORY_CONTENTS:
        return chc__err_set(err, CHC_ERR_USAGE, "%s content is not correct",
            t->kind == CHC_ENUM8 ? "Enum8" : t->kind == CHC_ENUM16 ? "Enum16" : "Map");
    default: break;
    }
    if (t->kind == CHC_TUPLE && !t->n_children)
        return chc__err_set(err, CHC_ERR_USAGE, "AST child element index out of bounds: 0");
    if (t->kind == CHC_DATETIME64 || t->kind == CHC_TIME64 ||
        t->kind == CHC_DECIMAL32 || t->kind == CHC_DECIMAL64 ||
        t->kind == CHC_DECIMAL128 || t->kind == CHC_FIXED_STRING) {
        if (!memchr(t->name, '(', t->name_len)) {
            if (t->kind == CHC_DATETIME64 || t->kind == CHC_TIME64) return CHC_ERR_TYPE;
            return chc__err_set(err, CHC_ERR_USAGE, "AST child element index out of bounds: 0");
        }
    }
    if (t->kind == CHC_SIMPLE_AGGREGATE_FUNCTION) {
        if (!t->n_children || !chc__php_terminal_kind(t->children[t->n_children - 1]->kind))
            return CHC_ERR_TYPE;
        size_t last = t->n_children - 1;
        int rc = chc__php_factory_type(&t->children[last], al, err);
        if (rc != CHC_OK) return rc;
        *slot = t->children[last];
        t->children[last] = NULL;
        chc_type_destroy(t, al);
        return CHC_OK;
    }
    if (t->kind == CHC_LOW_CARDINALITY) {
        if (t->n_children != 1) return CHC_ERR_TYPE;
        chc_type *nested = t->children[0];
        if (nested->kind != CHC_STRING && nested->kind != CHC_FIXED_STRING && nested->kind != CHC_NULLABLE)
            return chc__php_low_cardinality_error(nested, err);
        int rc = chc__php_factory_type(&t->children[0], al, err);
        if (rc == CHC_ERR_TYPE) return chc__php_low_cardinality_error(nested, err);
        if (rc != CHC_OK) return rc;
        const chc_type *dictionary = t->children[0];
        while (dictionary->kind == CHC_NULLABLE) dictionary = dictionary->children[0];
        if (dictionary->kind != CHC_STRING && dictionary->kind != CHC_FIXED_STRING)
            return chc__err_set(err, CHC_ERR_USAGE,
                "Unexpected dictionary column type: %.*s", (int) dictionary->name_len, dictionary->name);
        return CHC_OK;
    }
    if ((t->kind == CHC_ARRAY || t->kind == CHC_NULLABLE) && t->n_children != 1)
        return CHC_ERR_TYPE;
    if (t->kind == CHC_MAP && t->n_children != 2) return CHC_ERR_TYPE;
    if (!chc__php_terminal_kind(t->kind) && t->kind != CHC_ARRAY && t->kind != CHC_NULLABLE &&
        t->kind != CHC_TUPLE && t->kind != CHC_MAP && t->kind != CHC_ENUM8 && t->kind != CHC_ENUM16)
        return CHC_ERR_TYPE;
    for (size_t i = 0; i < t->n_children; ++i) {
        int rc = chc__php_factory_type(&t->children[i], al, err);
        if (rc != CHC_OK) return rc;
    }
    return CHC_OK;
}

static int
chc__php_parse_error(const char *name, size_t len, chc_err *err)
{
    return chc__err_set(err, CHC_ERR_TYPE, "unsupported column type: %.*s", (int) len, name);
}

int
chc_type_parse(const char *name, size_t name_len,
               const chc_alloc *al, chc_type **out, chc_err *err)
{
    *out = NULL;
    chc__type_spelling *spelling = NULL;
    const char *input = name;
    if (CHC_PHP_COMPAT) {
        if (name_len > SIZE_MAX - sizeof *spelling - 1)
            return chc__err_set(err, CHC_ERR_OOM, "type spelling size overflow");
        size_t bytes = sizeof *spelling + name_len + 1;
        spelling = chc__alloc(al, bytes, err);
        if (!spelling) return CHC_ERR_OOM;
        spelling->refs = 1;
        spelling->bytes = bytes;
        char *data = (char *) (spelling + 1);
        memcpy(data, name, name_len);
        data[name_len] = '\0';
        input = data;
    }
    chc__lex lx = { input, input + name_len, {0}, false, spelling };
    int rc = chc__parse_type(&lx, al, input, input + name_len, 0, out, err);
    chc__type_spelling_release(spelling, al);
    if (rc != CHC_OK) {
        if (CHC_PHP_COMPAT && rc == CHC_ERR_TYPE) return chc__php_parse_error(name, name_len, err);
        return rc;
    }
    chc__tok tail = chc__eat_tok(&lx);
    if (tail.kind != CHC__TOK_EOS) {
        chc_type_destroy(*out, al);
        *out = NULL;
        return CHC_PHP_COMPAT ? chc__php_parse_error(name, name_len, err)
                             : chc__err_set(err, CHC_ERR_TYPE, "trailing tokens in type name");
    }
    if (CHC_PHP_COMPAT) {
        rc = chc__php_factory_type(out, al, err);
        if (rc != CHC_OK) {
            chc_type_destroy(*out, al);
            *out = NULL;
            return rc == CHC_ERR_TYPE ? chc__php_parse_error(name, name_len, err) : rc;
        }
    }
    return CHC_OK;
}

size_t
chc_type_format(const chc_type *t, char *buf, size_t buf_len)
{
    if (!t) return 0;
    if (t->name && t->name_len) {
        if (buf && buf_len) {
            size_t take = t->name_len < buf_len - 1 ? t->name_len : buf_len - 1;
            memcpy(buf, t->name, take);
            buf[take] = '\0';
        }
        return t->name_len;
    }
    return 0;
}

/* -------- column internals ---------- */
/* Public API defines struct chc_column */

chc_col_kind chc_column_layout(const chc_column *c) { return c ? c->layout : (chc_col_kind) 0; }
size_t       chc_column_n_rows(const chc_column *c) { return c ? c->n_rows : 0; }

const void *chc_column_fixed_data(const chc_column *c, size_t *elem_size)
{
    if (!c || c->layout != CHC_COL_FIXED) { if (elem_size) *elem_size = 0; return NULL; }
    if (elem_size) *elem_size = c->fixed.elem_size;
    return c->fixed.data;
}
const uint8_t  *chc_column_string_data(const chc_column *c)
    { return (c && c->layout == CHC_COL_STRING) ? c->str.data : NULL; }
const uint64_t *chc_column_string_offsets(const chc_column *c)
    { return (c && c->layout == CHC_COL_STRING) ? c->str.offsets : NULL; }
const uint8_t    *chc_column_null_map(const chc_column *c)
    { return (c && c->layout == CHC_COL_NULLABLE) ? c->nullable.null_map : NULL; }
const chc_column *chc_column_nullable_inner(const chc_column *c)
    { return (c && c->layout == CHC_COL_NULLABLE) ? c->nullable.inner : NULL; }
const uint64_t   *chc_column_array_offsets(const chc_column *c)
    { return (c && c->layout == CHC_COL_ARRAY) ? c->array.offsets : NULL; }
const chc_column *chc_column_array_values(const chc_column *c)
    { return (c && c->layout == CHC_COL_ARRAY) ? c->array.values : NULL; }
size_t            chc_column_tuple_arity(const chc_column *c)
    { return (c && c->layout == CHC_COL_TUPLE) ? c->tuple.arity : 0; }
const chc_column *chc_column_tuple_child(const chc_column *c, size_t i)
    { return (c && c->layout == CHC_COL_TUPLE && i < c->tuple.arity) ? c->tuple.children[i] : NULL; }
int               chc_column_lc_key_size(const chc_column *c)
    { return (c && c->layout == CHC_COL_LOW_CARDINALITY) ? c->lc.key_size : 0; }
const void       *chc_column_lc_keys(const chc_column *c)
    { return (c && c->layout == CHC_COL_LOW_CARDINALITY) ? c->lc.keys : NULL; }
const chc_column *chc_column_lc_dict(const chc_column *c)
    { return (c && c->layout == CHC_COL_LOW_CARDINALITY) ? c->lc.dict : NULL; }

static void chc__column_destroy(chc_column *c, const chc_alloc *al);

static void
chc__column_destroy(chc_column *c, const chc_alloc *al)
{
    if (!c) return;
    switch (c->layout) {
    case CHC_COL_FIXED:
        al->free(al->ud, c->fixed.data, c->n_rows * c->fixed.elem_size);
        break;
    case CHC_COL_STRING:
        al->free(al->ud, c->str.data, c->str.bytes);
        al->free(al->ud, c->str.offsets, c->n_rows * sizeof(uint64_t));
        break;
    case CHC_COL_NULLABLE:
        al->free(al->ud, c->nullable.null_map, c->n_rows);
        chc__column_destroy(c->nullable.inner, al);
        break;
    case CHC_COL_ARRAY:
        al->free(al->ud, c->array.offsets, c->n_rows * sizeof(uint64_t));
        chc__column_destroy(c->array.values, al);
        break;
    case CHC_COL_TUPLE:
        for (size_t i = 0; i < c->tuple.arity; i++)
            chc__column_destroy(c->tuple.children[i], al);
        al->free(al->ud, c->tuple.children, c->tuple.arity * sizeof *c->tuple.children);
        break;
    case CHC_COL_LOW_CARDINALITY:
        al->free(al->ud, c->lc.keys, c->n_rows * c->lc.key_size);
        chc__column_destroy(c->lc.dict, al);
        break;
    case CHC_COL_NOTHING:
        break;
    }
    al->free(al->ud, c, sizeof *c);
}

void
chc_column_destroy(chc_column *c, const chc_alloc *al)
{
    chc__column_destroy(c, al);
}

int
chc_column_validate(const chc_column *c, chc_err *err)
{
    if (!c) return CHC_OK;
    switch (c->layout) {
    case CHC_COL_ARRAY: {
        const uint64_t *offs = c->array.offsets;
        uint64_t prev = 0;
        for (size_t i = 0; i < c->n_rows; i++) {
            if (offs[i] < prev)
                return chc__err_set(err, CHC_ERR_PROTOCOL,
                    "array offsets not monotonic at row %zu: %llu < %llu",
                    i, (unsigned long long) offs[i], (unsigned long long) prev);
            prev = offs[i];
        }
        return chc_column_validate(c->array.values, err);
    }
    case CHC_COL_LOW_CARDINALITY: {
        size_t dn = c->lc.dict_n;
        const void *k = c->lc.keys;
        for (size_t i = 0; i < c->n_rows; i++) {
            uint64_t v;
            switch (c->lc.key_size) {
            case 1: v = ((const uint8_t  *) k)[i]; break;
            case 2: v = ((const uint16_t *) k)[i]; break;
            case 4: v = ((const uint32_t *) k)[i]; break;
            case 8: v = ((const uint64_t *) k)[i]; break;
            default:
                return chc__err_set(err, CHC_ERR_PROTOCOL,
                    "LowCardinality: invalid key_size %d", c->lc.key_size);
            }
            if (v >= dn)
                return chc__err_set(err, CHC_ERR_PROTOCOL,
                    "LowCardinality key out of range at row %zu: %llu >= dict_n %zu",
                    i, (unsigned long long) v, dn);
        }
        return chc_column_validate(c->lc.dict, err);
    }
    case CHC_COL_NULLABLE:
        return chc_column_validate(c->nullable.inner, err);
    case CHC_COL_TUPLE:
        for (size_t i = 0; i < c->tuple.arity; i++) {
            int rc = chc_column_validate(c->tuple.children[i], err);
            if (rc != CHC_OK) return rc;
        }
        return CHC_OK;
    default:
        return CHC_OK;
    }
}

/* -------- column reader (recursive on type kind) ---------- */

/* Elem-size table for FIXED kinds. Returns 0 if `t` isn't a FIXED kind. */
size_t
chc_type_elem_size(const chc_type *t)
{
    switch (t->kind) {
    case CHC_INT8:  case CHC_UINT8:  case CHC_BOOL:                 return 1;
    case CHC_INT16: case CHC_UINT16: case CHC_DATE:
    case CHC_ENUM16: case CHC_BFLOAT16:                             return 2;
    case CHC_INT32: case CHC_UINT32: case CHC_DATE32:
    case CHC_DATETIME: case CHC_FLOAT32: case CHC_DECIMAL32:
    case CHC_TIME: case CHC_IPV4:                                   return 4;
    case CHC_INT64: case CHC_UINT64: case CHC_DATETIME64:
    case CHC_FLOAT64: case CHC_DECIMAL64: case CHC_TIME64:
    case CHC_INTERVAL:                                              return 8;
    case CHC_INT128: case CHC_UINT128: case CHC_DECIMAL128:
    case CHC_UUID: case CHC_IPV6:                                   return 16;
    case CHC_INT256: case CHC_UINT256: case CHC_DECIMAL256:         return 32;
    case CHC_ENUM8:                                                 return 1;
    case CHC_FIXED_STRING:                                          return (size_t) t->fixed_string.n;
    default: return 0;
    }
}

/* LowCardinality on-wire flag word constants. */
#define CHC__LC_INDEX_TYPE_MASK     0xffu
#define CHC__LC_NEED_GLOBAL_DICT    (1u << 8)
#define CHC__LC_HAS_ADDITIONAL_KEYS (1u << 9)
#define CHC__LC_NEED_UPDATE_DICT    (1u << 10)

static int chc__col_read(chc_in *in, const chc_type *t,
                         size_t n_rows, chc_column **out, chc_err *err);

static int
chc__col_read_fixed(chc_in *in, size_t elem_size, size_t n_rows,
                    chc_column **out, chc_err *err)
{
    const chc_alloc *al = in->al;
    chc_column *c = chc__calloc(al, sizeof *c, err);
    if (!c) return CHC_ERR_OOM;
    c->layout = CHC_COL_FIXED;
    c->n_rows = n_rows;
    c->fixed.elem_size = elem_size;
    if (n_rows && elem_size) {
        size_t nbytes;
        if (chc__mul_size(n_rows, elem_size, &nbytes)) {
            chc__column_destroy(c, al);
            return chc__err_set(err, CHC_ERR_PROTOCOL, "column size overflow");
        }
        int rc = chc__read_allocated(in, nbytes, &c->fixed.data, err);
        if (rc != CHC_OK) { chc__column_destroy(c, al); return rc; }
    }
    *out = c;
    return CHC_OK;
}

static int
chc__col_read_string(chc_in *in, size_t n_rows,
                     chc_column **out, chc_err *err)
{
    const chc_alloc *al = in->al;
    chc_column *c = chc__calloc(al, sizeof *c, err);
    if (!c) return CHC_ERR_OOM;
    c->layout = CHC_COL_STRING;
    c->n_rows = n_rows;
    if (!n_rows) { *out = c; return CHC_OK; }
    size_t offs_bytes;
    if (chc__mul_size(n_rows, sizeof(uint64_t), &offs_bytes)) {
        chc__column_destroy(c, al);
        return chc__err_set(err, CHC_ERR_PROTOCOL, "string column size overflow");
    }
    size_t offsets_cap = 0;
    size_t cap = 256;
    size_t total = 0;
    c->str.data = chc__alloc(al, cap, err);
    if (!c->str.data) { chc__column_destroy(c, al); return CHC_ERR_OOM; }
    c->str.bytes = cap;
    for (size_t i = 0; i < n_rows; i++) {
        uint64_t len;
        int rc = chc__read_varuint(in, &len, err);
        if (rc == CHC_OK && (len > CHC_MAX_STRING_SIZE || len > SIZE_MAX - total))
            rc = chc__err_set(err, CHC_ERR_PROTOCOL, "string row too long");
        if (rc == CHC_OK)
            rc = chc__read_growing(in, &c->str.data, &c->str.bytes,
                                   total, total + (size_t) len, 256, true, err);
        if (rc == CHC_OK && (i + 1) * sizeof(uint64_t) > offsets_cap) {
            size_t next = offsets_cap > offs_bytes / 8 ? offs_bytes : offsets_cap * 8;
            if (next < (1u << 20)) next = 1u << 20;
            if (next > offs_bytes) next = offs_bytes;
            uint64_t *grown = chc__realloc(al, c->str.offsets, offsets_cap, next, err);
            if (!grown) rc = CHC_ERR_OOM;
            else { c->str.offsets = grown; offsets_cap = next; }
        }
        if (rc != CHC_OK) {
            al->free(al->ud, c->str.offsets, offsets_cap);
            c->str.offsets = NULL;
            chc__column_destroy(c, al);
            return rc;
        }
        total += (size_t) len;
        c->str.offsets[i] = total;
    }
    *out = c;
    return CHC_OK;
}

/* Composite columns might have a prefix sub-stream, emitted for whole
 * column tree before any body bytes. In formats handled here:
 * LowCardinality has a uint64 key version, JSON a uint64 serialization
 * version, legacy Object a uint8 serialization kind */
static int
chc__col_read_prefix(chc_in *in, const chc_type *t, chc_err *err)
{
    if (t->kind == CHC_LOW_CARDINALITY) {
        uint64_t v;
        int rc = chc__read_u64_le(in, &v, err);
        if (rc != CHC_OK) return rc;
        if (v != 1)
            return chc__err_set(err, CHC_ERR_PROTOCOL,
                "LowCardinality: unexpected key version %llu", (unsigned long long) v);
        return CHC_OK;
    }
    if (t->kind == CHC_JSON) {
        /* Only STRING (=1) in scope; other versions need the consumer to
         * set output_format_native_write_json_as_string=1 on the SELECT */
        uint64_t v;
        int rc = chc__read_u64_le(in, &v, err);
        if (rc != CHC_OK) return rc;
        if (v != 1)
            return chc__err_set(err, CHC_ERR_TYPE,
                "unsupported JSON serialization version %llu "
                "(set output_format_native_write_json_as_string=1)",
                (unsigned long long) v);
        return CHC_OK;
    }
    if (t->kind == CHC_OBJECT) {
        /* Legacy Object Native format uses a one-byte kind: TUPLE=0,
         * STRING=1. Tuple decoding needs its dynamic type descriptor */
        uint8_t kind;
        int rc = chc__read_byte(in, &kind, err);
        if (rc != CHC_OK) return rc;
        if (kind != 1)
            return chc__err_set(err, CHC_ERR_TYPE,
                "unsupported Object serialization kind %u (STRING=1 required)",
                (unsigned) kind);
        return CHC_OK;
    }
    if (t->kind == CHC_NULLABLE || t->kind == CHC_ARRAY
        || t->kind == CHC_TUPLE || t->kind == CHC_MAP
        || t->kind == CHC_NESTED
        || t->kind == CHC_SIMPLE_AGGREGATE_FUNCTION) {
        for (size_t i = 0; i < t->n_children; i++) {
            int rc = chc__col_read_prefix(in, t->children[i], err);
            if (rc != CHC_OK) return rc;
        }
    }
    return CHC_OK;
}

/* Geo types alias nested Arrays over Point = Tuple(Float64, Float64),
 * as in the server's type factory. Never mutated */
static chc_type  chc__geo_f64 = { .kind = CHC_FLOAT64, .name = (char *) "Float64", .name_len = 7 };
static chc_type *chc__geo_point_elems[] = { &chc__geo_f64, &chc__geo_f64 };
static chc_type  chc__geo_point = {
    .kind = CHC_TUPLE, .name = (char *) "Point", .name_len = 5,
    .n_children = 2, .children = chc__geo_point_elems,
};
static chc_type *chc__geo_ring_elems[] = { &chc__geo_point };
static chc_type  chc__geo_ring = {
    .kind = CHC_ARRAY, .name = (char *) "Array(Point)", .name_len = 12,
    .n_children = 1, .children = chc__geo_ring_elems,
};
static chc_type *chc__geo_polygon_elems[] = { &chc__geo_ring };
static chc_type  chc__geo_polygon = {
    .kind = CHC_ARRAY, .name = (char *) "Array(Array(Point))", .name_len = 19,
    .n_children = 1, .children = chc__geo_polygon_elems,
};
static chc_type *chc__geo_multi_polygon_elems[] = { &chc__geo_polygon };
static chc_type  chc__geo_multi_polygon = {
    .kind = CHC_ARRAY, .name = (char *) "Array(Array(Array(Point)))", .name_len = 26,
    .n_children = 1, .children = chc__geo_multi_polygon_elems,
};

static const chc_type *
chc__geo_alias(const chc_type *t)
{
    switch (t->kind) {
    case CHC_POINT:             return &chc__geo_point;
    case CHC_RING:
    case CHC_LINE_STRING:       return &chc__geo_ring;
    case CHC_POLYGON:
    case CHC_MULTI_LINE_STRING: return &chc__geo_polygon;
    case CHC_MULTI_POLYGON:     return &chc__geo_multi_polygon;
    default:                    return t;
    }
}

/* Resolve types serialized as another type, SimpleAggregateFunction stores
 * values using first argument type */
static const chc_type *
chc__serial_type(const chc_type *t)
{
    while (t->kind == CHC_SIMPLE_AGGREGATE_FUNCTION && t->n_children)
        t = t->children[0];
    return chc__geo_alias(t);
}

/* Serialization kind, DEFAULT is 0. 23.3 ISerialization::Kind & current
 * KindStackBinarySerializationType agree on both */
#define CHC__KIND_SPARSE 1u

/* Final sparse offset group carries this flag (Native Format spec,
 * kind_stack & sparse encoding). */
#define CHC__SPARSE_END (UINT64_C(1) << 62)

static int chc__col_read_kinds(chc_in *in, const chc_type *t, size_t n_rows,
                               const uint8_t **kinds, chc_column **out,
                               chc_err *err);

/* Byte-swap a host-typed uint64/keys array in place on BE hosts. No-op on LE. */
static void
chc__swap_offsets(CHC_MAYBE_UNUSED uint64_t *p, CHC_MAYBE_UNUSED size_t n)
{
#if CHC_BIG_ENDIAN
    for (size_t i = 0; i < n; i++) p[i] = chc__bswap64(p[i]);
#endif
}

static void
chc__swap_keys(CHC_MAYBE_UNUSED void *p, CHC_MAYBE_UNUSED size_t n,
               CHC_MAYBE_UNUSED int key_size)
{
#if CHC_BIG_ENDIAN
    switch (key_size) {
    case 1: break;
    case 2: { uint16_t *a = p; for (size_t i = 0; i < n; i++) a[i] = chc__bswap16(a[i]); break; }
    case 4: { uint32_t *a = p; for (size_t i = 0; i < n; i++) a[i] = chc__bswap32(a[i]); break; }
    case 8: { uint64_t *a = p; for (size_t i = 0; i < n; i++) a[i] = chc__bswap64(a[i]); break; }
    }
#endif
}

/* Read t's children as Tuple column, serving Tuple, Map & Nested values.
 * kinds NULL when dense */
static int
chc__col_read_tuple(chc_in *in, const chc_type *t, size_t n_rows,
                    const uint8_t **kinds, chc_column **out, chc_err *err)
{
    const chc_alloc *al = in->al;
    chc_column *c = chc__calloc(al, sizeof *c, err);
    if (!c) return CHC_ERR_OOM;
    c->layout = CHC_COL_TUPLE;
    c->n_rows = n_rows;
    if (t->n_children) {
        c->tuple.children = chc__calloc(al, t->n_children * sizeof *c->tuple.children, err);
        if (!c->tuple.children) { chc__column_destroy(c, al); return CHC_ERR_OOM; }
        c->tuple.arity = t->n_children;
    }
    for (size_t i = 0; i < t->n_children; i++) {
        chc_column **slot = &c->tuple.children[i];
        int rc = kinds ? chc__col_read_kinds(in, t->children[i], n_rows, kinds, slot, err)
                       : chc__col_read(in, t->children[i], n_rows, slot, err);
        if (rc != CHC_OK) { chc__column_destroy(c, al); return rc; }
    }
    *out = c;
    return CHC_OK;
}

/* Sparse body: varuint gap groups locating non-default rows, then those rows'
 * values densely. Gaps become zero bytes or empty strings, matching
 * ColumnSparse storage default even where type default differs (Enum) */
static int
chc__col_read_sparse(chc_in *in, const chc_type *t, size_t n_rows,
                     chc_column **out, chc_err *err)
{
    const chc_alloc *al = in->al;
    size_t es = chc_type_elem_size(t);
    /* Bounds positions, dense values & string offsets alike */
    size_t unit = es > sizeof(uint64_t) ? es : sizeof(uint64_t), bytes;
    if (chc__mul_size(n_rows, unit, &bytes))
        return chc__err_set(err, CHC_ERR_PROTOCOL, "column size overflow");

    size_t *pos = NULL, n = 0, cap = 0, row = 0;
    chc_column *c = NULL;
    int rc;
    for (;;) {
        uint64_t g;
        if ((rc = chc__read_varuint(in, &g, err))) goto fail;
        bool end = g & CHC__SPARSE_END;
        g &= ~CHC__SPARSE_END;
        if (g >= CHC__SPARSE_END || g > n_rows - row) {
            rc = chc__err_set(err, CHC_ERR_PROTOCOL,
                "sparse offsets overrun %zu rows", n_rows);
            goto fail;
        }
        row += (size_t) g;
        if (end) break;
        if (row == n_rows) {
            rc = chc__err_set(err, CHC_ERR_PROTOCOL,
                "sparse offsets overrun %zu rows", n_rows);
            goto fail;
        }
        if (n == cap) {
            size_t ncap = cap ? cap * 2 : 64;
            if (ncap > n_rows) ncap = n_rows;
            size_t *np = chc__realloc(al, pos, cap * sizeof *pos, ncap * sizeof *pos, err);
            if (!np) { rc = CHC_ERR_OOM; goto fail; }
            pos = np;
            cap = ncap;
        }
        pos[n++] = row++;
    }
    if (row != n_rows) {
        rc = chc__err_set(err, CHC_ERR_PROTOCOL,
            "sparse offsets end at row %zu of %zu", row, n_rows);
        goto fail;
    }

    rc = chc__col_read(in, t, n, &c, err);
    if (rc != CHC_OK) goto fail;

    if (n < n_rows && c->layout == CHC_COL_FIXED) {
        uint8_t *dense = chc__calloc(al, n_rows * es, err);
        if (!dense) { rc = CHC_ERR_OOM; goto fail; }
        const uint8_t *src = c->fixed.data;
        for (size_t k = 0; k < n; k++)
            memcpy(dense + pos[k] * es, src + k * es, es);
        al->free(al->ud, c->fixed.data, n * es);
        c->fixed.data = dense;
    } else if (n < n_rows && c->layout == CHC_COL_STRING) {
        uint64_t *offs = chc__alloc(al, n_rows * sizeof *offs, err);
        if (!offs) { rc = CHC_ERR_OOM; goto fail; }
        uint64_t end = 0;
        for (size_t r = 0, k = 0; r < n_rows; r++) {
            if (k < n && pos[k] == r) end = c->str.offsets[k++];
            offs[r] = end;
        }
        al->free(al->ud, c->str.offsets, n * sizeof *c->str.offsets);
        c->str.offsets = offs;
    }
    c->n_rows = n_rows;
    al->free(al->ud, pos, cap * sizeof *pos);
    *out = c;
    return CHC_OK;

fail:
    chc__column_destroy(c, al);
    al->free(al->ud, pos, cap * sizeof *pos);
    return rc;
}

/* Dispatch on serialization kinds read by chc__read_kinds, advancing cursor
 * in same order: Tuple own kind, ignored as ClickHouse ignores it, then each
 * element's */
static int
chc__col_read_kinds(chc_in *in, const chc_type *t, size_t n_rows,
                    const uint8_t **kinds, chc_column **out, chc_err *err)
{
    t = chc__serial_type(t);
    uint8_t kind = *(*kinds)++;
    if (t->kind == CHC_TUPLE) return chc__col_read_tuple(in, t, n_rows, kinds, out, err);
    if (kind == CHC__KIND_SPARSE) return chc__col_read_sparse(in, t, n_rows, out, err);
    return chc__col_read(in, t, n_rows, out, err);
}

static int
chc__col_read(chc_in *in, const chc_type *t,
              size_t n_rows, chc_column **out, chc_err *err)
{
    const chc_alloc *al = in->al;
    /* Tier 1 / FIXED scalar */
    size_t es = chc_type_elem_size(t);
    if (CHC_PHP_COMPAT && t->kind == CHC_FIXED_STRING && es && n_rows > SIZE_MAX / es)
        return chc__err_set(err, CHC_ERR_PROTOCOL,
            "FixedString(%zu) column with %zu rows exceeds the addressable size", es, n_rows);
    if (es) return chc__col_read_fixed(in, es, n_rows, out, err);

    switch (t->kind) {
    case CHC_STRING:
    case CHC_JSON:
    case CHC_OBJECT:
        /* JSON/Object STRING-mode body rows use writeStringBinary, identical
         * to String. Prefix lives in chc__col_read_prefix */
        return chc__col_read_string(in, n_rows, out, err);

    case CHC_NULLABLE: {
        if (t->n_children != 1)
            return chc__err_set(err, CHC_ERR_TYPE, "Nullable expects 1 child");
        chc_column *c = chc__calloc(al, sizeof *c, err);
        if (!c) return CHC_ERR_OOM;
        c->layout = CHC_COL_NULLABLE;
        c->n_rows = n_rows;
        if (n_rows) {
            void *null_map = NULL;
            int rc = chc__read_allocated(in, n_rows, &null_map, err);
            c->nullable.null_map = null_map;
            if (rc != CHC_OK) { chc__column_destroy(c, al); return rc; }
        }
        int rc = chc__col_read(in, t->children[0], n_rows, &c->nullable.inner, err);
        if (rc != CHC_OK) { chc__column_destroy(c, al); return rc; }
        *out = c;
        return CHC_OK;
    }

    case CHC_ARRAY:
    case CHC_NESTED:
    case CHC_MAP: {
        if (t->kind == CHC_ARRAY && t->n_children != 1)
            return chc__err_set(err, CHC_ERR_TYPE, "Array expects 1 child");
        if (t->kind == CHC_MAP && t->n_children != 2)
            return chc__err_set(err, CHC_ERR_TYPE, "Map expects 2 children");
        if (t->kind == CHC_NESTED && !t->n_children)
            return chc__err_set(err, CHC_ERR_TYPE, "Nested has no fields");
        chc_column *c = chc__calloc(al, sizeof *c, err);
        if (!c) return CHC_ERR_OOM;
        c->layout = CHC_COL_ARRAY;
        c->n_rows = n_rows;
        uint64_t total = 0;
        if (n_rows) {
            size_t offs_bytes;
            if (chc__mul_size(n_rows, sizeof(uint64_t), &offs_bytes)) {
                chc__column_destroy(c, al);
                return chc__err_set(err, CHC_ERR_PROTOCOL, "array offsets size overflow");
            }
            void *offsets = NULL;
            int rc = chc__read_allocated(in, offs_bytes, &offsets, err);
            c->array.offsets = offsets;
            if (rc != CHC_OK) { chc__column_destroy(c, al); return rc; }
            chc__swap_offsets(c->array.offsets, n_rows);
            if (CHC_PHP_COMPAT) {
                uint64_t previous = 0;
                for (size_t row = 0; row < n_rows; ++row) {
                    uint64_t offset = c->array.offsets[row];
                    if (offset < previous) {
                        chc__column_destroy(c, al);
                        return chc__err_set(err, CHC_ERR_PROTOCOL,
                            "Array offsets must be non-decreasing: offset %llu at row %zu is less than previous offset %llu",
                            (unsigned long long) offset, row, (unsigned long long) previous);
                    }
                    previous = offset;
                }
            }
            total = c->array.offsets[n_rows - 1];
            if (total > CHC_MAX_NUM_ROWS || total > SIZE_MAX) {
                chc__column_destroy(c, al);
                return chc__err_set(err, CHC_ERR_PROTOCOL,
                    "array nested length too large: %llu",
                    (unsigned long long) total);
            }
        }
        /* ClickHouse sends Map and Nested values as arrays of tuples */
        int rc = t->kind == CHC_ARRAY
            ? chc__col_read(in, t->children[0], (size_t) total, &c->array.values, err)
            : chc__col_read_tuple(in, t, (size_t) total, NULL, &c->array.values, err);
        if (rc != CHC_OK) { chc__column_destroy(c, al); return rc; }
        *out = c;
        return CHC_OK;
    }

    case CHC_TUPLE:
        return chc__col_read_tuple(in, t, n_rows, NULL, out, err);

    case CHC_QBIT: {
        /* Wire form is Tuple(FixedString(ceil(N/8)) x element_size): one
         * bit-plane column per element bit, MSB plane first. */
        size_t bits = chc_type_qbit_element_size(t);
        if (!bits || t->n_children != 1)
            return chc__err_set(err, CHC_ERR_TYPE, "QBit: invalid element type");
        size_t bytes_per_plane = (t->qbit.dimension + 7) / 8;
        chc_column *c = chc__calloc(al, sizeof *c, err);
        if (!c) return CHC_ERR_OOM;
        c->layout = CHC_COL_TUPLE;
        c->n_rows = n_rows;
        c->tuple.children = chc__calloc(al, bits * sizeof *c->tuple.children, err);
        if (!c->tuple.children) { chc__column_destroy(c, al); return CHC_ERR_OOM; }
        c->tuple.arity = bits;
        for (size_t i = 0; i < bits; i++) {
            int rc = chc__col_read_fixed(in, bytes_per_plane, n_rows,
                                         &c->tuple.children[i], err);
            if (rc != CHC_OK) { chc__column_destroy(c, al); return rc; }
        }
        *out = c;
        return CHC_OK;
    }

    case CHC_LOW_CARDINALITY: {
        if (t->n_children != 1)
            return chc__err_set(err, CHC_ERR_TYPE, "LowCardinality expects 1 child");
        const chc_type *inner = t->children[0];
        const chc_type *dict_type = inner;
        bool nullable_wrap = false;
        if (inner->kind == CHC_NULLABLE) {
            nullable_wrap = true;
            dict_type = inner->children[0];
        }

        chc_column *c = chc__calloc(al, sizeof *c, err);
        if (!c) return CHC_ERR_OOM;
        c->layout = CHC_COL_LOW_CARDINALITY;
        c->n_rows = n_rows;

        if (n_rows == 0) {
            /* Empty LC column has no body at all. */
            chc_column *empty_dict = chc__calloc(al, sizeof *empty_dict, err);
            if (!empty_dict) { chc__column_destroy(c, al); return CHC_ERR_OOM; }
            empty_dict->layout = (dict_type->kind == CHC_STRING) ? CHC_COL_STRING
                              : (chc_type_elem_size(dict_type) ? CHC_COL_FIXED : CHC_COL_NOTHING);
            if (empty_dict->layout == CHC_COL_FIXED)
                empty_dict->fixed.elem_size = chc_type_elem_size(dict_type);
            c->lc.dict = empty_dict;
            c->lc.key_size = 1;
            *out = c;
            return CHC_OK;
        }

        uint64_t flags;
        int rc = chc__read_u64_le(in, &flags, err);
        if (rc != CHC_OK) { chc__column_destroy(c, al); return rc; }
        if (flags & CHC__LC_NEED_GLOBAL_DICT) {
            chc__column_destroy(c, al);
            return chc__err_set(err, CHC_ERR_PROTOCOL,
                "LowCardinality: global dictionary not supported");
        }
        if (!(flags & CHC__LC_HAS_ADDITIONAL_KEYS)) {
            chc__column_destroy(c, al);
            return chc__err_set(err, CHC_ERR_PROTOCOL,
                "LowCardinality: HasAdditionalKeys missing");
        }
        unsigned idx_type = (unsigned) (flags & CHC__LC_INDEX_TYPE_MASK);
        switch (idx_type) {
        case 0: c->lc.key_size = 1; break;
        case 1: c->lc.key_size = 2; break;
        case 2: c->lc.key_size = 4; break;
        case 3: c->lc.key_size = 8; break;
        default: chc__column_destroy(c, al);
            return chc__err_set(err, CHC_ERR_PROTOCOL,
                "LowCardinality: invalid index type %u", idx_type);
        }

        uint64_t dict_n;
        rc = chc__read_u64_le(in, &dict_n, err);
        if (rc != CHC_OK) { chc__column_destroy(c, al); return rc; }
        if (dict_n > CHC_MAX_NUM_ROWS || dict_n > SIZE_MAX) {
            chc__column_destroy(c, al);
            return chc__err_set(err, CHC_ERR_PROTOCOL,
                "LowCardinality dictionary too large: %llu",
                (unsigned long long) dict_n);
        }
        chc_column *inner_dict = NULL;
        rc = chc__col_read(in, dict_type, (size_t) dict_n, &inner_dict, err);
        if (rc != CHC_OK) { chc__column_destroy(c, al); return rc; }
        c->lc.dict_n = (size_t) dict_n;

        if (nullable_wrap) {
            /* Wire convention: slot 0 of the inner-typed dict is the NULL
             * sentinel (see the LowCardinality section of the Native Format
             * spec). Wrap the dict in a Nullable column so the caller's
             * standard null-map dispatch covers the LC(Nullable) case. */
            chc_column *wrapped = chc__calloc(al, sizeof *wrapped, err);
            if (!wrapped) { chc__column_destroy(inner_dict, al); chc__column_destroy(c, al); return CHC_ERR_OOM; }
            wrapped->layout = CHC_COL_NULLABLE;
            wrapped->n_rows = (size_t) dict_n;
            wrapped->nullable.inner = inner_dict;
            if (dict_n) {
                wrapped->nullable.null_map = chc__calloc(al, (size_t) dict_n, err);
                if (!wrapped->nullable.null_map) {
                    chc__column_destroy(wrapped, al); chc__column_destroy(c, al);
                    return CHC_ERR_OOM;
                }
                wrapped->nullable.null_map[0] = 1;
            }
            c->lc.dict = wrapped;
        } else {
            c->lc.dict = inner_dict;
        }

        uint64_t key_rows;
        rc = chc__read_u64_le(in, &key_rows, err);
        if (rc != CHC_OK) { chc__column_destroy(c, al); return rc; }
        if (key_rows != n_rows) {
            chc__column_destroy(c, al);
            return chc__err_set(err, CHC_ERR_PROTOCOL,
                "LowCardinality: key_rows %llu != block rows %zu",
                (unsigned long long) key_rows, n_rows);
        }
        size_t keys_bytes;
        if (chc__mul_size(n_rows, c->lc.key_size, &keys_bytes)) {
            chc__column_destroy(c, al);
            return chc__err_set(err, CHC_ERR_PROTOCOL, "LowCardinality keys size overflow");
        }
        rc = chc__read_allocated(in, keys_bytes, &c->lc.keys, err);
        if (rc != CHC_OK) { chc__column_destroy(c, al); return rc; }
        chc__swap_keys(c->lc.keys, n_rows, c->lc.key_size);

        *out = c;
        return CHC_OK;
    }

    case CHC_SIMPLE_AGGREGATE_FUNCTION:
        /* ClickHouse stores values using this function's first argument type */
        if (t->n_children < 1)
            return chc__err_set(err, CHC_ERR_TYPE, "SimpleAggregateFunction has no inner type");
        return chc__col_read(in, t->children[0], n_rows, out, err);

    case CHC_POINT:
    case CHC_RING:
    case CHC_LINE_STRING:
    case CHC_POLYGON:
    case CHC_MULTI_LINE_STRING:
    case CHC_MULTI_POLYGON:
        return chc__col_read(in, chc__geo_alias(t), n_rows, out, err);

    case CHC_NOTHING:
    case CHC_VOID: {
        chc_column *c = chc__calloc(al, sizeof *c, err);
        if (!c) return CHC_ERR_OOM;
        c->layout = CHC_COL_NOTHING;
        c->n_rows = n_rows;
        /* Wire shape for Nothing is a sequence of UInt8 bytes per row. */
        int rc = chc__skip_bytes(in, n_rows, err);
        if (rc != CHC_OK) { chc__column_destroy(c, al); return rc; }
        *out = c;
        return CHC_OK;
    }

    default: {
        size_t nl;
        const char *nm = chc_type_name(t, &nl);
        return chc__err_set(err, CHC_ERR_TYPE,
            "unsupported column type: %.*s", (int) nl, nm ? nm : "");
    }
    }
}

/* -------- block reader ---------- */

/* Serialization kind count, Tuple emits own kind then each element's */
static size_t
chc__kinds_len(const chc_type *t)
{
    t = chc__serial_type(t);
    size_t n = 1;
    if (t->kind == CHC_TUPLE)
        for (size_t i = 0; i < t->n_children; i++)
            n += chc__kinds_len(t->children[i]);
    return n;
}

/* Validate kinds in chc__kinds_len order. Sparse applies to leaves stored
 * as fixed width values, String or Nothing. Other kinds carry payloads not
 * handled here */
static int
chc__kinds_check(const chc_type *t, const uint8_t **kinds, chc_err *err)
{
    t = chc__serial_type(t);
    uint8_t kind = *(*kinds)++;
    if (kind > CHC__KIND_SPARSE)
        return chc__err_set(err, CHC_ERR_PROTOCOL,
            "unsupported serialization kind %u", (unsigned) kind);
    if (t->kind == CHC_TUPLE) {
        for (size_t i = 0; i < t->n_children; i++) {
            int rc = chc__kinds_check(t->children[i], kinds, err);
            if (rc != CHC_OK) return rc;
        }
        return CHC_OK;
    }
    if (kind == CHC__KIND_SPARSE && !chc_type_elem_size(t) && t->kind != CHC_STRING
        && t->kind != CHC_NOTHING && t->kind != CHC_VOID) {
        size_t nl;
        const char *nm = chc_type_name(t, &nl);
        return chc__err_set(err, CHC_ERR_PROTOCOL,
            "sparse serialization unsupported for %.*s", (int) nl, nm ? nm : "");
    }
    return CHC_OK;
}

/* Read has_custom flag & serialization kinds following column type. *out
 * stays NULL when every kind is DEFAULT */
static int
chc__read_kinds(chc_in *in, const chc_type *t, uint8_t **out, size_t *out_len,
                chc_err *err)
{
    uint8_t has_custom;
    int rc = chc__read_byte(in, &has_custom, err);
    if (rc != CHC_OK || !has_custom) return rc;
    if (has_custom != 1)
        return chc__err_set(err, CHC_ERR_PROTOCOL,
            "invalid custom serialization flag %u", (unsigned) has_custom);
    size_t n = chc__kinds_len(t);
    uint8_t *k = chc__alloc(in->al, n, err);
    if (!k) return CHC_ERR_OOM;
    const uint8_t *cursor = k;
    rc = chc__read_bytes(in, k, n, err);
    if (rc == CHC_OK) rc = chc__kinds_check(t, &cursor, err);
    if (rc != CHC_OK || !memchr(k, CHC__KIND_SPARSE, n)) {
        in->al->free(in->al->ud, k, n);
        return rc;
    }
    *out = k;
    *out_len = n;
    return CHC_OK;
}

struct chc_block {
    size_t        n_columns;
    size_t        allocated_columns;
    size_t        n_rows;
    bool          is_overflows;
    int32_t       bucket_num;
    char        **names;
    size_t       *name_lens;
    chc_type    **types;
    chc_column  **columns;
};

void
chc_block_destroy(chc_block *b, const chc_alloc *al)
{
    if (!b) return;
    for (size_t i = 0; i < b->allocated_columns; i++) {
        if (b->names && b->names[i])
            al->free(al->ud, b->names[i], b->name_lens[i] + 1);
        if (b->types) chc_type_destroy(b->types[i], al);
        if (b->columns) chc__column_destroy(b->columns[i], al);
    }
    al->free(al->ud, b->names,     b->allocated_columns * sizeof *b->names);
    al->free(al->ud, b->name_lens, b->allocated_columns * sizeof *b->name_lens);
    al->free(al->ud, b->types,     b->allocated_columns * sizeof *b->types);
    al->free(al->ud, b->columns,   b->allocated_columns * sizeof *b->columns);
    al->free(al->ud, b, sizeof *b);
}

static int
chc__block_reserve_column(chc_block *b, size_t index, const chc_alloc *al, chc_err *err)
{
    if (index < b->allocated_columns) return CHC_OK;
    size_t next = b->allocated_columns > b->n_columns / 2
        ? b->n_columns : b->allocated_columns * 2;
    if (next < 16) next = 16;
    if (next > b->n_columns) next = b->n_columns;
    char **names = chc__calloc(al, next * sizeof *names, err);
    size_t *lengths = chc__calloc(al, next * sizeof *lengths, err);
    chc_type **types = chc__calloc(al, next * sizeof *types, err);
    chc_column **columns = chc__calloc(al, next * sizeof *columns, err);
    if (!names || !lengths || !types || !columns) {
        al->free(al->ud, names, next * sizeof *names);
        al->free(al->ud, lengths, next * sizeof *lengths);
        al->free(al->ud, types, next * sizeof *types);
        al->free(al->ud, columns, next * sizeof *columns);
        return CHC_ERR_OOM;
    }
    size_t previous = b->allocated_columns;
    if (previous) {
        memcpy(names, b->names, previous * sizeof *names);
        memcpy(lengths, b->name_lens, previous * sizeof *lengths);
        memcpy(types, b->types, previous * sizeof *types);
        memcpy(columns, b->columns, previous * sizeof *columns);
    }
    al->free(al->ud, b->names, previous * sizeof *names);
    al->free(al->ud, b->name_lens, previous * sizeof *lengths);
    al->free(al->ud, b->types, previous * sizeof *types);
    al->free(al->ud, b->columns, previous * sizeof *columns);
    b->names = names;
    b->name_lens = lengths;
    b->types = types;
    b->columns = columns;
    b->allocated_columns = next;
    return CHC_OK;
}

size_t            chc_block_n_rows(const chc_block *b)    { return b ? b->n_rows : 0; }
size_t            chc_block_n_columns(const chc_block *b) { return b ? b->n_columns : 0; }
const char       *chc_block_column_name(const chc_block *b, size_t i, size_t *out_len) {
    if (!b || i >= b->n_columns) { if (out_len) *out_len = 0; return NULL; }
    if (out_len) *out_len = b->name_lens[i];
    return b->names[i];
}
const chc_type   *chc_block_column_type(const chc_block *b, size_t i)
    { return (b && i < b->n_columns) ? b->types[i] : NULL; }
const chc_column *chc_block_column(const chc_block *b, size_t i)
    { return (b && i < b->n_columns) ? b->columns[i] : NULL; }
bool              chc_block_is_overflows(const chc_block *b) { return b ? b->is_overflows : false; }
int32_t           chc_block_bucket_num(const chc_block *b)   { return b ? b->bucket_num : 0; }

/* Resumable block reader. Continues a (possibly partial) block parse across
 * CHC_WOULD_BLOCK without re-parsing completed columns. Owns the `in`
 * checkpoint/rewind at packet-start and per-column granularity.
 *
 *   *blk == NULL : fresh packet. Checkpoint packet start, alloc block, parse
 *                  block-info header + ncols/nrows + per-column arrays, then
 *                  read columns from index 0.
 *   *blk != NULL : resume. Header already parsed; continue at *next_col,
 *                  re-reading only the in-progress column.
 *
 * Per-column retain is active only for ioless in (in->io == NULL). For
 * io-backed in (blocking socket, or the compressed dec_in wrapper) it behaves
 * as baseline: never sets a checkpoint (io-backed refill clobbers the mark),
 * and on any non-OK frees the partial block and returns.
 *
 * On CHC_WOULD_BLOCK (ioless only):
 *   - mid-header (only when *blk was NULL): rewind to packet start, free the
 *     block, leave *blk == NULL.
 *   - mid-column i: retain *blk with columns [0,i) intact, free column i's
 *     partial slot allocations, set *next_col = i, rewind in to the column-i
 *     checkpoint.
 * On CHC_OK: *blk is the completed block (ownership to caller), *next_col == ncols.
 * On real error: free *blk, set it NULL. */
static int
chc__block_resume_in(chc_in *in, const chc_alloc *al,
                     const chc_block_opts *opts,
                     chc_block **blk, size_t *next_col, chc_err *err)
{
    bool ioless = CHC__IOLESS(in);
    chc_block *b = *blk;
    int rc;

    if (b == NULL) {
        b = chc__calloc(al, sizeof *b, err);
        if (!b) return CHC_ERR_OOM;
        uint64_t consumed_before = in->consumed;

        /* Checkpoint packet start so a mid-header would-block rewinds here. */
        if (ioless) chc__in_checkpoint(in);

        if (opts->has_block_info) {
            uint64_t fid;
            rc = chc__read_varuint(in, &fid, err);
            if (rc == CHC_ERR_EOF && in->consumed == consumed_before) {
                chc_block_destroy(b, al);
                *blk = NULL;
                chc_err_reset(err);
                return CHC_OK;
            }
            if (rc != CHC_OK) goto header_fail;
            if (fid != 1) { rc = chc__err_set(err, CHC_ERR_PROTOCOL,
                                "BlockInfo: expected field 1, got %llu",
                                (unsigned long long) fid); goto header_fail; }
            uint8_t ov;
            rc = chc__read_byte(in, &ov, err); if (rc != CHC_OK) goto header_fail;
            b->is_overflows = ov != 0;
            rc = chc__read_varuint(in, &fid, err); if (rc != CHC_OK) goto header_fail;
            if (fid != 2) { rc = chc__err_set(err, CHC_ERR_PROTOCOL,
                                "BlockInfo: expected field 2"); goto header_fail; }
            uint32_t bn;
            rc = chc__read_u32_le(in, &bn, err); if (rc != CHC_OK) goto header_fail;
            b->bucket_num = (int32_t) bn;
            rc = chc__read_varuint(in, &fid, err); if (rc != CHC_OK) goto header_fail;
            if (fid != 0) { rc = chc__err_set(err, CHC_ERR_PROTOCOL,
                                "BlockInfo: expected terminator"); goto header_fail; }
        }

        uint64_t ncols, nrows;
        rc = chc__read_varuint(in, &ncols, err);
        if (rc == CHC_ERR_EOF && !opts->has_block_info
            && in->consumed == consumed_before) {
            chc_block_destroy(b, al);
            *blk = NULL;
            chc_err_reset(err);
            return CHC_OK;
        }
        if (rc != CHC_OK) goto header_fail;
        rc = chc__read_varuint(in, &nrows, err);
        if (rc != CHC_OK) goto header_fail;

        if (CHC_PHP_COMPAT && (ncols > SIZE_MAX || nrows > SIZE_MAX)) {
            rc = chc__err_set(err, CHC_ERR_PROTOCOL,
                "block declares %llu columns and %llu rows, more than size_t can hold",
                (unsigned long long) ncols, (unsigned long long) nrows);
            goto header_fail;
        }
        if (ncols > CHC_MAX_NUM_COLUMNS ||
            ncols > SIZE_MAX / sizeof *b->names ||
            ncols > SIZE_MAX / sizeof *b->name_lens ||
            ncols > SIZE_MAX / sizeof *b->types ||
            ncols > SIZE_MAX / sizeof *b->columns) {
            rc = chc__err_set(err, CHC_ERR_PROTOCOL,
                "suspiciously many columns: %llu", (unsigned long long) ncols);
            goto header_fail;
        }
        if (nrows > CHC_MAX_NUM_ROWS || nrows > SIZE_MAX) {
            rc = chc__err_set(err, CHC_ERR_PROTOCOL,
                "suspiciously many rows: %llu", (unsigned long long) nrows);
            goto header_fail;
        }

        b->n_columns = (size_t) ncols;
        b->n_rows    = (size_t) nrows;

        /* Header fully parsed; commit the block and start at column 0. */
        *blk = b;
        *next_col = 0;
    }

    size_t nrows = b->n_rows;
    for (size_t i = *next_col; i < b->n_columns; i++) {
        char *type_name; size_t type_len;
        uint8_t *kinds = NULL; size_t kinds_len = 0;
        rc = chc__block_reserve_column(b, i, al, err);
        if (rc != CHC_OK) goto col_fail;

        /* Checkpoint column start so a mid-column would-block rewinds here,
         * dropping completed columns' wire bytes on the caller's next submit. */
        if (ioless) chc__in_checkpoint(in);

        rc = chc__read_string(in, &b->names[i], &b->name_lens[i], err);
        if (rc != CHC_OK) goto col_fail;

        rc = chc__read_string(in, &type_name, &type_len, err);
        if (rc != CHC_OK) goto col_fail;
        rc = chc_type_parse(type_name, type_len, al, &b->types[i], err);
        al->free(al->ud, type_name, type_len + 1);
        if (rc != CHC_OK) goto col_fail;

        if (opts->has_custom_serialization) {
            rc = chc__read_kinds(in, b->types[i], &kinds, &kinds_len, err);
            if (rc != CHC_OK) goto col_fail;
        }

        /* Zero-row column carries kinds but no body, not even sparse offsets */
        if (nrows) {
            rc = chc__col_read_prefix(in, b->types[i], err);
            if (rc != CHC_OK) goto col_fail;

            const uint8_t *cursor = kinds;
            rc = kinds ? chc__col_read_kinds(in, b->types[i], nrows, &cursor, &b->columns[i], err)
                       : chc__col_read(in, b->types[i], nrows, &b->columns[i], err);
            if (CHC_PHP_COMPAT && rc == CHC_OK &&
                chc_column_validate(b->columns[i], err) != CHC_OK)
                rc = chc__err_set(err, CHC_ERR_PROTOCOL, "can't load column '%.*s' of type %.*s",
                    (int) b->name_lens[i], b->names[i],
                    (int) b->types[i]->name_len, b->types[i]->name);
            if (CHC_PHP_COMPAT && rc == CHC_ERR_EOF)
                rc = chc__err_set(err, CHC_ERR_PROTOCOL, "can't load column '%.*s' of type %.*s",
                    (int) b->name_lens[i], b->names[i],
                    (int) b->types[i]->name_len, b->types[i]->name);
            if (rc != CHC_OK) goto col_fail;
        }
        al->free(al->ud, kinds, kinds_len);
        continue;

    col_fail:
        al->free(al->ud, kinds, kinds_len);
        if (ioless && rc == CHC_WOULD_BLOCK) {
            /* Retain columns [0,i); reset slot i to a destroy-safe NULL state
             * and rewind so column i re-parses from its checkpoint. */
            if (b->names[i]) {
                al->free(al->ud, b->names[i], b->name_lens[i] + 1);
                b->names[i] = NULL;
                b->name_lens[i] = 0;
            }
            chc_type_destroy(b->types[i], al);
            b->types[i] = NULL;
            chc__column_destroy(b->columns[i], al);
            b->columns[i] = NULL;
            chc__in_rewind(in);
            *next_col = i;
            return CHC_WOULD_BLOCK;
        }
        chc_block_destroy(b, al);
        *blk = NULL;
        return rc;
    }

    *next_col = b->n_columns;
    return CHC_OK;

header_fail:
    if (ioless && rc == CHC_WOULD_BLOCK) chc__in_rewind(in);
    chc_block_destroy(b, al);
    *blk = NULL;
    return rc;
}

/* Block read from an already-initialized chc_in. Also used by
 * clickhouse-client.h's recv_packet (persistent buffer). Thin non-looping
 * wrapper over chc__block_resume_in. Returns 0 with *out == NULL on clean EOF
 * at block boundary (only when opts->has_block_info is false; TCP path has no
 * clean-EOF concept). */
int
chc_block_read(chc_in *in, const chc_alloc *al,
               const chc_block_opts *opts, chc_block **out, chc_err *err)
{
    chc_block_opts def = {0};
    if (!opts) opts = &def;
    /* Snapshot to roll back resume's per-column checkpoint progress: this
     * non-resuming entry is baseline (rewind to packet start), so a would-block
     * must leave `in` exactly as found. Callers that own the packet-start
     * checkpoint/rewind (io-backed in, the legacy ioless driver) then see
     * untouched mark/pos/consumed. */
#ifndef CHC_NO_ASYNC
    size_t   entry_pos = in->pos, entry_mark = in->mark;
    uint64_t entry_consumed = in->consumed;
#endif
    chc_block *blk = NULL;
    size_t next_col = 0;
    int rc = chc__block_resume_in(in, al, opts, &blk, &next_col, err);
#ifndef CHC_NO_ASYNC
    /* io-backed callers never see WOULD_BLOCK; the compressed dec_in path may
     * (propagated from the underlying ioless raw in), in which case the async
     * recv driver rewinds the raw in to packet start -- baseline. */
    if (rc == CHC_WOULD_BLOCK) {
        if (blk) { chc_block_destroy(blk, al); blk = NULL; }
        in->pos = entry_pos;
        in->mark = entry_mark;
        in->consumed = entry_consumed;
    }
#endif
    *out = blk;
    return rc;
}

/* -------- block writer ---------- */

void
chc_block_builder_init(chc_block_builder *bb, chc_block_col *cols)
{
    *bb = (chc_block_builder) { .cols = cols };
}

/* -------- compositional column builders ---------- */
/* Cast away const for reader-compatible tree shape; writer never mutates
 * caller trees */

chc_column
chc_build_fixed(const void *data, size_t elem_size, size_t n_rows)
{
    return (chc_column) {
        .layout = CHC_COL_FIXED,
        .n_rows = n_rows,
        .fixed.data = (void *) data,
        .fixed.elem_size = elem_size,
    };
}

chc_column
chc_build_string(const uint64_t *offsets, const uint8_t *data, size_t n_rows)
{
    return (chc_column) {
        .layout = CHC_COL_STRING,
        .n_rows = n_rows,
        .str.offsets = (uint64_t *) offsets,
        .str.data = (uint8_t *) data,
    };
}

chc_column
chc_build_nullable(const uint8_t *null_map, chc_column *inner)
{
    return (chc_column) {
        .layout = CHC_COL_NULLABLE,
        .n_rows = inner ? inner->n_rows : 0,
        .nullable.null_map = (uint8_t *) null_map,
        .nullable.inner = inner,
    };
}

chc_column
chc_build_array(const uint64_t *offsets, size_t n_rows, chc_column *values)
{
    return (chc_column) {
        .layout = CHC_COL_ARRAY,
        .n_rows = n_rows,
        .array.offsets = (uint64_t *) offsets,
        .array.values = values,
    };
}

chc_column
chc_build_tuple(chc_column **children, size_t arity)
{
    return (chc_column) {
        .layout = CHC_COL_TUPLE,
        .n_rows = arity ? children[0]->n_rows : 0,
        .tuple.children = children,
        .tuple.arity = arity,
    };
}

chc_column
chc_build_lc(int key_size, const void *keys, size_t n_rows, chc_column *dict)
{
    return (chc_column) {
        .layout = CHC_COL_LOW_CARDINALITY,
        .n_rows = n_rows,
        .lc.key_size = key_size,
        .lc.keys = (void *) keys,
        .lc.dict = dict,
        .lc.dict_n = dict ? dict->n_rows : 0,
    };
}

void
chc_block_builder_append(chc_block_builder *bb,
                         const char *name, size_t name_len,
                         const chc_type *t, const chc_column *col)
{
    bb->n_rows = col->n_rows;
    bb->cols[bb->n_cols++] = (chc_block_col) {
        .name = name, .name_len = name_len, .type = t, .col = col,
    };
}

/* -------- write helpers ---------- */

static int
chc__write_bytes(chc_io *io, const void *buf, size_t n, chc_err *err)
{
    return io->write(io->ud, buf, n, err);
}

static int
chc__write_varuint(chc_io *io, uint64_t v, chc_err *err)
{
    uint8_t b[10];
    int n = 0;
    do {
        uint8_t byte = (uint8_t) (v & 0x7f);
        v >>= 7;
        if (v) byte |= 0x80;
        b[n++] = byte;
    } while (v);
    return chc__write_bytes(io, b, (size_t) n, err);
}

static int
chc__write_u32_le(chc_io *io, uint32_t v, chc_err *err)
{
    v = chc__bswap32(v);
    return chc__write_bytes(io, &v, sizeof v, err);
}

static int
chc__write_u64_le(chc_io *io, uint64_t v, chc_err *err)
{
    v = chc__bswap64(v);
    return chc__write_bytes(io, &v, sizeof v, err);
}

static int
chc__write_block_info(chc_io *io, chc_err *err)
{
    int rc;
    uint8_t overflows = 0;
    if ((rc = chc__write_varuint(io, 1, err))) return rc;
    if ((rc = chc__write_bytes(io, &overflows, 1, err))) return rc;
    if ((rc = chc__write_varuint(io, 2, err))) return rc;
    if ((rc = chc__write_u32_le(io, (uint32_t) -1, err))) return rc;
    return chc__write_varuint(io, 0, err);
}

static int
chc__write_string(chc_io *io, const char *s, size_t n, chc_err *err)
{
    int rc = chc__write_varuint(io, (uint64_t) n, err);
    if (rc != CHC_OK) return rc;
    if (n) return chc__write_bytes(io, s, n, err);
    return CHC_OK;
}

/* Emit a contiguous u64 array as little-endian. */
static int
chc__write_u64_le_array(chc_io *io, const uint64_t *p, size_t n, chc_err *err)
{
#if CHC_BIG_ENDIAN
    for (size_t i = 0; i < n; i++) {
        int rc = chc__write_u64_le(io, p[i], err);
        if (rc != CHC_OK) return rc;
    }
    return CHC_OK;
#else
    if (!n) return CHC_OK;
    return chc__write_bytes(io, p, n * sizeof(uint64_t), err);
#endif
}

/* Emit LC keys (host BO -> LE on BE hosts). */
static int
chc__write_keys_array(chc_io *io, const void *p, size_t n, int key_size,
                      chc_err *err)
{
#if CHC_BIG_ENDIAN
    int rc;
    switch (key_size) {
    case 1: return n ? chc__write_bytes(io, p, n, err) : CHC_OK;
    case 2: { const uint16_t *a = p;
        for (size_t i = 0; i < n; i++) {
            uint8_t b[2] = { (uint8_t) a[i], (uint8_t) (a[i] >> 8) };
            if ((rc = chc__write_bytes(io, b, 2, err))) return rc;
        }
        return CHC_OK;
    }
    case 4: { const uint32_t *a = p;
        for (size_t i = 0; i < n; i++)
            if ((rc = chc__write_u32_le(io, a[i], err))) return rc;
        return CHC_OK;
    }
    case 8: { const uint64_t *a = p;
        for (size_t i = 0; i < n; i++)
            if ((rc = chc__write_u64_le(io, a[i], err))) return rc;
        return CHC_OK;
    }
    }
    return chc__err_set(err, CHC_ERR_USAGE, "bad key_size %d", key_size);
#else
    if (!n) return CHC_OK;
    return chc__write_bytes(io, p, n * (size_t) key_size, err);
#endif
}

/* Emit a String column body (varuint length + bytes, per row). */
static int
chc__write_string_body(chc_io *io, const uint64_t *offsets,
                       const uint8_t *data, size_t n, chc_err *err)
{
    uint64_t prev = 0;
    for (size_t r = 0; r < n; r++) {
        uint64_t end = offsets[r];
        uint64_t len = end - prev;
        int rc = chc__write_varuint(io, len, err);
        if (rc != CHC_OK) return rc;
        if (len) {
            rc = chc__write_bytes(io, data + prev, (size_t) len, err);
            if (rc != CHC_OK) return rc;
        }
        prev = end;
    }
    return CHC_OK;
}

/* -------- recursive column writer ---------- */

static int
chc__col_write_mismatch(chc_err *err, const chc_type *t)
{
    size_t nl;
    const char *nm = chc_type_name(t, &nl);
    return chc__err_set(err, CHC_ERR_TYPE,
        "column layout does not match type %.*s", (int) nl, nm ? nm : "");
}

/* Emit stream prefix before column bodies, mirroring chc__col_read_prefix:
 * LowCardinality key version 1, JSON serialization version 1, legacy Object
 * serialization kind 1. Recurse through composites */
static int
chc__col_write_prefix(chc_io *io, const chc_type *t, chc_err *err)
{
    if (t->kind == CHC_LOW_CARDINALITY || t->kind == CHC_JSON)
        return chc__write_u64_le(io, 1, err);
    if (t->kind == CHC_OBJECT) {
        uint8_t kind = 1;
        return chc__write_bytes(io, &kind, 1, err);
    }
    if (t->kind == CHC_NULLABLE || t->kind == CHC_ARRAY
        || t->kind == CHC_TUPLE || t->kind == CHC_MAP
        || t->kind == CHC_NESTED
        || t->kind == CHC_SIMPLE_AGGREGATE_FUNCTION) {
        for (size_t i = 0; i < t->n_children; i++) {
            int rc = chc__col_write_prefix(io, t->children[i], err);
            if (rc != CHC_OK) return rc;
        }
    }
    return CHC_OK;
}

/* Emit body from chc_column tree. Each node carries row count for its level.
 * Assume top-level block contains rows */
static int
chc__col_write(chc_io *io, const chc_column *c, const chc_type *t, chc_err *err)
{
    int rc;
    size_t es = chc_type_elem_size(t);
    if (es) {
        if (c->layout != CHC_COL_FIXED) return chc__col_write_mismatch(err, t);
        if (c->n_rows)
            return chc__write_bytes(io, c->fixed.data, c->n_rows * es, err);
        return CHC_OK;
    }

    switch (t->kind) {
    case CHC_STRING:
    case CHC_JSON:
    case CHC_OBJECT:
        if (c->layout != CHC_COL_STRING) return chc__col_write_mismatch(err, t);
        return chc__write_string_body(io, c->str.offsets, c->str.data,
                                      c->n_rows, err);

    case CHC_NULLABLE:
        if (c->layout != CHC_COL_NULLABLE || t->n_children != 1)
            return chc__col_write_mismatch(err, t);
        if (c->n_rows
            && (rc = chc__write_bytes(io, c->nullable.null_map, c->n_rows, err)))
            return rc;
        return chc__col_write(io, c->nullable.inner, t->children[0], err);

    case CHC_ARRAY:
        if (c->layout != CHC_COL_ARRAY || t->n_children != 1)
            return chc__col_write_mismatch(err, t);
        if ((rc = chc__write_u64_le_array(io, c->array.offsets, c->n_rows, err)))
            return rc;
        return chc__col_write(io, c->array.values, t->children[0], err);

    case CHC_MAP:
    case CHC_NESTED: {
        size_t arity = t->kind == CHC_MAP ? 2 : t->n_children;
        if (c->layout != CHC_COL_ARRAY || !arity || t->n_children != arity)
            return chc__col_write_mismatch(err, t);
        if ((rc = chc__write_u64_le_array(io, c->array.offsets, c->n_rows, err)))
            return rc;
        const chc_column *tup = c->array.values;
        if (!tup || tup->layout != CHC_COL_TUPLE || tup->tuple.arity != arity)
            return chc__col_write_mismatch(err, t);
        for (size_t i = 0; i < arity; i++)
            if ((rc = chc__col_write(io, tup->tuple.children[i], t->children[i], err)))
                return rc;
        return CHC_OK;
    }

    case CHC_TUPLE:
        if (c->layout != CHC_COL_TUPLE || c->tuple.arity != t->n_children)
            return chc__col_write_mismatch(err, t);
        for (size_t i = 0; i < t->n_children; i++)
            if ((rc = chc__col_write(io, c->tuple.children[i], t->children[i], err)))
                return rc;
        return CHC_OK;

    case CHC_QBIT: {
        size_t bits = chc_type_qbit_element_size(t);
        if (!bits || t->n_children != 1
            || c->layout != CHC_COL_TUPLE || c->tuple.arity != bits)
            return chc__col_write_mismatch(err, t);
        size_t bytes_per_plane = (chc_type_qbit_dimension(t) + 7) / 8;
        for (size_t i = 0; i < bits; i++) {
            const chc_column *pl = c->tuple.children[i];
            if (pl->layout != CHC_COL_FIXED) return chc__col_write_mismatch(err, t);
            if (c->n_rows && (rc = chc__write_bytes(io, pl->fixed.data,
                                                    c->n_rows * bytes_per_plane, err)))
                return rc;
        }
        return CHC_OK;
    }

    case CHC_LOW_CARDINALITY: {
        if (c->layout != CHC_COL_LOW_CARDINALITY || t->n_children != 1)
            return chc__col_write_mismatch(err, t);
        /* Empty LC, including one under empty Array, has no body */
        if (c->n_rows == 0) return CHC_OK;
        const chc_type *inner = t->children[0];
        const chc_type *dict_type = (inner->kind == CHC_NULLABLE
                                     && inner->n_children == 1)
                                  ? inner->children[0] : inner;
        /* Reader wraps LC(Nullable) dict in Nullable for caller dispatch. Wire
         * body contains inner-typed strings with null sentinel at slot 0.
         * Unwrap before writing */
        const chc_column *dictc = c->lc.dict;
        if (dictc && dictc->layout == CHC_COL_NULLABLE)
            dictc = dictc->nullable.inner;
        uint64_t flags;
        switch (c->lc.key_size) {
        case 1: flags = 0; break;
        case 2: flags = 1; break;
        case 4: flags = 2; break;
        case 8: flags = 3; break;
        default: return chc__err_set(err, CHC_ERR_USAGE,
            "LowCardinality: bad key_size %d", c->lc.key_size);
        }
        flags |= CHC__LC_HAS_ADDITIONAL_KEYS;
        flags |= CHC__LC_NEED_UPDATE_DICT;
        if ((rc = chc__write_u64_le(io, flags, err))) return rc;
        if ((rc = chc__write_u64_le(io, (uint64_t) c->lc.dict_n, err))) return rc;
        if ((rc = chc__col_write(io, dictc, dict_type, err))) return rc;
        if ((rc = chc__write_u64_le(io, (uint64_t) c->n_rows, err))) return rc;
        return chc__write_keys_array(io, c->lc.keys, c->n_rows,
                                     c->lc.key_size, err);
    }

    case CHC_SIMPLE_AGGREGATE_FUNCTION:
        if (t->n_children < 1) return chc__col_write_mismatch(err, t);
        return chc__col_write(io, c, t->children[0], err);

    case CHC_POINT:
    case CHC_RING:
    case CHC_LINE_STRING:
    case CHC_POLYGON:
    case CHC_MULTI_LINE_STRING:
    case CHC_MULTI_POLYGON:
        return chc__col_write(io, c, chc__geo_alias(t), err);

    case CHC_NOTHING:
    case CHC_VOID: {
        uint8_t zeros[256] = {0};
        size_t left = c->n_rows;
        while (left) {
            size_t take = left < sizeof zeros ? left : sizeof zeros;
            if ((rc = chc__write_bytes(io, zeros, take, err))) return rc;
            left -= take;
        }
        return CHC_OK;
    }

    default:
        return chc__col_write_mismatch(err, t);
    }
}

int
chc_block_write_cols(chc_io *io, const chc_block_col *cols,
                     size_t n_cols, size_t n_rows,
                     const chc_block_opts *opts, chc_err *err)
{
    chc_block_opts def = {0};
    if (!opts) opts = &def;

    if (opts->has_block_info) {
        int rc = chc__write_block_info(io, err);
        if (rc != CHC_OK) return rc;
    }

    int rc = chc__write_varuint(io, (uint64_t) n_cols, err);
    if (rc != CHC_OK) return rc;
    rc = chc__write_varuint(io, (uint64_t) n_rows, err);
    if (rc != CHC_OK) return rc;

    for (size_t i = 0; i < n_cols; i++) {
        const chc_block_col *e = &cols[i];
        if (!e->type || !e->col)
            return chc__err_set(err, CHC_ERR_USAGE,
                "block_write: column %zu has NULL type or tree", i);
        if (e->col->n_rows != n_rows)
            return chc__err_set(err, CHC_ERR_USAGE,
                "block_write: column %zu row count %zu != block %zu",
                i, e->col->n_rows, n_rows);

        rc = chc__write_string(io, e->name, e->name_len, err);
        if (rc != CHC_OK) return rc;

        rc = chc__write_string(io, e->type->name, e->type->name_len, err);
        if (rc != CHC_OK) return rc;

        if (opts->has_custom_serialization) {
            uint8_t z = 0;
            rc = chc__write_bytes(io, &z, 1, err);
            if (rc != CHC_OK) return rc;
        }

        if (n_rows == 0) continue;

        rc = chc__col_write_prefix(io, e->type, err);
        if (rc != CHC_OK) return rc;
        rc = chc__col_write(io, e->col, e->type, err);
        if (rc != CHC_OK) return rc;
    }
    return CHC_OK;
}

int
chc_block_write(chc_io *io, const chc_block_builder *bb,
                const chc_block_opts *opts, chc_err *err)
{
    return chc_block_write_cols(io, bb->cols, bb->n_cols,
                                bb->n_rows, opts, err);
}

#endif /* CHC_IMPLEMENTATION */

#ifdef __cplusplus
}
#endif

#endif /* CLICKHOUSE_H */
