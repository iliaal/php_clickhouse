# clickhouse-compression.h

Compressed-frame layout, CityHash128, codec vtable, and ready-made LZ4 &
ZSTD adapters. Link `-llz4 -lzstd` by default; opt out of either adapter
(and its `<lz4.h>` / `<zstd.h>` include) by defining `CHC_NO_LZ4` or
`CHC_NO_ZSTD` before including. Custom codecs are wired by filling
`chc_codec` directly. The TCP packet loop in
[clickhouse-client.md](clickhouse-client.md) consumes this header.

## Frame layout

Per the [Native Format spec, compression
frame](https://clickhouse.com/docs/reference/interfaces/specs/NativeFormat#compression-frame):

```
[ 16 B CityHash128 of the rest of the frame                ]
[  1 B method (0x82 LZ4, 0x90 ZSTD, 0x02 none)              ]
[  4 B LE: compressed_size_with_header (= 9 + payload bytes)]
[  4 B LE: original_size                                    ]
[    payload                                                ]
```

CityHash128 covers method + size fields + payload — i.e. everything from
offset 16 to end-of-frame.

## Compression mode & codec vtable

```c
typedef enum chc_compression {
    CHC_COMP_NONE = 0,
    CHC_COMP_LZ4  = 1,
    CHC_COMP_ZSTD = 2,
} chc_compression;

typedef struct chc_codec {
    void *ud;

    int (*lz4_compress)  (void *ud, const void *src, size_t src_len,
                          void *dst, size_t dst_cap, size_t *dst_n,
                          chc_err *err);
    int (*lz4_decompress)(void *ud, const void *src, size_t src_len,
                          void *dst, size_t original_size,
                          chc_err *err);

    int (*zstd_compress)  (void *ud, const void *src, size_t src_len,
                           void *dst, size_t dst_cap, size_t *dst_n,
                           chc_err *err);
    int (*zstd_decompress)(void *ud, const void *src, size_t src_len,
                           void *dst, size_t original_size,
                           chc_err *err);

    size_t (*lz4_bound) (size_t src_len);   /* may be NULL */
    size_t (*zstd_bound)(size_t src_len);   /* may be NULL */
} chc_codec;

#define CHC_COMPRESS_MAX_CHUNK 65535u
```

`*_compress` returns the compressed size via `*dst_n`. Insufficient
`dst_cap` returns `CHC_ERR_OOM`. `*_decompress` is given the known
uncompressed length & must produce exactly that many bytes.

Bound callbacks may be NULL; the frame writer falls back to the LZ4
classic `n + 256 + n/255` formula.

Outgoing blocks are split into chunks of at most `CHC_COMPRESS_MAX_CHUNK`
bytes (65 535) before each is wrapped in its own frame, matching the
historical 64 KiB client convention. The spec bounds frames at 1 GiB.

## Built-in LZ4 & ZSTD adapters

```c
void chc_lz4_codec_init (chc_codec *out);   /* fills lz4_*  slots */
void chc_zstd_codec_init(chc_codec *out);   /* fills zstd_* slots */
```

Each helper only populates its own slots, so a caller wanting both
codecs starts from a zero-initialized `chc_codec` & calls both inits in
either order:

```c
chc_codec codec = {};
chc_lz4_codec_init (&codec);
chc_zstd_codec_init(&codec);
```

Defining `CHC_NO_LZ4` (or `CHC_NO_ZSTD`) before including
`clickhouse-compression.h` drops the corresponding adapter & its
`<lz4.h>` / `<zstd.h>` include, so builds without those libraries
present link cleanly. Custom codecs can still be wired by filling
`chc_codec` manually.

### LZ4 errors

* Source larger than `LZ4_MAX_INPUT_SIZE` returns `CHC_ERR_USAGE`.
* `LZ4_compress_default` returning `<= 0` (output capacity too small)
  returns `CHC_ERR_OOM`.
* `LZ4_decompress_safe` rc `< 0`, or producing a different size than the
  caller declared, returns `CHC_ERR_PROTOCOL`.

### ZSTD errors

* `ZSTD_compress` reporting an error returns `CHC_ERR_OOM` with the zstd
  error name appended.
* `ZSTD_decompress` reporting an error, or producing a different byte
  count than the caller declared, returns `CHC_ERR_PROTOCOL`.

## CityHash128

```c
void chc_cityhash128(const void *data, size_t len,
                     uint64_t *out_lo, uint64_t *out_hi);
```

Frozen v1.0.3 variant ported from Google's `city.cc` (MIT). Returns the
two 64-bit halves the wire format encodes (lo first, hi second). Exposed
because the implementation already ships it; useful for callers driving
the frame format themselves.
