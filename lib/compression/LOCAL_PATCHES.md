# Local compression patches

The LZ4 1.11.1 and ZSTD 1.5.7 sources carry no local patches. Add a new patch
under `patches/` with a level-two heading here that names its file.

Retired patches:

- `0001-lz4-empty-dictionary.patch` avoided null-pointer arithmetic when LZ4
  used an empty dictionary. LZ4 1.11.1 contains the same guards in
  `LZ4_compress_generic()` and `LZ4_setStreamDecode()`, so the patch was
  dropped with the move from LZ4 1.9.2.
