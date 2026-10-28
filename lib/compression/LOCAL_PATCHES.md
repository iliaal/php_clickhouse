# Local compression patches

## 0001-lz4-empty-dictionary.patch

Avoid null-pointer arithmetic when LZ4 uses an empty dictionary. This retains
the existing extension patch when moving the C codec sources out of the former
client directory. The sanitizer build keeps pointer-overflow checking enabled.
