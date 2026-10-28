# Zend allocator experiment

This branch moves extension-owned protocol buffers, decoded results, connection
storage, and ZSTD contexts/workspaces onto PHP's request heap. PHP values and
insert builders already use that heap. With Zend MM enabled,
`memory_get_usage()` includes retained native results, and multiple clients and
iterators share the request's `memory_limit`.

The allocator rejects a request before calling Zend when its worst-case heap
growth would exceed the available limit. Small allocations may need a new
2 MiB Zend chunk. Huge allocations also need rounded payload storage and a
tracking node. Reallocation budgets for the old and new storage to coexist.
These checks preserve catchable native allocation errors without raising the
configured limit or catching a Zend bailout.

This is conservative: an allocation can be rejected while a few MiB remain,
even if an existing free slot could accommodate it. Physical OS allocation
failure still follows PHP's fatal OOM policy. Newer PHP fallible allocators
queue a fatal OOM when they return NULL; they do not make that failure a
catchable `ClickHouseException`.

OpenSSL internals, resolver results, socket buffers, and stacks remain outside
Zend accounting. Their platform/library ownership rules still apply. This
branch does not claim that PHP accounts for all process RSS.

`USE_ZEND_ALLOC=0` disables the accounting guarantee. The existing per-iterator
estimate remains active in that diagnostic configuration. Memory-accounting
tests must run with Zend MM enabled; sanitizer runs with it disabled verify a
different property.

Use the normal extension build, then run these tests against a disposable
ClickHouse server with `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT`, `CLICKHOUSE_USER`,
and `CLICKHOUSE_PASSWD` configured:

```sh
NO_INTERACTION=1 php run-tests.php -j1 -d extension="$PWD/modules/clickhouse.so" \
    tests/211_stream_native_memory_limit.phpt \
    tests/213_stream_untyped_null_column.phpt \
    tests/283_zend_native_memory_accounting.phpt \
    tests/284_zend_native_memory_shared_limit.phpt \
    tests/285_zend_native_memory_no_double_charge.phpt
```

The new tests cover retained-byte accounting and release, iterator ownership
after connection destruction, aggregate limits across iterators, recovery, and
avoidance of duplicate memory charges. Existing tests 211 and 213 retain their
original expectations.
