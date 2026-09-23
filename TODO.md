# TODO

Gap analysis against [smi2/phpClickHouse](https://github.com/smi2/phpClickHouse), the most-used pure-PHP HTTP client. Items are either missing capability or porting friction for users coming from smi2.

HTTP-only features (sessions, curl options, file-based `WHERE IN`/write-to-file, HTTP auth methods, `X-ClickHouse-Summary` header) are excluded since they have no native-binary analogue.

## Real functional gaps

### Totals / Extremes capture

`SELECT ... WITH TOTALS` and `SETTINGS extremes=1` currently throw `unimplemented 7` / `unimplemented 8` from the vendored library. clickhouse-cpp (through at least v2.6.2) doesn't dispatch the Totals / Extremes packet types ([upstream issue #297](https://github.com/ClickHouse/clickhouse-cpp/issues/297)).

Scope: vendor blocker. Two paths:

1. Local patch: extend `BlockInfo` and add the two cases to the packet dispatch (~30-50 LOC). Ships in `lib/clickhouse-cpp/LOCAL_PATCHES.md`.
2. Wait for upstream.

Once decoded, surface via `getTotals()` / `getExtremes()` methods on `ClickHouse`.

## Deferred porting friction (smi2 -> php_clickhouse)

### Full chainable settings builder (deferred)

smi2: `$client->settings()->max_execution_time(30)->max_memory_usage(...)`. We have `setSetting($key, $value)` for chainable single-key writes and `setSettings($array)` for bulk replacement, both returning `$this`. The remaining gap is the magic-method-per-key API.

A full `ClickHouseSettings` builder class with one method per setting key is mostly cosmetic; the existing API already chains. ClickHouse has 200+ settings, half of them unstable, so one method per key would tie us to upstream churn. Defer unless users ask for it.

### Bindings / placeholder syntax compatibility (deferred)

smi2: `:param` style with `bindParams([':a' => 1])`. php_clickhouse: `{name}` (client-side identifier substitution) and `{name:Type}` (server-side typed parameter).

The native-protocol typed form is strictly better. Adding `:param` as a third alias would mean more documentation surface and more cases where a query author has to remember which form does what. We should be steering smi2 users toward `{name:Type}`, not making the unsafe-substitution path feel familiar. Skip on principle.

## Differentiators to keep highlighting

(Not gaps; reminders for README/marketing.)

- Multi-endpoint failover at the native protocol level.
- Native LZ4 / ZSTD (smi2 only has HTTP gzip).
- Native TLS via `--enable-clickhouse-openssl`.
- Sub-millisecond timeout precision.
- TCP socket knobs (`tcp_nodelay`, full `tcp_keepalive_*`).
- True native decoding of `Map`, `LowCardinality`, `Tuple` (HTTP returns strings or JSON).
- Custom `zend_object` model: ZTS-safe, no module-global state, `free_obj` reaps on bailout.
