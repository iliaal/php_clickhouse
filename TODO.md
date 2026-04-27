# TODO

Post-0.8.0 work. Gap analysis against [smi2/phpClickHouse](https://github.com/smi2/phpClickHouse), the canonical pure-PHP HTTP client. Items here are functional gaps (real missing capability) or porting friction for users coming from smi2.

HTTP-only features (sessions, curl options, file-based `WHERE IN`/write-to-file, HTTP auth methods, `X-ClickHouse-Summary` header) are excluded since they have no native-binary analogue.

## Real functional gaps

### Async query execution

`selectAsync()` / `executeAsync()` style. Queue several queries, then block until all complete. clickhouse-cpp exposes the primitives (`Client::ExecuteAsync` and friends); we don't bind them.

Scope: architectural. Needs a `ClickHouseFuture` (or similar) result handle, a way to drive completion (poll vs blocking wait-all), and per-future statistics capture. Most natural fit is a 0.9.0 release with its own design pass.

### Cluster / replica awareness

Multi-node cluster support: replica health checks, automatic node selection, table-on-replica discovery, shard/replica counting. smi2's `Cluster` class is the reference shape.

Scope: large. Likely a separate `ClickHouseCluster` class on top of N `ClickHouse` instances, with health probes via the existing `ping()`. 0.9.0+ scope.

### Totals / Extremes capture

`SELECT ... WITH TOTALS` and `SETTINGS extremes=1` currently throw `unimplemented 7` / `unimplemented 8` from the vendored library. clickhouse-cpp v2.6.1 doesn't dispatch the Totals / Extremes packet types ([upstream issue #297](https://github.com/ClickHouse/clickhouse-cpp/issues/297)).

Scope: vendor blocker. Two paths:

1. Local patch: extend `BlockInfo` and add the two cases to the packet dispatch (~30-50 LOC). Ships in `lib/clickhouse-cpp/LOCAL_PATCHES.md`.
2. Wait for upstream.

Once decoded, surface via `getTotals()` / `getExtremes()` methods on `ClickHouse`.

### Protocol-level verbose tracing (landed in 0.8.0)

`setVerbose(bool|callable $sink): static`. `true` writes JSON lines to STDERR; `false` disables; a callable is invoked with `($event, $context)` per event. Events emitted at lifecycle points (`select_start`, `data_block`, `select_finish`, `execute_start`, `execute_finish`) plus `server_exception` from the protocol's exception packet path. Existing progress/profile callbacks are unaffected.

True byte-level wire tracing (raw packet hex dumps) was not implemented. The event-level surface above is what the native protocol's hookable callbacks expose, and matches the operational debugging value of smi2's HTTP request/response logging without the dependency on the underlying transport.

## Cosmetic / porting friction (smi2 -> php_clickhouse)

Landed in 0.8.0:
- `setSettings()` returns `$this` (was `bool`).
- `setSetting(string $key, mixed $value): static` for chainable single-key writes.
- `setDatabase(string $database): static` issues `USE` on the server.
- `ClickHouseException::getServerCode()` / `getServerName()` / `getQueryId()` getter aliases.
- `selectStatement(): ClickHouseStatement` opt-in result wrapper. New `ClickHouseStatement` class implements Iterator, Countable, ArrayAccess, JsonSerializable plus `fetchOne` / `fetchKeyPair` / `fetchColumn` / `toArray` / `statistics`. `select()` is unchanged.

Remaining items are lower-leverage and stay deferred.

### Full chainable settings builder (deferred)

smi2: `$client->settings()->max_execution_time(30)->max_memory_usage(...)`. We have `setSetting($key, $value)` for chainable single-key writes and `setSettings($array)` for bulk replacement, both returning `$this`. The remaining gap is the magic-method-per-key API.

A full `ClickHouseSettings` builder class with one method per setting key is mostly cosmetic; the existing API already chains. ClickHouse has 200+ settings, half unstable, so generating one method per key would couple us to vendor-side churn for marginal benefit. Defer unless users specifically ask for it.

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
- Custom `zend_object` model: ZTS first-class, no module-global state, `free_obj` reaps on bailout.
