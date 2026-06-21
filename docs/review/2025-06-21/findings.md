# Code Review: php_clickhouse Extension

**Date:** 2025-06-21
**Scope:** Full codebase (clickhouse.cpp, typesToPhp.cpp/hpp, php_clickhouse.h, php7_wrapper.h, clickhouse.stub.php, config.m4, config.w32, composer.json, CI workflows, tests/)
**Method:** 5 specialist subagents covering core architecture, type conversion, build/CI, security, and test suite, then consolidated.
**Lines reviewed:** ~9K C++ source + headers, 151 test files, build/CI config

---

## Critical

- **CR-001. `clickhouse.cpp:1418+` — C++ exceptions through callback lambdas into clickhouse-cpp packet loop.** Score: 0.95. Every `OnData`/`OnProgress`/`OnProfile`/`OnLog`/`OnVerbose` lambda registered on the `Query` object calls `call_user_function` into userland PHP. If PHP code throws, the zend engine longjmps through C++ stack frames inside clickhouse-cpp's packet receive/process loop. The `try`/`catch` wrapping `client->Select` is on the outer call frame, not inside the callbacks — the unwinding goes through ClickHouse C++ client internals that are not exception-safe. **What happens:** corrupted connection state, heap metadata corruption, undefined behavior. **Fix:** wrap each callback body in a zend try/catch, log/ignore the exception (or stash it for re-throw after the packet loop), and resume the ClickHouse packet loop.

- **CR-002. `clickhouse.cpp:5640` — `selectStreamCallback` callback zval not pinned.** Score: 0.85. `setProgressCallback`/`setProfileCallback` both pin their callback zvals with `ZVAL_COPY` before `client->SetCallback`. `selectStreamCallback` does not. If the callback zval is freed (userland `unset` of a captured variable, or GC cycle break) before the callback fires, `call_user_function` accesses freed memory. **Fix:** pin the callback with `ZVAL_COPY` before registration, matching the existing pattern.

---

## Important

- **CR-003. `typesToPhp.cpp:2223` vs `typesToPhp.cpp:2969` — UUID formatting inconsistency.** Standalone UUID read produces raw hex (`00000000000000000000000000000000`). Map UUID key read produces canonical dashed format (`00000000-0000-0000-0000-000000000000`). Users comparing `SELECT uuid` vs `SELECT map` output will observe different representations for the same stored type. Round-trips work (both formats accepted on write), but it's a user-visible inconsistency. **Fix:** unify on the dashed canonical format.

- **CR-004. `clickhouse.cpp:97` — `parse_uint128_dec` no overflow guard.** Multiplies by 10 in a loop without pre-multiply overflow detection. A 40-digit decimal string wraps silently. Affects `appendUInt128Column`/`appendUInt256Column` on insert. `"340282366920938463463374607431768211456"` (2^128) wraps to 0. **Fix:** add `v > umax/10` pre-check before `v *= 10; v += digit;`.

- **CR-005. `typesToPhp.cpp:1495` — `Time` insert missing int32 range check.** `value->Append((int32_t)strict_zval_long(...))` silently truncates values outside `[INT32_MIN, INT32_MAX]`. `Time64` at line 1529 has `int64_t` so is safe. **Fix:** add explicit range check for `Time` (int32) before cast.

- **CR-006. Multiple `typesToPhp.cpp` lines — Missing `As<>` null checks in create/insert paths.** Nine sites use `type->As<DateTime64Type>()` and similar without null-checking the result (lines 651, 667, 682, 1423, 1502, 1613, 2479, 2575, 2787). The `convertToZval` read paths consistently use `as_or_throw` for this pattern. If a server schema/type mismatch occurs (crafted server response or code bug), these dereference null. **Fix:** convert to `as_or_throw` for consistency with the read side.

- **CR-007. `typesToPhp.cpp:1617` — `Decimal` insert silently stringifies non-scalar values.** `zval_get_string` on an array produces `"Array"` with only an `E_WARNING`, then `ColumnDecimal::Append` fails to parse it producing a server-side protocol error instead of a clear PHP exception. **Fix:** reject non-scalar zvals before `zval_get_string`.

- **CR-008. `clickhouse.cpp:3630-3641` — Dead `column_names` vector in `insertFromStream`.** `std::vector<zend_string*> column_names` is populated without `zend_string_addref` and never read. The subsequent `getInsertSql` call uses the original `columns` zval. Wastes a per-column allocation per call. **Fix:** remove the dead vector.

- **CR-009. Callback-detach discipline mismatch.** `do_select_into` (line 2234-2240) explicitly detaches 7 callbacks post-`Select`. `selectStream`, `selectStreamCallback`, and `do_select_to_stream` do not detach any. Either the detach calls are dead (Query destructor handles it) or the non-detaching paths rely on undefined behavior. This inconsistency invites a future regression when captures are modified. **Fix:** pick one discipline, apply everywhere, document why.

---

## Medium

- **CR-010. `tests/_clickhouse.inc:48,104` — Skip messages leak host:port.** `echo "skip ClickHouse not reachable at $h:$p"` prints internal network topology to CI output. Minor information disclosure. **Fix:** emit generic "ClickHouse server not reachable".

- **CR-011. `php_clickhouse.h:29` — `PHP_CLICKHOUSE_VERSION` stale.** Still `"0.8.8"` after the 0.8.8 tag. Per AGENTS.md convention this holds the next-to-tag version. **Fix:** bump to `"0.8.9"`.

- **CR-012. `typesToPhp.cpp:1406-1417` — `FixedString` insert no length validation.** Strings longer than the declared `FixedString(N)` width are truncated silently or produce a server-side protocol error. **Fix:** add client-side `sg.len() > width` check.

- **CR-013. `typesToPhp.cpp:730-743,1785-1802,2792-2794` — `LowCardinality` limited to `String`/`FixedString`.** ClickHouse supports `LowCardinality` over any type. Users with `LowCardinality(Int32)` columns get thrown errors on write. **Fix:** expand LC support or document the gap explicitly.

- **CR-014. `typesToPhp.cpp:1092-1138,1892-1950` — Map write limited to specific K/V type combinations.** `Map(Date, String)`, `Map(DateTime, Float64)`, `Map(String, Array(Int64))` fall through to `default` and throw "Unsupported Map value type". Significant surface gap for real-world ClickHouse schemas. **Fix:** expand Map type coverage or document the matrix.

- **CR-015. `clickhouse.cpp:690-697` — Shared `_rv` buffer for seven `sc_zend_read_property` calls.** All seven use the same `_rv` zval. The comment acknowledges this — each result consumed before next overwrites. Fragile: inserting code between the reads would read stale data. **Fix:** per-call `_rv` buffers.

- **CR-016. Test coverage gaps.** Public API methods with no direct test coverage: `selectStatement::statistics()`, `setSetting(string, mixed)` (single-key variant), `selectStatement::offsetSet`/`offsetUnset` exception paths, `jsonSerialize()` return value.

- **CR-017. `clickhouse.cpp:834-847` — `SSL_CTX_load_verify_locations` failure silently ignored.** If `ssl_ca` file doesn't exist, the fallback to default CA paths succeeds silently. User thinks they're pinning to a custom CA but validation uses system CAs. **Fix:** check return value and warn/error.

---

## Minor

- CR-018. `config.w32:33-42` — OpenSSL failure produces `WARNING` (silent non-TLS build) while `config.m4` uses hard error. Asymmetric behavior.
- CR-019. `tests.yml:115-121` — TLS rebuild step compiles only, tests not re-run. Acknowledged CI gap.
- CR-020. `typesToPhp.cpp:1218` — `strtoul` for `UInt32` — on 32-bit Windows, `ULONG_MAX == UINT32_MAX`, so `0x100000000` wraps past the `> MaxV` check. Niche platform edge case.
- CR-021. `typesToPhp.cpp:558` — `to_time_t_with_frac` declares unused `scale` parameter, marked `(void)scale`.
- CR-022. `typesToPhp.cpp:255-256` — Comment says "next representable double above 2^64" but `18446744073709551616.0` is exactly 2^64. Logic correct, comment inaccurate.
- CR-023. `clickhouse.cpp:1540` — `emitVerbose` calls `zend_clear_exception()` on JSON encode failure. Intentional, but a verbose-triggered encode failure masks the user's real error.
- CR-024. `clickhouse.cpp:4961` — `selectStream` catch block writes `ZVAL_NULL(return_value)` before `throwClickHouseError`. Harmless dead assignment.
- CR-025. No test for `send_timeout_ms` / `receive_timeout_ms` config (only `connect_timeout_ms` tested).
- CR-026. No TLS test execution in CI — `027.phpt` never runs because the Docker service container has no TLS port.
- CR-027. `insertAssoc()` with `$settings` parameter not explicitly tested.
- CR-028. `selectToStream()` with `DATE_AS_STRINGS` flag not tested.

---

## What's Working Well

- **Reentry guard** (`QueryActiveGuard` RAII at clickhouse.cpp:1613): prevents wire corruption from reentrant client access.
- **GC cycle breaking** (clickhouse.cpp:215-254): correct `get_gc` handler for callback-holding objects, properly handles PHP 7 vs 8 API differences.
- **Exception safety architecture**: all C++->PHP boundaries go through `throwClickHouseError` (line 1178), which preserves existing PHP exceptions.
- **Callback pinning** (clickhouse.cpp:1457-1496): `ZVAL_COPY` before `call_user_function` prevents callback self-unregistration during invocation. Applied correctly in `setProgressCallback`/`setProfileCallback` — just missed in `selectStreamCallback`.
- **Placeholder validation** (clickhouse.cpp:1808-1871): `validatePlaceholderToken` rigorously rejects identifiers and literals that aren't single tokens, preventing SQL injection through `{name}` substitution.
- **`strict_zval_long`/`strict_zval_u64`/`strict_zval_double`** (typesToPhp.cpp): comprehensive numeric input validation — rejects NaN/Inf, fractional doubles for integer columns, non-numeric strings.
- **`ConvertDepthGuard`** (typesToPhp.cpp): prevents stack overflow on deeply nested types from hostile server schemas.
- **`to_time_t` round-trip validation**: catches silent date normalization (Feb 30 -> Mar 2) via `gmtime_r` round-trip before storing.
- **`sqlStringLiteral` / `sqlQuotedIdentifier`** (clickhouse.cpp:1004-1041): correct escaping for SQL string values and backtick-quoted identifiers.
- **Test suite breadth**: 151 tests covering type safety, error paths, streaming I/O, callbacks, GC/cycles, and security hardening. Exceptionally thorough for an extension of this size.
- **ASAN CI job** with documented suppression rationale, ZTS canary build, Windows matrix — mature CI for a v0.8.x project.

---

## Residual Risks

- **Callback exception safety (CR-001)** is the top risk. Real, reachable from userland (any callback that throws), and lands in undefined behavior through C++ client internals. Needs a zend_longjmp barrier inside each callback lambda.
- **The two `std::string` password fields** sit in process heap until object destruction. No zeroing on error paths. Core dump exposure is accepted risk per PHP extension convention but worth noting.
- **UInt64 placeholder saturation** (`applyPlaceholders` via `strtol`/`strtoul`): out-of-range values silently saturate to `LONG_MAX`/`ULONG_MAX` rather than erroring. Test coverage exists for column insert path (060, 068) but not the placeholder path.
- **`isStreamableColumnType` recursion** (clickhouse.cpp:2547-2551): unbounded for hostile server schemas with deep `Nullable(LowCardinality(Nullable(...)))` nesting. Runtime stack is the only guard.
- **Test parallel safety**: all tests share the `test.*` database namespace. Runner is single-threaded, but a leftover table from a mid-test crash contaminates subsequent runs.

---

## Verdict

**Ready with fixes** — CR-001 (callback exception safety) and CR-002 (unpinned callback zval) must be resolved before next release. CR-003 (UUID formatting inconsistency) and CR-004 (uint128 overflow) are important behavioral bugs that users will hit. The remaining issues are medium/minor — documentable tech debt.
