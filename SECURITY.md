# Security policy

## Supported versions

| Version | Supported          |
|---------|--------------------|
| 0.12.x  | :white_check_mark: |
| 0.11.x  | :white_check_mark: |
| 0.10.x  | security fixes only |
| ≤ 0.9.x | :x:                |

Once 1.0 ships, the two most recent minor versions will receive
security fixes.

## Reporting a vulnerability

**Do not file a public GitHub issue for security vulnerabilities.**

Use GitHub's private security advisory feature at
<https://github.com/iliaal/php_clickhouse/security/advisories/new>
or email Ilia Alshanetsky <ilia@ilia.ws> directly.

Please include:

- Affected php_clickhouse version (`php -r 'echo phpversion("clickhouse");'`)
- Affected ClickHouse server version
- A minimal reproducing case (PHP code + the SQL or input that triggers it)
- Impact: crash / RCE / info disclosure / DoS / etc.
- Whether you've coordinated disclosure with anyone else

Acknowledgement within 7 days, fix or status update within 30. Once a
fix is released the advisory becomes public.

## Scope

php_clickhouse uses the official ClickHouse/clickhouse-c protocol client
with C bindings and transport code. Vulnerabilities in the vendored client
and its LZ4, ZSTD, and CityHash dependencies are reported to their
respective upstream projects; we'll re-vendor as soon as a fix is tagged.

User input passes from PHP into the wire protocol via the typed column
conversion in `ch_types_write.c`; `ch_types_read.c` converts server results
to PHP values. These paths use portable wide-integer arithmetic in
`ch_wide.c`. Bugs in the extension's bindings, conversion, and transport
code (out-of-bounds reads, use-after-free, integer truncation that leads
to incorrect SQL) are in scope.

The TLS/SSL build (`--enable-clickhouse-openssl`) links against the
system OpenSSL. Misuse of `ssl_skip_verify => true` is documented as a
dev-only knob; it disables certificate validation entirely.
