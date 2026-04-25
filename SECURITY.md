# Security policy

## Supported versions

| Version | Supported          |
|---------|--------------------|
| 0.5.x   | :white_check_mark: |

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

php_clickhouse is a thin binding to the official ClickHouse/clickhouse-cpp
client library. Vulnerabilities in the vendored client (LZ4 / ZSTD /
cityhash / absl int128 / clickhouse-cpp itself) are reported to their
respective upstream projects; we'll re-vendor as soon as a fix is tagged.

User input passes from PHP into the wire protocol via the typed column
machinery in `typesToPhp.cpp`. Bugs in that layer (out-of-bounds reads,
use-after-free, integer truncation that leads to incorrect SQL) are
in scope.

The TLS/SSL build (`--enable-clickhouse-openssl`) links against the
system OpenSSL. Misuse of `ssl_skip_verify => true` is documented as a
dev-only knob; it disables certificate validation entirely.
