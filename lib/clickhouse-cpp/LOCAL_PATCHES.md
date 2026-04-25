# Local patches against vendored clickhouse-cpp v2.6.1

Anything listed here must be re-applied (or upstreamed and dropped)
when the vendored library is bumped.

## clickhouse/client.cpp: `Client::Impl::BeginInsert` drops `query_id`

`Client::Impl::BeginInsert(Query query)` constructs the wire-level
`SendQuery` from `query.GetText()` rather than from the full `Query`
object. The implicit `Query(const std::string&)` constructor uses
`Query::default_query_id` (empty), so any `query_id` passed into
`Client::BeginInsert(query, query_id)` is silently discarded and the
INSERT lands in `system.query_log` with an auto-generated id.

The one-shot `Client::Impl::Insert` path is unaffected because it
calls `SendQuery(query)` directly with the `Query` object.

Patch: change `SendQuery(query.GetText())` to `SendQuery(query)` in
`Client::Impl::BeginInsert`.

This is exercised by `tests/028.phpt` (writeStart query_id propagation).
