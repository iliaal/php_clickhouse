clickhouse_binding_headers = \
	$(srcdir)/ch_backend.h \
	$(srcdir)/ch_protocol.h \
	$(srcdir)/ch_types.h \
	$(srcdir)/ch_wide.h \
	$(srcdir)/clickhouse_c_internal.h \
	$(srcdir)/clickhouse_arginfo.h \
	$(srcdir)/php7_wrapper.h \
	$(srcdir)/php_clickhouse.h \
	$(srcdir)/lib/clickhouse-c/clickhouse.h \
	$(srcdir)/lib/clickhouse-c/clickhouse-client.h \
	$(srcdir)/lib/clickhouse-c/clickhouse-compression.h \
	$(srcdir)/lib/compression/lz4/lz4.h \
	$(srcdir)/lib/compression/zstd/zstd.h

$(builddir)/clickhouse.lo \
$(builddir)/ch_backend.lo \
$(builddir)/ch_protocol.lo \
$(builddir)/ch_query.lo \
$(builddir)/ch_insert.lo \
$(builddir)/ch_results.lo \
$(builddir)/ch_types_read.lo \
$(builddir)/ch_types_write.lo \
$(builddir)/ch_wide.lo \
$(builddir)/metadata.lo \
$(builddir)/query_log.lo: $(clickhouse_binding_headers)
