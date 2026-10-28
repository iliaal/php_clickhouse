#ifndef PHP_CLICKHOUSE_C_INTERNAL_H
#define PHP_CLICKHOUSE_C_INTERNAL_H

#include "php_clickhouse.h"
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include "Zend/zend_exceptions.h"
#include "Zend/zend_smart_str.h"
#include "php7_wrapper.h"
#include "ch_backend.h"
#include "ch_types.h"

/* PHP-facing helpers return SUCCESS/FAILURE; FAILURE leaves EG(exception).
 * Backend helpers return CHC_* and never invoke PHP. The query boundary
 * translates CHC failures once, retaining an already-pending PHP exception. */

typedef struct {
	uint64_t rows_read;
	uint64_t bytes_read;
	uint64_t total_rows;
	uint64_t written_rows;
	uint64_t written_bytes;
	uint64_t blocks;
	uint64_t rows_before_limit;
	bool applied_limit;
	double elapsed_ms;
	zend_string *last_query_id;
} ch_stats;

typedef struct {
	ch_connection *backend;
	/* Root a replacement before cloning/connecting can allocate. */
	ch_connection *pending_backend;
	/* Native INSERT outlives one PHP method's reentry guard. The schema can
	 * be NULL while BeginInsert is still receiving the server's response. */
	bool insert_active;
	chc_block *insert_schema;
	bool query_active;
	zend_string *failure_message;
	zend_long failure_code;
	ch_stats stats;
	/* String keys and owned string zvals, never unconverted user values. */
	HashTable settings;
	zval progress_callback;
	zval profile_callback;
	zval verbose_callback;
	bool verbose_to_stderr;
	bool log_enabled;
	zend_string *insert_sql;
	zend_string *insert_query_id;
	uint64_t insert_started_ns;
	/* Owned scalar log arrays; preserve the existing 1024-entry cap. */
	HashTable query_log;
	zend_ulong query_log_next;
#if PHP_VERSION_ID < 80000
	zval gc_buf[3];
#endif
	zend_object std;
} clickhouse_object;

/* init initializes both tables, including on a subsequently failing prepare.
 * Wire arrays borrow strings from these owned snapshots. log_sql retains the
 * original template; sql carries executable text. Populate upstream explicit
 * string lengths for settings/params, including embedded bytes. */
typedef struct {
	zend_string *sql;
	zend_string *log_sql;
	zend_string *query_id;
	HashTable settings;
	HashTable params;
	chc_query_setting *wire_settings;
	chc_query_param *wire_params;
	chc_query_opts wire;
	bool distrust_braces;
} ch_query;

/* Borrowed view; its owner retains name, type ASTs, columns, and builder
 * arrays through completion, cancellation, or failure. */
typedef struct {
	zend_string *name;
	const chc_block_builder *block;
} ch_external_table;

typedef struct ch_external_tables ch_external_tables;

typedef enum {
	CH_BLOCK_CONTINUE,
	CH_BLOCK_CANCEL,
	CH_BLOCK_FAIL
} ch_block_action;

/* Runner owns *block. A consumer takes ownership by setting *block = NULL.
 * CANCEL suppresses later data callbacks and drains or resets before returning.
 * FAIL preserves the callback's exception while restoring connection state. */
typedef ch_block_action (*ch_on_block)(clickhouse_object *obj,
	chc_block **block, void *context);

static inline clickhouse_object *clickhouse_from_obj(zend_object *obj)
{
	return (clickhouse_object *)((char *)obj - offsetof(clickhouse_object, std));
}

#define Z_CLICKHOUSE_P(zv) clickhouse_from_obj(Z_OBJ_P(zv))

extern zend_class_entry *clickhouse_ce;
extern zend_class_entry *clickhouse_exception_ce;
extern zend_class_entry *clickhouse_iter_ce;
extern zend_class_entry *clickhouse_statement_ce;

/* ch_query.c: acquire before preparation or callback-capable coercion.
 * Only a caller whose begin succeeded calls end. allow_insert permits
 * write/writeEnd on the existing session; nested calls remain prohibited. */
int ch_operation_begin(clickhouse_object *obj, bool allow_insert);
void ch_operation_end(clickhouse_object *obj);
void ch_query_init(ch_query *query);
int ch_query_prepare(clickhouse_object *obj, ch_query *query,
	zend_string *sql, zval *params, zend_string *query_id,
	zval *settings, const char *params_error);
void ch_query_clear(ch_query *query);
int ch_format_scalar(zend_string **out, zval *value);
zend_string *ch_sanitize_error(const char *text, size_t length);
int ch_query_event(clickhouse_object *obj, const chc_packet *packet,
	zend_string *query_id, bool callbacks);
int ch_validate_block(const chc_block *block, chc_err *error);
int ch_query_run(clickhouse_object *obj, ch_query *query,
	const ch_external_table *externals, size_t external_count,
	ch_on_block on_block, void *context);

/* ch_insert.c: begin retains its schema on obj. send borrows its builder.
 * end clears session ownership after successful server completion. clear
 * frees retained state and performs no I/O. */
int ch_insert_begin(clickhouse_object *obj, ch_query *query);
int ch_insert_send(clickhouse_object *obj, const chc_block_builder *block);
int ch_insert_end(clickhouse_object *obj);
void ch_insert_clear(clickhouse_object *obj);

/* Recovery preserves an existing exception. finalize=true commits already
 * accepted blocks after local conversion failure; false resets incomplete
 * transport. Failed recovery must not falsely report an idle session. */
int ch_insert_recover(clickhouse_object *obj, bool finalize);

/* Finalize an abandoned INSERT with the existing 5s I/O cap, suppress
 * callbacks, and never emit a new PHP exception during destruction. */
void ch_insert_abandon(clickhouse_object *obj);

/* Root *out before nested allocations; destroy accepts partial state.
 * Snapshot descriptors/rows before conversion can invoke user code. */
int ch_external_tables_create(ch_external_tables **out, zval *descriptors);
const ch_external_table *ch_external_tables_data(const ch_external_tables *tables);
size_t ch_external_tables_count(const ch_external_tables *tables);
void ch_external_tables_destroy(ch_external_tables *tables);

/* ch_query.c: error inputs are borrowed; caller frees native exceptions.
 * Recovery preserves both the original exception and configured database. */
void ch_throw_error(int status, const chc_err *error,
	const chc_exception *server_error, zend_string *query_id);
/* Call only when the pending exception was created by internal validation. */
void ch_annotate_local_exception(zend_string *query_id);
void ch_failure_set(clickhouse_object *obj, zend_long code,
	const char *message, size_t length);
void ch_failure_native(clickhouse_object *obj, int status, const chc_err *error,
	const chc_exception *server_error);
void ch_failure_clear(clickhouse_object *obj);
int ch_reset_after_error(clickhouse_object *obj);
void ch_stats_reset(ch_stats *stats);
void ch_stats_array(zval *out, const ch_stats *stats);
void ch_add_assoc_uint64(zval *out, const char *name, uint64_t value);
uint64_t ch_monotonic_ns(void);

/* Consumes context, leaving IS_UNDEF even on failure. Copy the callback
 * before invoking it so self-unregistration remains safe. */
int ch_emit_verbose(clickhouse_object *obj, const char *event, zval *context);

/* ch_results.c: these helpers own their operation guard and borrow inputs.
 * positional_out may be NULL; otherwise initialize it to UNDEF and populate
 * it only when duplicate result column names require positional rows. */
int ch_select_into(zval *out, zval *this_obj, zend_string *sql,
	zval *params, zend_long flags, zend_string *query_id, zval *settings,
	const ch_external_table *externals, size_t external_count,
	zval *positional_out);
int ch_execute_into(zval *this_obj, zend_string *sql, zval *params,
	zend_string *query_id, zval *settings);
int ch_results_minit(INIT_FUNC_ARGS);

/* metadata.c: string outputs are owned; failure leaves *out == NULL. */
int ch_sql_string_literal(zend_string **out, zend_string *value);
int ch_sql_quoted_identifier(zend_string **out, zend_string *value);
int ch_parse_identifier(zend_string **out, const char *value, size_t length,
	const char *what, bool allow_dot);
int ch_validate_identifier(const char *value, size_t length,
	const char *what, bool allow_dot);
int ch_validate_metadata_filter_name(zend_string *value, const char *what);
int ch_current_database(zend_string **out, zval *this_obj);
int ch_helper_select(zval *out, zval *this_obj, zend_string *sql,
	zend_long flags);
int ch_helper_execute(zval *this_obj, zend_string *sql);
int ch_helper_select_first_row(zval *out, zval *this_obj, zend_string *sql);

/* query_log.c: logging never replaces an existing PHP exception.
 * distrust_braces preserves malformed-placeholder redaction semantics. */
zend_string *ch_query_log_string(zend_string *value);
zend_string *ch_redact_sql_literals(zend_string *sql, bool distrust_braces);
void ch_record_query_success(clickhouse_object *obj, zend_string *sql,
	zend_string *query_id, bool distrust_braces);
void ch_record_query_error(clickhouse_object *obj, zend_string *sql,
	zend_string *query_id, bool distrust_braces);
void ch_record_query_local_error(clickhouse_object *obj, zend_string *sql,
	zend_string *query_id, bool distrust_braces);

/* Keep the parent's existing ch_types.h declarations unchanged.
 * The immutable ch_php_allocator has process lifetime; its allocations are
 * request-scoped. A retained block can therefore outlive its connection.
 * Conversion depth starts at zero; allow_null is granted only by Nullable.
 * A failed read leaves out IS_UNDEF after destroying partial PHP values.
 * An owned column owns its tree, slabs, and normalized immutable type AST.
 * Preserve existing depth/type-name/JSON/decoding limits; add no new limit.
 *
 * Implementation ownership:
 * clickhouse.c: lifecycle, configuration, simple methods, module registration.
 * ch_query.c: preparation, parameter/settings encoding, packet loop, events.
 * ch_insert.c: insert lifecycle, external data, insertFromStream, write*.
 * ch_results.c: select variants, selectToStream, iterators and statements.
 * ch_types_read.c / ch_types_write.c: conversion through ch_types.h.
 * ch_wide.h/.c: portable integer/decimal internals shared by converters.
 * metadata.c / query_log.c: existing helper SQL and query-log semantics.
 * ch_backend.h/.c: copied config, socket/TLS, retries, raw server information.
 * Generated arginfo remains the source of PHP_METHOD declarations.
 */

#endif
