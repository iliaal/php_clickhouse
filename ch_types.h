#ifndef PHP_CLICKHOUSE_TYPES_H
#define PHP_CLICKHOUSE_TYPES_H

#include "php.h"
#include "Zend/zend_exceptions.h"
#include "php_clickhouse.h"
#include "ch_protocol.h"

extern zend_class_entry *clickhouse_exception_ce;
extern const chc_alloc ch_php_allocator;

typedef struct ch_owned_column ch_owned_column;

/* Writer failures distinguish new validation errors from user exceptions. */
#define CH_TYPE_LOCAL_ERROR (-2)
#define CH_TYPE_JSON_CALLBACK_ERROR (-3)

/* Output is one PHP value; callers own row/result shaping. */
int ch_read_value(zval *out, const chc_type *type, const chc_column *column,
                 size_t row, zend_long flags, unsigned depth);

/* The returned tree owns data and its normalized outgoing type. Returns SUCCESS,
 * CH_TYPE_LOCAL_ERROR, FAILURE for Stringable, or CH_TYPE_JSON_CALLBACK_ERROR. */
int ch_build_column(ch_owned_column **out, const chc_type *type,
                    HashTable *values, unsigned depth, bool allow_null);
const chc_column *ch_owned_column_data(const ch_owned_column *column);
const chc_type *ch_owned_column_type(const ch_owned_column *column);
void ch_owned_column_free(ch_owned_column *column);

int ch_validate_type_name(const char *name, size_t length, const char *context);
/* Empty Tuple nodes throw the legacy factory error; other unsupported types do not. */
bool ch_insert_type_supported(const chc_type *type);
zval *ch_extract_row_cell(zval *row, size_t column_index,
                         zend_string *const *column_names, size_t columns_count);

#endif
