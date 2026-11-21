#ifndef PHP_CLICKHOUSE_PROTOCOL_H
#define PHP_CLICKHOUSE_PROTOCOL_H

/* Keep one byte beyond the PHP error cap so final sanitization can mark truncation. */
#define CHC_ERR_MSG_LEN 4098
#include "lib/clickhouse-c/clickhouse-client.h"

/* Records a refused native allocation for the PHP method boundary (clickhouse.c). */
void ch_native_oom_note(size_t bytes);
/* True when a Zend allocation of bytes fits memory_limit; a refusal is recorded
 * as a native one, so the method boundary reports it the same way. */
bool ch_native_reserve(size_t bytes);

#endif
