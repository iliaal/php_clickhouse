/*
  +----------------------------------------------------------------------+
  | php_clickhouse                                                       |
  +----------------------------------------------------------------------+
  | Copyright (c) 1997-2026 The PHP Group                                |
  +----------------------------------------------------------------------+
  | This source file is subject to version 3.01 of the PHP license,      |
  | that is bundled with this package in the file LICENSE, and is        |
  | available through the world-wide-web at the following url:           |
  | http://www.php.net/license/3_01.txt                                  |
  | If you did not receive a copy of the PHP license and are unable to   |
  | obtain it through the world-wide-web, please send a note to          |
  | license@php.net so we can mail you a copy immediately.               |
  +----------------------------------------------------------------------+
  | Author: Ilia Alshanetsky <ilia@ilia.ws>                              |
  +----------------------------------------------------------------------+
*/
#ifndef CH_BACKEND_H
#define CH_BACKEND_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include "ch_protocol.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct ch_connection ch_connection;

typedef struct {
	const char *data;
	size_t len;
} ch_backend_string;

typedef struct {
	ch_backend_string host;
	uint16_t port;
} ch_backend_endpoint;

typedef enum {
	CH_BACKEND_TLS_1_0 = 0x0301,
	CH_BACKEND_TLS_1_1 = 0x0302,
	CH_BACKEND_TLS_1_2 = 0x0303,
	CH_BACKEND_TLS_1_3 = 0x0304
} ch_backend_tls_version;

typedef struct {
	const ch_backend_endpoint *endpoints;
	size_t endpoint_count;
	ch_backend_string database;
	ch_backend_string user;
	ch_backend_string password;
	uint32_t connect_timeout_ms;
	uint32_t receive_timeout_ms;
	uint32_t send_timeout_ms;
	uint32_t retry_count;
	uint64_t retry_timeout_seconds;
	bool ping_before_query;
	bool tcp_nodelay;
	bool tcp_keepalive;
	uint32_t keepalive_idle_seconds;
	uint32_t keepalive_interval_seconds;
	uint32_t keepalive_count;
	chc_compression compression;
	uint32_t max_compression_chunk_size;
	bool ssl;
	bool ssl_skip_verify;
	bool ssl_use_default_ca;
	int ssl_min_protocol_version;
	ch_backend_string ssl_ca_directory;
	const ch_backend_string *ssl_ca_files;
	size_t ssl_ca_file_count;
} ch_backend_options;

void ch_backend_options_init(ch_backend_options *options);

/* Inputs are borrowed. Publish out into the owning object's cleanup slot:
 * create/clone copy their inputs, publish before nested allocation, and do
 * not connect. The allocator must outlive the connection and decoded blocks. */
int ch_backend_create(ch_connection **out, const ch_backend_options *options,
	const chc_alloc *allocator, chc_err *error);
int ch_backend_clone_database(ch_connection **out, const ch_connection *source,
	const char *database, size_t database_len, chc_err *error);

/* Exceptions returned through exception belong to the caller, using the
 * connection allocator. These functions do not execute PHP callbacks. */
int ch_backend_connect(ch_connection *connection, chc_exception **exception, chc_err *error);
int ch_backend_reset(ch_connection *connection, chc_exception **exception, chc_err *error);
int ch_backend_before_query(ch_connection *connection, chc_exception **exception, chc_err *error);
int ch_backend_ping(ch_connection *connection, chc_exception **exception, chc_err *error);
int ch_backend_limit_io_timeouts(ch_connection *connection, uint32_t maximum_ms, chc_err *error);

/* The query owner finalizes an abandoned insert before destroying its backend. */
void ch_backend_disconnect(ch_connection *connection);
void ch_backend_destroy(ch_connection *connection);
chc_client *ch_backend_client(ch_connection *connection);
const ch_backend_endpoint *ch_backend_current_endpoint(const ch_connection *connection);
/* Last successfully connected Hello remains available after disconnect or a
 * failed reconnect; the returned strings belong to the connection. */
const chc_server_info *ch_backend_server_info(const ch_connection *connection);

#ifdef __cplusplus
}
#endif

#endif
