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
#if defined(__APPLE__) && !defined(_DARWIN_C_SOURCE)
/* Strict POSIX mode hides Darwin keepalive tuning and SO_NOSIGPIPE. */
#define _DARWIN_C_SOURCE 1
#endif
#ifndef _WIN32
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif
#endif
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
typedef SOCKET ch_socket;
#define CH_INVALID_SOCKET INVALID_SOCKET
#else
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <poll.h>
#include <fcntl.h>
#include <unistd.h>
typedef int ch_socket;
#define CH_INVALID_SOCKET (-1)
#endif

#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#ifdef WITH_OPENSSL
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/x509v3.h>
#endif

#include "ch_backend.h"

#define ZSTD_STATIC_LINKING_ONLY
#include "lib/compression/zstd/zstd.h"
#include "lib/compression/zstd/zstd_errors.h"

struct ch_connection {
	const chc_alloc *allocator;
	ch_backend_options options;
	ch_backend_endpoint *endpoints;
	ch_backend_string *ca_files;
	size_t cursor;
	size_t current;
	bool has_current;
	ch_socket socket;
	struct addrinfo *addresses;
	chc_io io;
	chc_codec codec;
	chc_client *client;
	chc_server_info server_info;
	bool has_server_info;
	uint32_t receive_timeout_ms;
	uint32_t send_timeout_ms;
#ifdef _WIN32
	bool winsock_started;
#endif
#ifdef WITH_OPENSSL
	SSL_CTX *ssl_context;
	SSL *ssl;
	BIO_METHOD *bio_method;
	BIO *pending_bio;
#endif
};

typedef struct {
	bool finite;
	uint64_t expires;
} ch_deadline;

static int ch_error(chc_err *error, int status, const char *format, ...)
{
	if (error) {
		va_list arguments;
		va_start(arguments, format);
		vsnprintf(error->msg, sizeof(error->msg), format, arguments);
		va_end(arguments);
	}
	return status;
}

/* ZSTD frees without a size and requires an eight-byte-aligned payload. */
typedef union {
	size_t bytes;
	uint64_t alignment;
} ch_zstd_allocation;

_Static_assert(sizeof(ch_zstd_allocation) % 8 == 0, "ZSTD allocation prefix alignment");

static void *ch_zstd_alloc(void *context, size_t bytes)
{
	ch_connection *connection = (ch_connection *) context;
	const chc_alloc *allocator = connection->allocator;
	ch_zstd_allocation *allocation;
	if (bytes > SIZE_MAX - sizeof(*allocation)) {
		return NULL;
	}
	bytes += sizeof(*allocation);
	allocation = (ch_zstd_allocation *) allocator->alloc(allocator->ud, bytes);
	if (!allocation) {
		return NULL;
	}
	allocation->bytes = bytes;
	return allocation + 1;
}

static void ch_zstd_free(void *context, void *pointer)
{
	ch_connection *connection = (ch_connection *) context;
	const chc_alloc *allocator = connection->allocator;
	if (pointer) {
		ch_zstd_allocation *allocation = (ch_zstd_allocation *) pointer - 1;
		allocator->free(allocator->ud, allocation, allocation->bytes);
	}
}

static int ch_zstd_compress(void *context, const void *source, size_t source_len,
	void *destination, size_t capacity, size_t *written, chc_err *error)
{
	ZSTD_customMem memory = {ch_zstd_alloc, ch_zstd_free, context};
	ZSTD_CCtx *compressor = ZSTD_createCCtx_advanced(memory);
	if (!compressor) {
		return ch_error(error, CHC_ERR_OOM, "ZSTD_compress failed: %s",
			ZSTD_getErrorString(ZSTD_error_memory_allocation));
	}
	size_t size = ZSTD_compressCCtx(compressor, destination, capacity, source, source_len, 1);
	ZSTD_freeCCtx(compressor);
	if (ZSTD_isError(size)) {
		return ch_error(error, CHC_ERR_OOM, "ZSTD_compress failed: %s", ZSTD_getErrorName(size));
	}
	*written = size;
	return CHC_OK;
}

static int ch_zstd_decompress(void *context, const void *source, size_t source_len,
	void *destination, size_t original, chc_err *error)
{
	ZSTD_customMem memory = {ch_zstd_alloc, ch_zstd_free, context};
	ZSTD_DCtx *decompressor = ZSTD_createDCtx_advanced(memory);
	if (!decompressor) {
		return ch_error(error, CHC_ERR_OOM, "can't decompress ZSTD-encoded data, ZSTD error: %s",
			ZSTD_getErrorString(ZSTD_error_memory_allocation));
	}
	size_t size = ZSTD_decompressDCtx(decompressor, destination, original, source, source_len);
	ZSTD_freeDCtx(decompressor);
	if (ZSTD_isError(size)) {
		int status = ZSTD_getErrorCode(size) == ZSTD_error_memory_allocation ? CHC_ERR_OOM : CHC_ERR_PROTOCOL;
		return ch_error(error, status, "can't decompress ZSTD-encoded data, ZSTD error: %s",
			ZSTD_getErrorName(size));
	}
	if (size != original) {
		return ch_error(error, CHC_ERR_PROTOCOL,
			"ZSTD-encoded block decompressed to %zu bytes, expected %zu", size, original);
	}
	return CHC_OK;
}

static int ch_socket_error_code(void)
{
#ifdef _WIN32
	return WSAGetLastError();
#else
	return errno;
#endif
}

static bool ch_interrupted(int code)
{
#ifdef _WIN32
	return code == WSAEINTR;
#else
	return code == EINTR;
#endif
}

static bool ch_would_block(int code)
{
#ifdef _WIN32
	return code == WSAEWOULDBLOCK || code == WSAEINPROGRESS;
#else
	return code == EAGAIN || code == EWOULDBLOCK || code == EINPROGRESS;
#endif
}

static int ch_socket_error(chc_err *error, const char *operation, int code)
{
#ifdef _WIN32
	char text[192];
	DWORD length = FormatMessageA(FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
		NULL, (DWORD) code, 0, text, (DWORD) sizeof(text), NULL);
	if (!length) {
		return ch_error(error, CHC_ERR_IO, "%s failed (socket error %d)", operation, code);
	}
	while (length && (text[length - 1] == '\r' || text[length - 1] == '\n')) {
		text[--length] = '\0';
	}
	return ch_error(error, CHC_ERR_IO, "%s: %s", operation, text);
#else
	char text[192];
#if defined(__GLIBC__) && defined(_GNU_SOURCE)
	const char *message = strerror_r(code, text, sizeof(text));
	return ch_error(error, CHC_ERR_IO, "%s: %s", operation, message);
#else
	if (strerror_r(code, text, sizeof(text)) != 0) {
		return ch_error(error, CHC_ERR_IO, "%s failed (socket error %d)", operation, code);
	}
	return ch_error(error, CHC_ERR_IO, "%s: %s", operation, text);
#endif
#endif
}

static uint64_t ch_now_ms(void)
{
#ifdef _WIN32
	return (uint64_t) GetTickCount64();
#else
	struct timespec now;
	if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) {
		struct timeval fallback;
		gettimeofday(&fallback, NULL);
		return (uint64_t) fallback.tv_sec * 1000 + (uint64_t) fallback.tv_usec / 1000;
	}
	return (uint64_t) now.tv_sec * 1000 + (uint64_t) now.tv_nsec / 1000000;
#endif
}

static ch_deadline ch_deadline_start(uint32_t milliseconds, bool zero_is_infinite)
{
	ch_deadline deadline;
	deadline.finite = milliseconds != 0 || !zero_is_infinite;
	deadline.expires = ch_now_ms() + milliseconds;
	return deadline;
}

static int ch_poll(ch_connection *connection, bool write_ready,
	const ch_deadline *deadline, const char *operation, chc_err *error)
{
	assert(connection->socket != CH_INVALID_SOCKET);
	for (;;) {
		int timeout = -1;
		if (deadline->finite) {
			uint64_t now = ch_now_ms();
			uint64_t remaining = now < deadline->expires ? deadline->expires - now : 0;
			timeout = remaining > INT_MAX ? INT_MAX : (int) remaining;
		}
#ifdef _WIN32
		WSAPOLLFD descriptor;
#else
		struct pollfd descriptor;
#endif
		descriptor.fd = connection->socket;
		descriptor.events = write_ready ? POLLOUT : POLLIN;
		descriptor.revents = 0;
#ifdef _WIN32
		int result = WSAPoll(&descriptor, 1, timeout);
#else
		int result = poll(&descriptor, 1, timeout);
#endif
		if (result > 0) {
			return CHC_OK;
		}
		if (result == 0) {
			if (!deadline->finite || ch_now_ms() < deadline->expires) {
				continue;
			}
			return ch_error(error, CHC_ERR_IO, "%s timed out", operation);
		}
		int code = ch_socket_error_code();
		if (!ch_interrupted(code)) {
			return ch_socket_error(error, operation, code);
		}
	}
}

static void ch_close_socket(ch_connection *connection)
{
	assert(connection != NULL);
	if (connection->socket != CH_INVALID_SOCKET) {
#ifdef _WIN32
		closesocket(connection->socket);
#else
		close(connection->socket);
#endif
		connection->socket = CH_INVALID_SOCKET;
	}
}

static int ch_nonblocking(ch_connection *connection, chc_err *error)
{
	assert(connection->socket != CH_INVALID_SOCKET);
#ifdef _WIN32
	u_long enabled = 1;
	if (ioctlsocket(connection->socket, FIONBIO, &enabled) != 0) {
		return ch_socket_error(error, "ioctlsocket", ch_socket_error_code());
	}
#else
	int flags = fcntl(connection->socket, F_GETFL, 0);
	if (flags < 0 || fcntl(connection->socket, F_SETFL, flags | O_NONBLOCK) < 0) {
		return ch_socket_error(error, "fcntl", errno);
	}
#endif
	return CHC_OK;
}

static int ch_apply_timeouts(ch_connection *connection, chc_err *error)
{
	assert(connection->socket != CH_INVALID_SOCKET);
#ifdef _WIN32
	DWORD receive = connection->receive_timeout_ms;
	DWORD send_timeout = connection->send_timeout_ms;
#else
	struct timeval receive;
	struct timeval send_timeout;
	receive.tv_sec = (time_t) (connection->receive_timeout_ms / 1000);
	receive.tv_usec = (suseconds_t) ((connection->receive_timeout_ms % 1000) * 1000);
	send_timeout.tv_sec = (time_t) (connection->send_timeout_ms / 1000);
	send_timeout.tv_usec = (suseconds_t) ((connection->send_timeout_ms % 1000) * 1000);
#endif
	if (setsockopt(connection->socket, SOL_SOCKET, SO_RCVTIMEO,
		(const char *) &receive, sizeof(receive)) != 0 ||
		setsockopt(connection->socket, SOL_SOCKET, SO_SNDTIMEO,
		(const char *) &send_timeout, sizeof(send_timeout)) != 0) {
		return ch_socket_error(error, "setsockopt timeout", ch_socket_error_code());
	}
	return CHC_OK;
}

static void ch_apply_socket_options(ch_connection *connection)
{
	assert(connection->socket != CH_INVALID_SOCKET);
	int enabled = connection->options.tcp_nodelay ? 1 : 0;
	(void) setsockopt(connection->socket, IPPROTO_TCP, TCP_NODELAY,
		(const char *) &enabled, sizeof(enabled));
	enabled = connection->options.tcp_keepalive ? 1 : 0;
	(void) setsockopt(connection->socket, SOL_SOCKET, SO_KEEPALIVE,
		(const char *) &enabled, sizeof(enabled));
#ifndef _WIN32
	if (enabled) {
		int idle = (int) connection->options.keepalive_idle_seconds;
		int interval = (int) connection->options.keepalive_interval_seconds;
		int count = (int) connection->options.keepalive_count;
#if defined(TCP_KEEPIDLE)
		(void) setsockopt(connection->socket, IPPROTO_TCP, TCP_KEEPIDLE, &idle, sizeof(idle));
#elif defined(TCP_KEEPALIVE)
		(void) setsockopt(connection->socket, IPPROTO_TCP, TCP_KEEPALIVE, &idle, sizeof(idle));
#endif
#ifdef TCP_KEEPINTVL
		(void) setsockopt(connection->socket, IPPROTO_TCP, TCP_KEEPINTVL, &interval, sizeof(interval));
#endif
#ifdef TCP_KEEPCNT
		(void) setsockopt(connection->socket, IPPROTO_TCP, TCP_KEEPCNT, &count, sizeof(count));
#endif
	}
#endif
#ifdef SO_NOSIGPIPE
	enabled = 1;
	(void) setsockopt(connection->socket, SOL_SOCKET, SO_NOSIGPIPE, &enabled, sizeof(enabled));
#endif
}

static int ch_socket_recv(ch_connection *connection, void *buffer, int length)
{
#ifdef _WIN32
	return recv(connection->socket, (char *) buffer, length, 0);
#else
	return (int) recv(connection->socket, buffer, (size_t) length, 0);
#endif
}

static int ch_socket_send(ch_connection *connection, const void *buffer, int length)
{
#ifdef _WIN32
	return send(connection->socket, (const char *) buffer, length, 0);
#else
	int flags = 0;
#ifdef MSG_NOSIGNAL
	flags = MSG_NOSIGNAL;
#endif
	return (int) send(connection->socket, buffer, (size_t) length, flags);
#endif
}

#ifdef WITH_OPENSSL
static int ch_ssl_error(chc_err *error, const char *operation, int ssl_error)
{
	char text[192];
	unsigned long code = ERR_get_error();
	if (code) {
		ERR_error_string_n(code, text, sizeof(text));
		return ch_error(error, CHC_ERR_IO, "%s: %s", operation, text);
	}
	return ch_error(error, CHC_ERR_IO, "%s failed (SSL error %d)", operation, ssl_error);
}

static int ch_ssl_ca_error(chc_err *error, const char *kind, const char *path)
{
	char text[192];
	unsigned long code = ERR_get_error();
	ERR_error_string_n(code, text, sizeof(text));
	return ch_error(error, CHC_ERR_IO, "Failed to load CA %s '%s': %s", kind, path, text);
}

static int ch_bio_create(BIO *bio)
{
	BIO_set_init(bio, 1);
	BIO_set_data(bio, NULL);
	BIO_set_shutdown(bio, 0);
	return 1;
}

static int ch_bio_destroy(BIO *bio)
{
	if (!bio) {
		return 0;
	}
	BIO_set_data(bio, NULL);
	BIO_set_init(bio, 0);
	return 1;
}

static int ch_bio_read(BIO *bio, char *buffer, int length)
{
	ch_connection *connection = (ch_connection *) BIO_get_data(bio);
	BIO_clear_retry_flags(bio);
	if (!connection || !buffer || length <= 0) {
		return 0;
	}
	int result = ch_socket_recv(connection, buffer, length);
	if (result < 0) {
		int code = ch_socket_error_code();
		if (ch_would_block(code) || ch_interrupted(code)) {
			BIO_set_retry_read(bio);
		}
	}
	return result;
}

static int ch_bio_write(BIO *bio, const char *buffer, int length)
{
	ch_connection *connection = (ch_connection *) BIO_get_data(bio);
	BIO_clear_retry_flags(bio);
	if (!connection || !buffer || length <= 0) {
		return 0;
	}
	int result = ch_socket_send(connection, buffer, length);
	if (result < 0) {
		int code = ch_socket_error_code();
		if (ch_would_block(code) || ch_interrupted(code)) {
			BIO_set_retry_write(bio);
		}
	}
	return result;
}

static long ch_bio_ctrl(BIO *bio, int command, long number, void *pointer)
{
	(void) pointer;
	switch (command) {
		case BIO_CTRL_FLUSH:
			return 1;
		case BIO_CTRL_GET_CLOSE:
			return BIO_get_shutdown(bio);
		case BIO_CTRL_SET_CLOSE:
			BIO_set_shutdown(bio, (int) number);
			return 1;
		default:
			return 0;
	}
}

static int ch_ssl_wait(ch_connection *connection, int result, const ch_deadline *deadline,
	const char *operation, chc_err *error)
{
	int ssl_error = SSL_get_error(connection->ssl, result);
	if (ssl_error == SSL_ERROR_WANT_READ || ssl_error == SSL_ERROR_WANT_WRITE) {
		if (deadline->finite && ch_now_ms() >= deadline->expires) {
			return ch_error(error, CHC_ERR_IO, "%s timed out", operation);
		}
		return ch_poll(connection, ssl_error == SSL_ERROR_WANT_WRITE, deadline, operation, error);
	}
	if (!connection->options.ssl_skip_verify) {
		long verification = SSL_get_verify_result(connection->ssl);
		if (verification != X509_V_OK) {
			return ch_error(error, CHC_ERR_IO, "Failed to verify SSL connection, X509_v error: %ld %s",
				verification, X509_verify_cert_error_string(verification));
		}
	}
	if (ssl_error == SSL_ERROR_ZERO_RETURN) {
		return ch_error(error, CHC_ERR_EOF, "%s: TLS connection closed", operation);
	}
	return ch_ssl_error(error, operation, ssl_error);
}

static int ch_tls_connect(ch_connection *connection, const ch_backend_endpoint *endpoint,
	chc_err *error)
{
	assert(connection->socket != CH_INVALID_SOCKET && connection->ssl == NULL);
	ERR_clear_error();
	connection->ssl_context = SSL_CTX_new(TLS_client_method());
	if (!connection->ssl_context) {
		return ch_ssl_error(error, "SSL_CTX_new", 0);
	}
	if (SSL_CTX_set_min_proto_version(connection->ssl_context,
		connection->options.ssl_min_protocol_version) != 1) {
		return ch_ssl_error(error, "SSL minimum protocol", 0);
	}
	SSL_CTX_set_verify(connection->ssl_context,
		connection->options.ssl_skip_verify ? SSL_VERIFY_NONE : SSL_VERIFY_PEER, NULL);
	if (connection->options.ssl_use_default_ca &&
		SSL_CTX_set_default_verify_paths(connection->ssl_context) != 1) {
		return ch_ssl_error(error, "loading default CA paths", 0);
	}
	if (connection->options.ssl_ca_directory.len) {
		ERR_clear_error();
		if (SSL_CTX_load_verify_locations(connection->ssl_context, NULL,
			connection->options.ssl_ca_directory.data) != 1) {
			return ch_ssl_ca_error(error, "directory", connection->options.ssl_ca_directory.data);
		}
	}
	for (size_t i = 0; i < connection->options.ssl_ca_file_count; i++) {
		ERR_clear_error();
		if (SSL_CTX_load_verify_locations(connection->ssl_context,
			connection->ca_files[i].data, NULL) != 1) {
			return ch_ssl_ca_error(error, "file", connection->ca_files[i].data);
		}
	}
	connection->ssl = SSL_new(connection->ssl_context);
	if (!connection->ssl) {
		return ch_ssl_error(error, "SSL_new", 0);
	}
	SSL_set_mode(connection->ssl, SSL_MODE_AUTO_RETRY | SSL_MODE_ENABLE_PARTIAL_WRITE);
	if (SSL_set_tlsext_host_name(connection->ssl, endpoint->host.data) != 1) {
		return ch_ssl_error(error, "TLS server name", 0);
	}
	if (!connection->options.ssl_skip_verify) {
		unsigned char address[16];
		bool is_ip = inet_pton(AF_INET, endpoint->host.data, address) == 1 ||
			inet_pton(AF_INET6, endpoint->host.data, address) == 1;
		int configured = is_ip ?
			X509_VERIFY_PARAM_set1_ip_asc(SSL_get0_param(connection->ssl), endpoint->host.data) :
			SSL_set1_host(connection->ssl, endpoint->host.data);
		if (configured != 1) {
			return ch_ssl_error(error, "TLS hostname verification", 0);
		}
	}
	/* A custom BIO preserves full-width Windows SOCKET values and suppresses
 * SIGPIPE per write, without changing the PHP process's signal disposition. */
	connection->bio_method = BIO_meth_new(BIO_TYPE_SOURCE_SINK, "clickhouse socket");
	if (!connection->bio_method ||
		BIO_meth_set_create(connection->bio_method, ch_bio_create) != 1 ||
		BIO_meth_set_destroy(connection->bio_method, ch_bio_destroy) != 1 ||
		BIO_meth_set_read(connection->bio_method, ch_bio_read) != 1 ||
		BIO_meth_set_write(connection->bio_method, ch_bio_write) != 1 ||
		BIO_meth_set_ctrl(connection->bio_method, ch_bio_ctrl) != 1) {
		return ch_ssl_error(error, "TLS socket BIO", 0);
	}
	connection->pending_bio = BIO_new(connection->bio_method);
	if (!connection->pending_bio) {
		return ch_ssl_error(error, "BIO_new", 0);
	}
	BIO_set_data(connection->pending_bio, connection);
	SSL_set_bio(connection->ssl, connection->pending_bio, connection->pending_bio);
	connection->pending_bio = NULL;
	ch_deadline deadline = ch_deadline_start(connection->options.connect_timeout_ms, false);
	for (;;) {
		ERR_clear_error();
		int result = SSL_connect(connection->ssl);
		if (result == 1) {
			break;
		}
		int status = ch_ssl_wait(connection, result, &deadline, "TLS handshake", error);
		if (status != CHC_OK) {
			return status;
		}
	}
	if (!connection->options.ssl_skip_verify) {
		long verification = SSL_get_verify_result(connection->ssl);
		if (verification != X509_V_OK) {
			return ch_error(error, CHC_ERR_IO, "Failed to verify SSL connection, X509_v error: %ld %s",
				verification, X509_verify_cert_error_string(verification));
		}
	}
	return CHC_OK;
}
#endif

static int ch_io_read(void *context, void *buffer, size_t length, size_t *received,
	chc_err *error)
{
	ch_connection *connection = (ch_connection *) context;
	*received = 0;
	if (!length) {
		return CHC_OK;
	}
	if (connection->socket == CH_INVALID_SOCKET) {
		return ch_error(error, CHC_ERR_IO, "connection is closed");
	}
	int amount = length > INT_MAX ? INT_MAX : (int) length;
	ch_deadline deadline = ch_deadline_start(connection->receive_timeout_ms, true);
	for (;;) {
		int result;
#ifdef WITH_OPENSSL
		if (connection->ssl) {
			ERR_clear_error();
			result = SSL_read(connection->ssl, buffer, amount);
			if (result <= 0) {
				int status = ch_ssl_wait(connection, result, &deadline, "SSL read", error);
				if (status != CHC_OK) {
					return status;
				}
				continue;
			}
		} else
#endif
		{
			result = ch_socket_recv(connection, buffer, amount);
			if (result < 0) {
				int code = ch_socket_error_code();
				if (ch_interrupted(code)) {
					continue;
				}
				if (!ch_would_block(code)) {
					return ch_socket_error(error, "read", code);
				}
				int status = ch_poll(connection, false, &deadline, "read", error);
				if (status != CHC_OK) {
					return status;
				}
				continue;
			}
		}
		*received = (size_t) result;
		return CHC_OK;
	}
}

static int ch_io_write(void *context, const void *buffer, size_t length, chc_err *error)
{
	ch_connection *connection = (ch_connection *) context;
	if (connection->socket == CH_INVALID_SOCKET) {
		return ch_error(error, CHC_ERR_IO, "connection is closed");
	}
	const unsigned char *position = (const unsigned char *) buffer;
	ch_deadline deadline = ch_deadline_start(connection->send_timeout_ms, true);
	while (length) {
		int amount = length > INT_MAX ? INT_MAX : (int) length;
		int result;
#ifdef WITH_OPENSSL
		if (connection->ssl) {
			ERR_clear_error();
			result = SSL_write(connection->ssl, position, amount);
			if (result <= 0) {
				int status = ch_ssl_wait(connection, result, &deadline, "SSL write", error);
				if (status != CHC_OK) {
					return status;
				}
				continue;
			}
		} else
#endif
		{
			result = ch_socket_send(connection, position, amount);
			if (result < 0) {
				int code = ch_socket_error_code();
				if (ch_interrupted(code)) {
					continue;
				}
				if (!ch_would_block(code)) {
					return ch_socket_error(error, "write", code);
				}
				int status = ch_poll(connection, true, &deadline, "write", error);
				if (status != CHC_OK) {
					return status;
				}
				continue;
			}
			if (!result) {
				return ch_error(error, CHC_ERR_IO, "write: connection closed");
			}
		}
		position += (size_t) result;
		length -= (size_t) result;
	}
	return CHC_OK;
}

void ch_backend_options_init(ch_backend_options *options)
{
	assert(options != NULL);
	memset(options, 0, sizeof(*options));
	options->database.data = "default";
	options->database.len = sizeof("default") - 1;
	options->user = options->database;
	options->password.data = "";
	options->connect_timeout_ms = 5000;
	options->retry_count = 1;
	options->retry_timeout_seconds = 5;
	options->tcp_nodelay = true;
	options->keepalive_idle_seconds = 60;
	options->keepalive_interval_seconds = 5;
	options->keepalive_count = 3;
	options->max_compression_chunk_size = 65535;
	options->ssl_use_default_ca = true;
	options->ssl_min_protocol_version = CH_BACKEND_TLS_1_2;
}

static int ch_copy_string(ch_connection *connection, ch_backend_string *destination,
	ch_backend_string source, chc_err *error)
{
	assert(destination->data == NULL);
	if ((source.len && !source.data) || source.len == SIZE_MAX) {
		return ch_error(error, CHC_ERR_USAGE, "invalid connection option string");
	}
	destination->data = (char *) connection->allocator->alloc(connection->allocator->ud, source.len + 1);
	if (!destination->data) {
		return ch_error(error, CHC_ERR_OOM, "allocating connection option");
	}
	destination->len = source.len;
	if (source.len) {
		memcpy((char *) destination->data, source.data, source.len);
	}
	((char *) destination->data)[source.len] = '\0';
	return CHC_OK;
}

static void ch_free_string(ch_connection *connection, ch_backend_string *string)
{
	assert(connection->allocator != NULL);
	if (string->data) {
		connection->allocator->free(connection->allocator->ud, (void *) string->data, string->len + 1);
		string->data = NULL;
		string->len = 0;
	}
}

static void ch_free_server_info(ch_connection *connection, chc_server_info *info)
{
	assert(connection->allocator != NULL);
	if (info->full_name) {
		connection->allocator->free(connection->allocator->ud, info->full_name, info->full_name_len + 1);
	}
	if (info->full_timezone) {
		connection->allocator->free(connection->allocator->ud, info->full_timezone, info->full_timezone_len + 1);
	}
	if (info->full_display_name) {
		connection->allocator->free(connection->allocator->ud, info->full_display_name, info->full_display_name_len + 1);
	}
	memset(info, 0, sizeof(*info));
}

static int ch_copy_metadata_string(ch_connection *connection, char **destination,
	const char *source, size_t length, chc_err *error)
{
	assert(*destination == NULL);
	if (!source) {
		return CHC_OK;
	}
	ch_backend_string copy = {NULL, 0};
	ch_backend_string value = {source, length};
	int status = ch_copy_string(connection, &copy, value, error);
	*destination = (char *) copy.data;
	return status;
}

static int ch_save_server_info(ch_connection *connection, chc_err *error)
{
	assert(connection->client != NULL);
	const chc_server_info *source = chc_client_server_info(connection->client);
	chc_server_info replacement = *source;
	replacement.full_name = NULL;
	replacement.full_timezone = NULL;
	replacement.full_display_name = NULL;
	int status = ch_copy_metadata_string(connection, &replacement.full_name,
		source->full_name, source->full_name_len, error);
	if (status == CHC_OK) {
		status = ch_copy_metadata_string(connection, &replacement.full_timezone,
			source->full_timezone, source->full_timezone_len, error);
	}
	if (status == CHC_OK) {
		status = ch_copy_metadata_string(connection, &replacement.full_display_name,
			source->full_display_name, source->full_display_name_len, error);
	}
	if (status != CHC_OK) {
		ch_free_server_info(connection, &replacement);
		return status;
	}
	ch_free_server_info(connection, &connection->server_info);
	connection->server_info = replacement;
	connection->has_server_info = true;
	return CHC_OK;
}

static bool ch_no_nul(ch_backend_string string)
{
	return string.data && memchr(string.data, '\0', string.len) == NULL;
}

int ch_backend_create(ch_connection **out, const ch_backend_options *options,
	const chc_alloc *allocator, chc_err *error)
{
	if (!out || *out || !options || !allocator || !allocator->alloc || !allocator->free ||
		!allocator->realloc || !options->endpoints || !options->endpoint_count ||
		options->endpoint_count > SIZE_MAX / sizeof(ch_backend_endpoint) ||
		options->ssl_ca_file_count > SIZE_MAX / sizeof(ch_backend_string) ||
		(options->ssl_ca_file_count && !options->ssl_ca_files) ||
		options->connect_timeout_ms > INT_MAX ||
		options->keepalive_idle_seconds > INT_MAX || options->keepalive_interval_seconds > INT_MAX ||
		options->keepalive_count > INT_MAX || options->max_compression_chunk_size > INT_MAX ||
		options->compression < CHC_COMP_NONE || options->compression > CHC_COMP_ZSTD) {
		return ch_error(error, CHC_ERR_USAGE, "invalid connection options");
	}
#ifndef WITH_OPENSSL
	if (options->ssl) {
		return ch_error(error, CHC_ERR_USAGE, "Library was built with no SSL support");
	}
#endif
	ch_connection *connection = (ch_connection *) allocator->alloc(allocator->ud, sizeof(*connection));
	if (!connection) {
		return ch_error(error, CHC_ERR_OOM, "allocating connection");
	}
	memset(connection, 0, sizeof(*connection));
	connection->allocator = allocator;
	connection->socket = CH_INVALID_SOCKET;
	*out = connection;
	connection->options = *options;
	connection->options.endpoints = NULL;
	connection->options.ssl_ca_files = NULL;
	connection->options.database.data = NULL;
	connection->options.user.data = NULL;
	connection->options.password.data = NULL;
	connection->options.ssl_ca_directory.data = NULL;
	connection->receive_timeout_ms = options->receive_timeout_ms;
	connection->send_timeout_ms = options->send_timeout_ms;
	connection->cursor = options->endpoint_count - 1;
	connection->io.ud = connection;
	connection->io.read = ch_io_read;
	connection->io.write = ch_io_write;
	chc_lz4_codec_init(&connection->codec);
	chc_zstd_codec_init(&connection->codec);
	connection->codec.ud = connection;
	connection->codec.zstd_compress = ch_zstd_compress;
	connection->codec.zstd_decompress = ch_zstd_decompress;
	int status = ch_copy_string(connection, &connection->options.database, options->database, error);
	if (status != CHC_OK) return status;
	status = ch_copy_string(connection, &connection->options.user, options->user, error);
	if (status != CHC_OK) return status;
	status = ch_copy_string(connection, &connection->options.password, options->password, error);
	if (status != CHC_OK) return status;
	if (options->ssl_ca_directory.data && !ch_no_nul(options->ssl_ca_directory)) {
		return ch_error(error, CHC_ERR_USAGE, "ssl_ca_directory must not contain NUL bytes");
	}
	status = ch_copy_string(connection, &connection->options.ssl_ca_directory, options->ssl_ca_directory, error);
	if (status != CHC_OK) return status;
	connection->endpoints = (ch_backend_endpoint *) allocator->alloc(allocator->ud,
		options->endpoint_count * sizeof(*connection->endpoints));
	if (!connection->endpoints) {
		return ch_error(error, CHC_ERR_OOM, "allocating endpoints");
	}
	memset(connection->endpoints, 0, options->endpoint_count * sizeof(*connection->endpoints));
	connection->options.endpoints = connection->endpoints;
	for (size_t i = 0; i < options->endpoint_count; i++) {
		if (!ch_no_nul(options->endpoints[i].host) || !options->endpoints[i].port) {
			return ch_error(error, CHC_ERR_USAGE, "invalid connection endpoint");
		}
		connection->endpoints[i].port = options->endpoints[i].port;
		status = ch_copy_string(connection, &connection->endpoints[i].host, options->endpoints[i].host, error);
		if (status != CHC_OK) return status;
	}
	if (options->ssl_ca_file_count) {
		connection->ca_files = (ch_backend_string *) allocator->alloc(allocator->ud,
			options->ssl_ca_file_count * sizeof(*connection->ca_files));
		if (!connection->ca_files) {
			return ch_error(error, CHC_ERR_OOM, "allocating CA paths");
		}
		memset(connection->ca_files, 0, options->ssl_ca_file_count * sizeof(*connection->ca_files));
		connection->options.ssl_ca_files = connection->ca_files;
		for (size_t i = 0; i < options->ssl_ca_file_count; i++) {
			if (!ch_no_nul(options->ssl_ca_files[i])) {
				return ch_error(error, CHC_ERR_USAGE, "ssl_ca_files must not contain NUL bytes");
			}
			status = ch_copy_string(connection, &connection->ca_files[i], options->ssl_ca_files[i], error);
			if (status != CHC_OK) return status;
		}
	}
	return CHC_OK;
}

int ch_backend_clone_database(ch_connection **out, const ch_connection *source,
	const char *database, size_t database_len, chc_err *error)
{
	if (!source || (!database && database_len)) {
		return ch_error(error, CHC_ERR_USAGE, "invalid database connection clone");
	}
	ch_backend_options options = source->options;
	options.database.data = database;
	options.database.len = database_len;
	return ch_backend_create(out, &options, source->allocator, error);
}

void ch_backend_disconnect(ch_connection *connection)
{
	if (!connection) {
		return;
	}
	if (connection->client) {
		chc_client_close(connection->client);
		connection->client = NULL;
	}
#ifdef WITH_OPENSSL
	if (connection->ssl) {
		SSL_free(connection->ssl);
		connection->ssl = NULL;
	}
	if (connection->pending_bio) {
		BIO_free(connection->pending_bio);
		connection->pending_bio = NULL;
	}
	if (connection->bio_method) {
		BIO_meth_free(connection->bio_method);
		connection->bio_method = NULL;
	}
	if (connection->ssl_context) {
		SSL_CTX_free(connection->ssl_context);
		connection->ssl_context = NULL;
	}
#endif
	ch_close_socket(connection);
	if (connection->addresses) {
		freeaddrinfo(connection->addresses);
		connection->addresses = NULL;
	}
}

void ch_backend_destroy(ch_connection *connection)
{
	if (!connection) {
		return;
	}
	ch_backend_disconnect(connection);
	ch_free_server_info(connection, &connection->server_info);
	ch_free_string(connection, &connection->options.database);
	ch_free_string(connection, &connection->options.user);
	ch_free_string(connection, &connection->options.password);
	ch_free_string(connection, &connection->options.ssl_ca_directory);
	if (connection->endpoints) {
		for (size_t i = 0; i < connection->options.endpoint_count; i++) {
			ch_free_string(connection, &connection->endpoints[i].host);
		}
		connection->allocator->free(connection->allocator->ud, connection->endpoints,
			connection->options.endpoint_count * sizeof(*connection->endpoints));
	}
	if (connection->ca_files) {
		for (size_t i = 0; i < connection->options.ssl_ca_file_count; i++) {
			ch_free_string(connection, &connection->ca_files[i]);
		}
		connection->allocator->free(connection->allocator->ud, connection->ca_files,
			connection->options.ssl_ca_file_count * sizeof(*connection->ca_files));
	}
#ifdef _WIN32
	if (connection->winsock_started) {
		WSACleanup();
	}
#endif
	connection->allocator->free(connection->allocator->ud, connection, sizeof(*connection));
}

#if !defined(_WIN32) && defined(AI_ADDRCONFIG)
static bool ch_is_loopback_name(const char *name)
{
	return strcmp(name, "localhost") == 0 || strcmp(name, "localhost.localdomain") == 0 ||
		strcmp(name, "localhost6") == 0 || strcmp(name, "localhost6.localdomain6") == 0 ||
		strcmp(name, "::1") == 0 || strcmp(name, "127.0.0.1") == 0;
}
#endif

static int ch_connect_address(ch_connection *connection, const struct addrinfo *address,
	chc_err *error)
{
	assert(connection->socket == CH_INVALID_SOCKET);
	connection->socket = socket(address->ai_family, address->ai_socktype, address->ai_protocol);
	if (connection->socket == CH_INVALID_SOCKET) {
		return ch_socket_error(error, "socket", ch_socket_error_code());
	}
	int status = ch_nonblocking(connection, error);
	if (status != CHC_OK) {
		return status;
	}
	ch_apply_socket_options(connection);
	status = ch_apply_timeouts(connection, error);
	if (status != CHC_OK) {
		return status;
	}
	ch_deadline deadline = ch_deadline_start(connection->options.connect_timeout_ms, false);
#ifdef _WIN32
	int result = connect(connection->socket, address->ai_addr, (int) address->ai_addrlen);
#else
	int result = connect(connection->socket, address->ai_addr, address->ai_addrlen);
#endif
	if (result == 0) {
		return CHC_OK;
	}
	int code = ch_socket_error_code();
	if (!ch_would_block(code) && !ch_interrupted(code)) {
		return ch_socket_error(error, "connect", code);
	}
	status = ch_poll(connection, true, &deadline, "connect", error);
	if (status != CHC_OK) {
		return status;
	}
	int socket_error = 0;
#ifdef _WIN32
	int error_size = sizeof(socket_error);
#else
	socklen_t error_size = sizeof(socket_error);
#endif
	if (getsockopt(connection->socket, SOL_SOCKET, SO_ERROR,
		(char *) &socket_error, &error_size) != 0) {
		return ch_socket_error(error, "getsockopt", ch_socket_error_code());
	}
	return socket_error ? ch_socket_error(error, "connect", socket_error) : CHC_OK;
}

static int ch_connect_endpoint(ch_connection *connection, size_t endpoint_index,
	chc_exception **exception, chc_err *error)
{
	assert(endpoint_index < connection->options.endpoint_count);
	if (exception && *exception) {
		chc_exception_free(*exception, connection->allocator);
		*exception = NULL;
	}
	ch_backend_disconnect(connection);
	connection->current = endpoint_index;
	connection->has_current = true;
	connection->receive_timeout_ms = connection->options.receive_timeout_ms;
	connection->send_timeout_ms = connection->options.send_timeout_ms;
#ifdef _WIN32
	if (!connection->winsock_started) {
		WSADATA data;
		int code = WSAStartup(MAKEWORD(2, 2), &data);
		if (code != 0) {
			return ch_socket_error(error, "WSAStartup", code);
		}
		connection->winsock_started = true;
	}
#endif
	const ch_backend_endpoint *endpoint = &connection->endpoints[endpoint_index];
	struct addrinfo hints;
	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
#if !defined(_WIN32) && defined(AI_ADDRCONFIG)
	if (!ch_is_loopback_name(endpoint->host.data)) {
		hints.ai_flags = AI_ADDRCONFIG;
	}
#endif
	char service[6];
	snprintf(service, sizeof(service), "%u", (unsigned int) endpoint->port);
	int lookup = getaddrinfo(endpoint->host.data, service, &hints, &connection->addresses);
	if (lookup != 0) {
#ifdef _WIN32
		return ch_error(error, CHC_ERR_IO, "Cannot resolve %s: %s", endpoint->host.data, gai_strerrorA(lookup));
#else
		return ch_error(error, CHC_ERR_IO, "Cannot resolve %s: %s", endpoint->host.data, gai_strerror(lookup));
#endif
	}
	int status = CHC_ERR_IO;
	for (const struct addrinfo *address = connection->addresses; address; address = address->ai_next) {
		status = ch_connect_address(connection, address, error);
		if (status == CHC_OK) {
			break;
		}
		ch_close_socket(connection);
	}
	freeaddrinfo(connection->addresses);
	connection->addresses = NULL;
	if (status != CHC_OK) {
		return status;
	}
#ifdef WITH_OPENSSL
	if (connection->options.ssl) {
		status = ch_tls_connect(connection, endpoint, error);
		if (status != CHC_OK) {
			ch_backend_disconnect(connection);
			return status;
		}
	}
#endif
	chc_client_opts options;
	memset(&options, 0, sizeof(options));
	options.client_name = "clickhouse-cpp";
	options.client_name_len = sizeof("clickhouse-cpp") - 1;
	options.client_version_major = 2;
	options.client_version_minor = 6;
	options.client_version_patch = 2;
	options.protocol_revision = 54459;
	options.skip_init_ping = true;
	options.allow_legacy_server = true;
	options.strings_have_lengths = true;
	options.database = connection->options.database.data;
	options.database_len = connection->options.database.len;
	options.user = connection->options.user.data;
	options.user_len = connection->options.user.len;
	options.password = connection->options.password.data;
	options.password_len = connection->options.password.len;
	options.compression = connection->options.compression;
	options.codec = &connection->codec;
	options.has_max_compression_chunk_size = true;
	options.max_compression_chunk_size = connection->options.max_compression_chunk_size;
	status = chc_client_init(&connection->client, &options, connection->allocator,
		&connection->io, exception, error);
	if (status == CHC_OK) {
		status = ch_save_server_info(connection, error);
	}
	if (status != CHC_OK) {
		ch_backend_disconnect(connection);
	}
	return status;
}

static size_t ch_next_endpoint(ch_connection *connection)
{
	assert(connection->options.endpoint_count > 0);
	connection->cursor = connection->cursor + 1 == connection->options.endpoint_count ? 0 : connection->cursor + 1;
	return connection->cursor;
}

static bool ch_retryable(int status)
{
	return status == CHC_ERR_IO || status == CHC_ERR_EOF || status == CHC_ERR_PROTOCOL ||
		status == CHC_ERR_TYPE || status == CHC_ERR_SERVER;
}

int ch_backend_connect(ch_connection *connection, chc_exception **exception, chc_err *error)
{
	if (exception) *exception = NULL;
	chc_err_reset(error);
	if (!connection) {
		return ch_error(error, CHC_ERR_USAGE, "connection is not initialized");
	}
	uint32_t rounds = connection->options.retry_count ? connection->options.retry_count : 1;
	int status = CHC_ERR_IO;
	for (uint32_t round = 0; round < rounds; round++) {
		for (size_t i = 0; i < connection->options.endpoint_count; i++) {
			status = ch_connect_endpoint(connection, ch_next_endpoint(connection), exception, error);
			if (status == CHC_OK || !ch_retryable(status)) {
				return status;
			}
		}
	}
	connection->has_current = false;
	return status;
}

int ch_backend_reset(ch_connection *connection, chc_exception **exception, chc_err *error)
{
	if (exception) *exception = NULL;
	chc_err_reset(error);
	if (!connection) {
		return ch_error(error, CHC_ERR_USAGE, "connection is not initialized");
	}
	int status = CHC_ERR_IO;
	if (connection->has_current) {
		status = ch_connect_endpoint(connection, connection->current, exception, error);
		if (status == CHC_OK || !ch_retryable(status)) {
			return status;
		}
	}
	for (size_t i = 0; i < connection->options.endpoint_count; i++) {
		status = ch_connect_endpoint(connection, ch_next_endpoint(connection), exception, error);
		if (status == CHC_OK || !ch_retryable(status)) {
			return status;
		}
	}
	connection->has_current = false;
	return status;
}

int ch_backend_ping(ch_connection *connection, chc_exception **exception, chc_err *error)
{
	if (exception) *exception = NULL;
	chc_err_reset(error);
	if (!connection || !connection->client) {
		return ch_error(error, CHC_ERR_IO, "connection is closed");
	}
	int status = chc_client_send_ping(connection->client, error);
	if (status != CHC_OK) {
		return status;
	}
	chc_packet packet;
	memset(&packet, 0, sizeof(packet));
	status = chc_client_recv_packet(connection->client, &packet, error);
	if (status != CHC_OK) {
		return status;
	}
	if (packet.kind == CHC_PKT_EXCEPTION) {
		if (exception) {
			*exception = packet.exception;
			packet.exception = NULL;
		}
		status = CHC_ERR_SERVER;
	} else if (packet.kind != CHC_PKT_PONG) {
		status = ch_error(error, CHC_ERR_PROTOCOL, "fail to ping server");
	}
	chc_packet_clear(connection->client, &packet);
	return status;
}

static void ch_retry_sleep(uint64_t seconds)
{
	/* Keep valid PHP integers in seconds; converting the whole delay to
 * milliseconds would overflow on 64-bit builds. */
	while (seconds) {
		uint32_t chunk = seconds > UINT32_C(86400) ? UINT32_C(86400) : (uint32_t) seconds;
#ifdef _WIN32
		Sleep(chunk * 1000);
#else
		struct timespec remaining;
		remaining.tv_sec = (time_t) chunk;
		remaining.tv_nsec = 0;
		while (nanosleep(&remaining, &remaining) != 0 && errno == EINTR) {
		}
#endif
		seconds -= chunk;
	}
}

int ch_backend_before_query(ch_connection *connection, chc_exception **exception, chc_err *error)
{
	if (exception) *exception = NULL;
	chc_err_reset(error);
	if (!connection) {
		return ch_error(error, CHC_ERR_USAGE, "connection is not initialized");
	}
	if (!connection->options.ping_before_query) {
		return connection->client ? CHC_OK : ch_error(error, CHC_ERR_IO, "connection is closed");
	}
	int status = CHC_ERR_IO;
	if (connection->has_current) {
		status = ch_backend_ping(connection, exception, error);
		if (status == CHC_OK || !ch_retryable(status) || connection->options.retry_count == 0) {
			return status;
		}
	}
	for (uint32_t round = 0; round < connection->options.retry_count; round++) {
		for (size_t i = 0; i < connection->options.endpoint_count; i++) {
			ch_retry_sleep(connection->options.retry_timeout_seconds);
			status = ch_connect_endpoint(connection, ch_next_endpoint(connection), exception, error);
			if (status == CHC_OK) {
				status = ch_backend_ping(connection, exception, error);
			}
			if (status == CHC_OK || !ch_retryable(status)) {
				return status;
			}
		}
	}
	connection->has_current = false;
	if (connection->options.retry_count == 0) {
		return ch_error(error, CHC_ERR_PROTOCOL, "operation retry attempts exhausted");
	}
	return status;
}

int ch_backend_limit_io_timeouts(ch_connection *connection, uint32_t maximum_ms, chc_err *error)
{
	if (!connection || !maximum_ms) {
		return ch_error(error, CHC_ERR_USAGE, "invalid cleanup timeout");
	}
	if (!connection->receive_timeout_ms || connection->receive_timeout_ms > maximum_ms) {
		connection->receive_timeout_ms = maximum_ms;
	}
	if (!connection->send_timeout_ms || connection->send_timeout_ms > maximum_ms) {
		connection->send_timeout_ms = maximum_ms;
	}
	return connection->socket == CH_INVALID_SOCKET ? CHC_OK : ch_apply_timeouts(connection, error);
}

chc_client *ch_backend_client(ch_connection *connection)
{
	return connection ? connection->client : NULL;
}

const ch_backend_endpoint *ch_backend_current_endpoint(const ch_connection *connection)
{
	return connection && connection->has_current ? &connection->endpoints[connection->current] : NULL;
}

const chc_server_info *ch_backend_server_info(const ch_connection *connection)
{
	return connection && connection->has_server_info ? &connection->server_info : NULL;
}
