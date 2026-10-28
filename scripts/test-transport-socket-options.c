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
#include "../ch_backend.c"

#include <signal.h>

#if defined(__APPLE__)
#if !defined(TCP_KEEPALIVE) || !defined(TCP_KEEPINTVL) || !defined(TCP_KEEPCNT) || !defined(SO_NOSIGPIPE)
#error "Darwin transport requires keepalive controls and SO_NOSIGPIPE"
#endif
#define CH_TEST_KEEPIDLE TCP_KEEPALIVE
#elif defined(__linux__)
#define CH_TEST_KEEPIDLE TCP_KEEPIDLE
#else
#error "This socket-option test requires Darwin or Linux"
#endif

static int ch_test_option(int descriptor, int level, int option, int expected, const char *name)
{
	int actual = -1;
	socklen_t size = sizeof(actual);
	if (getsockopt(descriptor, level, option, &actual, &size) != 0) {
		perror(name);
		return 0;
	}
	if (size != sizeof(actual) || actual != expected) {
		fprintf(stderr, "%s: expected %d, got %d\n", name, expected, actual);
		return 0;
	}
	return 1;
}

static int ch_test_tcp_options(void)
{
	ch_connection connection = {0};
	ch_backend_options_init(&connection.options);
	connection.options.tcp_keepalive = true;
	connection.options.keepalive_idle_seconds = 19;
	connection.options.keepalive_interval_seconds = 7;
	connection.options.keepalive_count = 4;
	connection.socket = socket(AF_INET, SOCK_STREAM, 0);
	if (connection.socket == CH_INVALID_SOCKET) {
		perror("socket");
		return 0;
	}
	ch_apply_socket_options(&connection);
	int valid = ch_test_option(connection.socket, SOL_SOCKET, SO_KEEPALIVE, 1, "SO_KEEPALIVE") &&
		ch_test_option(connection.socket, IPPROTO_TCP, TCP_NODELAY, 1, "TCP_NODELAY") &&
		ch_test_option(connection.socket, IPPROTO_TCP, CH_TEST_KEEPIDLE, 19, "keepalive idle") &&
		ch_test_option(connection.socket, IPPROTO_TCP, TCP_KEEPINTVL, 7, "TCP_KEEPINTVL") &&
		ch_test_option(connection.socket, IPPROTO_TCP, TCP_KEEPCNT, 4, "TCP_KEEPCNT");
#ifdef __APPLE__
	valid = ch_test_option(connection.socket, SOL_SOCKET, SO_NOSIGPIPE, 1, "SO_NOSIGPIPE") && valid;
#endif
	connection.options.tcp_keepalive = false;
	connection.options.tcp_nodelay = false;
	ch_apply_socket_options(&connection);
	valid = ch_test_option(connection.socket, SOL_SOCKET, SO_KEEPALIVE, 0, "disabled SO_KEEPALIVE") && valid;
	valid = ch_test_option(connection.socket, IPPROTO_TCP, TCP_NODELAY, 0, "disabled TCP_NODELAY") && valid;
	ch_close_socket(&connection);
	return valid;
}

static int ch_test_closed_peer(void)
{
	int descriptors[2];
	if (socketpair(AF_UNIX, SOCK_STREAM, 0, descriptors) != 0) {
		perror("socketpair");
		return 0;
	}
	struct sigaction original;
	struct sigaction action;
	memset(&action, 0, sizeof(action));
	action.sa_handler = SIG_DFL;
	if (sigemptyset(&action.sa_mask) != 0 || sigaction(SIGPIPE, &action, &original) != 0) {
		perror("sigaction");
		close(descriptors[0]);
		close(descriptors[1]);
		return 0;
	}
	ch_connection connection = {0};
	ch_backend_options_init(&connection.options);
	connection.socket = descriptors[0];
	ch_apply_socket_options(&connection);
	int valid = 1;
#ifdef __APPLE__
	valid = ch_test_option(connection.socket, SOL_SOCKET, SO_NOSIGPIPE, 1, "socketpair SO_NOSIGPIPE");
#endif
	if (close(descriptors[1]) != 0) {
		perror("close peer");
		valid = 0;
	} else {
		errno = 0;
		int sent = ch_socket_send(&connection, "x", 1);
		int error = errno;
		if (sent != -1 || error != EPIPE) {
			fprintf(stderr, "closed peer: expected EPIPE, got result %d and errno %d\n", sent, error);
			valid = 0;
		}
	}
	ch_close_socket(&connection);
	if (sigaction(SIGPIPE, &original, NULL) != 0) {
		perror("restoring sigaction");
		valid = 0;
	}
	return valid;
}

int main(void)
{
	if (!ch_test_tcp_options() || !ch_test_closed_peer()) {
		return 1;
	}
	puts("Transport socket options and closed-peer EPIPE passed");
	return 0;
}
