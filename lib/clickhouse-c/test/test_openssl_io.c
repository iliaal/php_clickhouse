/*
 * test_openssl_io.c -- exercise clickhouse-openssl.h against an in-process
 * TLS peer reachable over loopback.
 *
 * Generates a fresh self-signed RSA cert at startup, listens on
 * 127.0.0.1:<ephemeral>, runs the server side in a pthread, drives the
 * client side through chc_openssl_io_init's chc_io vtable.
 *
 * Compile:
 *   cc -std=c11 -O2 -I. test/test_openssl_io.c -o /tmp/chc_test_openssl_io \
 *      -lssl -lcrypto -lpthread
 */

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>

#define CHC_PROVIDE_STDLIB_ALLOC
#define CHC_IMPLEMENTATION
#include "clickhouse.h"
#include "clickhouse-openssl.h"

static int fail_count = 0;
static const char *current_test = "";

#include "test_common.h"

/* ----- self-signed cert keypair (one per process) ----- */

static int
make_keypair(EVP_PKEY **out_pkey, X509 **out_cert)
{
    EVP_PKEY     *pkey = NULL;
    X509         *cert = NULL;
    EVP_PKEY_CTX *ctx  = EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, NULL);
    if (!ctx) return -1;
    if (EVP_PKEY_keygen_init(ctx) <= 0) goto fail;
    if (EVP_PKEY_CTX_set_rsa_keygen_bits(ctx, 2048) <= 0) goto fail;
    if (EVP_PKEY_keygen(ctx, &pkey) <= 0) goto fail;
    EVP_PKEY_CTX_free(ctx); ctx = NULL;

    cert = X509_new();
    if (!cert) goto fail;
    ASN1_INTEGER_set(X509_get_serialNumber(cert), 1);
    X509_gmtime_adj(X509_getm_notBefore(cert), 0);
    X509_gmtime_adj(X509_getm_notAfter(cert), 60 * 60);
    X509_set_pubkey(cert, pkey);
    X509_NAME *name = X509_get_subject_name(cert);
    X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC,
                               (const unsigned char *) "localhost", -1, -1, 0);
    X509_set_issuer_name(cert, name);
    if (!X509_sign(cert, pkey, EVP_sha256())) goto fail;

    *out_pkey = pkey;
    *out_cert = cert;
    return 0;
fail:
    EVP_PKEY_CTX_free(ctx);
    X509_free(cert);
    EVP_PKEY_free(pkey);
    return -1;
}

/* ----- server side ----- */

enum { MODE_ECHO_UPCASE, MODE_CLOSE_AFTER_HANDSHAKE, MODE_DRAIN_UNTIL_CLOSE,
       MODE_GARBAGE };

typedef struct {
    int       listen_fd;
    EVP_PKEY *pkey;
    X509     *cert;
    int       mode;
} server_args;

static void *
server_thread(void *p)
{
    server_args *a = p;
    int fd = accept(a->listen_fd, NULL, NULL);
    if (fd < 0) return NULL;

    SSL_CTX *ctx = SSL_CTX_new(TLS_server_method());
    if (!ctx) { close(fd); return NULL; }
    SSL_CTX_use_certificate(ctx, a->cert);
    SSL_CTX_use_PrivateKey(ctx, a->pkey);
    SSL *ssl = SSL_new(ctx);
    SSL_set_fd(ssl, fd);

    if (SSL_accept(ssl) <= 0) {
        ERR_print_errors_fp(stderr);
        SSL_free(ssl);
        SSL_CTX_free(ctx);
        close(fd);
        return NULL;
    }

    if (a->mode == MODE_ECHO_UPCASE) {
        char buf[64];
        for (int i = 0; i < 2; i++) {
            int n = SSL_read(ssl, buf, 5);
            if (n != 5) break;
            for (int j = 0; j < n; j++)
                if (buf[j] >= 'a' && buf[j] <= 'z') buf[j] = (char) (buf[j] - 32);
            if (SSL_write(ssl, buf, n) != n) break;
        }
    } else if (a->mode == MODE_DRAIN_UNTIL_CLOSE) {
        char buf[256];
        while (SSL_read(ssl, buf, sizeof buf) > 0) { /* discard */ }
    } else if (a->mode == MODE_GARBAGE) {
        /* Raw bytes where a TLS record belongs: the peer's record layer
         * rejects them with a queued error. */
        static const char junk[] = "this is not a tls record at all";
        (void) write(fd, junk, sizeof junk - 1);
        char buf[256];
        while (SSL_read(ssl, buf, sizeof buf) > 0) { /* discard */ }
    }
    /* MODE_CLOSE_AFTER_HANDSHAKE falls straight to shutdown. */

    SSL_shutdown(ssl);
    SSL_free(ssl);
    SSL_CTX_free(ctx);
    close(fd);
    return NULL;
}

/* ----- socket helpers ----- */

static int
listen_loopback(int *out_port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    struct sockaddr_in sa = {};
    sa.sin_family = AF_INET;
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    sa.sin_port = 0;
    if (bind(fd, (struct sockaddr *) &sa, sizeof sa) != 0) { close(fd); return -1; }
    if (listen(fd, 1) != 0) { close(fd); return -1; }
    socklen_t slen = sizeof sa;
    if (getsockname(fd, (struct sockaddr *) &sa, &slen) != 0) { close(fd); return -1; }
    *out_port = ntohs(sa.sin_port);
    return fd;
}

static int
connect_loopback(int port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    struct sockaddr_in sa = {};
    sa.sin_family = AF_INET;
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    sa.sin_port = htons((uint16_t) port);
    if (connect(fd, (struct sockaddr *) &sa, sizeof sa) != 0) { close(fd); return -1; }
    return fd;
}

/* Pairs a fresh listening socket with a client-side SSL ready to use
 * via chc_openssl_io_init. Returns 0 on success. */
typedef struct {
    int         listen_fd;
    int         cli_fd;
    SSL_CTX    *cli_ctx;
    SSL        *cli_ssl;
    pthread_t   th;
    server_args sa;
    bool        thread_started;
} fixture;

static int
fixture_up(fixture *f, EVP_PKEY *pkey, X509 *cert, int mode)
{
    memset(f, 0, sizeof *f);
    f->cli_fd = -1;
    int port = 0;
    f->listen_fd = listen_loopback(&port);
    if (f->listen_fd < 0) return -1;

    f->sa = (server_args){
        .listen_fd = f->listen_fd, .pkey = pkey, .cert = cert, .mode = mode
    };
    if (pthread_create(&f->th, NULL, server_thread, &f->sa) != 0) return -1;
    f->thread_started = true;

    f->cli_fd = connect_loopback(port);
    if (f->cli_fd < 0) return -1;

    f->cli_ctx = SSL_CTX_new(TLS_client_method());
    if (!f->cli_ctx) return -1;
    SSL_CTX_set_verify(f->cli_ctx, SSL_VERIFY_NONE, NULL);
    f->cli_ssl = SSL_new(f->cli_ctx);
    SSL_set_fd(f->cli_ssl, f->cli_fd);
    if (SSL_connect(f->cli_ssl) <= 0) {
        ERR_print_errors_fp(stderr);
        return -1;
    }
    return 0;
}

static void
fixture_down(fixture *f)
{
    SSL_free(f->cli_ssl); f->cli_ssl = NULL;
    SSL_CTX_free(f->cli_ctx); f->cli_ctx = NULL;
    if (f->cli_fd >= 0) { close(f->cli_fd); f->cli_fd = -1; }
    if (f->thread_started) { pthread_join(f->th, NULL); f->thread_started = false; }
    if (f->listen_fd >= 0) { close(f->listen_fd); f->listen_fd = -1; }
}

/* ----- tests ----- */

static void
test_echo_roundtrip(EVP_PKEY *pkey, X509 *cert)
{
    current_test = "echo_roundtrip";
    fixture f;
    if (fixture_up(&f, pkey, cert, MODE_ECHO_UPCASE) != 0) {
        fail_count++;
        goto out;
    }

    chc_openssl_io state;
    chc_io io;
    chc_openssl_io_init(&state, &io, f.cli_ssl, NULL, NULL);
    CHECK(io.read != NULL);
    CHECK(io.write != NULL);
    CHECK(io.check_cancel == NULL);

    chc_err err = {};
    CHECK(io.write(io.ud, "hello", 5, &err) == CHC_OK);
    char rb[8] = {};
    size_t got = 0;
    CHECK(io.read(io.ud, rb, sizeof rb, &got, &err) == CHC_OK);
    CHECK(got == 5);
    CHECK(memcmp(rb, "HELLO", 5) == 0);

    CHECK(io.write(io.ud, "ping!", 5, &err) == CHC_OK);
    memset(rb, 0, sizeof rb);
    got = 0;
    CHECK(io.read(io.ud, rb, 5, &got, &err) == CHC_OK);
    CHECK(got == 5);
    CHECK(memcmp(rb, "PING!", 5) == 0);

out:
    fixture_down(&f);
}

static void
test_eof_after_handshake(EVP_PKEY *pkey, X509 *cert)
{
    current_test = "eof_after_handshake";
    fixture f;
    if (fixture_up(&f, pkey, cert, MODE_CLOSE_AFTER_HANDSHAKE) != 0) {
        fail_count++;
        goto out;
    }

    chc_openssl_io state;
    chc_io io;
    chc_openssl_io_init(&state, &io, f.cli_ssl, NULL, NULL);

    char rb[16];
    size_t got = 99;
    chc_err err = {};
    int rc = io.read(io.ud, rb, sizeof rb, &got, &err);
    CHECK(rc == CHC_OK);
    CHECK(got == 0);

out:
    fixture_down(&f);
}

static bool g_cancel = false;
static bool cancel_cb(void *ud) { (void) ud; return g_cancel; }

static void
test_cancel(EVP_PKEY *pkey, X509 *cert)
{
    current_test = "cancel";
    fixture f;
    if (fixture_up(&f, pkey, cert, MODE_DRAIN_UNTIL_CLOSE) != 0) {
        fail_count++;
        goto out;
    }

    chc_openssl_io state;
    chc_io io;
    chc_openssl_io_init(&state, &io, f.cli_ssl, cancel_cb, NULL);
    CHECK(io.check_cancel != NULL);
    CHECK(io.check_cancel(io.ud) == 0);

    g_cancel = true;
    chc_err err = {};
    char rb[8] = {};
    size_t got = 99;
    int rc = io.read(io.ud, rb, 5, &got, &err);
    CHECK(rc == CHC_ERR_CANCELLED);

    chc_err err2 = {};
    rc = io.write(io.ud, "x", 1, &err2);
    CHECK(rc == CHC_ERR_CANCELLED);

    g_cancel = false;

out:
    fixture_down(&f);
}

static int64_t
now_us(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t) ts.tv_sec * 1000000 + ts.tv_nsec / 1000;
}

/* Cancel hook that lets the first `g_cancel_after` polls through, so a read
 * that retries on WANT_READ can be stopped on its second pass. */
static int  g_cancel_after = -1;
static int  g_cancel_calls = 0;
static bool cancel_after_cb(void *ud)
{
    (void) ud;
    return g_cancel_after >= 0 && g_cancel_calls++ >= g_cancel_after;
}

/* A non-blocking socket with nothing to read makes SSL_read report
 * WANT_READ; the loop must retry rather than fail. */
static void
test_want_read_retry(EVP_PKEY *pkey, X509 *cert)
{
    current_test = "want_read_retry";
    fixture f;
    if (fixture_up(&f, pkey, cert, MODE_DRAIN_UNTIL_CLOSE) != 0) {
        fail_count++;
        goto out;
    }

    int fl = fcntl(f.cli_fd, F_GETFL, 0);
    CHECK(fcntl(f.cli_fd, F_SETFL, fl | O_NONBLOCK) == 0);

    chc_openssl_io state;
    chc_io io;
    chc_openssl_io_init(&state, &io, f.cli_ssl, cancel_after_cb, NULL);
    g_cancel_calls = 0;
    g_cancel_after = 1;

    char rb[8];
    size_t got = 0;
    chc_err err = {};
    CHECK(io.read(io.ud, rb, sizeof rb, &got, &err) == CHC_ERR_CANCELLED);
    CHECK(g_cancel_calls >= 2);
    g_cancel_after = -1;
    fcntl(f.cli_fd, F_SETFL, fl);

out:
    fixture_down(&f);
}

/* Closing the socket under the SSL object turns both directions into
 * SSL_ERROR_SYSCALL with errno set. */
static void
test_syscall_failure(EVP_PKEY *pkey, X509 *cert)
{
    current_test = "syscall_failure";
    fixture f;
    if (fixture_up(&f, pkey, cert, MODE_DRAIN_UNTIL_CLOSE) != 0) {
        fail_count++;
        goto out;
    }

    chc_openssl_io state;
    chc_io io;
    chc_openssl_io_init(&state, &io, f.cli_ssl, NULL, NULL);

    close(f.cli_fd);
    f.cli_fd = -1;

    char rb[8];
    size_t got = 0;
    chc_err err = {};
    CHECK(io.read(io.ud, rb, sizeof rb, &got, &err) == CHC_ERR_IO);
    CHECK(strstr(err.msg, "SSL_read") != NULL);

    chc_err werr = {};
    CHECK(io.write(io.ud, "hello", 5, &werr) == CHC_ERR_IO);
    CHECK(strstr(werr.msg, "SSL_write") != NULL);

out:
    fixture_down(&f);
}

/* Non-TLS bytes on the wire surface as a protocol error carrying the
 * OpenSSL error-queue text. */
static void
test_protocol_error(EVP_PKEY *pkey, X509 *cert)
{
    current_test = "protocol_error";
    fixture f;
    if (fixture_up(&f, pkey, cert, MODE_GARBAGE) != 0) {
        fail_count++;
        goto out;
    }

    chc_openssl_io state;
    chc_io io;
    chc_openssl_io_init(&state, &io, f.cli_ssl, NULL, NULL);

    char rb[64];
    size_t got = 0;
    chc_err err = {};
    CHECK(io.read(io.ud, rb, sizeof rb, &got, &err) == CHC_ERR_IO);
    CHECK(strstr(err.msg, "SSL_read") != NULL);
    CHECK(strchr(err.msg, '(') != NULL);        /* error-queue detail */

out:
    fixture_down(&f);
}

/* chc__openssl_fail maps each SSL_get_error class to a message. The
 * classes the transport cannot be coaxed into producing are exercised
 * directly. */
static void
test_fail_messages(void)
{
    current_test = "fail_messages";
    chc_err err = {};

    ERR_clear_error();
    CHECK(chc__openssl_fail(&err, 0, SSL_ERROR_ZERO_RETURN, "op") == CHC_ERR_IO);
    CHECK(strstr(err.msg, "peer closed") != NULL);

    CHECK(chc__openssl_fail(&err, 0, SSL_ERROR_SYSCALL, "op") == CHC_ERR_IO);
    CHECK(strstr(err.msg, "EOF before close_notify") != NULL);

    errno = 0;
    CHECK(chc__openssl_fail(&err, -1, SSL_ERROR_SYSCALL, "op") == CHC_ERR_IO);
    CHECK(strstr(err.msg, "syscall failed") != NULL);

    CHECK(chc__openssl_fail(&err, -1, SSL_ERROR_WANT_X509_LOOKUP, "op") == CHC_ERR_IO);
    CHECK(strstr(err.msg, "I/O error") != NULL);
}

/* Deadlines gate the read side through poll. */
static void
test_deadline_without_fd(void)
{
    current_test = "deadline_without_fd";
    SSL_CTX *ctx = SSL_CTX_new(TLS_client_method());
    CHECK(ctx != NULL);
    if (!ctx) return;
    SSL *ssl = SSL_new(ctx);
    CHECK(ssl != NULL);
    if (ssl) {
        SSL_set_bio(ssl, BIO_new(BIO_s_mem()), BIO_new(BIO_s_mem()));

        chc_openssl_io state;
        chc_io io;
        chc_openssl_io_init(&state, &io, ssl, NULL, NULL);
        chc_openssl_io_set_deadline(&state, now_us() + 1000 * 1000);

        char rb[8];
        size_t got = 0;
        chc_err err = {};
        CHECK(io.read(io.ud, rb, sizeof rb, &got, &err) == CHC_ERR_IO);
        CHECK(strstr(err.msg, "SSL_get_fd") != NULL);
        SSL_free(ssl);
    }
    SSL_CTX_free(ctx);
}

static volatile sig_atomic_t poll_alarm = 0;
static void on_sigalrm(int sig) { (void) sig; poll_alarm = 1; }

static void
test_deadlines(EVP_PKEY *pkey, X509 *cert)
{
    current_test = "deadlines";
    fixture f;
    if (fixture_up(&f, pkey, cert, MODE_ECHO_UPCASE) != 0) {
        fail_count++;
        goto out;
    }

    chc_openssl_io state;
    chc_io io;
    chc_openssl_io_init(&state, &io, f.cli_ssl, NULL, NULL);

    /* Data on the way: poll reports readable inside the deadline. Reading
     * part of the record leaves the rest pending, which short-circuits the
     * next poll. */
    chc_err err = {};
    CHECK(io.write(io.ud, "hello", 5, &err) == CHC_OK);
    chc_openssl_io_set_deadline(&state, now_us() + 5 * 1000 * 1000);
    char rb[8];
    size_t got = 0;
    CHECK(io.read(io.ud, rb, 2, &got, &err) == CHC_OK);
    CHECK_EQ_U64(got, 2);
    got = 0;
    CHECK(io.read(io.ud, rb, sizeof rb, &got, &err) == CHC_OK);
    CHECK_EQ_U64(got, 3);

    /* Deadline already behind us. */
    chc_openssl_io_set_deadline(&state, now_us() - 1000);
    chc_err past = {};
    CHECK(io.read(io.ud, rb, sizeof rb, &got, &past) == CHC_ERR_IO);
    CHECK(strstr(past.msg, "read timeout") != NULL);

    /* Nothing more arrives, so poll runs out of time. */
    chc_openssl_io_set_deadline(&state, now_us() + 80 * 1000);
    chc_err late = {};
    CHECK(io.read(io.ud, rb, sizeof rb, &got, &late) == CHC_ERR_IO);
    CHECK(strstr(late.msg, "read timeout") != NULL);

    /* A signal mid-poll is retried against the same deadline. */
    struct sigaction sa = {}, old = {};
    sa.sa_handler = on_sigalrm;
    sigemptyset(&sa.sa_mask);
    CHECK(sigaction(SIGALRM, &sa, &old) == 0);
    poll_alarm = 0;
    /* Repeating so a descheduled process still gets poll interrupted. */
    struct itimerval it = {};
    it.it_value.tv_usec = it.it_interval.tv_usec = 20 * 1000;
    CHECK(setitimer(ITIMER_REAL, &it, NULL) == 0);
    chc_openssl_io_set_deadline(&state, now_us() + 200 * 1000);
    chc_err eintr = {};
    CHECK(io.read(io.ud, rb, sizeof rb, &got, &eintr) == CHC_ERR_IO);
    CHECK(poll_alarm == 1);
    struct itimerval off = {};
    setitimer(ITIMER_REAL, &off, NULL);
    sigaction(SIGALRM, &old, NULL);

    /* poll failing outright is reported rather than retried. */
    struct rlimit saved;
    if (getrlimit(RLIMIT_NOFILE, &saved) == 0) {
        struct rlimit none = { 0, saved.rlim_max };
        CHECK(setrlimit(RLIMIT_NOFILE, &none) == 0);
        chc_openssl_io_set_deadline(&state, now_us() + 1000 * 1000);
        chc_err perr = {};
        int rc = io.read(io.ud, rb, sizeof rb, &got, &perr);
        CHECK(setrlimit(RLIMIT_NOFILE, &saved) == 0);
        CHECK(rc == CHC_ERR_IO);
        CHECK(strstr(perr.msg, "poll(") != NULL);
    } else
        fail_count++;

    chc_openssl_io_set_deadline(&state, 0);

out:
    fixture_down(&f);
}

int
main(void)
{
    signal(SIGPIPE, SIG_IGN);

    EVP_PKEY *pkey = NULL;
    X509     *cert = NULL;
    if (make_keypair(&pkey, &cert) != 0) {
        fprintf(stderr, "keygen failed\n");
        return 1;
    }

    test_echo_roundtrip(pkey, cert);
    test_eof_after_handshake(pkey, cert);
    test_cancel(pkey, cert);
    test_want_read_retry(pkey, cert);
    test_syscall_failure(pkey, cert);
    test_protocol_error(pkey, cert);
    test_fail_messages();
    test_deadline_without_fd();
    test_deadlines(pkey, cert);

    X509_free(cert);
    EVP_PKEY_free(pkey);

    if (fail_count) {
        fprintf(stderr, "%d failure(s)\n", fail_count);
        return 1;
    }
    printf("ok\n");
    return 0;
}
