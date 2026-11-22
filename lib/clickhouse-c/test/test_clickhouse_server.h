/*
 * test_clickhouse_server.h -- live clickhouse-server test fixture
 */

#ifndef CLICKHOUSE_TEST_CLICKHOUSE_SERVER_H
#define CLICKHOUSE_TEST_CLICKHOUSE_SERVER_H

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

CHC_MAYBE_UNUSED static int
test_clickhouse_on_path(void)
{
    return system("command -v clickhouse >/dev/null 2>&1") == 0;
}

CHC_MAYBE_UNUSED static int
test_clickhouse_connect(uint16_t port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    struct sockaddr_in sa = {};
    sa.sin_family = AF_INET;
    sa.sin_port = htons(port);
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    for (int i = 0; i < 30; i++) {
        if (connect(fd, (struct sockaddr *) &sa, sizeof sa) == 0)
            return fd;
        struct timespec ts = { .tv_sec = 0, .tv_nsec = 200 * 1000 * 1000 };
        nanosleep(&ts, NULL);
    }
    close(fd);
    return -1;
}

CHC_MAYBE_UNUSED static int
test_write_if_absent(const char *path, const char *body)
{
    struct stat st;
    if (stat(path, &st) == 0) return 0;
    FILE *f = fopen(path, "w");
    if (!f) return -1;
    fputs(body, f);
    return fclose(f);
}

CHC_MAYBE_UNUSED static int
test_join_path(char *buf, size_t len, const char *root, const char *name)
{
    int n = snprintf(buf, len, "%s/%s", root, name);
    return n > 0 && (size_t) n < len ? 0 : -1;
}

CHC_MAYBE_UNUSED static int
test_mkdir_child(const char *root, const char *name)
{
    char path[512];
    if (test_join_path(path, sizeof path, root, name)) return -1;
    mkdir(path, 0700);
    return 0;
}

CHC_MAYBE_UNUSED static int
test_clickhouse_wait_port(uint16_t port)
{
    for (int i = 0; i < 60; i++) {
        int probe = socket(AF_INET, SOCK_STREAM, 0);
        if (probe >= 0) {
            struct sockaddr_in sa = {};
            sa.sin_family = AF_INET;
            sa.sin_port = htons(port);
            sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
            int rc = connect(probe, (struct sockaddr *) &sa, sizeof sa);
            close(probe);
            if (rc == 0) return 0;
        }
        struct timespec ts = { .tv_sec = 0, .tv_nsec = 500 * 1000 * 1000 };
        nanosleep(&ts, NULL);
    }
    return -1;
}

CHC_MAYBE_UNUSED static void
test_clickhouse_server_stop(pid_t *pid)
{
    if (!pid || *pid <= 0) return;
    kill(*pid, SIGTERM);
    for (int i = 0; i < 50; i++) {
        int status;
        pid_t r = waitpid(*pid, &status, WNOHANG);
        if (r == *pid) {
            *pid = -1;
            return;
        }
        struct timespec ts = { .tv_sec = 0, .tv_nsec = 100 * 1000 * 1000 };
        nanosleep(&ts, NULL);
    }
    kill(*pid, SIGKILL);
    waitpid(*pid, NULL, 0);
    *pid = -1;
}

CHC_MAYBE_UNUSED static pid_t
test_clickhouse_server_start(const char *root, uint16_t port)
{
    char config_path[512], users_path[512];
    if (test_join_path(config_path, sizeof config_path, root, "config.xml") ||
        test_join_path(users_path, sizeof users_path, root, "users.xml"))
        return -1;

    mkdir(root, 0700);
    if (test_mkdir_child(root, "data") ||
        test_mkdir_child(root, "tmp") ||
        test_mkdir_child(root, "user_files"))
        return -1;

    if (test_write_if_absent(config_path,
        "<clickhouse>\n"
        "  <logger>\n"
        "    <level>warning</level>\n"
        "    <console>0</console>\n"
        "  </logger>\n"
        "  <users_config>users.xml</users_config>\n"
        "  <default_profile>default</default_profile>\n"
        "  <default_database>default</default_database>\n"
        "  <mark_cache_size>5368709120</mark_cache_size>\n"
        "</clickhouse>\n") != 0)
        return -1;

    if (test_write_if_absent(users_path,
        "<clickhouse>\n"
        "  <profiles><default><load_balancing>random</load_balancing></default></profiles>\n"
        "  <users><default>\n"
        "    <password></password>\n"
        "    <networks><ip>::/0</ip></networks>\n"
        "    <profile>default</profile>\n"
        "    <quota>default</quota>\n"
        "    <access_management>1</access_management>\n"
        "  </default></users>\n"
        "  <quotas><default/></quotas>\n"
        "</clickhouse>\n") != 0)
        return -1;

    pid_t pid = fork();
    if (pid < 0) return -1;
    if (pid == 0) {
        char tcp_arg[64];
        snprintf(tcp_arg, sizeof tcp_arg, "--tcp_port=%u", (unsigned) port);
        if (chdir(root) != 0) _exit(127);
        int devnull = open("/dev/null", O_WRONLY);
        if (devnull >= 0) {
            dup2(devnull, STDOUT_FILENO);
            dup2(devnull, STDERR_FILENO);
            close(devnull);
        }
        execlp("clickhouse", "clickhouse", "server",
               "--config-file", "config.xml",
               "--",
               tcp_arg,
               "--http_port=0",
               "--interserver_http_port=0",
               "--mysql_port=0",
               "--postgresql_port=0",
               "--listen_host=127.0.0.1",
               "--path=data/",
               "--tmp_path=tmp/",
               "--user_files_path=user_files/",
               "--logger.log=server.log",
               "--logger.errorlog=server.err",
               "--logger.level=warning",
               (char *) NULL);
        _exit(127);
    }

    if (test_clickhouse_wait_port(port) == 0) return pid;

    int status;
    if (waitpid(pid, &status, WNOHANG) == pid) {
        fprintf(stderr, "clickhouse-server died during startup (status=%d)\n",
                status);
    } else {
        test_clickhouse_server_stop(&pid);
    }
    return -1;
}

#endif
