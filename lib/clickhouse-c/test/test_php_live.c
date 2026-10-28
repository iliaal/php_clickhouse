/* Live operations using the PHP compatibility implementation mode. */
#define CHC_PHP_COMPAT 1
#define CHC_ZSTD_COMPRESSION_LEVEL 1
#define main upstream_client_test_main
#include "test_client_tcp.c"
#undef main

int main(void)
{
    signal(SIGPIPE, SIG_IGN);
    g_port = 19006;
    g_server_pid = test_clickhouse_server_start("/tmp/ch-test-php-srv", g_port);
    if (g_server_pid < 0) {
        fprintf(stderr, "failed to start PHP compatibility test server\n");
        return 77;
    }
    atexit(stop_server);
    test_select_simple();
    test_select_many();
    test_select_with_param();
    test_select_with_array_param();
    test_insert_lz4_roundtrip();
    test_insert_zstd_roundtrip();
    test_external_data_compat(CHC_COMP_NONE, 0);
    test_external_data_compat(CHC_COMP_LZ4, 64);
    test_external_data_compat(CHC_COMP_LZ4, 0);
    test_external_data_compat(CHC_COMP_ZSTD, 64);
    test_external_data_compat(CHC_COMP_ZSTD, 0);
    stop_server();
    if (fail_count) return 1;
    fprintf(stderr, "all PHP compatibility live tests passed\n");
    return 0;
}
