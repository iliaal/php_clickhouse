/*
   +----------------------------------------------------------------------+
   | This source file is subject to version 3.01 of the PHP license,       |
   | available in the LICENSE file distributed with this package.         |
   +----------------------------------------------------------------------+
   | Author: Vulcan                                                       |
   +----------------------------------------------------------------------+
*/

#include <pthread.h>
#include <stdio.h>
#include <sapi/embed/php_embed.h>

#ifndef ZTS
#error This regression requires a ZTS PHP embed library.
#endif

static void *run_request(void *result)
{
    int *status = result;
    zval value;
    *status = 1;
    ts_resource(0);
    ZEND_TSRMLS_CACHE_UPDATE();
    if (php_request_startup() == FAILURE) {
        ts_free_thread();
        return NULL;
    }
    SG(headers_sent) = 1;
    SG(request_info).no_headers = 1;
    ZVAL_UNDEF(&value);
    zend_first_try {
        /* Host parsing touches the extension's own TSRM cache. An invalid
         * port then gives a deterministic exception without network I/O. */
        if (zend_eval_string(
                "(function () {"
                "try { new ClickHouse(['host' => '127.0.0.1', 'port' => 0]); }"
                "catch (ClickHouseException $e) {"
                "return $e->getMessage() === 'port out of 1..65535 range';"
                "} return false; })()",
                &value, "ZTS request-thread regression") == SUCCESS &&
                Z_TYPE(value) == IS_TRUE) {
            *status = 0;
        }
    } zend_catch {
        *status = 1;
    } zend_end_try();
    zval_ptr_dtor(&value);
    php_request_shutdown(NULL);
    ts_free_thread();
    return NULL;
}

int main(int argc, char **argv)
{
    if (argc != 2) {
        fprintf(stderr, "usage: %s /absolute/path/to/php.ini\n", argv[0]);
        return 1;
    }
    php_embed_module.php_ini_path_override = argv[1];
    php_embed_module.php_ini_ignore_cwd = 1;
    if (php_embed_init(argc, argv) == FAILURE) {
        fprintf(stderr, "embed startup failed\n");
        return 1;
    }
    int status = 0;
    /* MINIT ran above on the main thread. Each worker must initialize the
     * extension cache independently; a CLI PHPT cannot exercise this path. */
    for (int i = 0; i < 8; ++i) {
        pthread_t worker;
        int request_status = 1;
        if (pthread_create(&worker, NULL, run_request, &request_status) != 0) {
            status = 1;
            break;
        }
        if (pthread_join(worker, NULL) != 0) {
            /* Do not tear PHP down while a worker might still be running. */
            return 1;
        }
        if (request_status != 0) {
            status = 1;
            break;
        }
    }
    php_embed_shutdown();
    if (status != 0) {
        fprintf(stderr, "ZTS request-thread regression failed\n");
        return 1;
    }
    puts("ZTS request-thread regression passed (8 worker requests)");
    return 0;
}
