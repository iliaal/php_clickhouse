/*
   +----------------------------------------------------------------------+
   | This source file is subject to version 3.01 of the PHP license,       |
   | available in the LICENSE file distributed with this package.         |
   +----------------------------------------------------------------------+
   | Author: Ilia Alshanetsky <ilia@ilia.ws>                              |
   +----------------------------------------------------------------------+
*/

#include <pthread.h>
#include <stdio.h>
#include <sapi/embed/php_embed.h>

#ifndef ZTS
#error This regression requires a ZTS PHP embed library.
#endif

static char **scripts;
static int script_count;

/* Every script runs as its own request on one worker thread, as consecutive
 * requests do on a FrankenPHP or mod_php worker thread. */
static void *run_requests(void *result)
{
    int *status = result;
    *status = 0;
    ts_resource(0);
    ZEND_TSRMLS_CACHE_UPDATE();
    for (int i = 0; i < script_count; ++i) {
        if (php_request_startup() == FAILURE) {
            *status = 1;
            break;
        }
        SG(headers_sent) = 1;
        SG(request_info).no_headers = 1;
        zend_first_try {
            zend_file_handle fh;
            zend_stream_init_filename(&fh, scripts[i]);
            php_execute_script(&fh);
            zend_destroy_file_handle(&fh);
        } zend_end_try();
        php_request_shutdown(NULL);
        fflush(stdout);
    }
    ts_free_thread();
    return NULL;
}

int main(int argc, char **argv)
{
    if (argc < 3) {
        fprintf(stderr, "usage: %s /absolute/path/to/php.ini script...\n", argv[0]);
        return 1;
    }
    php_embed_module.php_ini_path_override = argv[1];
    php_embed_module.php_ini_ignore_cwd = 1;
    if (php_embed_init(1, argv) == FAILURE) {
        fprintf(stderr, "embed startup failed\n");
        return 1;
    }
    scripts = &argv[2];
    script_count = argc - 2;
    pthread_t worker;
    int status = 1;
    if (pthread_create(&worker, NULL, run_requests, &status) != 0) {
        php_embed_shutdown();
        return 1;
    }
    if (pthread_join(worker, NULL) != 0) {
        return 1;
    }
    php_embed_shutdown();
    return status;
}
