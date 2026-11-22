/*
 * minimal_decode.c — subprocess Native blocks → printf.
 *
 * Spawns:    clickhouse-local --format Native -q "<query>"
 * Reads:     Native blocks off the child's stdout pipe via clickhouse-posix-io.h
 * Prints:    one row per line, tab-separated columns, NULL as "\N"
 *
 * Uses only clickhouse.h + clickhouse-posix-io.h. No TCP, no compression,
 * no extra link-time deps beyond libc.
 *
 * Build (from clickhouse-c/):
 *     cc -std=c11 -O2 -I. examples/minimal_decode.c -o minimal_decode
 *
 * Run:
 *     ./minimal_decode "SELECT number, toString(number*number) FROM numbers(5)"
 *
 * Required server settings — passed via `--` so clickhouse-local emits
 * printable type names & non-Sparse columns:
 *     --output_format_native_encode_types_in_binary_format=0
 *     --output_format_native_write_use_sparse_columns_optimization=0
 */

#include <inttypes.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#define CHC_PROVIDE_STDLIB_ALLOC
#define CHC_IMPLEMENTATION
#include "clickhouse.h"
#include "clickhouse-posix-io.h"

static pid_t
spawn_local(const char *query, int *out_fd)
{
    int pipefd[2];
    if (pipe(pipefd) < 0) { perror("pipe"); return -1; }

    pid_t pid = fork();
    if (pid < 0) { perror("fork"); close(pipefd[0]); close(pipefd[1]); return -1; }

    if (pid == 0) {
        /* child */
        close(pipefd[0]);
        if (dup2(pipefd[1], STDOUT_FILENO) < 0) _exit(127);
        close(pipefd[1]);
        execlp("clickhouse",
               "clickhouse", "local",
               "--format", "Native",
               "--output_format_native_encode_types_in_binary_format=0",
               "-q", query,
               (char *) NULL);
        _exit(127);
    }

    close(pipefd[1]);
    *out_fd = pipefd[0];
    return pid;
}

static volatile sig_atomic_t cancel_flag = 0;
static void on_sigint(int sig) { (void) sig; cancel_flag = 1; }
static bool check_cancel(void *ud) { (void) ud; return cancel_flag != 0; }

/* Print one row's value for a single (non-composite) column. Variable types
 * fall back to a "<type>" placeholder — extend as you need. */
static void
print_value(const chc_type *t, const chc_column *c, size_t row)
{
    /* Nullable wrapper — unwrap & recurse on the inner column. */
    if (chc_column_layout(c) == CHC_COL_NULLABLE) {
        if (chc_column_null_map(c)[row]) { fputs("\\N", stdout); return; }
        print_value(chc_type_child(t, 0), chc_column_nullable_inner(c), row);
        return;
    }

    switch (chc_column_layout(c)) {
    case CHC_COL_FIXED: {
        size_t es;
        const void *data = chc_column_fixed_data(c, &es);
        const uint8_t *p = (const uint8_t *) data + row * es;
        switch (chc_type_kind(t)) {
        case CHC_INT8:    printf("%d",       (int)       *(const int8_t   *) p); break;
        case CHC_INT16:   printf("%d",       (int)       *(const int16_t  *) p); break;
        case CHC_INT32:   printf("%" PRId32, *(const int32_t  *) p); break;
        case CHC_INT64:   printf("%" PRId64, *(const int64_t  *) p); break;
        case CHC_UINT8:   printf("%u",       (unsigned)  *(const uint8_t  *) p); break;
        case CHC_UINT16:  printf("%u",       (unsigned)  *(const uint16_t *) p); break;
        case CHC_UINT32:  printf("%" PRIu32, *(const uint32_t *) p); break;
        case CHC_UINT64:  printf("%" PRIu64, *(const uint64_t *) p); break;
        case CHC_FLOAT32: printf("%g",       (double)    *(const float    *) p); break;
        case CHC_FLOAT64: printf("%g",                   *(const double   *) p); break;
        case CHC_BOOL:    fputs(*p ? "true" : "false", stdout); break;
        default:          printf("<%.*s>", (int) es * 2, "?????????????????"); break;
        }
        break;
    }
    case CHC_COL_STRING: {
        const uint8_t  *bytes   = chc_column_string_data(c);
        const uint64_t *offsets = chc_column_string_offsets(c);
        uint64_t start = row == 0 ? 0 : offsets[row - 1];
        uint64_t end   = offsets[row];
        fwrite(bytes + start, 1, (size_t) (end - start), stdout);
        break;
    }
    default: {
        size_t nlen;
        const char *n = chc_type_name(t, &nlen);
        printf("<%.*s>", (int) nlen, n);
        break;
    }
    }
}

int
main(int argc, char **argv)
{
    if (argc != 2) {
        fprintf(stderr, "usage: %s \"SELECT ... \"\n", argv[0]);
        return 2;
    }
    signal(SIGINT, on_sigint);

    int fd;
    pid_t pid = spawn_local(argv[1], &fd);
    if (pid < 0) return 1;

    chc_alloc al = chc_alloc_stdlib();

    chc_posix_io state;
    chc_io io;
    chc_posix_io_init(&state, &io, fd, check_cancel, NULL);

    /* clickhouse-local emits Native without BlockInfo or
     * has_custom_serialization (no protocol revision negotiated). */
    chc_block_opts opts = {};

    /* Streaming successive blocks needs one chc_in across all reads so bytes
     * read past a block boundary stay buffered for the next block. */
    chc_err ierr = {};
    chc_in in;
    if (chc_in_init(&in, &io, &al, opts.read_buffer_bytes, &ierr) != CHC_OK) {
        fprintf(stderr, "init: %s\n", ierr.msg);
        close(fd);
        waitpid(pid, &(int){0}, 0);
        return 1;
    }

    int rc = 0;
    for (;;) {
        chc_block *block = NULL;
        chc_err err = {};
        if (chc_block_read(&in, &al, &opts, &block, &err) != CHC_OK) {
            fprintf(stderr, "decode: %s\n", err.msg);
            rc = 1; break;
        }
        if (!block) break;   /* clean EOF at block boundary */

        size_t ncols = chc_block_n_columns(block);
        size_t nrows = chc_block_n_rows(block);
        for (size_t r = 0; r < nrows; r++) {
            for (size_t c = 0; c < ncols; c++) {
                if (c) fputc('\t', stdout);
                print_value(chc_block_column_type(block, c),
                            chc_block_column(block, c),
                            r);
            }
            fputc('\n', stdout);
        }
        chc_block_destroy(block, &al);
    }

    chc_in_free(&in);
    close(fd);
    int status;
    waitpid(pid, &status, 0);
    return rc;
}
