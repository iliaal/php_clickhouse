/*
 * test_block_roundtrip.c -- encode a block via chc_block_builder, feed it
 * to `clickhouse local --input-format Native -q 'SELECT ...'`, & verify
 * the values come back through clickhouse-local's own decoder.
 */

#include <assert.h>
#include <inttypes.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#define CHC_PROVIDE_STDLIB_ALLOC
#define CHC_IMPLEMENTATION
#include "clickhouse.h"
#include "clickhouse-posix-io.h"

static int fail_count = 0;
static const char *current_test = "";

#include "test_common.h"

/* Spawn `clickhouse local --input-format Native -q "<sql>"`, returning a
 * pipe to feed bytes in. The child's stdout is captured into `child_out`
 * if non-NULL, otherwise inherited from this process. */
static pid_t
spawn_local_input(const char *sql, const char *struct_clause,
                  int *write_fd, int *read_fd)
{
    int in_pipe[2], out_pipe[2];
    if (pipe(in_pipe) < 0) return -1;
    if (pipe(out_pipe) < 0) { close(in_pipe[0]); close(in_pipe[1]); return -1; }

    pid_t pid = fork();
    if (pid < 0) { close(in_pipe[0]); close(in_pipe[1]); close(out_pipe[0]); close(out_pipe[1]); return -1; }
    if (pid == 0) {
        dup2(in_pipe[0], STDIN_FILENO);
        dup2(out_pipe[1], STDOUT_FILENO);
        close(in_pipe[0]); close(in_pipe[1]);
        close(out_pipe[0]); close(out_pipe[1]);
        execlp("clickhouse", "clickhouse", "local",
               "--input-format", "Native",
               "--structure", struct_clause,
               "--output_format_native_encode_types_in_binary_format=0",
               "-q", sql, (char *) NULL);
        _exit(127);
    }
    close(in_pipe[0]);
    close(out_pipe[1]);
    *write_fd = in_pipe[1];
    *read_fd  = out_pipe[0];
    return pid;
}

static char *
read_all(int fd, size_t *out_len)
{
    size_t cap = 256, len = 0;
    char *buf = malloc(cap);
    for (;;) {
        if (len == cap) { cap *= 2; buf = realloc(buf, cap); }
        ssize_t n = read(fd, buf + len, cap - len);
        if (n <= 0) break;
        len += (size_t) n;
    }
    buf[len < cap ? len : cap - 1] = '\0';
    *out_len = len;
    return buf;
}

static char *capture_roundtrip(const char *test_name, chc_block_builder *bb,
                               const char *structure, const char *sql,
                               size_t *out_len);
static char *capture_column_roundtrip(const char *test_name,
                                      const char *name,
                                      const char *type_name,
                                      const chc_column *col,
                                      const char *sql, size_t *out_len);
static void run_column_roundtrip(const char *test_name, const char *name,
                                 const char *type_name, const chc_column *col,
                                 const char *sql, const char *expected);

static void
test_write_uint32(void)
{
    current_test = "write_uint32";
    uint32_t a[5] = { 10, 20, 30, 40, 50 };
    chc_column cx = chc_build_fixed(a, sizeof a[0], 5);
    run_column_roundtrip(current_test, "x", "UInt32", &cx,
                         "SELECT sum(x) FROM table", "150");
}

static void
test_write_string(void)
{
    current_test = "write_string";
    /* "ab", "cde", "" -> offsets {2, 5, 5}, bytes "abcde". */
    uint64_t offs[3] = { 2, 5, 5 };
    const uint8_t bytes[] = { 'a','b','c','d','e' };
    chc_column cs = chc_build_string(offs, bytes, 3);
    run_column_roundtrip(current_test, "s", "String", &cs,
        "SELECT groupArray(s) FROM table FORMAT TSV", "['ab','cde','']");
}

static char *
capture_roundtrip(const char *test_name, chc_block_builder *bb,
                  const char *structure, const char *sql, size_t *out_len)
{
    chc_err err = {};
    int wfd, rfd;
    pid_t pid = spawn_local_input(sql, structure, &wfd, &rfd);
    CHECK(pid > 0);
    if (pid <= 0) return NULL;

    chc_posix_io state; chc_io io;
    chc_posix_io_init(&state, &io, wfd, NULL, NULL);
    if (chc_block_write(&io, bb, NULL, &err) < 0) {
        fprintf(stderr, "%s: write: %s\n", test_name, err.msg);
        fail_count++;
    }
    close(wfd);

    char *out = read_all(rfd, out_len);
    close(rfd);
    int status; waitpid(pid, &status, 0);
    CHECK(WIFEXITED(status));
    CHECK(WEXITSTATUS(status) == 0);
    return out;
}

static char *
capture_column_roundtrip(const char *test_name, const char *name,
                         const char *type_name, const chc_column *col,
                         const char *sql, size_t *out_len)
{
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {};
    chc_type *type = NULL;
    char *out = NULL;
    size_t name_len = strlen(name);
    size_t type_len = strlen(type_name);
    size_t structure_len = name_len + type_len + 2;
    char *structure = malloc(structure_len);
    if (!structure) {
        fprintf(stderr, "%s: structure allocation failed\n", test_name);
        fail_count++;
        return NULL;
    }
    memcpy(structure, name, name_len);
    structure[name_len] = ' ';
    memcpy(structure + name_len + 1, type_name, type_len + 1);

    if (chc_type_parse(type_name, type_len, &al, &type, &err) < 0) {
        fprintf(stderr, "%s: type: %s\n", test_name, err.msg);
        fail_count++;
        goto done;
    }

    chc_block_col cols[1];
    chc_block_builder bb;
    chc_block_builder_init(&bb, cols);
    chc_block_builder_append(&bb, name, name_len, type, col);
    out = capture_roundtrip(test_name, &bb, structure, sql, out_len);

done:
    chc_type_destroy(type, &al);
    free(structure);
    return out;
}

static void
run_column_roundtrip(const char *test_name, const char *name,
                     const char *type_name, const chc_column *col,
                     const char *sql, const char *expected)
{
    size_t out_len = 0;
    char *out = capture_column_roundtrip(test_name, name, type_name, col,
                                         sql, &out_len);
    if (out && !strstr(out, expected)) {
        fprintf(stderr, "%s: missing '%s' in output: %.*s\n",
                test_name, expected, (int) out_len, out);
        fail_count++;
    }
    free(out);
}

/* Drive `clickhouse local` with a built block & assert each substring
 * in `expect_substrings` (NULL-terminated) appears in TSV output. */
static void
run_roundtrip(const char *test_name,
              chc_block_builder *bb,
              const char *structure,
              const char *sql,
              const char **expect_substrings)
{
    size_t out_len;
    char *out = capture_roundtrip(test_name, bb, structure, sql, &out_len);
    if (!out) return;
    for (const char **p = expect_substrings; *p; p++) {
        if (!strstr(out, *p)) {
            fprintf(stderr, "%s: missing substring '%s' in output: %.*s\n",
                    test_name, *p, (int) out_len, out);
            fail_count++;
        }
    }
    free(out);
}

static void
test_write_nullable_fixed(void)
{
    current_test = "write_nullable_fixed";
    /* Rows: NULL, 100, NULL, 200. Inner cells under NULL are arbitrary. */
    uint8_t  nulls[4]  = { 1, 0, 1, 0 };
    uint32_t values[4] = { 0, 100, 0, 200 };
    chc_column inner = chc_build_fixed(values, sizeof values[0], 4);
    chc_column col = chc_build_nullable(nulls, &inner);
    run_column_roundtrip(current_test, "x", "Nullable(UInt32)", &col,
        "SELECT sum(x), countIf(x IS NULL) FROM table", "300\t2");
}

static void
test_write_nullable_string(void)
{
    current_test = "write_nullable_string";
    /* Rows: "a", NULL, "bc". Inner offsets/data still cover all 3 strings. */
    uint8_t  nulls[3]   = { 0, 1, 0 };
    uint64_t offs[3]    = { 1, 1, 3 };
    const uint8_t buf[] = "abc";
    chc_column inner = chc_build_string(offs, buf, 3);
    chc_column col = chc_build_nullable(nulls, &inner);
    run_column_roundtrip(current_test, "s", "Nullable(String)", &col,
        "SELECT countIf(s IS NULL), countIf(s IS NOT NULL) FROM table",
        "1\t2");
}

static void
test_write_array_fixed(void)
{
    current_test = "write_array_fixed";
    /* Rows: [1,2,3], [4,5]. Cumulative ends: [3, 5]. */
    uint64_t offs[2]    = { 3, 5 };
    uint32_t values[5]  = { 1, 2, 3, 4, 5 };
    chc_column inner = chc_build_fixed(values, sizeof values[0], 5);
    chc_column col = chc_build_array(offs, 2, &inner);
    run_column_roundtrip(current_test, "a", "Array(UInt32)", &col,
        "SELECT sum(arraySum(a)), sum(length(a)) FROM table", "15\t5");
}

static void
test_write_array_string(void)
{
    current_test = "write_array_string";
    /* Rows: ['x', 'yy'], ['zzz']. */
    uint64_t arr_offs[2]    = { 2, 3 };
    uint64_t val_offs[3]    = { 1, 3, 6 };
    const uint8_t buf[]     = "xyyzzz";
    chc_column values = chc_build_string(val_offs, buf, 3);
    chc_column col = chc_build_array(arr_offs, 2, &values);
    run_column_roundtrip(current_test, "a", "Array(String)", &col,
        "SELECT arrayStringConcat(arrayFlatten(groupArray(a))) FROM table",
        "xyyzzz");
}

static void
test_write_array_nested_fixed(void)
{
    current_test = "write_array_nested_fixed";
    /* Rows: [[1,2],[3]], [[4,5,6]].
     * Outer level: row 0 has 2 inner arrays, row 1 has 1 -> {2,3}.
     * Inner level: inner arrays of sizes 2,1,3 -> cumulative {2,3,6}. */
    uint64_t l0[2] = { 2, 3 };
    uint64_t l1[3] = { 2, 3, 6 };
    uint32_t values[6] = { 1, 2, 3, 4, 5, 6 };
    chc_column leaf = chc_build_fixed(values, sizeof values[0], 6);
    chc_column inner = chc_build_array(l1, 3, &leaf);
    chc_column col = chc_build_array(l0, 2, &inner);
    run_column_roundtrip(current_test, "a", "Array(Array(UInt32))", &col,
        "SELECT arraySum(arrayFlatten(arrayFlatten(groupArray(a)))), "
        "length(arrayFlatten(arrayFlatten(groupArray(a)))) FROM table",
        "21\t6");
}

static void
test_write_array_nested_fixed_3d(void)
{
    current_test = "write_array_nested_fixed_3d";
    /* Rows: [[[1],[2,3]],[[4]]], [[[5,6]]].
     * L0 (outer): row 0 has 2, row 1 has 1 -> {2,3}.
     * L1 (mid):   sizes 2,1,1 -> {2,3,4}.
     * L2 (inner): sizes 1,2,1,2 -> {1,3,4,6}. */
    uint64_t l0[2] = { 2, 3 };
    uint64_t l1[3] = { 2, 3, 4 };
    uint64_t l2[4] = { 1, 3, 4, 6 };
    uint32_t values[6] = { 1, 2, 3, 4, 5, 6 };
    chc_column leaf = chc_build_fixed(values, sizeof values[0], 6);
    chc_column inner2 = chc_build_array(l2, 4, &leaf);
    chc_column inner1 = chc_build_array(l1, 3, &inner2);
    chc_column col = chc_build_array(l0, 2, &inner1);
    run_column_roundtrip(current_test, "a", "Array(Array(Array(UInt32)))",
        &col,
        "SELECT arraySum(arrayFlatten(arrayFlatten(arrayFlatten(groupArray(a))))), "
        "length(arrayFlatten(arrayFlatten(arrayFlatten(groupArray(a))))) FROM table",
        "21\t6");
}

static void
test_write_array_nested_string(void)
{
    current_test = "write_array_nested_string";
    /* Rows: [['x','yy'],['zzz']], [['a']].
     * L0: {2, 3}. L1 inner-arr sizes 2,1,1 -> {2,3,4}.
     * Leaf strings: 'x','yy','zzz','a' -> offs {1,3,6,7}, buf "xyyzzza". */
    uint64_t l0[2] = { 2, 3 };
    uint64_t l1[3] = { 2, 3, 4 };
    uint64_t val_offs[4] = { 1, 3, 6, 7 };
    const uint8_t buf[] = "xyyzzza";
    chc_column leaf = chc_build_string(val_offs, buf, 4);
    chc_column inner = chc_build_array(l1, 3, &leaf);
    chc_column col = chc_build_array(l0, 2, &inner);
    run_column_roundtrip(current_test, "a", "Array(Array(String))", &col,
        "SELECT arrayStringConcat(arrayFlatten(arrayFlatten(groupArray(a)))) FROM table",
        "xyyzzza");

    /* Recursive writer, not append, catches String tree under Array type */
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {};
    chc_type *t = NULL;
    if (chc_type_parse("Array(Array(String))", 20, &al, &t, &err) < 0) {
        fprintf(stderr, "%s: type: %s\n", current_test, err.msg);
        fail_count++;
        return;
    }
    chc_block_col badcols[1];
    chc_block_builder bad;
    chc_block_builder_init(&bad, badcols);
    chc_err bad_err = {};
    test_mem_sink sink;
    chc_io io;
    test_mem_sink_init(&sink, &io);
    chc_block_builder_append(&bad, "x", 1, t, &leaf);
    CHECK(chc_block_write(&io, &bad, NULL, &bad_err) == CHC_ERR_TYPE);
    test_mem_sink_free(&sink);

    chc_type_destroy(t, &al);
}

static void
test_write_array_nullable(void)
{
    current_test = "write_array_nullable";
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {};
    chc_block_col bbcols[8];
    chc_block_builder bb;
    chc_block_builder_init(&bb, bbcols);

    const char *type_names[] = {
        "Array(Nullable(UInt32))",
        "Array(Nullable(String))",
        "Array(Array(Nullable(UInt32)))",
        "Array(Array(Nullable(String)))",
    };
    chc_type *types[4] = {};
    for (size_t i = 0; i < 4; i++) {
        if (chc_type_parse(type_names[i], strlen(type_names[i]),
                           &al, &types[i], &err) < 0) {
            fprintf(stderr, "%s: type: %s\n", current_test, err.msg);
            fail_count++;
            goto done;
        }
    }

    uint64_t arr_offs[2] = { 2, 5 };
    uint8_t fixed_nulls[5] = { 0, 1, 0, 1, 0 };
    uint32_t fixed_values[5] = { 1, 0, 3, 0, 5 };
    chc_column fixed_leaf = chc_build_fixed(
        fixed_values, sizeof fixed_values[0], 5);
    chc_column fixed_nullable = chc_build_nullable(fixed_nulls, &fixed_leaf);
    chc_column fixed_array = chc_build_array(arr_offs, 2, &fixed_nullable);
    chc_block_builder_append(&bb, "af", 2, types[0], &fixed_array);

    uint8_t string_nulls[5] = { 0, 1, 1, 0, 0 };
    uint64_t string_offs[5] = { 1, 1, 1, 3, 4 };
    const uint8_t string_data[] = "abcd";
    chc_column string_leaf = chc_build_string(string_offs, string_data, 5);
    chc_column string_nullable = chc_build_nullable(string_nulls, &string_leaf);
    chc_column string_array = chc_build_array(arr_offs, 2, &string_nullable);
    chc_block_builder_append(&bb, "astr", 4, types[1], &string_array);

    uint64_t outer_offs[2] = { 2, 4 };
    uint64_t inner_offs[4] = { 2, 2, 3, 5 };
    chc_column fixed_inner = chc_build_array(inner_offs, 4, &fixed_nullable);
    chc_column fixed_nested = chc_build_array(outer_offs, 2, &fixed_inner);
    chc_block_builder_append(&bb, "nf", 2, types[2], &fixed_nested);
    chc_column string_inner = chc_build_array(inner_offs, 4, &string_nullable);
    chc_column string_nested = chc_build_array(outer_offs, 2, &string_inner);
    chc_block_builder_append(&bb, "ns", 2, types[3], &string_nested);

    {
        const char *expect[] = { "2\t9\t2\tabcd\t2\t9\t2\tabcd", NULL };
        run_roundtrip(current_test, &bb,
            "af Array(Nullable(UInt32)), astr Array(Nullable(String)), "
            "nf Array(Array(Nullable(UInt32))), ns Array(Array(Nullable(String)))",
            "WITH arrayFlatten(groupArray(af)) AS afv, "
            "arrayFlatten(groupArray(astr)) AS asv, "
            "arrayFlatten(arrayFlatten(groupArray(nf))) AS nfv, "
            "arrayFlatten(arrayFlatten(groupArray(ns))) AS nsv "
            "SELECT arrayCount(x -> isNull(x), afv), "
            "arraySum(x -> ifNull(x, 0), afv), "
            "arrayCount(x -> isNull(x), asv), "
            "arrayStringConcat(arrayMap(x -> ifNull(x, ''), asv)), "
            "arrayCount(x -> isNull(x), nfv), "
            "arraySum(x -> ifNull(x, 0), nfv), "
            "arrayCount(x -> isNull(x), nsv), "
            "arrayStringConcat(arrayMap(x -> ifNull(x, ''), nsv)) FROM table",
            expect);
    }

done:
    for (size_t i = 0; i < 4; i++) chc_type_destroy(types[i], &al);
}

static void
test_write_lc_string(void)
{
    current_test = "write_lc_string";
    /* Dict ["red","green","blue"], keys [0,1,2,0] -> red, green, blue, red */
    uint8_t  keys[4]      = { 0, 1, 2, 0 };
    uint64_t dict_offs[3] = { 3, 8, 12 };
    const uint8_t dict_buf[] = "redgreenblue";
    chc_column dict = chc_build_string(dict_offs, dict_buf, 3);
    chc_column col = chc_build_lc(1, keys, 4, &dict);
    run_column_roundtrip(current_test, "s", "LowCardinality(String)", &col,
        "SELECT count(), uniqExact(s), countIf(s='red') FROM table",
        "4\t3\t2");
}

/* Exercise recursive prefixes & empty LowCardinality body */
static void
test_write_array_lc_nullable_string(void)
{
    current_test = "write_array_lc_nullable_string";
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {};

    chc_block_col bbcols[8];
    chc_block_builder bb;
    chc_block_builder_init(&bb, bbcols);
    const char *type_name = "Array(LowCardinality(Nullable(String)))";
    chc_type *t = NULL;
    if (chc_type_parse(type_name, strlen(type_name), &al, &t, &err) < 0) {
        fprintf(stderr, "%s: type: %s\n", current_test, err.msg); fail_count++; return;
    }

    /* Dict ["", "a", "b"], slot 0 = null sentinel, keys [1,0,2,0,1]:
     * row0='a',NULL,'b', row1=[], row2=NULL,'a' */
    uint64_t offsets[3]      = { 3, 3, 5 };
    uint8_t  keys[5]         = { 1, 0, 2, 0, 1 };
    uint64_t dict_offsets[3] = { 0, 1, 2 };
    const uint8_t dict_data[] = "ab";

    chc_column dict = chc_build_string(dict_offsets, dict_data, 3);
    chc_column lc   = chc_build_lc(1, keys, 5, &dict);
    chc_column a    = chc_build_array(offsets, 3, &lc);
    chc_block_builder_append(&bb, "a", 1, t, &a);

    uint64_t empty_offsets[3] = { 0, 0, 0 };
    chc_column edict = chc_build_string(NULL, NULL, 0);
    chc_column elc   = chc_build_lc(1, NULL, 0, &edict);
    chc_column e     = chc_build_array(empty_offsets, 3, &elc);
    chc_block_builder_append(&bb, "e", 1, t, &e);

    const char *expect[] = { "2\taba\t0", NULL };
    run_roundtrip(current_test, &bb,
        "a Array(LowCardinality(Nullable(String))), "
        "e Array(LowCardinality(Nullable(String)))",
        "WITH arrayFlatten(groupArray(a)) AS av "
        "SELECT arrayCount(x -> isNull(x), av), "
        "arrayStringConcat(arrayMap(x -> ifNull(x, ''), av)), "
        "sum(length(e)) FROM table",
        expect);

    chc_type_destroy(t, &al);
}

/* Exercise dict unwrapping with reader-produced Nullable shape for
 * LowCardinality(Nullable(String)) */
static void
test_write_lc_nullable_string(void)
{
    current_test = "write_lc_nullable_string";
    /* Dict ["", "a", "b"], null_map[0]=1 marks sentinel. Keys [1,0,2,0] ->
     * 'a', NULL, 'b', NULL */
    uint8_t  keys[4]       = { 1, 0, 2, 0 };
    uint64_t dict_offs[3]  = { 0, 1, 2 };
    const uint8_t dict_buf[] = "ab";
    uint8_t  dict_null[3]  = { 1, 0, 0 };

    chc_column sdict = chc_build_string(dict_offs, dict_buf, 3);
    chc_column ndict = chc_build_nullable(dict_null, &sdict);
    chc_column lc    = chc_build_lc(1, keys, 4, &ndict);
    run_column_roundtrip(current_test, "s",
        "LowCardinality(Nullable(String))", &lc,
        "SELECT countIf(s IS NULL), countIf(s IS NOT NULL) FROM table",
        "2\t2");
}

/* JSON string mode is selected by an 8-byte LE version=1 prefix on the
 * stream. Output may normalize {"a":1} to {"a":"1"} */
static void
test_write_json_string(void)
{
    current_test = "write_json_string";
    /* Rows '{}', '{"a":1}': offsets {2, 9}, bytes '{}{"a":1}' */
    uint64_t offs[2]    = { 2, 9 };
    const uint8_t buf[] = "{}{\"a\":1}";
    chc_column cj = chc_build_string(offs, buf, 2);
    /* CH 25.8+ may widen {"a":1} to {"a":"1"}; accept both */
    size_t out_len;
    char *out = capture_column_roundtrip(current_test, "j", "JSON", &cj,
        "SELECT toString(j) FROM table FORMAT TSV", &out_len);
    if (out) {
        CHECK(strstr(out, "{}") != NULL);
        CHECK(strstr(out, "{\"a\":\"1\"}") != NULL ||
              strstr(out, "{\"a\":1}") != NULL);
        free(out);
    }
}

/* Nullable(JSON) stream order: version prefix, null map, string body.
 * Server rejects insert if version lands after null map */
static void
test_write_json_nullable(void)
{
    current_test = "write_json_nullable";
    /* Rows '{"a":1}', NULL. Unlike other types, cells under NULL aren't
     * arbitrary: server parses every row's string as JSON doc */
    uint64_t offs[2]    = { 7, 9 };
    const uint8_t buf[] = "{\"a\":1}{}";
    uint8_t nulls[2]    = { 0, 1 };
    chc_column cj = chc_build_string(offs, buf, 2);
    chc_column nj = chc_build_nullable(nulls, &cj);
    run_column_roundtrip(current_test, "j", "Nullable(JSON)", &nj,
        "SELECT countIf(j IS NULL), countIf(j IS NOT NULL) FROM table",
        "1\t1");
}

/* Legacy Object uses uint8 STRING kind, modern JSON uses uint64 version */
static void
test_write_object_string_wire(void)
{
    current_test = "write_object_string_wire";
    enum { OBJECT_KIND_POS = 19 };
    static const uint8_t expected[] = {
        1, 2,                               /* columns, rows */
        1, 'o',                            /* name */
        14, 'O','b','j','e','c','t','(',   /* type */
        '\'', 'j','s','o','n', '\'',')',
        1,                                 /* STRING kind */
        2, '{','}',                        /* row 0 */
        7, '{','"','a','"',':','1','}',    /* row 1 */
    };
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {};
    chc_type *type = NULL;
    chc_block *block = NULL;
    uint64_t offs[2] = { 2, 9 };
    const uint8_t data[] = "{}{\"a\":1}";
    chc_column col = chc_build_string(offs, data, 2);
    chc_block_col cols[1];
    chc_block_builder bb;
    test_mem_sink sink = {};
    chc_io io;

    int rc = chc_type_parse("Object('json')", 14, &al, &type, &err);
    CHECK_OK(rc, err);
    chc_block_builder_init(&bb, cols);
    chc_block_builder_append(&bb, "o", 1, type, &col);
    test_mem_sink_init(&sink, &io);
    rc = chc_block_write(&io, &bb, NULL, &err);
    CHECK_OK(rc, err);
    CHECK_EQ_U64(sink.len, sizeof expected);
    CHECK(sink.len == sizeof expected &&
          memcmp(sink.data, expected, sizeof expected) == 0);

    test_mem_src src;
    chc_block_opts opts = {};
    test_mem_src_init(&src, &io, sink.data, sink.len);
    rc = test_block_read_io(&io, &al, &opts, &block, &err);
    CHECK_OK(rc, err);
    CHECK_EQ_I64(chc_type_kind(chc_block_column_type(block, 0)), CHC_OBJECT);
    const chc_column *decoded = chc_block_column(block, 0);
    CHECK_EQ_I64(chc_column_layout(decoded), CHC_COL_STRING);
    const uint64_t *decoded_offs = chc_column_string_offsets(decoded);
    const uint8_t *decoded_data = chc_column_string_data(decoded);
    CHECK_EQ_U64(decoded_offs[0], 2);
    CHECK_EQ_U64(decoded_offs[1], 9);
    CHECK(memcmp(decoded_data, data, sizeof data - 1) == 0);
    chc_block_destroy(block, &al);
    block = NULL;

    sink.data[OBJECT_KIND_POS] = 0;
    test_mem_src_init(&src, &io, sink.data, sink.len);
    chc_err_reset(&err);
    rc = test_block_read_io(&io, &al, &opts, &block, &err);
    CHECK(rc == CHC_ERR_TYPE);
    CHECK(block == NULL);
    CHECK(strstr(err.msg, "Object serialization kind 0") != NULL);

out:
    chc_block_destroy(block, &al);
    chc_type_destroy(type, &al);
    test_mem_sink_free(&sink);
}

static void
test_write_tuple(void)
{
    current_test = "write_tuple";
    uint32_t ns[3]   = { 10, 20, 30 };
    uint64_t offs[3] = { 1, 3, 6 };
    const uint8_t buf[] = "abbccc";      /* "a", "bb", "ccc" */
    chc_column c0 = chc_build_fixed(ns, sizeof ns[0], 3);
    chc_column c1 = chc_build_string(offs, buf, 3);
    chc_column *kids[2] = { &c0, &c1 };
    chc_column tup = chc_build_tuple(kids, 2);
    run_column_roundtrip(current_test, "t", "Tuple(UInt32, String)", &tup,
        "SELECT sum(t.1), arrayStringConcat(groupArray(t.2)) FROM table",
        "60\tabbccc");
}

/* Map rides an Array(Tuple(K, V)) tree, mirroring the reader's decode. */
static void
test_write_map(void)
{
    current_test = "write_map";
    /* Rows: {'a':1,'b':2}, {'c':3}. 3 pairs, map offsets {2,3}. */
    uint64_t map_offs[2] = { 2, 3 };
    uint64_t koff[3]     = { 1, 2, 3 };
    const uint8_t kbuf[] = "abc";
    int32_t vals[3]      = { 1, 2, 3 };
    chc_column keys = chc_build_string(koff, kbuf, 3);
    chc_column vs   = chc_build_fixed(vals, sizeof vals[0], 3);
    chc_column *kv[2] = { &keys, &vs };
    chc_column tup = chc_build_tuple(kv, 2);
    chc_column map = chc_build_array(map_offs, 2, &tup);
    run_column_roundtrip(current_test, "m", "Map(String, Int32)", &map,
        "SELECT sum(arraySum(mapValues(m))), "
        "arrayStringConcat(arraySort(arrayFlatten(groupArray(mapKeys(m))))) "
        "FROM table",
        "6\tabc");
}

/* Point = Tuple(Float64, Float64); exercises the geo writer (depth 0). */
static void
test_write_point(void)
{
    current_test = "write_point";
    double xs[2] = { 1.5, 2.5 };
    double ys[2] = { 10.0, 20.0 };
    chc_column cx = chc_build_fixed(xs, sizeof xs[0], 2);
    chc_column cy = chc_build_fixed(ys, sizeof ys[0], 2);
    chc_column *kids[2] = { &cx, &cy };
    chc_column pt = chc_build_tuple(kids, 2);
    run_column_roundtrip(current_test, "p", "Point", &pt,
        "SELECT sum(p.1), sum(p.2) FROM table", "4\t30");
}

/* LineString = Array(Point); exercises the geo writer one Array deep. */
static void
test_write_linestring(void)
{
    current_test = "write_linestring";
    /* Rows: [(1,2),(3,4)], [(5,6)]. Point offsets are cumulative. */
    uint64_t offs[2] = { 2, 3 };
    double xs[3] = { 1.0, 3.0, 5.0 };
    double ys[3] = { 2.0, 4.0, 6.0 };
    chc_column cx = chc_build_fixed(xs, sizeof xs[0], 3);
    chc_column cy = chc_build_fixed(ys, sizeof ys[0], 3);
    chc_column *kids[2] = { &cx, &cy };
    chc_column pt = chc_build_tuple(kids, 2);
    chc_column line = chc_build_array(offs, 2, &pt);
    run_column_roundtrip(current_test, "l", "LineString", &line,
        "SELECT sum(length(l)), sum(arraySum(arrayMap(p -> p.1 + p.2, l))) "
        "FROM table",
        "3\t21");
}

/* Builder path: differing row counts append fine; mismatch surfaces at write. */
static void
test_builder_guards(void)
{
    current_test = "builder_guards";
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {};
    chc_type *tu = NULL;
    if (chc_type_parse("UInt32", 6, &al, &tu, &err) < 0) {
        fprintf(stderr, "%s: type: %s\n", current_test, err.msg); fail_count++; return;
    }
    uint32_t a[3] = { 1, 2, 3 };
    uint32_t b[2] = { 4, 5 };
    chc_column ca = chc_build_fixed(a, sizeof a[0], 3);
    chc_column cb = chc_build_fixed(b, sizeof b[0], 2);

    chc_block_col bbcols[2];
    chc_block_builder bb;
    chc_block_builder_init(&bb, bbcols);
    chc_block_builder_append(&bb, "a", 1, tu, &ca);
    chc_block_builder_append(&bb, "b", 1, tu, &cb);
    test_mem_sink sink; chc_io mio;
    test_mem_sink_init(&sink, &mio);
    CHECK(chc_block_write(&mio, &bb, NULL, &err) == CHC_ERR_USAGE);
    test_mem_sink_free(&sink);

    chc_type_destroy(tu, &al);
}

/* Builderless path: assemble a chc_block_col array on the stack and hand it
 * straight to chc_block_write_cols. Also checks the row-count guard. */
static void
test_write_cols_direct(void)
{
    current_test = "write_cols_direct";
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {};
    chc_type *tu = NULL, *ts = NULL;
    if (chc_type_parse("UInt32", 6, &al, &tu, &err) < 0
        || chc_type_parse("String", 6, &al, &ts, &err) < 0) {
        fprintf(stderr, "%s: type: %s\n", current_test, err.msg); fail_count++; return;
    }
    uint32_t xs[3]   = { 10, 20, 30 };
    uint64_t offs[3] = { 1, 3, 6 };
    const uint8_t buf[] = "abbccc";       /* "a", "bb", "ccc" */
    chc_column cx = chc_build_fixed(xs, sizeof xs[0], 3);
    chc_column cs = chc_build_string(offs, buf, 3);
    chc_block_col cols[2] = {
        { "x", 1, tu, &cx },
        { "s", 1, ts, &cs },
    };

    /* Row-count guard: a column disagreeing with n_rows is rejected. */
    test_mem_sink sink; chc_io mio;
    test_mem_sink_init(&sink, &mio);
    chc_err berr = {};
    CHECK(chc_block_write_cols(&mio, cols, 2, 4, NULL, &berr) == CHC_ERR_USAGE);
    test_mem_sink_free(&sink);

    int wfd, rfd;
    pid_t pid = spawn_local_input(
        "SELECT sum(x), arrayStringConcat(groupArray(s)) FROM table",
        "x UInt32, s String", &wfd, &rfd);
    CHECK(pid > 0);
    if (pid > 0) {
        chc_posix_io state; chc_io io;
        chc_posix_io_init(&state, &io, wfd, NULL, NULL);
        if (chc_block_write_cols(&io, cols, 2, 3, NULL, &err) < 0) {
            fprintf(stderr, "%s: write: %s\n", current_test, err.msg); fail_count++;
        }
        close(wfd);
        size_t out_len;
        char *out = read_all(rfd, &out_len);
        close(rfd);
        int status; waitpid(pid, &status, 0);
        CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0);
        if (out) {
            CHECK(strstr(out, "60") != NULL);
            CHECK(strstr(out, "abbccc") != NULL);
            free(out);
        }
    }
    chc_type_destroy(tu, &al);
    chc_type_destroy(ts, &al);
}

int main(void)
{
    test_write_uint32();
    test_write_string();
    test_write_nullable_fixed();
    test_write_nullable_string();
    test_write_array_fixed();
    test_write_array_string();
    test_write_array_nested_fixed();
    test_write_array_nested_fixed_3d();
    test_write_array_nested_string();
    test_write_array_nullable();
    test_write_lc_string();
    test_write_lc_nullable_string();
    test_write_array_lc_nullable_string();
    test_write_json_string();
    test_write_json_nullable();
    test_write_object_string_wire();
    test_write_tuple();
    test_write_map();
    test_write_point();
    test_write_linestring();
    test_builder_guards();
    test_write_cols_direct();
    if (fail_count) {
        fprintf(stderr, "FAIL: %d check(s)\n", fail_count);
        return 1;
    }
    fprintf(stderr, "ok\n");
    return 0;
}
