/*
 * test_type_parse.c -- chc_type_parse rejection paths, accessors queried on
 * types that lack the field, and an allocation-failure sweep over the parser.
 * Pure library, no io & no server.
 */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHC_PROVIDE_STDLIB_ALLOC
#define CHC_IMPLEMENTATION
#include "clickhouse.h"

static int fail_count = 0;
static const char *current_test = "";

#include "test_common.h"

static void
expect_type_error(const char *src)
{
    chc_alloc al = chc_alloc_stdlib();
    chc_type *t = NULL;
    chc_err err = {};
    int rc = chc_type_parse(src, strlen(src), &al, &t, &err);
    if (rc != CHC_ERR_TYPE || t) {
        fprintf(stderr, "%s: FAIL: \"%s\" rc=%d t=%p (want CHC_ERR_TYPE, NULL)\n",
                current_test, src, rc, (void *) t);
        fail_count++;
        chc_type_destroy(t, &al);
        return;
    }
    CHECK(err.msg[0] != '\0');
}

static chc_type *
parse_ok(const chc_alloc *al, const char *src)
{
    chc_type *t = NULL;
    chc_err err = {};
    int rc = chc_type_parse(src, strlen(src), al, &t, &err);
    if (rc != CHC_OK || !t) {
        fprintf(stderr, "%s: FAIL: \"%s\" rc=%d err='%s'\n",
                current_test, src, rc, err.msg);
        fail_count++;
        return NULL;
    }
    return t;
}

static void
test_rejects(void)
{
    current_test = "rejects";
    static const char *bad[] = {
        "",                             /* no head token */
        "@",                            /* invalid character */
        "`quoted`",                     /* quoted head is never a type name */
        "NoSuchType",
        "Enum8(x = 1)",                 /* unquoted enum member */
        "Enum8('a' 1)",                 /* missing '=' */
        "Enum8('a' = b)",               /* non-numeric value */
        "Enum8('a' = 999)",             /* outside Int8 */
        "FixedString(x)",
        "FixedString(0)",
        "Decimal(x, 2)",
        "Decimal(10 2)",
        "Decimal(10, x)",
        "Decimal(0, 0)",
        "Decimal32(x)",
        "Decimal32(99)",
        "DateTime64(x)",
        "DateTime64(99)",
        "DateTime64(3, x)",
        "DateTime(x)",
        "Object(x)",
        "QBit(Int32, 16)",              /* element must be a float kind */
        "QBit(Float32 16)",             /* missing ',' */
        "QBit(Float32, x)",
        "QBit(Float32, 0)",
        "Array(NoSuchType)",            /* child parse fails, no field names */
        "Tuple(Int32, NoSuchType)",     /* child parse fails, field names live */
        "Array(Int32 Int32)",           /* neither ',' nor ')' */
        "Tuple(x Int32 5)",             /* same, with field names live */
        "FixedString(3",                /* unterminated parameter list */
        "SimpleAggregateFunction()",    /* aggregate function name missing */
        "SimpleAggregateFunction('sum', UInt64)",   /* quoted function name */
        "AggregateFunction(`sum`, Int64)",          /* quoted function name */
        "AggregateFunction(sum)",                   /* no argument type */
        "AggregateFunction(2 sum, Int64)",          /* missing comma after version */
        "AggregateFunction(quantile(0.5, Float64",  /* unclosed parameter list */
        "AggregateFunction(sequenceMatch('(?1), DateTime)",   /* unclosed quote */
        "AggregateFunction(sum, NoSuchType)",       /* argument type unknown */
        "JSON(`a.b` UInt32",                        /* unclosed parameter list */
        "Int32 Int32",                  /* trailing tokens */
        "Enum8('unterminated = 1)",     /* unterminated quote */
    };
    for (size_t i = 0; i < sizeof bad / sizeof *bad; i++)
        expect_type_error(bad[i]);
}

static void
test_depth_limit(void)
{
    current_test = "depth_limit";
    size_t depth = CHC_MAX_TYPE_DEPTH + 2;
    char *src = malloc(depth * 7 + 8);
    CHECK(src != NULL);
    if (!src) return;

    char *p = src;
    for (size_t i = 0; i < depth; i++) p += sprintf(p, "Array(");
    p += sprintf(p, "Int32");
    for (size_t i = 0; i < depth; i++) *p++ = ')';
    *p = '\0';

    expect_type_error(src);
    free(src);
}

static void
test_accessors(void)
{
    current_test = "accessors";
    chc_alloc al = chc_alloc_stdlib();
    chc_type *fs = parse_ok(&al, "FixedString(5)");
    chc_type *i32 = parse_ok(&al, "Int32");
    chc_type *en = parse_ok(&al, "Enum8('a' = 1, 'b' = 2)");

    CHECK_EQ_I64(chc_type_fixed_size(fs), 5);
    CHECK_EQ_I64(chc_type_fixed_size(i32), 0);
    CHECK_EQ_I64(chc_type_fixed_size(NULL), 0);

    CHECK_EQ_U64(chc_type_enum_count(en), 2);
    CHECK_EQ_U64(chc_type_enum_count(i32), 0);

    const char *name = "x";
    size_t name_len = 99;
    int64_t value = 99;
    chc_type_enum_at(en, 1, &name, &name_len, &value);
    CHECK_STR_EQ(name, name_len, "b");
    CHECK_EQ_I64(value, 2);

    chc_type_enum_at(en, 2, &name, &name_len, &value);   /* past the last item */
    CHECK(name == NULL);
    CHECK_EQ_U64(name_len, 0);
    CHECK_EQ_I64(value, 0);
    chc_type_enum_at(i32, 0, NULL, NULL, NULL);          /* not an enum, no outputs */

    chc_type *agg  = parse_ok(&al, "SimpleAggregateFunction(sum, UInt64)");
    chc_type *bare = parse_ok(&al, "AggregateFunction");
    size_t fn_len = 99;
    const char *fn = chc_type_agg_function(agg, &fn_len);
    CHECK(fn != NULL);
    if (fn) CHECK_STR_EQ(fn, fn_len, "sum");
    CHECK(chc_type_agg_function(bare, &fn_len) == NULL);
    CHECK_EQ_U64(fn_len, 0);
    CHECK(chc_type_agg_function(i32, NULL) == NULL);
    CHECK(chc_type_agg_function(NULL, &fn_len) == NULL);

    chc_type_destroy(fs, &al);
    chc_type_destroy(i32, &al);
    chc_type_destroy(en, &al);
    chc_type_destroy(agg, &al);
    chc_type_destroy(bare, &al);
}

static void
test_decimal_precision(void)
{
    current_test = "decimal_precision";
    chc_alloc al = chc_alloc_stdlib();
    static const struct { const char *src; int precision, scale; } cases[] = {
        { "Decimal32(2)",    9,  2 },
        { "Decimal64(2)",   18,  2 },
        { "Decimal128(2)",  38,  2 },
        { "Decimal256(2)",  76,  2 },
        { "Decimal(20, 4)", 20,  4 },
        { "Decimal(40, 4)", 40,  4 },
        { "Decimal",        38,  0 },   /* bare alias widens to Decimal128 */
    };
    for (size_t i = 0; i < sizeof cases / sizeof *cases; i++) {
        chc_type *t = parse_ok(&al, cases[i].src);
        if (!t) continue;
        CHECK_EQ_I64(chc_type_decimal_precision(t), cases[i].precision);
        CHECK_EQ_I64(chc_type_decimal_scale(t), cases[i].scale);
        chc_type_destroy(t, &al);
    }
    CHECK_EQ_I64(chc_type_decimal_precision(NULL), 0);

    chc_type *i32 = parse_ok(&al, "Int32");                  /* not a decimal */
    CHECK_EQ_I64(chc_type_decimal_precision(i32), 0);
    CHECK_EQ_I64(chc_type_decimal_scale(i32), 0);
    chc_type_destroy(i32, &al);
}

static void
test_format_unnamed(void)
{
    current_test = "format_unnamed";
    chc_type t = {};                    /* only the parser fills in name */
    char buf[8] = "junk";
    CHECK_EQ_U64(chc_type_format(&t, buf, sizeof buf), 0);
    CHECK_EQ_U64(chc_type_format(NULL, buf, sizeof buf), 0);
}

static void
test_datetime_timezone(void)
{
    current_test = "datetime_timezone";
    chc_alloc al = chc_alloc_stdlib();
    static const char *srcs[] = { "DateTime('UTC')", "DateTime64(6, 'UTC')" };
    for (size_t i = 0; i < sizeof srcs / sizeof *srcs; i++) {
        chc_type *t = parse_ok(&al, srcs[i]);
        if (!t) continue;
        size_t tz_len = 0;
        const char *tz = chc_type_timezone(t, &tz_len);
        CHECK(tz != NULL);
        if (tz) CHECK_STR_EQ(tz, tz_len, "UTC");
        chc_type_destroy(t, &al);
    }
}

static void
test_aggregate_function(void)
{
    current_test = "aggregate_function";
    chc_alloc al = chc_alloc_stdlib();
    static const struct {
        const char *src;
        chc_kind    kind;
        size_t      n_children;
        const char *func;
        const char *storage;
    } cases[] = {
        { "SimpleAggregateFunction(sum, UInt64)",
          CHC_SIMPLE_AGGREGATE_FUNCTION, 1, "sum", "UInt64" },
        { "SimpleAggregateFunction(maxMap, Tuple(Array(UInt8), Array(UInt64)))",
          CHC_SIMPLE_AGGREGATE_FUNCTION, 1, "maxMap",
          "Tuple(Array(UInt8), Array(UInt64))" },
        { "AggregateFunction(sum, Int64)",
          CHC_AGGREGATE_FUNCTION, 1, "sum", "Int64" },
        { "AggregateFunction(uniqCombined(12), String)",
          CHC_AGGREGATE_FUNCTION, 1, "uniqCombined", "String" },
        { "AggregateFunction(quantiles((0.5)), Float64)",   /* nested parentheses */
          CHC_AGGREGATE_FUNCTION, 1, "quantiles", "Float64" },
        { "AggregateFunction(sequenceMatch('(?1)(?2)'), DateTime, UInt8, UInt8)",
          CHC_AGGREGATE_FUNCTION, 3, "sequenceMatch", "DateTime" },
        { "AggregateFunction(2, sumMap, UInt8, UInt8)",
          CHC_AGGREGATE_FUNCTION, 2, "sumMap", "UInt8" },
    };
    for (size_t i = 0; i < sizeof cases / sizeof *cases; i++) {
        chc_type *t = parse_ok(&al, cases[i].src);
        if (!t) continue;
        CHECK_EQ_I64(chc_type_kind(t), cases[i].kind);
        CHECK_EQ_U64(chc_type_n_children(t), cases[i].n_children);
        size_t len = 0;
        const char *nm = chc_type_agg_function(t, &len);
        CHECK_STR_EQ(nm, len, cases[i].func);
        nm = chc_type_name(chc_type_child(t, 0), &len);
        CHECK_STR_EQ(nm, len, cases[i].storage);
        nm = chc_type_name(t, &len);
        CHECK_STR_EQ(nm, len, cases[i].src);
        chc_type_destroy(t, &al);
    }
}

static void
test_nested_json_params(void)
{
    current_test = "nested_json_params";
    chc_alloc al = chc_alloc_stdlib();
    chc_type *n = parse_ok(&al, "Nested(a UInt32, b Array(String))");
    if (n) {
        CHECK_EQ_I64(chc_type_kind(n), CHC_NESTED);
        CHECK_EQ_U64(chc_type_n_children(n), 2);
        CHECK_EQ_I64(chc_type_kind(chc_type_child(n, 1)), CHC_ARRAY);
        static const char *labels[] = { "a", "b" };
        for (size_t i = 0; i < 2; i++) {
            size_t len = 0;
            const char *f = chc_type_tuple_field_name(n, i, &len);
            CHECK(f != NULL);
            if (f) CHECK_STR_EQ(f, len, labels[i]);
        }
        chc_type_destroy(n, &al);
    }

    static const char *jsons[] = {
        "JSON",
        "JSON(`a.b` UInt32, SKIP `a.e`)",
        "JSON(max_dynamic_paths=16, SKIP REGEXP 'a(b)c', a.b UInt32)",
    };
    for (size_t i = 0; i < sizeof jsons / sizeof *jsons; i++) {
        chc_type *j = parse_ok(&al, jsons[i]);
        if (!j) continue;
        CHECK_EQ_I64(chc_type_kind(j), CHC_JSON);
        CHECK_EQ_U64(chc_type_n_children(j), 0);
        size_t len = 0;
        const char *nm = chc_type_name(j, &len);
        CHECK_STR_EQ(nm, len, jsons[i]);
        chc_type_destroy(j, &al);
    }
}

/* Re-parse each type once per allocation, failing that one allocation. Every
 * unwind must report OOM & hand every byte back. */
static void
test_oom_sweep(void)
{
    current_test = "oom_sweep";
    static const char *srcs[] = {
        "Int32",
        "Array(Nullable(Int32))",
        "Tuple(a Int32, b String)",
        "Tuple(`q u o t e d` Int32, Int32)",
        "Enum16('a' = 1, 'b' = 2)",
        "DateTime('UTC')",
        "DateTime64(3, 'UTC')",
        "Map(String, Array(LowCardinality(String)))",
        "QBit(Float64, 16)",
        "Nested(a Int32, b String)",
        "AggregateFunction(sumMapFiltered([1, 2]), Array(UInt8), Array(UInt8))",
    };
    for (size_t i = 0; i < sizeof srcs / sizeof *srcs; i++) {
        for (size_t fail_at = 0; ; fail_at++) {
            test_fail_alloc fa;
            chc_alloc al = test_fail_alloc_init(&fa, fail_at);
            chc_type *t = NULL;
            chc_err err = {};
            int rc = chc_type_parse(srcs[i], strlen(srcs[i]), &al, &t, &err);
            if (rc == CHC_OK) {
                chc_type_destroy(t, &al);
                CHECK_EQ_U64(fa.live, 0);
                CHECK_EQ_U64(fa.live_bytes, 0);
                if (fail_at >= fa.calls) break;      /* swept past the last alloc */
                continue;
            }
            if (rc != CHC_ERR_OOM) {
                fprintf(stderr, "%s: FAIL: \"%s\" fail_at=%zu rc=%d err='%s'\n",
                        current_test, srcs[i], fail_at, rc, err.msg);
                fail_count++;
                break;
            }
            CHECK(t == NULL);
            if (fa.live || fa.live_bytes) {
                fprintf(stderr, "%s: FAIL: \"%s\" fail_at=%zu leaked %zu blocks %zu bytes\n",
                        current_test, srcs[i], fail_at, fa.live, fa.live_bytes);
                fail_count++;
            }
        }
    }
}

int
main(void)
{
    test_rejects();
    test_depth_limit();
    test_accessors();
    test_decimal_precision();
    test_format_unnamed();
    test_datetime_timezone();
    test_aggregate_function();
    test_nested_json_params();
    test_oom_sweep();

    if (fail_count) {
        fprintf(stderr, "%d failure(s)\n", fail_count);
        return 1;
    }
    printf("all type_parse tests passed\n");
    return 0;
}
