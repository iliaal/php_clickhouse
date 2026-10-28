/*
 * test_block_compare.h -- recursive block/column comparators
 */

#ifndef CLICKHOUSE_TEST_BLOCK_COMPARE_H
#define CLICKHOUSE_TEST_BLOCK_COMPARE_H

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

static int test_col_eq(const chc_column *a, const chc_column *b);

static int
test_type_eq(const chc_type *a, const chc_type *b)
{
    if (!a || !b) return a == b;
    size_t na = chc_type_format(a, NULL, 0);
    size_t nb = chc_type_format(b, NULL, 0);
    if (na != nb) return 0;

    char *sa = malloc(na + 1);
    char *sb = malloc(nb + 1);
    if (!sa || !sb) {
        free(sa);
        free(sb);
        return 0;
    }
    chc_type_format(a, sa, na + 1);
    chc_type_format(b, sb, nb + 1);
    int eq = memcmp(sa, sb, na) == 0;
    free(sa);
    free(sb);
    return eq;
}

CHC_MAYBE_UNUSED static int
test_col_eq(const chc_column *a, const chc_column *b)
{
    if (!a || !b) return a == b;
    if (chc_column_layout(a) != chc_column_layout(b)) return 0;
    size_t n = chc_column_n_rows(a);
    if (n != chc_column_n_rows(b)) return 0;

    switch (chc_column_layout(a)) {
    case CHC_COL_FIXED: {
        size_t ea, eb;
        const void *da = chc_column_fixed_data(a, &ea);
        const void *db = chc_column_fixed_data(b, &eb);
        if (ea != eb) return 0;
        if (n == 0 || ea == 0) return 1;
        return memcmp(da, db, n * ea) == 0;
    }
    case CHC_COL_STRING: {
        const uint64_t *oa = chc_column_string_offsets(a);
        const uint64_t *ob = chc_column_string_offsets(b);
        if (n == 0) return 1;
        if (memcmp(oa, ob, n * sizeof *oa) != 0) return 0;
        uint64_t bytes = oa[n - 1];
        if (bytes == 0) return 1;
        return memcmp(chc_column_string_data(a),
                      chc_column_string_data(b), bytes) == 0;
    }
    case CHC_COL_NULLABLE: {
        if (n && memcmp(chc_column_null_map(a), chc_column_null_map(b), n) != 0)
            return 0;
        return test_col_eq(chc_column_nullable_inner(a),
                           chc_column_nullable_inner(b));
    }
    case CHC_COL_ARRAY: {
        const uint64_t *oa = chc_column_array_offsets(a);
        const uint64_t *ob = chc_column_array_offsets(b);
        if (n && memcmp(oa, ob, n * sizeof *oa) != 0) return 0;
        return test_col_eq(chc_column_array_values(a),
                           chc_column_array_values(b));
    }
    case CHC_COL_TUPLE: {
        size_t arity = chc_column_tuple_arity(a);
        if (arity != chc_column_tuple_arity(b)) return 0;
        for (size_t i = 0; i < arity; i++) {
            if (!test_col_eq(chc_column_tuple_child(a, i),
                             chc_column_tuple_child(b, i)))
                return 0;
        }
        return 1;
    }
    case CHC_COL_LOW_CARDINALITY: {
        int ks = chc_column_lc_key_size(a);
        if (ks != chc_column_lc_key_size(b)) return 0;
        if (n && memcmp(chc_column_lc_keys(a), chc_column_lc_keys(b),
                        n * (size_t) ks) != 0)
            return 0;
        return test_col_eq(chc_column_lc_dict(a), chc_column_lc_dict(b));
    }
    case CHC_COL_NOTHING:
        return 1;
    }
    return 0;
}

CHC_MAYBE_UNUSED static int
test_block_eq(const chc_block *a, const chc_block *b)
{
    if (!a || !b) return a == b;
    if (chc_block_n_rows(a) != chc_block_n_rows(b)) return 0;
    if (chc_block_n_columns(a) != chc_block_n_columns(b)) return 0;
    if (chc_block_is_overflows(a) != chc_block_is_overflows(b)) return 0;
    if (chc_block_bucket_num(a) != chc_block_bucket_num(b)) return 0;
    for (size_t i = 0; i < chc_block_n_columns(a); i++) {
        size_t la, lb;
        const char *na = chc_block_column_name(a, i, &la);
        const char *nb = chc_block_column_name(b, i, &lb);
        if (la != lb || (la && memcmp(na, nb, la) != 0)) return 0;
        if (!test_type_eq(chc_block_column_type(a, i),
                          chc_block_column_type(b, i)))
            return 0;
        if (!test_col_eq(chc_block_column(a, i), chc_block_column(b, i)))
            return 0;
    }
    return 1;
}

#endif
