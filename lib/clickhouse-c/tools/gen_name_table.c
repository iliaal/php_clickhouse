/* gen_name_table.c -- offline generator for chc__name_to_kind's perfect
 * hash table. Compiled & run by tools/regen_name_table.sh, never linked
 * into the library.
 *
 * Output: the table's #defines, storage & row array, intended to be
 * spliced into clickhouse.h between the
 *   AUTO-GENERATED-NAME-TABLE-{BEGIN,END}
 * sentinel comments.
 *
 * Hash function and key formula match the runtime in clickhouse.h:
 *   h = chc__city_hash_len_0_to_16(s, min(n, 16)) ^ (uint64_t) n;
 *   bucket = h & (M - 1);
 *
 * Strategy: try M = 64, 128, 256, ... until no two entries collide
 * on the same bucket. Emit the smallest M that works.
 *
 * Layout: names pack into one blob, rows hold {offset, length, kind,
 * unit}, buckets hold a 1-based uint8_t row index (0 = empty). Keeps the
 * M-sized array 1 byte per bucket & leaves the table free of pointers,
 * so it needs no load-time relocation.
 */

#define CHC_IMPLEMENTATION
#include "../clickhouse.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    const char *name;
    const char *kind;       /* enum spelling, e.g. "CHC_INT8" */
} row;

static const row rows[] = {
    {"Int8",     "CHC_INT8"},     {"Int16",     "CHC_INT16"},
    {"Int32",    "CHC_INT32"},    {"Int64",     "CHC_INT64"},
    {"Int128",   "CHC_INT128"},   {"Int256",    "CHC_INT256"},
    {"UInt8",    "CHC_UINT8"},    {"UInt16",    "CHC_UINT16"},
    {"UInt32",   "CHC_UINT32"},   {"UInt64",    "CHC_UINT64"},
    {"UInt128",  "CHC_UINT128"},  {"UInt256",   "CHC_UINT256"},
    {"Float32",  "CHC_FLOAT32"},  {"Float64",   "CHC_FLOAT64"},
    {"BFloat16", "CHC_BFLOAT16"}, {"Bool",      "CHC_BOOL"},
    {"String",   "CHC_STRING"},   {"FixedString", "CHC_FIXED_STRING"},
    {"Date",     "CHC_DATE"},     {"Date32",    "CHC_DATE32"},
    {"DateTime", "CHC_DATETIME"}, {"DateTime64","CHC_DATETIME64"},
    {"Time",     "CHC_TIME"},     {"Time64",    "CHC_TIME64"},
    {"UUID",     "CHC_UUID"},     {"IPv4",      "CHC_IPV4"},
    {"IPv6",     "CHC_IPV6"},
    {"Enum8",    "CHC_ENUM8"},    {"Enum16",    "CHC_ENUM16"},
    {"Decimal32",  "CHC_DECIMAL32"},  {"Decimal64",  "CHC_DECIMAL64"},
    {"Decimal128", "CHC_DECIMAL128"}, {"Decimal256", "CHC_DECIMAL256"},
    /* "Decimal" stays out -- handled inline by parser (decimal_alias). */
    {"Nullable", "CHC_NULLABLE"}, {"Array",     "CHC_ARRAY"},
    {"Tuple",    "CHC_TUPLE"},    {"Map",       "CHC_MAP"},
    {"Nested",   "CHC_NESTED"},
    {"LowCardinality", "CHC_LOW_CARDINALITY"},
    {"Nothing",  "CHC_NOTHING"},  {"Void",      "CHC_VOID"},
    {"Point",    "CHC_POINT"},    {"Ring",      "CHC_RING"},
    {"Polygon",  "CHC_POLYGON"},  {"MultiPolygon", "CHC_MULTI_POLYGON"},
    {"LineString", "CHC_LINE_STRING"},
    {"MultiLineString", "CHC_MULTI_LINE_STRING"},
    {"SimpleAggregateFunction", "CHC_SIMPLE_AGGREGATE_FUNCTION"},
    {"AggregateFunction",       "CHC_AGGREGATE_FUNCTION"},
    {"Variant",  "CHC_VARIANT"},  {"Dynamic",   "CHC_DYNAMIC"},
    {"JSON",     "CHC_JSON"},     {"Object",    "CHC_OBJECT"},
    {"QBit",     "CHC_QBIT"},
    {"IntervalNanosecond",  "CHC_INTERVAL"},
    {"IntervalMicrosecond", "CHC_INTERVAL"},
    {"IntervalMillisecond", "CHC_INTERVAL"},
    {"IntervalSecond",      "CHC_INTERVAL"},
    {"IntervalMinute",      "CHC_INTERVAL"},
    {"IntervalHour",        "CHC_INTERVAL"},
    {"IntervalDay",         "CHC_INTERVAL"},
    {"IntervalWeek",        "CHC_INTERVAL"},
    {"IntervalMonth",       "CHC_INTERVAL"},
    {"IntervalQuarter",     "CHC_INTERVAL"},
    {"IntervalYear",        "CHC_INTERVAL"},
};

/* Interval rows carry their unit, IntervalDay giving CHC_INTERVAL_DAY. */
static const char *unit_of(const char *name)
{
    static char buf[64];
    if (strncmp(name, "Interval", 8) != 0) return "CHC_INTERVAL_NONE";
    int n = snprintf(buf, sizeof buf, "CHC_INTERVAL_%s", name + 8);
    for (int i = 13; i < n; i++) buf[i] = (char) toupper((unsigned char) buf[i]);
    return buf;
}

/* Rows address the blob by offset+length, so a name already spelled out
 * inside another costs nothing. Placing longest first puts Int8 inside
 * UInt8, String inside FixedString, AggregateFunction inside
 * SimpleAggregateFunction. */
static char blob[4096];
static size_t blob_len;

static size_t blob_place(const char *name)
{
    size_t nlen = strlen(name);
    blob[blob_len] = '\0';
    const char *hit = strstr(blob, name);
    if (hit) return (size_t) (hit - blob);

    if (blob_len + nlen >= sizeof blob) {
        fprintf(stderr, "gen_name_table: blob overflow\n");
        exit(1);
    }
    memcpy(blob + blob_len, name, nlen);
    blob_len += nlen;
    return blob_len - nlen;
}

static int by_len_desc(const void *a, const void *b)
{
    int ia = *(const int *) a, ib = *(const int *) b;
    size_t la = strlen(rows[ia].name), lb = strlen(rows[ib].name);
    if (la != lb) return la < lb ? 1 : -1;
    return ia < ib ? -1 : 1;
}

static uint64_t key_of(const char *s, size_t n, uint64_t seed)
{
    size_t h_len = n < 16 ? n : 16;
    return chc__city_hash_len_16(
        chc__city_hash_len_0_to_16(s, h_len) + (uint64_t) n, seed);
}

static int try_M_seed(size_t M, uint64_t seed, size_t n_rows)
{
    unsigned char *seen = calloc(M, 1);
    int ok = 1;
    for (size_t i = 0; i < n_rows; i++) {
        size_t nlen = strlen(rows[i].name);
        uint64_t h = key_of(rows[i].name, nlen, seed);
        size_t b = (size_t)(h & (M - 1));
        if (seen[b]) { ok = 0; break; }
        seen[b] = 1;
    }
    free(seen);
    return ok;
}

int main(void)
{
    size_t n_rows = sizeof rows / sizeof rows[0];

    size_t M = 0;
    uint64_t seed = 0;
    for (size_t cand = 64; cand <= 4096; cand <<= 1) {
        int found = 0;
        for (uint64_t s = 0; s < 200000; s++) {
            if (try_M_seed(cand, s, n_rows)) { seed = s; found = 1; break; }
        }
        if (found) { M = cand; break; }
    }

    if (!M) {
        fprintf(stderr, "gen_name_table: no collision-free (M,seed) found\n");
        return 1;
    }

    fprintf(stderr, "gen_name_table: %zu entries, M=%zu, seed=%llu\n",
            n_rows, M, (unsigned long long) seed);

    if (n_rows > 254) {
        fprintf(stderr, "gen_name_table: too many rows for uint8_t index\n");
        return 1;
    }

    size_t maxlen = 0;
    int *order = malloc(n_rows * sizeof *order);
    size_t *off = malloc(n_rows * sizeof *off);
    for (size_t i = 0; i < n_rows; i++) {
        size_t nlen = strlen(rows[i].name);
        if (nlen > maxlen) maxlen = nlen;
        order[i] = (int) i;
    }
    qsort(order, n_rows, sizeof *order, by_len_desc);
    for (size_t k = 0; k < n_rows; k++)
        off[order[k]] = blob_place(rows[order[k]].name);
    for (size_t i = 0; i < n_rows; i++) {
        size_t nlen = strlen(rows[i].name);
        if (off[i] + nlen > blob_len
            || memcmp(blob + off[i], rows[i].name, nlen) != 0) {
            fprintf(stderr, "gen_name_table: %s misplaced in blob\n",
                    rows[i].name);
            return 1;
        }
    }
    if (blob_len > 65535) {
        fprintf(stderr, "gen_name_table: blob too large for uint16_t off\n");
        return 1;
    }
    fprintf(stderr, "gen_name_table: blob %zu bytes\n", blob_len);

    printf("#define CHC__NAME_TABLE_M %zuu\n", M);
    printf("#define CHC__NAME_TABLE_SEED %lluull\n", (unsigned long long) seed);
    printf("#define CHC__NAME_TABLE_MAXLEN %zuu\n", maxlen);
    printf("struct chc__name_row { uint16_t off; uint8_t len; uint8_t kind;"
           " uint8_t unit; };\n");

    printf("static const char chc__name_blob[] =\n");
    for (size_t i = 0; i < blob_len; i += 64) {
        size_t k = blob_len - i < 64 ? blob_len - i : 64;
        printf("    \"%.*s\"%s\n", (int) k, blob + i,
               i + k >= blob_len ? ";" : "");
    }

    printf("static const struct chc__name_row chc__name_rows[] = {\n");
    for (size_t i = 0; i < n_rows; i++)
        printf("    {%3zu, %2zu, %s, %s},\n", off[i], strlen(rows[i].name),
               rows[i].kind, unit_of(rows[i].name));
    printf("};\n");
    free(order);
    free(off);

    int *slot = malloc(M * sizeof *slot);
    for (size_t i = 0; i < M; i++) slot[i] = -1;
    for (size_t i = 0; i < n_rows; i++) {
        size_t nlen = strlen(rows[i].name);
        uint64_t h = key_of(rows[i].name, nlen, seed);
        slot[h & (M - 1)] = (int) i;
    }
    printf("static const uint8_t chc__name_slot[CHC__NAME_TABLE_M] = {\n   ");
    for (size_t b = 0, col = 0; b < M; b++) {
        if (slot[b] < 0) continue;
        if (col == 6) { printf("\n   "); col = 0; }
        printf(" [%3zu] = %2d,", b, slot[b] + 1);
        col++;
    }
    printf("\n};\n");
    free(slot);
    return 0;
}
