/* Compatibility contracts used by the PHP adapter; wire fixtures are unit tests. */
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHC_PROVIDE_STDLIB_ALLOC
#define CHC_NO_LZ4
#define CHC_NO_ZSTD
#define CHC_PHP_COMPAT 1
#define CHC_MAX_HELLO_STRING_SIZE 0xFFFFFFu
#define CHC_MAX_PROTOCOL_STRING_SIZE 0xFFFFFFu
#define CHC_IMPLEMENTATION
#include "clickhouse-async.h"

static int fail_count;
static const char *current_test;
#include "test_common.h"

typedef struct {
    test_mem_src input;
    test_mem_sink output;
} duplex;

static int
read_duplex(void *ud, void *buf, size_t len, size_t *n, chc_err *err)
{
    return test_mem_read(&((duplex *) ud)->input, buf, len, n, err);
}

static int
write_duplex(void *ud, const void *buf, size_t len, chc_err *err)
{
    return test_mem_sink_write(&((duplex *) ud)->output, buf, len, err);
}

static void
expect_number(chc_in *in, uint64_t value)
{
    uint64_t actual = 0;
    chc_err err = {0};
    CHECK(chc__read_varuint(in, &actual, &err) == CHC_OK);
    CHECK_EQ_U64(actual, value);
}

static void
expect_string(chc_in *in, const char *value, size_t len)
{
    char *actual = NULL;
    size_t actual_len = 0;
    chc_err err = {0};
    CHECK(chc__read_string(in, &actual, &actual_len, &err) == CHC_OK);
    CHECK(actual_len == len);
    if (actual && actual_len == len) CHECK(memcmp(actual, value, len) == 0);
    in->al->free(in->al->ud, actual, actual_len + 1);
}

static void
test_hello(uint64_t server_revision)
{
    current_test = "compat_hello";
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {0};
    chc_client *client = NULL;
    chc_async_client *async = NULL;
    test_mem_sink server = {0};
    chc_io server_io = {.ud = &server, .write = test_mem_sink_write};
    char name[5000], timezone[100], display_name[250];
    memset(name, 'n', sizeof name);
    memset(timezone, 't', sizeof timezone);
    memset(display_name, 'd', sizeof display_name);
    uint64_t revision = server_revision < 54459 ? server_revision : 54459;
    CHECK(chc__write_varuint(&server_io, 0, &err) == CHC_OK);
    CHECK(chc__write_string(&server_io, name, sizeof name, &err) == CHC_OK);
    CHECK(chc__write_varuint(&server_io, 1, &err) == CHC_OK);
    CHECK(chc__write_varuint(&server_io, 2, &err) == CHC_OK);
    CHECK(chc__write_varuint(&server_io, server_revision, &err) == CHC_OK);
    if (revision >= 54058)
        CHECK(chc__write_string(&server_io, timezone, sizeof timezone, &err) == CHC_OK);
    if (revision >= 54372)
        CHECK(chc__write_string(&server_io, display_name, sizeof display_name, &err) == CHC_OK);
    if (revision >= 54401)
        CHECK(chc__write_varuint(&server_io, 3, &err) == CHC_OK);

    const char identity[] = {'c', '\0', 'n'};
    const char database[] = {'d', '\0', 'b'};
    const char user[] = {'u', '\0', 's'};
    const char password[] = {'p', '\0', 'w'};
    chc_client_opts opts = {
        .protocol_revision = 54459, .allow_legacy_server = true, .skip_init_ping = true,
        .strings_have_lengths = true,
        .client_name = identity, .client_name_len = sizeof identity,
        .database = database, .database_len = sizeof database,
        .user = user, .user_len = sizeof user,
        .password = password, .password_len = sizeof password,
        .client_version_major = 4, .client_version_minor = 5,
    };
    duplex stream = {.input = {.data = server.data, .len = server.len}};
    chc_io io = {.ud = &stream, .read = read_duplex, .write = write_duplex};
    int rc = chc_client_init(&client, &opts, &al, &io, NULL, &err);
    CHECK_OK(rc, err);
    const chc_server_info *info = chc_client_server_info(client);
    CHECK_EQ_U64(info->server_revision, server_revision);
    CHECK_EQ_U64(info->revision, revision);
    CHECK(info->full_name_len == sizeof name);
    CHECK(memcmp(info->full_name, name, sizeof name) == 0);
    CHECK(info->full_timezone_len == (revision >= 54058 ? sizeof timezone : 0));
    CHECK(info->full_display_name_len == (revision >= 54372 ? sizeof display_name : 0));
    CHECK_EQ_U64(info->version_patch, revision >= 54401 ? 3 : 0);

    test_mem_src hello_src;
    chc_io hello_io;
    test_mem_src_init(&hello_src, &hello_io, stream.output.data, stream.output.len);
    chc_in hello;
    rc = chc_in_init(&hello, &hello_io, &al, 0, &err);
    CHECK_OK(rc, err);
    expect_number(&hello, 0);
    expect_string(&hello, identity, sizeof identity);
    expect_number(&hello, 4);
    expect_number(&hello, 5);
    expect_number(&hello, 54459);
    expect_string(&hello, database, sizeof database);
    expect_string(&hello, user, sizeof user);
    expect_string(&hello, password, sizeof password);
    if (revision >= 54458) expect_string(&hello, "", 0);
    uint64_t extra;
    CHECK(chc__read_varuint(&hello, &extra, &err) == CHC_ERR_EOF);
    chc_in_free(&hello);

    rc = chc_async_client_init(&async, &opts, &al, &err);
    CHECK_OK(rc, err);
    CHECK(chc_async_handshake(async, NULL, &err) == CHC_WOULD_BLOCK);
    for (size_t i = 0; i < server.len; ++i) {
        CHECK(chc_async_submit(async, server.data + i, 1, &err) == CHC_OK);
        rc = chc_async_handshake(async, NULL, &err);
        CHECK(rc == (i + 1 == server.len ? CHC_OK : CHC_WOULD_BLOCK));
    }
    const uint8_t *output;
    size_t output_len;
    chc_async_pending_out(async, &output, &output_len);
    CHECK(output_len == stream.output.len);
    if (output_len == stream.output.len) CHECK(memcmp(output, stream.output.data, output_len) == 0);
    CHECK(chc_async_server_info(async)->server_revision == server_revision);
out:
    chc_client_close(client);
    chc_async_client_free(async);
    test_mem_sink_free(&stream.output);
    test_mem_sink_free(&server);
}

static void
test_legacy_query_and_progress(void)
{
    current_test = "legacy_query_and_progress";
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {0};
    test_mem_sink output = {0};
    chc_io io = {.ud = &output, .write = test_mem_sink_write};
    chc_client client = {.al = &al, .io = &io, .server = {.revision = 732}};
    CHECK(chc_client_send_query(&client, "SELECT 1", 8, "q", 1, &err) == CHC_OK);
    const uint8_t expected[] = {1, 1, 'q', 0, 2, 0, 8, 'S', 'E', 'L', 'E', 'C', 'T', ' ', '1', 2, 0, 0};
    CHECK(output.len == sizeof expected);
    if (output.len == sizeof expected) CHECK(memcmp(output.data, expected, sizeof expected) == 0);
    size_t before = output.len;
    CHECK(chc_client_send_data_named(&client, NULL, "ext", 3, &err) == CHC_ERR_USAGE);
    CHECK(output.len == before);
    const uint8_t progress[] = {3, 7, 8, 4};
    CHECK(chc_in_init_ioless(&client.in, &al) == CHC_OK);
    CHECK(chc_in_submit(&client.in, progress, sizeof progress, &err) == CHC_OK);
    chc_packet packet;
    CHECK(chc_client_recv_packet(&client, &packet, &err) == CHC_OK);
    CHECK(packet.kind == CHC_PKT_PROGRESS);
    CHECK(packet.progress.rows == 7 && packet.progress.bytes == 8);
    CHECK(packet.progress.total_rows == 0 && packet.progress.written_rows == 0);
    CHECK(chc_client_recv_packet(&client, &packet, &err) == CHC_OK);
    CHECK(packet.kind == CHC_PKT_PONG);
    chc_in_free(&client.in);
    test_mem_sink_free(&output);
}

static void
check_type_spelling(const chc_type *type)
{
    size_t len = 0;
    const char *name = chc_type_name(type, &len);
    char formatted[128];
    memset(formatted, 0xa5, sizeof formatted);
    CHECK(len < sizeof formatted);
    CHECK_EQ_U64(chc_type_format(type, formatted, sizeof formatted), len);
    if (len < sizeof formatted) {
        CHECK(memcmp(formatted, name, len) == 0);
        CHECK(formatted[len] == '\0');
    }
    for (size_t i = 0; i < chc_type_n_children(type); ++i)
        check_type_spelling(chc_type_child(type, i));
}

static void
test_type_spelling_ownership(void)
{
    current_test = "type_spelling_ownership";
    static const char *sources[] = {
        "Array(Nullable(String))",
        "Tuple(label String, amount SimpleAggregateFunction(sum, UInt64))",
        "SimpleAggregateFunction(sum, UInt64)",
        "Map(String, Array(DateTime64(3, 'UTC')))",
    };
    for (size_t i = 0; i < sizeof sources / sizeof *sources; ++i) {
        size_t len = strlen(sources[i]);
        for (size_t fail_at = 0;; ++fail_at) {
            char *input = malloc(len + 1);
            CHECK(input != NULL);
            if (!input) return;
            memcpy(input, sources[i], len + 1);
            test_fail_alloc fa;
            chc_alloc al = test_fail_alloc_init(&fa, fail_at);
            chc_type *type = NULL;
            chc_err err = {0};
            int rc = chc_type_parse(input, len, &al, &type, &err);
            memset(input, 'x', len);
            free(input);
            if (rc == CHC_OK) {
                const char *expected = i == 2 ? "UInt64" : sources[i];
                size_t name_len = 0;
                const char *name = chc_type_name(type, &name_len);
                CHECK_STR_EQ(name, name_len, expected);
                check_type_spelling(type);
                if (i == 1) {
                    const chc_type *amount = chc_type_child(type, 1);
                    name = chc_type_name(amount, &name_len);
                    CHECK_STR_EQ(name, name_len, "UInt64");
                }
                chc_type_destroy(type, &al);
            } else {
                CHECK(rc == CHC_ERR_OOM);
                CHECK(type == NULL);
            }
            CHECK_EQ_U64(fa.live, 0);
            CHECK_EQ_U64(fa.live_bytes, 0);
            if (rc == CHC_OK && fail_at >= fa.calls) break;
            if (rc != CHC_OK && rc != CHC_ERR_OOM) break;
        }
    }
}

static void
test_type_spelling_growth(void)
{
    current_test = "type_spelling_growth";
    size_t retained[2] = {0};
    const size_t label_lengths[] = {64, 512};
    for (size_t sample = 0; sample < 2; ++sample) {
        char source[1024];
        size_t len = 0;
        for (size_t depth = 0; depth < 16; ++depth) {
            memcpy(source + len, "Array(", 6);
            len += 6;
        }
        memcpy(source + len, "Enum8('", 7);
        len += 7;
        memset(source + len, 'a', label_lengths[sample]);
        len += label_lengths[sample];
        memcpy(source + len, "'=1)", 4);
        len += 4;
        for (size_t depth = 0; depth < 16; ++depth) source[len++] = ')';
        test_fail_alloc fa;
        chc_alloc al = test_fail_alloc_init(&fa, SIZE_MAX);
        chc_type *type = NULL;
        chc_err err = {0};
        int rc = chc_type_parse(source, len, &al, &type, &err);
        CHECK(rc == CHC_OK);
        retained[sample] = fa.live_bytes;
        chc_type_destroy(type, &al);
        CHECK_EQ_U64(fa.live, 0);
        CHECK_EQ_U64(fa.live_bytes, 0);
    }
    /* Label growth must not be copied once for each enclosing Array. */
    CHECK(retained[1] >= retained[0]);
    CHECK(retained[1] - retained[0] <= 4 * (label_lengths[1] - label_lengths[0]));
}

static void
test_empty_type_arguments(void)
{
    current_test = "empty_type_arguments";
    static const struct {
        const char *input, *name;
        chc_kind kind;
        int scale;
        size_t nesting;
    } cases[] = {
        {"DateTime()", "DateTime", CHC_DATETIME, 0, 0},
        {"DateTime64()", "DateTime64(0)", CHC_DATETIME64, 0, 0},
        {"DateTime64( )", "DateTime64(0)", CHC_DATETIME64, 0, 0},
        {"Time64()", "Time64(0)", CHC_TIME64, 0, 0},
        {"Decimal32()", "Decimal(9,0)", CHC_DECIMAL32, 0, 0},
        {"Decimal64()", "Decimal(18,0)", CHC_DECIMAL64, 0, 0},
        {"Decimal128()", "Decimal(38,0)", CHC_DECIMAL128, 0, 0},
        {"Array(Nullable(Decimal64()))", "Decimal(18,0)", CHC_DECIMAL64, 0, 2},
        {"Array(DateTime64())", "DateTime64(0)", CHC_DATETIME64, 0, 1},
        {"DateTime64(3)", "DateTime64(3)", CHC_DATETIME64, 3, 0},
        {"Time64(3)", "Time64(3)", CHC_TIME64, 3, 0},
        {"Decimal32(2)", "Decimal32(2)", CHC_DECIMAL32, 2, 0},
    };
    for (size_t i = 0; i < sizeof cases / sizeof *cases; ++i) {
        for (size_t fail_at = 0;; ++fail_at) {
            test_fail_alloc fa;
            chc_alloc al = test_fail_alloc_init(&fa, fail_at);
            chc_type *type = NULL;
            chc_err err = {0};
            int rc = chc_type_parse(cases[i].input, strlen(cases[i].input), &al, &type, &err);
            if (rc == CHC_OK) {
                const chc_type *leaf = type;
                for (size_t depth = 0; depth < cases[i].nesting; ++depth)
                    leaf = chc_type_child(leaf, 0);
                CHECK(chc_type_kind(leaf) == cases[i].kind);
                int scale = chc__kind_is_decimal(cases[i].kind)
                    ? chc_type_decimal_scale(leaf) : chc_type_datetime64_scale(leaf);
                CHECK(scale == cases[i].scale);
                size_t len = 0;
                const char *name = chc_type_name(leaf, &len);
                CHECK_STR_EQ(name, len, cases[i].name);
                check_type_spelling(type);
                chc_type_destroy(type, &al);
            } else {
                CHECK(rc == CHC_ERR_OOM);
                CHECK(type == NULL);
            }
            CHECK_EQ_U64(fa.live, 0);
            CHECK_EQ_U64(fa.live_bytes, 0);
            if (rc == CHC_OK && fail_at >= fa.calls) break;
            if (rc != CHC_OK && rc != CHC_ERR_OOM) break;
        }
    }
    static const struct { const char *input, *message; } rejected[] = {
        {"Decimal32", "AST child element index out of bounds: 0"},
        {"Array(Decimal64)", "AST child element index out of bounds: 0"},
        {"Decimal()", "AST child element index out of bounds: -1"},
        {"FixedString()", "FixedString size must be between 1 and 16777215, got 0"},
        {"Enum8()", "Enum8 content is not correct"},
        {"Enum16()", "Enum16 content is not correct"},
        {"Array()", "AST child element index out of bounds: 0"},
        {"Nullable()", "AST child element index out of bounds: 0"},
        {"Map()", "Map content is not correct"},
        {"SimpleAggregateFunction()", "AST child element index out of bounds: -1"},
    };
    chc_alloc al = chc_alloc_stdlib();
    for (size_t i = 0; i < sizeof rejected / sizeof *rejected; ++i) {
        chc_type *type = NULL;
        chc_err err = {0};
        CHECK(chc_type_parse(rejected[i].input, strlen(rejected[i].input), &al, &type, &err) == CHC_ERR_USAGE);
        CHECK(type == NULL);
        CHECK(strcmp(err.msg, rejected[i].message) == 0);
        chc_type_destroy(type, &al);
    }
    chc_type *type = NULL;
    chc_err err = {0};
    const char *input = "LowCardinality(Array())";
    CHECK(chc_type_parse(input, strlen(input), &al, &type, &err) == CHC_ERR_USAGE);
    CHECK(type == NULL);
    CHECK(strcmp(err.msg, "LowCardinality(Array) is not supported") == 0);
    input = "SimpleAggregateFunction(sum, Array())";
    CHECK(chc_type_parse(input, strlen(input), &al, &type, &err) == CHC_ERR_TYPE);
    CHECK(type == NULL);
    CHECK(strcmp(err.msg, "unsupported column type: SimpleAggregateFunction(sum, Array())") == 0);
}

typedef struct {
    test_mem_sink bytes;
    size_t calls;
    size_t fail_at;
    const void *direct;
    size_t direct_len;
    bool saw_direct;
} counting_sink;

static int
write_counted(void *ud, const void *data, size_t len, chc_err *err)
{
    counting_sink *sink = ud;
    if (++sink->calls == sink->fail_at)
        return test_mem_err_set(err, CHC_ERR_IO, "sink write failed");
    if (data == sink->direct && len == sink->direct_len) sink->saw_direct = true;
    return test_mem_sink_write(&sink->bytes, data, len, err);
}

static void
test_data_coalescing(void)
{
    current_test = "data_coalescing";
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {0};
    chc_type *type = NULL;
    uint8_t data[1000 * 20 + 16384 + 3];
    uint64_t offsets[1002];
    memset(data, 'v', sizeof data);
    for (size_t i = 0; i < 1000; ++i) offsets[i] = (i + 1) * 20;
    offsets[1000] = 20000 + 16384;
    offsets[1001] = sizeof data;
    int rc = chc_type_parse("String", 6, &al, &type, &err);
    CHECK(rc == CHC_OK);
    if (rc != CHC_OK) return;
    chc_column column = chc_build_string(offsets, data, 1002);
    chc_block_col storage[1];
    chc_block_builder block;
    chc_block_builder_init(&block, storage);
    chc_block_builder_append(&block, "value", 5, type, &column);
    counting_sink output = {
        .fail_at = SIZE_MAX, .direct = data + 20000, .direct_len = 16384,
    };
    chc_io io = {.ud = &output, .write = write_counted};
    chc_client client = {.al = &al, .io = &io, .server = {.revision = 54459}};
    test_mem_sink expected = {0};
    chc_io raw = {.ud = &expected, .write = test_mem_sink_write};
    CHECK(chc__write_varuint(&raw, CHC__CLIENT_DATA, &err) == CHC_OK);
    CHECK(chc__write_string(&raw, "external", 8, &err) == CHC_OK);
    CHECK(chc__client_write_block_body(&client, &raw, &block, &err) == CHC_OK);
    CHECK(chc_client_send_data_named(&client, &block, "external", 8, &err) == CHC_OK);
    CHECK(output.bytes.len == expected.len);
    if (output.bytes.len == expected.len)
        CHECK(memcmp(output.bytes.data, expected.data, expected.len) == 0);
    CHECK(output.calls <= expected.len / 4096 + 3);
    CHECK(output.saw_direct);
    size_t writes = output.calls;
    for (size_t fail_at = 1; fail_at <= writes; ++fail_at) {
        output.calls = 0;
        output.bytes.len = 0;
        output.fail_at = fail_at;
        CHECK(chc_client_send_data_named(&client, &block, "external", 8, &err) == CHC_ERR_IO);
        CHECK(strcmp(err.msg, "sink write failed") == 0);
        CHECK(output.calls == fail_at);
    }
    output.calls = 0;
    output.bytes.len = 0;
    output.fail_at = SIZE_MAX;
    expected.len = 0;
    CHECK(chc__write_varuint(&raw, CHC__CLIENT_DATA, &err) == CHC_OK);
    CHECK(chc__write_string(&raw, "", 0, &err) == CHC_OK);
    CHECK(chc__client_write_block_body(&client, &raw, NULL, &err) == CHC_OK);
    CHECK(chc_client_send_data(&client, NULL, &err) == CHC_OK);
    CHECK(output.calls == 1);
    CHECK(output.bytes.len == expected.len);
    if (output.bytes.len == expected.len)
        CHECK(memcmp(output.bytes.data, expected.data, expected.len) == 0);
    test_mem_sink_free(&output.bytes);
    test_mem_sink_free(&expected);
    chc_type_destroy(type, &al);
}

int main(void)
{
    test_hello(732);
    test_hello(54401);
    test_hello(54470);
    test_legacy_query_and_progress();
    test_type_spelling_ownership();
    test_type_spelling_growth();
    test_empty_type_arguments();
    test_data_coalescing();
    if (fail_count) return 1;
    fprintf(stderr, "all PHP compatibility unit tests passed\n");
    return 0;
}
