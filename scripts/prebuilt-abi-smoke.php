<?php
// Exercises the smart_str growth paths of a prebuilt module, so an import the
// running PHP does not export fails here: PHP loads extensions with RTLD_LAZY,
// and an unresolved function only aborts when first called. The module's
// smart_str appends all sit on the selectToStream() path, which needs a live
// server; an unreachable server fails the smoke instead of skipping it.

function fail(string $msg): never
{
    fwrite(STDERR, "FAIL: $msg\n");
    exit(1);
}

if (!extension_loaded('clickhouse') || !class_exists('ClickHouse')) {
    fail('clickhouse is not loaded');
}

$config = [
    'host' => getenv('CLICKHOUSE_HOST') ?: '127.0.0.1',
    'port' => (int) (getenv('CLICKHOUSE_PORT') ?: '9000'),
];
$user = getenv('CLICKHOUSE_USER');
$passwd = getenv('CLICKHOUSE_PASSWD');
if ($user !== false && $user !== '') {
    $config['user'] = $user;
}
if ($passwd !== false && $passwd !== '') {
    $config['passwd'] = $passwd;
}

try {
    $db = new ClickHouse($config);
} catch (Throwable $e) {
    fail('cannot connect to ClickHouse at ' . $config['host'] . ':' . $config['port'] . ': ' . $e->getMessage());
}

$rows = 2000;
$sql = "SELECT number, concat('a\\tb\\\\c\"d,e', toString(number)) AS s, repeat('x', 64) AS pad"
    . " FROM system.numbers LIMIT $rows";
$expected = [
    'TabSeparated' => "0\ta\\tb\\\\c\"d,e0\t" . str_repeat('x', 64) . "\n",
    'CSVWithNames' => "number,s,pad\r\n0,\"a\tb\\c\"\"d,e0\"," . str_repeat('x', 64) . "\r\n",
];

$sizes = [];
foreach ($expected as $format => $head) {
    $stream = fopen('php://memory', 'w+b');
    try {
        $written = $db->selectToStream($sql, [], $stream, $format);
    } catch (Throwable $e) {
        fail("selectToStream($format) threw " . get_class($e) . ': ' . $e->getMessage());
    }
    if ($written !== $rows) {
        fail("selectToStream($format) reported $written rows, expected $rows");
    }
    rewind($stream);
    $out = stream_get_contents($stream);
    fclose($stream);
    if (strlen($out) < 64 * 1024) {
        fail("selectToStream($format) wrote " . strlen($out) . ' bytes, expected at least 64 KiB');
    }
    if (!str_starts_with($out, $head)) {
        fail("selectToStream($format) output starts with " . var_export(substr($out, 0, strlen($head)), true));
    }
    $sizes[] = "$format=" . strlen($out);
}

printf("ok php=%s clickhouse=%s %s\n", PHP_VERSION, phpversion('clickhouse'), implode(' ', $sizes));
