<?php
/**
 * php_clickhouse benchmark.
 *
 * Compares this extension (with and without LZ4 compression) against
 * smi2/phpClickHouse, a pure-PHP HTTP client
 * (https://github.com/smi2/phpClickHouse).
 *
 * Originally written by ahhhh.wang@gmail.com for SeasClick. Updated for
 * php_clickhouse (PHP 8.4 + clickhouse-cpp v2.6.1).
 *
 * Connection settings come from the same env vars the phpt suite uses:
 *   CLICKHOUSE_HOST (default: clickhouse), CLICKHOUSE_PORT (default 9000),
 *   CLICKHOUSE_HTTP_PORT (default 8123), CLICKHOUSE_USER, CLICKHOUSE_PASSWD.
 */

require_once __DIR__ . '/vendor/autoload.php';

$HOST   = getenv('CLICKHOUSE_HOST') ?: 'clickhouse';
$TCP    = (int)(getenv('CLICKHOUSE_PORT') ?: '9000');
$HTTP   = (int)(getenv('CLICKHOUSE_HTTP_PORT') ?: '8123');
$USER   = getenv('CLICKHOUSE_USER') ?: 'default';
$PASSWD = getenv('CLICKHOUSE_PASSWD') ?: '';

// $dataCount, $selectCount, $limit
$testDataSet = [
    [10000,   1,  5000],
    [10000,   1,  5000],
    [10000, 100,  5000],
    [10000, 100, 10000],
    [ 1000, 200,   500],
    [ 1000, 200,  1000],
    [ 1000, 500,   500],
    [ 1000, 500,  1000],
    [ 1000, 800,   500],
    [ 1000, 800,  1000],
];

$total = 0;

foreach ($testDataSet as $value) {
    [$dataCount, $selectCount, $limit] = $value;
    $insertData = initData($dataCount);

    echo "\n##### dataCount: {$dataCount}, selectCount: {$selectCount}, limit: {$limit} #####\n";

    $t0 = $t = start_test();
    testPhpClickhouse($insertData, $selectCount, $limit, $HOST, $HTTP, $USER, $PASSWD);
    $t = end_test($t, 'phpClickHouse (HTTP)');

    testClickhouseNoCompression($insertData, $selectCount, $limit, $HOST, $TCP, $USER, $PASSWD);
    $t = end_test($t, 'php_clickhouse (uncompressed)');

    testClickhouseLz4($insertData, $selectCount, $limit, $HOST, $TCP, $USER, $PASSWD);
    $t = end_test($t, 'php_clickhouse (LZ4)');

    testClickhouseZstd($insertData, $selectCount, $limit, $HOST, $TCP, $USER, $PASSWD);
    $t = end_test($t, 'php_clickhouse (ZSTD)');

    total($t0);
}

function start_test() { return microtime(true); }

function end_test($start, $name) {
    global $total;
    $end = microtime(true);
    $total += $end - $start;
    $num = number_format($end - $start, 3);
    $pad = str_repeat(' ', max(1, 60 - strlen($name) - strlen($num)));
    echo $name . $pad . $num . "\n";
    return microtime(true);
}

function total() {
    global $total;
    echo str_repeat('-', 32) . "\n";
    $num = number_format($total, 3);
    $pad = str_repeat(' ', max(1, 32 - strlen('Total') - strlen($num)));
    echo 'Total' . $pad . $num . "\n";
    $total = 0;
}

function makeClickhouse($host, $port, $user, $pass, $compression) {
    $cfg = [
        'host'        => $host,
        'port'        => $port,
        'compression' => $compression,
    ];
    if ($user !== '')  $cfg['user']   = $user;
    if ($pass !== '')  $cfg['passwd'] = $pass;
    return new ClickHouse($cfg);
}

function setupTable($db) {
    $db->execute('CREATE DATABASE IF NOT EXISTS test');
    $db->execute('DROP TABLE IF EXISTS test.summing_url_views');
    $db->execute('
        CREATE TABLE test.summing_url_views (
            event_date Date DEFAULT toDate(event_time),
            event_time DateTime,
            site_id    Int32,
            site_key   String,
            views      Int32,
            v_00       Int32,
            v_55       Int32
        )
        ENGINE = SummingMergeTree
        ORDER BY (site_id, site_key, event_time, event_date)
        PARTITION BY event_date
        SETTINGS index_granularity = 8192
    ');
}

function testClickhouseNoCompression($insertData, $num, $limit, $host, $port, $user, $pass) {
    $db = makeClickhouse($host, $port, $user, $pass, false);
    setupTable($db);
    $db->insert('test.summing_url_views',
        ['event_time', 'site_key', 'site_id', 'views', 'v_00', 'v_55'],
        $insertData);
    for ($a = 0; $a < $num; $a++) {
        $db->select('SELECT * FROM test.summing_url_views LIMIT ' . $limit);
    }
    $db->execute('DROP TABLE test.summing_url_views');
}

function testClickhouseLz4($insertData, $num, $limit, $host, $port, $user, $pass) {
    $db = makeClickhouse($host, $port, $user, $pass, 'lz4');
    setupTable($db);
    $db->insert('test.summing_url_views',
        ['event_time', 'site_key', 'site_id', 'views', 'v_00', 'v_55'],
        $insertData);
    for ($a = 0; $a < $num; $a++) {
        $db->select('SELECT * FROM test.summing_url_views LIMIT ' . $limit);
    }
    $db->execute('DROP TABLE test.summing_url_views');
}

function testClickhouseZstd($insertData, $num, $limit, $host, $port, $user, $pass) {
    $db = makeClickhouse($host, $port, $user, $pass, 'zstd');
    setupTable($db);
    $db->insert('test.summing_url_views',
        ['event_time', 'site_key', 'site_id', 'views', 'v_00', 'v_55'],
        $insertData);
    for ($a = 0; $a < $num; $a++) {
        $db->select('SELECT * FROM test.summing_url_views LIMIT ' . $limit);
    }
    $db->execute('DROP TABLE test.summing_url_views');
}

function testPhpClickhouse($insertData, $num, $limit, $host, $port, $user, $pass) {
    $db = new ClickHouseDB\Client([
        'host'     => $host,
        'port'     => (string)$port,
        'username' => $user ?: 'default',
        'password' => $pass,
    ]);
    $db->write('CREATE DATABASE IF NOT EXISTS test');
    $db->database('test');
    $db->setTimeout(10);
    $db->setConnectTimeOut(5);

    $db->write('DROP TABLE IF EXISTS summing_url_views');
    $db->write('
        CREATE TABLE summing_url_views (
            event_date Date DEFAULT toDate(event_time),
            event_time DateTime,
            site_id    Int32,
            site_key   String,
            views      Int32,
            v_00       Int32,
            v_55       Int32
        )
        ENGINE = SummingMergeTree
        ORDER BY (site_id, site_key, event_time, event_date)
        PARTITION BY event_date
        SETTINGS index_granularity = 8192
    ');
    $db->insert('summing_url_views', $insertData,
        ['event_time', 'site_key', 'site_id', 'views', 'v_00', 'v_55']);

    for ($a = 0; $a < $num; $a++) {
        $db->select('SELECT * FROM summing_url_views LIMIT ' . $limit)->rows();
    }
    $db->write('DROP TABLE summing_url_views');
}

function initData($num) {
    $rows = [];
    while ($num--) {
        $rows[] = [time(), 'HASH2', 2345, 12, 9, 3];
    }
    return $rows;
}
