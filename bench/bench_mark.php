<?php
/**
 * php_clickhouse benchmark.
 *
 * Compares this extension (with and without compression) against
 * smi2/phpClickHouse, a pure-PHP HTTP client
 * (https://github.com/smi2/phpClickHouse).
 *
 * Connection settings come from the same env vars the phpt suite uses:
 *   CLICKHOUSE_HOST (default: clickhouse), CLICKHOUSE_PORT (default 9000),
 *   CLICKHOUSE_HTTP_PORT (default 8123), CLICKHOUSE_USER, CLICKHOUSE_PASSWD.
 *
 * BENCH_REPETITIONS controls the measured repetitions (default 4).
 * BENCH_SMOKE=1 uses one small workload and two repetitions.
 */

require_once __DIR__ . '/vendor/autoload.php';

$host = getenv('CLICKHOUSE_HOST') ?: 'clickhouse';
$tcp = (int)(getenv('CLICKHOUSE_PORT') ?: '9000');
$http = (int)(getenv('CLICKHOUSE_HTTP_PORT') ?: '8123');
$user = getenv('CLICKHOUSE_USER') ?: 'default';
$passwd = getenv('CLICKHOUSE_PASSWD') ?: '';
$smoke = getenv('BENCH_SMOKE') === '1';
$repetitions = envPositiveInt('BENCH_REPETITIONS', $smoke ? 2 : 4);

$testDataSet = $smoke
    ? [[100, 2, 50]]
    : [
        [10000, 1, 5000],
        [10000, 100, 5000],
        [10000, 100, 10000],
        [1000, 200, 500],
        [1000, 500, 1000],
    ];

$columns = ['event_time', 'site_key', 'site_id', 'views', 'v_00', 'v_55'];
$httpClient = makeHttpClient($host, $http, $user, $passwd);
$clients = [
    'phpClickHouse (HTTP)' => makeHttpAdapter(
        $httpClient,
        'benchmark_http',
        $columns
    ),
    'php_clickhouse (uncompressed)' => makeNativeAdapter(
        makeClickhouse($host, $tcp, $user, $passwd, false),
        'benchmark_native',
        $columns
    ),
    'php_clickhouse (LZ4)' => makeNativeAdapter(
        makeClickhouse($host, $tcp, $user, $passwd, 'lz4'),
        'benchmark_lz4',
        $columns
    ),
    'php_clickhouse (ZSTD)' => makeNativeAdapter(
        makeClickhouse($host, $tcp, $user, $passwd, 'zstd'),
        'benchmark_zstd',
        $columns
    ),
];

$results = [];
$clientNames = array_keys($clients);

foreach ($testDataSet as $scenarioIndex => $scenario) {
    [$dataCount, $selectCount, $limit] = $scenario;
    $insertData = initData($dataCount);
    $warmupData = array_slice($insertData, 0, min(100, $dataCount));
    $samples = array_fill_keys($clientNames, []);

    fprintf(
        STDERR,
        "dataCount=%d selectCount=%d limit=%d repetitions=%d\n",
        $dataCount,
        $selectCount,
        $limit,
        $repetitions
    );

    try {
        foreach ($clientNames as $name) {
            $clients[$name]['setup']();
            $clients[$name]['reset']();
            $expected = count($warmupData);
            $clients[$name]['run']($warmupData, 1, min(50, $expected));
            /* Exclude incomplete work from timing comparisons. */
            $seen = $clients[$name]['count']();
            if ($seen !== $expected) {
                throw new RuntimeException(sprintf(
                    "%s inserted %d warm-up rows but reads back %d",
                    $name,
                    $expected,
                    $seen
                ));
            }
        }

        for ($repetition = 0; $repetition < $repetitions; ++$repetition) {
            $offset = ($scenarioIndex + $repetition) % count($clientNames);
            $order = rotate($clientNames, $offset);

            foreach ($order as $name) {
                $clients[$name]['reset']();
                $started = hrtime(true);
                $clients[$name]['run']($insertData, $selectCount, $limit);
                $samples[$name][] = (hrtime(true) - $started) / 1e9;
            }
        }
    } finally {
        foreach ($clients as $client) {
            /* A failed run usually leaves a dead connection, so DROP TABLE
             * throws from inside finally and replaces the real diagnostic. */
            try {
                $client['teardown']();
            } catch (Throwable $ignored) {
            }
        }
    }

    $row = [
        'label' => sprintf('%d × %d × %d', $dataCount, $selectCount, $limit),
        'medians' => [],
    ];
    foreach ($clientNames as $name) {
        $row['medians'][$name] = median($samples[$name]);
    }
    $results[] = $row;
}

echo "\n";
echo '| dataCount × selectCount × limit';
foreach ($clientNames as $name) {
    echo ' | ' . $name;
}
echo " |\n";
echo '|' . str_repeat('---:|', count($clientNames) + 1) . "\n";
foreach ($results as $row) {
    echo '| ' . $row['label'];
    foreach ($clientNames as $name) {
        printf(' | %.3f', $row['medians'][$name]);
    }
    echo " |\n";
}

function envPositiveInt($name, $default)
{
    $value = getenv($name);
    if ($value === false || $value === '') {
        return $default;
    }
    if (!preg_match('/^[1-9][0-9]*$/', $value)) {
        throw new InvalidArgumentException($name . ' must be a positive integer');
    }
    return (int)$value;
}

function rotate(array $values, $offset)
{
    if ($offset === 0) {
        return $values;
    }
    return array_merge(array_slice($values, $offset), array_slice($values, 0, $offset));
}

function median(array $samples)
{
    sort($samples, SORT_NUMERIC);
    $count = count($samples);
    $middle = intdiv($count, 2);
    if ($count % 2 === 1) {
        return $samples[$middle];
    }
    return ($samples[$middle - 1] + $samples[$middle]) / 2;
}

function makeClickhouse($host, $port, $user, $passwd, $compression)
{
    $config = [
        'host' => $host,
        'port' => $port,
        'compression' => $compression,
    ];
    if ($user !== '') {
        $config['user'] = $user;
    }
    if ($passwd !== '') {
        $config['passwd'] = $passwd;
    }
    return new ClickHouse($config);
}

function makeHttpClient($host, $port, $user, $passwd)
{
    $client = new ClickHouseDB\Client([
        'host' => $host,
        'port' => (string)$port,
        'username' => $user ?: 'default',
        'password' => $passwd,
    ]);
    $client->write('CREATE DATABASE IF NOT EXISTS test');
    $client->database('test');
    $client->setTimeout(30);
    $client->setConnectTimeOut(5);
    return $client;
}

function tableDefinition($table)
{
    return "
        CREATE TABLE {$table} (
            event_time DateTime,
            site_id    Int32,
            site_key   String,
            views      Int32,
            v_00       Int32,
            v_55       Int32
        )
        ENGINE = Memory
    ";
}

function makeNativeAdapter($client, $table, array $columns)
{
    $qualifiedTable = 'test.' . $table;
    return [
        'setup' => function () use ($client, $qualifiedTable) {
            $client->execute('CREATE DATABASE IF NOT EXISTS test');
            $client->execute('DROP TABLE IF EXISTS ' . $qualifiedTable);
            $client->execute(tableDefinition($qualifiedTable));
        },
        'reset' => function () use ($client, $qualifiedTable) {
            $client->execute('TRUNCATE TABLE ' . $qualifiedTable);
        },
        'run' => function (array $data, $selectCount, $limit) use (
            $client,
            $qualifiedTable,
            $columns
        ) {
            $client->insert($qualifiedTable, $columns, $data);
            for ($query = 0; $query < $selectCount; ++$query) {
                $client->select('SELECT * FROM ' . $qualifiedTable . ' LIMIT ' . $limit);
            }
        },
        'count' => function () use ($client, $qualifiedTable) {
            $rows = $client->select('SELECT count() AS c FROM ' . $qualifiedTable);
            return (int) $rows[0]['c'];
        },
        'teardown' => function () use ($client, $qualifiedTable) {
            $client->execute('DROP TABLE IF EXISTS ' . $qualifiedTable);
        },
    ];
}

function makeHttpAdapter($client, $table, array $columns)
{
    return [
        'setup' => function () use ($client, $table) {
            $client->write('DROP TABLE IF EXISTS ' . $table);
            $client->write(tableDefinition($table));
        },
        'reset' => function () use ($client, $table) {
            $client->write('TRUNCATE TABLE ' . $table);
        },
        'run' => function (array $data, $selectCount, $limit) use (
            $client,
            $table,
            $columns
        ) {
            $client->insert($table, $data, $columns);
            for ($query = 0; $query < $selectCount; ++$query) {
                $client->select('SELECT * FROM ' . $table . ' LIMIT ' . $limit)->rows();
            }
        },
        'count' => function () use ($client, $table) {
            $rows = $client->select('SELECT count() AS c FROM ' . $table)->rows();
            return (int) $rows[0]['c'];
        },
        'teardown' => function () use ($client, $table) {
            $client->write('DROP TABLE IF EXISTS ' . $table);
        },
    ];
}

function initData($count)
{
    /* Repeated identical rows would unfairly favor compressed clients. */
    $rows = [];
    $timestamp = time();
    /* Knuth multiplicative hash, so the string column varies without
     * repeating on any short cycle. */
    $scramble = 2654435761;
    for ($index = 0; $index < $count; ++$index) {
        $rows[] = [
            $timestamp + ($index % 86400),
            sprintf('HASH%06X', ($index * $scramble) & 0xFFFFFF),
            2345 + ($index % 100000),
            12 + ($index % 977),
            9 + ($index % 31),
            3 + ($index % 7),
        ];
    }
    return $rows;
}
