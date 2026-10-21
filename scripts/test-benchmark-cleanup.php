<?php
/* Exercise benchmark cleanup without loading the extension or contacting a server. */
$root = dirname(__DIR__);
$work = sys_get_temp_dir() . "/clickhouse-benchmark-" . bin2hex(random_bytes(8));
if (!mkdir($work . "/vendor", 0700, true)) {
    throw new RuntimeException("Unable to create benchmark fixture");
}
$mock = <<<'MOCK'
<?php
namespace {
    function recordDrop($sql) {
        if (strpos($sql, 'DROP TABLE IF EXISTS ') === 0) {
            file_put_contents(getenv('BENCH_TRACE'), $sql . "\n", FILE_APPEND);
        }
    }
    class ClickHouse {
        private $kind;
        public function __construct($config) {
            $this->kind = $config['compression'] === false ? 'uncompressed' : $config['compression'];
        }
        public function execute($sql) { recordDrop($sql); }
        public function insert($table, $columns, $data) {}
        public function select($sql) {
            if (strpos($sql, 'SELECT count()') === 0) {
                return [['c' => getenv('BENCH_FAIL_CLIENT') === $this->kind ? 0 : 100]];
            }
            return [];
        }
    }
}
namespace ClickHouseDB {
    class Client {
        public function __construct($config) {}
        public function write($sql) { \recordDrop($sql); }
        public function database($name) {}
        public function setTimeout($timeout) {}
        public function setConnectTimeOut($timeout) {}
        public function insert($table, $data, $columns) {}
        public function select($sql) { return new Result($sql); }
    }
    class Result {
        private $sql;
        public function __construct($sql) { $this->sql = $sql; }
        public function rows() {
            if (strpos($this->sql, 'SELECT count()') === 0) {
                return [['c' => getenv('BENCH_FAIL_CLIENT') === 'http' ? 0 : 100]];
            }
            return [];
        }
    }
}
MOCK;
try {
    if (!copy($root . "/bench/bench_mark.php", $work . "/bench_mark.php") ||
        file_put_contents($work . "/vendor/autoload.php", $mock) === false) {
        throw new RuntimeException("Unable to prepare benchmark fixture");
    }
    $clients = ['http', 'uncompressed', 'lz4', 'zstd'];
    $labels = ['phpClickHouse (HTTP)', 'php_clickhouse (uncompressed)', 'php_clickhouse (LZ4)', 'php_clickhouse (ZSTD)'];
    $tables = ['benchmark_http', 'test.benchmark_native', 'test.benchmark_lz4', 'test.benchmark_zstd'];
    foreach (array_merge($clients, ['none']) as $case => $failure) {
        $trace = $work . "/trace";
        file_put_contents($trace, "");
        $env = getenv();
        $env['BENCH_SMOKE'] = '1';
        $env['BENCH_REPETITIONS'] = '2';
        $env['BENCH_FAIL_CLIENT'] = $failure;
        $env['BENCH_TRACE'] = $trace;
        $command = escapeshellarg(PHP_BINARY) . ' -n ' . escapeshellarg($work . "/bench_mark.php");
        $process = proc_open($command, [0 => ['file', PHP_OS_FAMILY === 'Windows' ? 'NUL' : '/dev/null', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes, null, $env);
        if (!is_resource($process)) {
            throw new RuntimeException("Unable to start benchmark fixture");
        }
        $stdout = stream_get_contents($pipes[1]);
        $stderr = stream_get_contents($pipes[2]);
        fclose($pipes[1]);
        fclose($pipes[2]);
        $status = proc_close($process);
        $drops = array_count_values(file($trace, FILE_IGNORE_NEW_LINES));
        foreach ($tables as $index => $table) {
            // Every adapter tears down, including those not set up before the failure.
            $expected = 1 + (int)($failure === 'none' || $index <= $case);
            if (($drops['DROP TABLE IF EXISTS ' . $table] ?? 0) !== $expected) {
                throw new RuntimeException("$failure: cleanup count mismatch for $table");
            }
        }
        if ($failure === 'none') {
            if ($status !== 0 || strpos($stdout, '| 100 × 2 × 50') === false) {
                throw new RuntimeException("Successful benchmark did not produce its result table");
            }
        } elseif ($status === 0 || strpos($stdout . $stderr, $labels[$case] . ' inserted 100 warm-up rows but reads back 0') === false ||
            strpos($stdout, '| dataCount') !== false) {
            throw new RuntimeException("$failure: missing failure diagnostic or unexpected result table");
        }
    }
    echo "Benchmark cleanup self-test passed (4 failures, 1 success)\n";
} finally {
    foreach (['vendor/autoload.php', 'bench_mark.php', 'trace'] as $file) {
        if (is_file($work . "/" . $file)) {
            unlink($work . "/" . $file);
        }
    }
    rmdir($work . "/vendor");
    rmdir($work);
}
