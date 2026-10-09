<?php

require_once __DIR__ . "/phpt-summary.php";

function runCommand($command)
{
    $pipes = [];
    $process = proc_open($command, [
        0 => ["file", PHP_OS_FAMILY === "Windows" ? "NUL" : "/dev/null", "r"],
        1 => ["pipe", "w"],
        2 => ["redirect", 1],
    ], $pipes);
    if (!is_resource($process)) {
        fwrite(STDERR, "Unable to start PHP test process\n");
        exit(1);
    }
    $output = stream_get_contents($pipes[1]);
    fclose($pipes[1]);
    $status = proc_close($process);
    return [$status, $output];
}

$php = getenv("TEST_PHP_EXECUTABLE") ?: PHP_BINARY;
$phpArgs = trim((string)getenv("TEST_PHP_ARGS"));
$base = escapeshellarg($php) . ($phpArgs === "" ? "" : " " . $phpArgs);

$smoke = <<<'PHP'
<?php
if (!extension_loaded("clickhouse") ||
    !class_exists("ClickHouse", false) ||
    !class_exists("ClickHouseException", false) ||
    !class_exists("ClickHouseRowIterator", false) ||
    !class_exists("ClickHouseStatement", false) ||
    !is_string(phpversion("clickhouse"))) {
    fwrite(STDERR, "clickhouse module smoke failed\n");
    exit(1);
}
PHP;
$smokeFile = tempnam(sys_get_temp_dir(), "clickhouse-smoke-");
if ($smokeFile === false || file_put_contents($smokeFile, $smoke) === false) {
    if ($smokeFile !== false) {
        @unlink($smokeFile);
    }
    fwrite(STDERR, "Unable to create PHP module smoke script\n");
    exit(1);
}
list($smokeStatus, $smokeOutput) = runCommand(
    $base . " -f " . escapeshellarg($smokeFile)
);
if ($smokeStatus !== 0) {
    list($smokeStatus, $smokeOutput) = runCommand(
        $base . " -d " . escapeshellarg("extension=clickhouse") .
        " -f " . escapeshellarg($smokeFile)
    );
}
@unlink($smokeFile);
if ($smokeStatus !== 0) {
    fwrite(STDERR, $smokeOutput);
    exit($smokeStatus ?: 1);
}

$runner = dirname(__DIR__) . DIRECTORY_SEPARATOR . "run-tests.php";
$command = $base . " " . escapeshellarg($runner);
foreach (array_slice($argv, 1) as $argument) {
    $command .= " " . escapeshellarg($argument);
}

list($status, $output) = runCommand($command);
echo $output;
if ($status !== 0) {
    exit($status);
}
$summaryError = phptSummaryError($output);
if ($summaryError !== null) {
    fwrite(STDERR, "PHPT guard: $summaryError\n");
    exit(1);
}
/* Require explicit passes for REQUIRED_PHPT_TESTS so all-skip runs cannot
 * mask a missing Windows smoke test. */
$required = trim((string)getenv("REQUIRED_PHPT_TESTS"));
if ($required !== "") {
    foreach (preg_split('/\s+/', $required) as $requiredTest) {
        list($reqStatus, $reqOutput) = runCommand(
            $base . " " . escapeshellarg($runner) . " " . escapeshellarg($requiredTest)
        );
        echo $reqOutput;
        if ($reqStatus !== 0 || phptSummaryError($reqOutput) !== null) {
            fwrite(STDERR, "PHPT guard: required test did not pass: $requiredTest\n");
            exit(1);
        }
    }
}
