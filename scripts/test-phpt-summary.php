<?php

require_once __DIR__ . "/phpt-summary.php";

function summary($counts = [], $newline = "\n")
{
    $output = "";
    foreach ($counts + ["skipped" => 0, "warned" => 0, "failed" => 0, "passed" => 1] as $field => $count) {
        $output .= sprintf("Tests %-10s: %5d (100.0%%)%s", $field, $count, $newline);
    }
    return $output;
}

function checkSummary($output, $expected)
{
    $actual = phptSummaryError($output);
    if ($actual !== $expected) {
        fwrite(STDERR, "Unexpected PHPT summary result: " . var_export($actual, true) .
            "; expected " . var_export($expected, true) . "\n");
        exit(1);
    }
}

checkSummary(summary(), null);
checkSummary(summary([], "\r\n"), null);
checkSummary(summary(["skipped" => 2, "passed" => 5]), null);
checkSummary(summary(["borked" => 0, "leaked" => 0]), null);
foreach (["failed", "warned", "borked", "leaked"] as $field) {
    // A passing test must not hide any other unsuccessful outcome.
    checkSummary(summary([$field => 1]), "the test run reported $field tests");
}
checkSummary(summary(["passed" => 0]), "the test run reported zero passing tests");
foreach (["passed", "skipped", "failed", "warned"] as $field) {
    $incomplete = preg_replace('/^Tests ' . $field . '[^\n]*\n/m', '', summary());
    checkSummary($incomplete, "could not parse PHPT $field count");
}
checkSummary("", "could not parse PHPT passed count");

printf("PHP PHPT summary guard self-test passed\n");
