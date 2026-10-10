<?php

/* Validate run-tests.php output independently of REPORT_EXIT_STATUS. */
function phptSummaryError($output)
{
    $counts = [];
    foreach (["passed", "skipped", "failed", "warned", "borked", "leaked"] as $field) {
        if (preg_match('/^Tests ' . $field . '\s*:\s*([0-9]+)\b/m', $output, $matches)) {
            $counts[$field] = (int)$matches[1];
        } elseif ($field === "borked" || $field === "leaked") {
            // BORKED is omitted for clean runs; LEAKED needs a leak checker.
            $counts[$field] = 0;
        } else {
            return "could not parse PHPT $field count";
        }
    }
    // WARNED includes tests that only passed on retry, not a clean run.
    foreach (["failed", "warned", "borked", "leaked"] as $field) {
        if ($counts[$field] > 0) {
            return "the test run reported $field tests";
        }
    }
    if ($counts["passed"] === 0) {
        return "the test run reported zero passing tests";
    }
    return null;
}
