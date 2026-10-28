#!/bin/sh
set -eu
export LC_ALL=C

for notes in "$1"/*.gcno; do
    gcov --stdout --source-prefix "$PWD" "$notes"
done > "$1/coverage.gcov"

awk -F: '
    $2 == 0 && $3 == "Source" {
        file = substr($0, index($0, ":Source:") + 8)
        include = file ~ /^clickhouse(-[a-z-]+)?[.]h$/
        if (include) print "SF:" file
        next
    }
    # CHC_UNREACHABLE marks a state the type system rules out; gcc still emits
    # a line entry for it at -O0, so drop it rather than chase an unhittable line
    include && $2 > 0 && $1 !~ /^[ \t]*-[ \t]*$/ && $0 !~ /CHC_UNREACHABLE/ {
        if ($1 + 0 < 0) {
            print "Negative gcov count: " file ":" $2 > "/dev/stderr"
            exit 1
        }
        printf "DA:%d,%.0f\n", $2, $1 + 0
    }
' "$1/coverage.gcov" > "$1/coverage.lcov"

awk -f tools/lcov_merge.awk "$1/coverage.lcov" > coverage/lcov.info
: > coverage/missing-lines.txt
status=0
awk -F: '
    BEGIN { printf "%-28s %8s %8s %9s\n", "File", "Covered", "Lines", "Coverage" }
    /^SF:/ { file = substr($0, 4) }
    /^DA:/ {
        split($2, data, ",")
        if (data[2] + 0 == 0) print file ":" data[1] > "coverage/missing-lines.txt"
    }
    /^LF:/ { lines = $2; total += lines }
    /^LH:/ {
        hits = $2; covered += hits
        printf "%-28s %8d %8d %8.2f%%\n", file, hits, lines, lines ? 100 * hits / lines : 0
    }
    END {
        printf "%-28s %8d %8d %8.2f%%\n", "TOTAL", covered, total, total ? 100 * covered / total : 0
        if (!total || covered != total) {
            print "Require 100% line coverage, see coverage/missing-lines.txt"
            exit 2
        }
    }
' coverage/lcov.info > coverage/coverage.txt || status=$?
cat coverage/coverage.txt
exit "$status"
