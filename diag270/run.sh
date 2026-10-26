#!/bin/bash
# Temporary CI diagnostic for tests/270 RSS growth; removed before merge.
set -u
PHP=/tmp/php-asan/bin/php
EXT="$PWD/modules/clickhouse.so"
uname -a
cat /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null
grep -E 'MemTotal|MemAvailable|^Cached' /proc/meminfo
echo "== old measurement (no warm-up), 30 iterations"
$PHP -n -d extension="$EXT" diag270/old_loop.php 30 2>&1 | grep -E '^(exception-name|column-name|column-type):|raw|DELTA' | paste - - - > "$RUNNER_TEMP/old.txt"
cat "$RUNNER_TEMP/old.txt"
echo "== new measurement (warm-up), 30 iterations"
$PHP -n -r '$s=file_get_contents("tests/270_declared_protocol_strings.phpt"); preg_match("/--FILE--\n(.*)--EXPECT--/s", $s, $m); $b=$m[1]; $b=substr($b,0,strpos($b,"/* The binding truncates")); $b=str_replace("foreach (\$prefixes as", "for (\$it = 0; \$it < (int)\$argv[1]; \$it++) foreach (\$prefixes as", $b); $b=str_replace("    report(\$label, \$result, \$peak, \$hwm);", "    echo \"\$label \$result \$peak \$hwm\\n\";", $b); file_put_contents("/tmp/raw270.php", $b);'
$PHP -n -d extension="$EXT" /tmp/raw270.php 30
