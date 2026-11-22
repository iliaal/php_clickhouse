#!/bin/sh
# Regenerate the perfect-hash table inside chc__name_to_kind.
# Edits clickhouse.h in place between the sentinel comments.

set -eu
cd "$(dirname "$0")/.."

gen=/tmp/chc_gen_name_table
cc -std=c11 -O2 -Wall -Wextra -Wno-unused-parameter -I. \
   tools/gen_name_table.c -o "$gen"

new_block=$("$gen")

awk -v block="$new_block" '
    /AUTO-GENERATED-NAME-TABLE-BEGIN/ { print; print block; skip = 1; next }
    /AUTO-GENERATED-NAME-TABLE-END/   { skip = 0; print; next }
    !skip
' clickhouse.h > clickhouse.h.new
mv clickhouse.h.new clickhouse.h

echo "regen_name_table: clickhouse.h updated"
