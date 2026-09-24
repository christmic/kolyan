#!/bin/sh
# Enforce the production/test file boundary from code-conventions.md.
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"

# This is a lightweight source check, not a Rust parser. Rustdoc examples remain
# legal; only actual attribute/module lines in production files are checked.
find crates apps services -type f -name '*.rs' \
    ! -path '*/tests/*' ! -name 'tests.rs' |
while IFS= read -r source_file; do
    awk '
        /^[[:space:]]*mod[[:space:]]+tests[[:space:]]*\{/ ||
        /^[[:space:]]*#\[(tokio::)?test[^[:alnum:]_]/ {
            print FILENAME ":" FNR ": test implementation belongs in a separate test file"
            failed = 1
        }
        END { exit failed ? 1 : 0 }
    ' "$source_file"
done
