#!/bin/sh
# Read-only regression fixtures for independent tests versus inline production tests.
set -eu
repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$repo_root"
sh scripts/check-source-layout.sh scripts/fixtures/source-layout/valid
for fixture in scripts/fixtures/source-layout/invalid_attribute scripts/fixtures/source-layout/invalid_inline; do
    if sh scripts/check-source-layout.sh "$fixture"; then
        echo "source layout accepted prohibited fixture: $fixture" >&2
        exit 1
    fi
done
echo "source layout regression fixtures passed"
