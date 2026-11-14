#!/usr/bin/env bash
# Runs the correctness suite, then the 10-second-per-scenario benchmark.
# Any extra arguments go to the correctness run (e.g. --only=computed).
set -uo pipefail
cd "$(dirname "$0")"

echo "== PART 1: correctness =="
npx --yes tsx "c_test.ts" "$@"
CORRECTNESS=$?

echo
echo "== PART 2: throughput =="
npx --yes tsx "bench.ts"
BENCH=$?

echo
echo "correctness exit: ${CORRECTNESS}   benchmark exit: ${BENCH}"
exit $(( CORRECTNESS != 0 ? CORRECTNESS : BENCH ))
