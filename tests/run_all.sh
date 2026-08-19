#!/bin/sh
# run_all.sh — the whole deep suite, in order:
#   baseline (22)  ->  W1 read surface  ->  (dictionary;codes) ->
#   writer options/codecs/
#   encodings (L side, then pyarrow + duckdb) -> randomized matrix +
#   corners + hostile (gen, run, pyarrow check) -> 3 seeded shake
#   passes over the large parallel cases (one under RUST_BACKTRACE=1)
#   -> L-side edges -> adversarial subprocess harness -> leak loop.
# Env: L_BIN=<path to L binary> (required); SEED (default 20260706);
#      PQ_TMP (default /tmp/pq_deep) — set it to run two suites at once;
#      L_STRESS=1 expands the matrix and the leak loop.
# Requires: cargo build --release done, and on macOS the .dylib copied
# to target/release/libl_parquet.so (see README Quickstart).

set -u
cd "$(dirname "$0")/.."
: "${L_BIN:?set L_BIN to the L binary path}"
SEED="${SEED:-20260706}"
# Scratch root: PQ_TMP keeps two suites on one machine from deleting
# each other's fixtures (matrix.py `gen` rewrites the whole tree).
PQ_TMP="${PQ_TMP:-/tmp/pq_deep}"
export PQ_TMP
PY="uv run --with pyarrow --with numpy"
PYD="uv run --with pyarrow --with duckdb"
LOG="$PQ_TMP/logs"
mkdir -p "$LOG"
fail=0

runq() { # runq <script> <log> <required summary substring>
    "$L_BIN" "$1" </dev/null >"$2" 2>&1
    if ! grep -q "$3" "$2"; then
        echo "FAILED: $1 (want '$3'; see $2)"
        grep FAIL "$2" | head -20
        fail=1
    else
        grep -E "passed, [0-9]+ failed" "$2" | tail -1
    fi
}

echo "== baseline (existing 22) =="
$PY tests/make_fixtures.py >"$LOG/fixtures.out" 2>&1 || fail=1
runq tests/test_parquet.q "$LOG/base.out" "22 passed, 0 failed"
$PY tests/check_l_written.py || fail=1

echo "== W1 read surface (projection / meta / rg / multi-file) =="
runq tests/test_w1.q "$LOG/w1.out" ", 0 failed"

echo "== symbols as (dictionary; codes) =="
runq tests/test_codes.q "$LOG/codes.out" ", 0 failed"

echo "== writer options, codecs, encoding policy =="
runq tests/test_write.q "$LOG/write.out" ", 0 failed"
$PYD tests/check_write.py || fail=1

echo "== matrix + corners + hostile =="
$PY tests/matrix.py gen --seed "$SEED" || fail=1
runq "$PQ_TMP/driver.q" "$LOG/matrix.out" ", 0 failed"
$PY tests/matrix.py check --seed "$SEED" || fail=1

echo "== shake: large parallel cases x3 seeds =="
for s in 1 2 3; do
    $PY tests/matrix.py gen --shake --seed $((SEED + s)) || fail=1
    if [ "$s" = 1 ]; then
        RUST_BACKTRACE=1 "$L_BIN" "$PQ_TMP/driver_shake.q" \
            </dev/null >"$LOG/shake$s.out" 2>&1
    else
        "$L_BIN" "$PQ_TMP/driver_shake.q" \
            </dev/null >"$LOG/shake$s.out" 2>&1
    fi
    if ! grep -q ", 0 failed" "$LOG/shake$s.out"; then
        echo "FAILED: shake seed offset $s (see $LOG/shake$s.out)"
        fail=1
    else
        grep "SHAKE:" "$LOG/shake$s.out"
    fi
    $PY tests/matrix.py check --shake --seed $((SEED + s)) || fail=1
done

echo "== L-side edges =="
runq tests/test_edge.q "$LOG/edge.out" ", 0 failed"

echo "== adversarial (subprocess harness) =="
$PY tests/adversarial.py --bin "$L_BIN" || fail=1

echo "== leak loop =="
runq tests/test_leak.q "$LOG/leak.out" ", 0 failed"

if [ "$fail" = 0 ]; then
    echo "ALL SUITES GREEN"
else
    echo "SUITE FAILURES — see $LOG"
    exit 1
fi
