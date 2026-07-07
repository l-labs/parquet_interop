#!/bin/sh
# run_all.sh — the whole deep suite, in order:
#   baseline (22)  ->  randomized matrix + corners + hostile (gen, run,
#   pyarrow check) -> 3 seeded shake passes over the large parallel
#   cases (one under RUST_BACKTRACE=1) -> L-side edges -> adversarial
#   subprocess harness -> leak loop.
# Env: L_BIN=<path to L binary> (required); SEED (default 20260706);
#      L_STRESS=1 expands the matrix and the leak loop.
# Requires: cargo build --release done, and on macOS the .dylib copied
# to target/release/libl_parquet.so (see README Quickstart).

set -u
cd "$(dirname "$0")/.."
: "${L_BIN:?set L_BIN to the L binary path}"
SEED="${SEED:-20260706}"
PY="uv run --with pyarrow --with numpy"
LOG=/tmp/pq_deep/logs
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

echo "== matrix + corners + hostile =="
$PY tests/matrix.py gen --seed "$SEED" || fail=1
runq /tmp/pq_deep/driver.q "$LOG/matrix.out" ", 0 failed"
$PY tests/matrix.py check --seed "$SEED" || fail=1

echo "== shake: large parallel cases x3 seeds =="
for s in 1 2 3; do
    $PY tests/matrix.py gen --shake --seed $((SEED + s)) || fail=1
    if [ "$s" = 1 ]; then
        RUST_BACKTRACE=1 "$L_BIN" /tmp/pq_deep/driver_shake.q \
            </dev/null >"$LOG/shake$s.out" 2>&1
    else
        "$L_BIN" /tmp/pq_deep/driver_shake.q \
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
