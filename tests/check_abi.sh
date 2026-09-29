#!/bin/sh
# check_abi.sh — the built library's contract with the L host, checked
# from its symbol tables (no L binary needed):
#   1. it EXPORTS l_abi, the witness that makes `2:` bind it against the
#      host (without it L treats it as a kdb+ k.h library, see ffi.rs);
#   2. every host function it IMPORTS is a k.h-named one: in the static
#      list below and, when the kdb+ compatibility libraries are present,
#      exported by each of them.  An L-only helper (as `nt` once was)
#      fails here, not as `undefined symbol` on a user's machine.
# Usage: tests/check_abi.sh [lib] [shim dir]
#   lib      default target/release/libl_parquet.so (.dylib on macOS)
#   shim dir default $ELLEHOME/<l64|m64>, else ~/elle/<l64|m64>

set -u
cd "$(dirname "$0")/.."
KH="ktn kpn kp kj ki kh kg kb kc ke kf ks kd kz kt ku ktj knk xD xT sn ss
r0 r1 krr orr ja js jk jv b9 d9 dl dot ee k"
if [ "$(uname)" = Darwin ]; then
    lib=${1:-target/release/libl_parquet.dylib}; arch=m64
    exp() { nm -gU "$1" | awk '{sub(/^_/,"",$3); print $3}'; }
    imp() { nm -m -u "$1" | awk '/dynamically looked up/ {
        s = $3; sub(/^_/, "", s); print s }'; }
else
    lib=${1:-target/release/libl_parquet.so}; arch=l64
    exp() { nm -D --defined-only "$1" | awk '{print $3}'; }
    # host functions: unversioned (libc's carry @GLIBC), not weak
    imp() { nm -D --undefined-only "$1" | awk '$1 == "U" && $2 !~ /@/ {
        print $2 }'; }
fi
dir=${2:-${ELLEHOME:-$HOME/elle}/$arch}
[ -f "$lib" ] || { echo "check_abi: no $lib (cargo build --release)"; exit 1; }
fail=0
exp "$lib" | grep -qx l_abi || { echo "FAIL: l_abi not exported"; fail=1; }
for s in $(imp "$lib"); do
    echo " $KH " | tr '\n' ' ' | grep -q " $s " ||
        { echo "FAIL: imports $s, not a k.h function"; fail=1; }
    for f in "$dir"/kdbk0v2.so "$dir"/kdbk0v3.so; do
        [ -f "$f" ] || continue
        exp "$f" | grep -qx "$s" ||
            { echo "FAIL: imports $s, absent from $f"; fail=1; }
    done
done
[ "$fail" = 0 ] && echo "check_abi: OK ($lib: l_abi + $(imp "$lib" |
    tr '\n' ' '))"
exit $fail
