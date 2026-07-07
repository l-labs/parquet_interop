# adversarial.py — corrupted-file harness: every case runs pq_read AND
# pq_stream on a hostile file in a FRESH L subprocess, so a SIGSEGV in
# any single case is isolated and reported as that case's failure.
# All cases must end with the L process alive and the error trapped —
# krr text, never a signal.
#
# Cases: systematic truncations (magic / dict page / data page / footer),
# corrupted PAR1 magics, lying footer lengths, mid-file byte flips,
# empty/tiny files, non-Parquet inputs (.arrow IPC file, CSV text,
# random bytes), a directory path, and a missing path.
#
# Usage: uv run --with pyarrow --with numpy tests/adversarial.py \
#            --bin /path/to/l [--lib target/release/libl_parquet]

import argparse
import os
import struct
import subprocess
import sys

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

DIR = "/tmp/pq_adv"


def build_base():
    """A real multi-row-group zstd file whose regions we then attack."""
    n = 10_000
    rng = np.random.default_rng(7)
    t = pa.table({
        "a": pa.array(rng.integers(-2**40, 2**40, n)),
        "f": pa.array(rng.standard_normal(n)),
        "s": pa.array([f"sym{i % 50}" for i in range(n)]).dictionary_encode(),
    })
    pq.write_table(t, f"{DIR}/base.parquet", compression="zstd",
                   row_group_size=1000)
    with open(f"{DIR}/base.parquet", "rb") as f:
        return f.read()


def variants(raw):
    """(name, bytes) hostile rewrites of the base file."""
    total = len(raw)
    flen = struct.unpack("<I", raw[-8:-4])[0]
    fstart = total - 8 - flen                    # first byte of footer
    out = [("empty", b""), ("tiny_magic", b"PAR1"),
           ("double_magic", b"PAR1PAR1"), ("only_magic_pair", b"PAR1" * 3)]
    cuts = sorted({4, 8, total // 4, total // 2, 3 * total // 4,
                   fstart - 1, fstart + 2, total - 9, total - 8,
                   total - 5, total - 4, total - 1})
    for c in cuts:
        if 0 < c < total:
            out.append((f"trunc_{c}", raw[:c]))
    out.append(("bad_head_magic", b"XAR1" + raw[4:]))
    out.append(("bad_tail_magic", raw[:-4] + b"PAR2"))
    out.append(("bad_both_magic", b"XAR1" + raw[4:-4] + b"2RAP"))
    for lie in [0, 1, flen - 1, flen + 1, total, 0x7FFFFFFF, 0xFFFFFFFF]:
        out.append((f"footer_len_{lie}",
                    raw[:-8] + struct.pack("<I", lie & 0xFFFFFFFF)
                    + b"PAR1"))
    rng = np.random.default_rng(20260706)
    for k in range(6):                           # mid-file bit rot
        pos = int(rng.integers(8, max(9, fstart)))
        b = bytearray(raw)
        b[pos] ^= 0xFF
        out.append((f"flip_{k}_at_{pos}", bytes(b)))
    b = bytearray(raw)                           # footer bit rot
    b[fstart + 3] ^= 0xFF
    out.append(("flip_footer", bytes(b)))
    out.append(("appended_garbage", raw + b"\x00" * 512))
    return out


def junk_files():
    """Non-Parquet inputs that must reject cleanly."""
    t = pa.table({"x": pa.array([1, 2, 3])})
    with pa.ipc.new_file(f"{DIR}/notpq.arrow", t.schema) as w:
        w.write_table(t)                         # a valid .arrow file
    with open(f"{DIR}/notpq.csv", "w") as f:
        f.write("a,b\n1,2\n3,4\n")
    with open(f"{DIR}/random.bin", "wb") as f:
        f.write(np.random.default_rng(3).bytes(4096))
    return [("arrow_ipc_file", f"{DIR}/notpq.arrow"),
            ("csv_text", f"{DIR}/notpq.csv"),
            ("random_bytes", f"{DIR}/random.bin"),
            ("directory_path", DIR),
            ("missing_path", f"{DIR}/no_such_file.parquet")]


def run_case(l_bin, lib, name, path, results):
    qf = f"{DIR}/q/{name}.q"
    with open(qf, "w") as f:
        f.write(f'pr:hsym[`$"{lib}"] 2: (`pq_read; 1)\n'
                f'ps:hsym[`$"{lib}"] 2: (`pq_stream; 1)\n'
                f'r:@[pr;`$":{path}";{{"E: ",x}}]\n'
                f'show $[10=type r;r;"TYPE: ",string type r]\n'
                f's:@[ps;(`$":{path}";`$":{DIR}/out_{name}");'
                '{"E: ",x}]\n'
                'show $[10=type s;s;"TYPE: ",string type s]\n'
                'show "ADV-OK"\n\\\\\n')
    try:
        p = subprocess.run([l_bin, qf], capture_output=True, text=True,
                           timeout=120, stdin=subprocess.DEVNULL)
    except subprocess.TimeoutExpired:
        results.append((name, "TIMEOUT", ""))
        return
    out = p.stdout + p.stderr
    if p.returncode < 0:
        results.append((name, f"SIGNAL {-p.returncode}", out[-200:]))
    elif "ADV-OK" not in out:
        results.append((name, f"NO-MARKER rc={p.returncode}", out[-200:]))
    elif "internal panic" in out:
        results.append((name, "PANIC-CAUGHT", ""))  # no crash, but noted
    else:
        results.append((name, "OK", ""))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--lib", default="target/release/libl_parquet")
    a = ap.parse_args()
    lib = os.path.abspath(a.lib)
    os.makedirs(f"{DIR}/q", exist_ok=True)
    raw = build_base()
    cases = []
    for name, data in variants(raw):
        p = f"{DIR}/{name}.parquet"
        with open(p, "wb") as f:
            f.write(data)
        cases.append((name, p))
    cases += junk_files()
    results = []
    for name, path in cases:
        run_case(a.bin, lib, name, path, results)
    ok = sum(1 for _, st, _ in results if st in ("OK", "PANIC-CAUGHT"))
    panics = [n for n, st, _ in results if st == "PANIC-CAUGHT"]
    bad = [(n, st, o) for n, st, o in results
           if st not in ("OK", "PANIC-CAUGHT")]
    for n, st, o in bad:
        print(f"ADV FAIL {n}: {st}\n{o}")
    if panics:
        print(f"ADV note — caught panics (no crash): {panics}")
    print(f"ADVERSARIAL: {ok} passed, {len(bad)} failed"
          f" (of {len(results)} cases)")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
