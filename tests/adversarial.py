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

# Scratch root — see matrix.py: overridable so two suites can run on
# one machine without rewriting each other's files.
DIR = os.environ.get("PQ_TMP_ADV", os.environ.get("PQ_TMP", "/tmp/pq_deep")
                     + "_adv")


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
    m = pq.ParquetFile(f"{DIR}/base.parquet").metadata
    rg = [(m.row_group(g).total_byte_size, m.row_group(g).num_rows)
          for g in range(m.num_row_groups)]
    with open(f"{DIR}/base.parquet", "rb") as f:
        return f.read(), m.num_rows, rg


def zzvar(n):
    """Thrift compact zigzag varint bytes for an i64 value."""
    v = ((n << 1) ^ (n >> 63)) & 0xFFFFFFFFFFFFFFFF
    out = bytearray()
    while True:
        b, v = v & 0x7F, v >> 7
        out.append(b | 0x80 if v else b)
        if not v:
            return bytes(out)


def swap(raw, fstart, old, new, skip=0):
    """Rewrite one FOOTER occurrence of the byte string `old` with `new`,
    passing over the first `skip` hits.  The two must be the SAME WIDTH:
    the trailing 4-byte footer length is not what these cases test, and
    a shifted footer would test it instead.  Returns None when the
    pattern is not there to patch."""
    if len(old) != len(new):
        return None
    j = fstart - 1
    for _ in range(skip + 1):
        j = raw.find(old, j + 1)
        if j < 0:
            return None
    out = bytearray(raw)
    out[j:j + len(old)] = new
    return bytes(out)


def i64(v):
    """One thrift-compact i64 struct field: "next field id" header byte
    plus the zigzag varint."""
    return b"\x16" + zzvar(v)


def rg_rows(raw, fstart, tbs, old, new, skip=0):
    """Rewrite ONE row group's num_rows.  Thrift compact writes a
    RowGroup's fields in id order, so num_rows is the i64 that directly
    FOLLOWS total_byte_size — anchoring on that pair is what tells it
    apart from the identically encoded `num_values` every column chunk
    of the same row group also carries.  Equal-sized row groups of
    fixed-width columns share a total_byte_size, hence `skip`."""
    return swap(raw, fstart, i64(tbs) + i64(old), i64(tbs) + i64(new),
                skip)


# ── data PAGE forgeries ─────────────────────────────────────────────
# The footer forgeries above attack the row-group counts; these attack
# the PAGE headers underneath them, which is what the zero-copy read
# path decodes straight into host memory.  The file is uncompressed and
# statistics-free so the header of each first data page sits exactly at
# the footer's `data_page_offset` and can be parsed byte by byte.


def rvarint(b, i):
    """Read one unsigned LEB128 varint; returns (value, next index)."""
    v = sh = 0
    while True:
        x = b[i]
        i += 1
        v |= (x & 0x7F) << sh
        sh += 7
        if not x & 0x80:
            return v, i


def page_num_values(raw, off):
    """Locate the num_values varint of the data page header at `off`.
    Thrift compact: field 1 type, 2 uncompressed size, 3 compressed
    size, then the DataPageHeader struct (field 5, delta 2 -> 0x2C)
    whose own field 1 is num_values.  Returns (start, end, value)."""
    i = off
    for _ in range(3):
        assert raw[i] == 0x15, f"unexpected field header {raw[i]:#x}"
        _, i = rvarint(raw, i + 1)
    assert raw[i] == 0x2C, f"not a v1 data page header ({raw[i]:#x})"
    i += 1
    assert raw[i] == 0x15, f"num_values header {raw[i]:#x}"
    st = i + 1
    zz, en = rvarint(raw, st)
    return st, en, (zz >> 1) ^ -(zz & 1)


def wvarint(n):
    """Zigzag + LEB128, the encoding page_num_values reads."""
    v = (n << 1) ^ (n >> 63) if n < 0 else n << 1
    out = bytearray()
    while True:
        b, v = v & 0x7F, v >> 7
        out.append(b | 0x80 if v else b)
        if not v:
            return bytes(out)


def build_pages():
    """An uncompressed, stats-free file plus the page offsets to attack."""
    n = 2000
    t = pa.table({
        "a": pa.array(np.arange(n, dtype=np.int64)),
        "f": pa.array(np.arange(n, dtype=np.float64)),
        "s": pa.array([f"s{i % 50}" for i in range(n)]),
    })
    pq.write_table(t, f"{DIR}/pages.parquet", compression="none",
                   row_group_size=n, use_dictionary=["s"],
                   write_statistics=False, data_page_size=1 << 24)
    m = pq.ParquetFile(f"{DIR}/pages.parquet").metadata.row_group(0)
    off = [m.column(c).data_page_offset for c in range(3)]
    with open(f"{DIR}/pages.parquet", "rb") as f:
        return f.read(), off, n


def page_variants(raw, off, n):
    """Page-level forgeries: a page that claims more values than the row
    group has rows, one that claims fewer, one whose data is cut short,
    and a dictionary page whose indices point past the dictionary."""
    out = []
    for nm, col, lie in [("page_values_over_rg", 0, 8000),
                         ("page_values_under_rg", 0, 500),
                         ("page_values_over_f64", 1, 8000)]:
        st, en, was = page_num_values(raw, off[col])
        assert was == n, f"page num_values {was} != {n}"
        new = wvarint(lie)
        # Same varint width keeps every following byte where it was, so
        # the ONLY lie in the file is the count itself.
        assert len(new) == en - st, f"{nm}: varint width {len(new)}"
        out.append((nm, raw[:st] + new + raw[en:]))
    # The page still claims 2000 values but its bytes stop early: the
    # decoder must run out and error, never read past the buffer.
    st, en, _ = page_num_values(raw, off[1])
    cut = off[2]                                 # start of the next chunk
    out.append(("page_data_truncated",
                raw[:cut - 4096] + b"\x00" * 4096 + raw[cut:]))
    # RLE_DICTIONARY indices of 0x3F against a 50-entry dictionary.
    ds = off[2]
    _, en2, _ = page_num_values(raw, ds)
    body = en2 + 24                              # past the rest of the header
    out.append(("dict_index_past_end",
                raw[:body] + b"\xff" * 64 + raw[body + 64:]))
    return out


def variants(raw, nrows, rgs):
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
    # Forged ROW COUNTS.  A row group that claims more rows than its
    # pages hold leaves the tail of a pre-sized column untouched —
    # uninitialized longs, and a NULL symbol pointer whose length prefix
    # the host dereferences (SIGSEGV, not an error).  One that claims
    # fewer silently drops rows.  Both must reject.  The balanced case
    # keeps the FILE total honest so only the per-row-group check can
    # catch it; the lone ones are caught the moment the footer is read.
    (t0, n0), (t1, n1), (t5, n5) = rgs[0], rgs[1], rgs[5]
    for nm, tbs, was, lie, skip in [
            ("rg_rows_inflated", t0, n0, n0 + 200, 0),
            ("rg_rows_deflated", t0, n0, n0 - 200, 0),
            ("rg_rows_inflated_mid", t5, n5, n5 + 200, 5)]:
        v = rg_rows(raw, fstart, tbs, was, lie, skip)
        assert v, f"adversarial: could not forge {nm}"
        out.append((nm, v))
    # rg0 +200 and rg1 -200: the FILE total still adds up, so only the
    # per-row-group delivery check can catch this one.
    v = rg_rows(raw, fstart, t0, n0, n0 + 200)
    v = rg_rows(v, fstart, t1, n1, n1 - 200)
    assert v, "adversarial: could not forge rg_rows_balanced_lie"
    out.append(("rg_rows_balanced_lie", v))
    v = swap(raw, fstart, i64(nrows), i64(nrows + 2000))
    assert v, "adversarial: could not forge file_rows_mismatch"
    out.append(("file_rows_mismatch", v))
    praw, poff, pn = build_pages()
    out += page_variants(praw, poff, pn)
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
        f.write(f'p:"{lib}"\n'
                'LD:{[f] .[{hsym[`$y] 2: (x;1)};(f;p);'
                '{[f;e] hsym[`$p] 2: (f;1i)}[f]]}\n'
                'pr:LD`pq_read\n'
                'ps:LD`pq_stream\n'
                f'r:@[pr;`$":{path}";{{"E: ",x}}]\n'
                f'show $[10=type r;r;"TYPE: ",string type r]\n'
                # the same file through the (dictionary; codes) path,
                # which decodes indices STRAIGHT into a K payload and
                # so has to be as unshakeable as the plain one
                f'c:@[pr;(`$":{path}";();(enlist`codes)!enlist 1b);'
                '{"E: ",x}]\n'
                f'show $[10=type c;c;"TYPE: ",string type c]\n'
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
    raw, nrows, rgs = build_base()
    cases = []
    for name, data in variants(raw, nrows, rgs):
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
