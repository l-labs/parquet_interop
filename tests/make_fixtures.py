# make_fixtures.py — pyarrow-written Parquet fixtures for tests 13-16.
# Run: uv run --with pyarrow tests/make_fixtures.py
import pyarrow as pa
import pyarrow.parquet as pq

# 13: plain — no dictionary, no compression; exercises the raw decode
# path plus int/float nulls and a nulled timestamp[ns].
plain = pa.table({
    "a": pa.array([1, 2, 3], pa.int32()),
    "b": pa.array([1.5, 2.5, 3.5], pa.float64()),
    "s": pa.array(["x", "y", "z"], pa.string()),
    "bl": pa.array([True, False, True], pa.bool_()),
    "ts": pa.array([1577836800000000000, 1577923200000000000, None],
                   pa.timestamp("ns")),          # 2020-01-01, -02, null
    "ni": pa.array([1, None, 3], pa.int32()),
    "nf": pa.array([1.5, None, 3.5], pa.float64()),
})
pq.write_table(plain, "/tmp/pq_py_plain.parquet",
               use_dictionary=False, compression="none")

# 14: dictionary-encoded strings — exercises the dict-page decode path.
dic = pa.table({
    "s": pa.array(["a", "b", "a", "c", "b", "a"], pa.string()),
    "v": pa.array([1, 2, 3, 4, 5, 6], pa.int64()),
})
pq.write_table(dic, "/tmp/pq_py_dict.parquet",
               use_dictionary=True, compression="none")

# 15: zstd-compressed — exercises the decompression path.
zst = pa.table({
    "a": pa.array(range(1000), pa.int64()),
    "f": pa.array([i + 0.5 for i in range(1000)], pa.float64()),
})
pq.write_table(zst, "/tmp/pq_py_zstd.parquet", compression="zstd")

# 16: nested list column — must be REJECTED by pq_read with 'nyi.
nested = pa.table({
    "lst": pa.array([[1, 2], [3], []], pa.list_(pa.int64())),
})
pq.write_table(nested, "/tmp/pq_py_nested.parquet")

print("fixtures written: plain, dict, zstd, nested")

# 17: mixed dictionary / plain pages in ONE file.  `sd` is dictionary
# encoded, `sp` is written PLAIN, and `sf` is all-distinct with a 1 KB
# dictionary page limit so its dictionary overflows and later pages
# fall back to PLAIN mid-column — the case the reader's dictionary
# request has to survive without changing a single value.
n = 20000
mixed = pa.table({
    "sd": pa.array([f"s{i % 50}" for i in range(n)], pa.string()),
    "sp": pa.array([f"p{i % 37}" for i in range(n)], pa.string()),
    "sf": pa.array([f"u{i:06d}" for i in range(n)], pa.string()),
    "v": pa.array(range(n), pa.int64()),
})
pq.write_table(mixed, "/tmp/pq_py_mixed.parquet",
               use_dictionary=["sd", "sf"], compression="none",
               dictionary_pagesize_limit=1024, row_group_size=5000)

# 18: the SAME symbols written twice, once dictionary-encoded and once
# plain, so the two decode paths can be asserted value-identical.
same = pa.table({
    "s": pa.array([f"k{i % 13}" for i in range(5000)], pa.string()),
    "v": pa.array(range(5000), pa.int64()),
})
pq.write_table(same, "/tmp/pq_py_same_dict.parquet", use_dictionary=True,
               compression="none")
pq.write_table(same, "/tmp/pq_py_same_plain.parquet", use_dictionary=False,
               compression="none")

print("fixtures written: mixed pages, dict/plain twins")

# 19: a 3-file "glob" set — same schema, 4 row groups each, so global
# row-group windows can cross a file boundary — plus one file whose
# schema deliberately disagrees.
import os
os.makedirs("/tmp/pq_multi", exist_ok=True)
for k in range(3):
    part = pa.table({
        "sym": pa.array([f"m{(i + k) % 7}" for i in range(4000)], pa.string()),
        "v": pa.array([i + 1000 * k for i in range(4000)], pa.int64()),
        "f": pa.array([0.5 + i for i in range(4000)], pa.float64()),
    })
    pq.write_table(part, f"/tmp/pq_multi/part{k}.parquet",
                   row_group_size=1000, compression="zstd")
pq.write_table(pa.table({"sym": pa.array(["x"], pa.string()),
                         "v": pa.array([1], pa.int64())}),
               "/tmp/pq_multi/bad.parquet")

# 20: a flat column beside a nested one — projection reads the flat one,
# 'nyi is raised only when the list column is actually requested.
pq.write_table(pa.table({
    "lst": pa.array([[1, 2], [3], []], pa.list_(pa.int64())),
    "v": pa.array([1, 2, 3], pa.int64()),
}), "/tmp/pq_py_mixnest.parquet")

# 20b: a MULTI-LEAF root (a struct is two column chunks) ahead of the
# flat columns, which is what separates ROOT indices from LEAF ones:
# `v` is root 1 but leaf 2, and a reader that confuses the two decodes
# `s.b` into `v` and never says so.  Several row groups so the raw path
# and the (dictionary; codes) path both walk more than one chunk.
_n = 5000
pq.write_table(pa.table({
    "s": pa.array([{"a": i, "b": 1000000 + i} for i in range(_n)],
                  pa.struct([("a", pa.int64()), ("b", pa.int64())])),
    "v": pa.array([i * 7 for i in range(_n)], pa.int64()),
    "w": pa.array(["x%d" % (i % 13) for i in range(_n)], pa.string()),
}), "/tmp/pq_py_structfirst.parquet", row_group_size=1000)

# 20c: two files of ONE read whose timestamps carry different UNITS.
# Both map to KP, so they are the same table as far as L cares — and
# their raw i64s do not mean the same thing, which is the whole of the
# zero-copy path's contract.  Same instants in both, so the read has
# one right answer whichever path each file takes.
_b = 1_700_000_000_000_000_000
pq.write_table(pa.table({
    "ts": pa.array([_b, _b + 10**9], pa.timestamp("ns")),
    "v": pa.array([1, 2], pa.int64()),
}), "/tmp/pq_py_unit_ns.parquet", compression="none")
pq.write_table(pa.table({
    "ts": pa.array([_b // 1000, _b // 1000 + 10**6], pa.timestamp("us")),
    "v": pa.array([3, 4], pa.int64()),
}), "/tmp/pq_py_unit_us.parquet", compression="none")

print("fixtures written: multi-file set, flat-beside-nested")

# 21: FORGED footer row counts.  Thrift-compact encodes a RowGroup's
# num_rows as the i64 that directly follows total_byte_size, so
# anchoring on that pair rewrites the count the reader lays its window
# out from — without disturbing the identically encoded num_values each
# column chunk carries.  Same-width varints only: the footer length
# must stay honest so these files test the row counts and nothing else.
import struct


def _zzvar(n):
    v = ((n << 1) ^ (n >> 63)) & 0xFFFFFFFFFFFFFFFF
    out = bytearray()
    while True:
        b, v = v & 0x7F, v >> 7
        out.append(b | 0x80 if v else b)
        if not v:
            return bytes(out)


def _i64(v):
    return b"\x16" + _zzvar(v)


def _forge(src, dst, edits):
    raw = open(src, "rb").read()
    flen = struct.unpack("<I", raw[-8:-4])[0]
    start = len(raw) - 8 - flen
    md = pq.ParquetFile(src).metadata
    for g, new in edits:
        r = md.row_group(g)
        a = _i64(r.total_byte_size) + _i64(r.num_rows)
        b = _i64(r.total_byte_size) + _i64(new)
        assert len(a) == len(b), "varint width changed"
        j = raw.find(a, start)
        assert j >= 0, f"row group {g} pattern not found"
        raw = raw[:j] + b + raw[j + len(a):]
    open(dst, "wb").write(raw)


# rg0 alone claims 6000 of its 5000 rows: the FILE total no longer adds
# up, so the footer is refused the moment it is read.
_forge("/tmp/pq_py_mixed.parquet", "/tmp/pq_py_lie_rg.parquet",
       [(0, 6000)])
# rg0 +1000 and rg1 -1000: the file total still adds up, so only the
# per-row-group delivery check can catch this one.
_forge("/tmp/pq_py_mixed.parquet", "/tmp/pq_py_lie_bal.parquet",
       [(0, 6000), (1, 4000)])

print("fixtures written: forged footer row counts")
