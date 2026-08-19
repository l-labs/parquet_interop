# check_write.py — third-party re-read of everything tests/test_write.q
# wrote: pyarrow (values, schema, encodings, statistics, row groups) and
# duckdb (a different reader entirely, over every codec).
#   uv run --with pyarrow --with duckdb tests/check_write.py
# Run AFTER tests/test_write.q.
import datetime as dt
import os
import sys

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

# Same scratch root the L side used (run_all.sh exports PQ_TMP).
D = os.environ.get("PQ_TMP", "/tmp/pq_deep") + "/w1write/"
CODECS = ["none", "zstd", "snappy", "lz4", "gzip"]
fail = 0


def check(name, ok, extra=""):
    global fail
    if not ok:
        fail += 1
        print(f"  FAIL {name} {extra}")
    else:
        print(f"  PASS {name}")


def f(n):
    return f"{D}{n}.parquet"


# ── the fixture L wrote for every codec: exact values, both readers ────
# t:([]b;g;h;i;j;f;s;dt;ts;sp) of 1000 rows, cycled from short literals.
N = 1000


def expect():
    """The Python-side truth for tests/test_write.q's `t` fixture."""
    cyc = lambda xs: [xs[k % len(xs)] for k in range(N)]
    return {
        "b": cyc([True, False, True]),
        "g": cyc([0, 1, 255]),
        "h": cyc([-32768, 1, -32767, 32767]),
        "i": cyc([None, 1, -2147483647]),
        "j": cyc([None, 1, -9223372036854775806]),
        "f": cyc([None, 1.5, -2.5, 1e308]),
        "s": cyc(["aa", "bb", "", "ccddeeff"]),
        "dt": cyc([None, dt.date(2020, 1, 1), dt.date(1999, 12, 31)]),
        # ns since 1970 — the writer shifts L's 2000 epoch on the way
        # out: 0N, 2020.01.01D12:00:00.000000001, 1970.01.01D0.
        "ts": cyc([None, 1577880000000000001, 0]),
        "sp": cyc([None, 1000000001, -43200000000000]),
    }


def check_values(path, who):
    t = pq.read_table(path)
    e = expect()
    ok = True
    for c in ("b", "g", "h", "i", "j", "s", "dt"):
        got = t.column(c).to_pylist()
        if got != e[c]:
            ok = False
            print(f"    {c}: {got[:6]} != {e[c][:6]}")
    # floats: exact, and a null must be None and never a smuggled NaN
    got = t.column("f").to_pylist()
    for k in range(N):
        a, b = got[k], e["f"][k]
        if (a is None) != (b is None) or (a is not None and a != b):
            ok = False
            print(f"    f[{k}]: {a} != {b}")
            break
    # ns temporals: python datetime cannot hold …000000001, so the
    # comparison is on the raw i64 the file actually carries.
    for c in ("ts", "sp"):
        got = t.column(c).cast(pa.int64()).to_pylist()
        if got != e[c]:
            ok = False
            print(f"    {c}: {got[:6]} != {e[c][:6]}")
    check(f"pyarrow values {who}", ok)


for c in CODECS:
    check_values(f(f"codec_{c}"), c)
check_values(f("codec_default"), "default")

# ── schema: symbols must present as plain strings, not dictionaries ───
s = pq.ParquetFile(f("codec_none")).schema_arrow
check("symbol column reads as string", str(s.field("s").type) == "string",
      str(s.field("s").type))
check("timestamp[ns] preserved", str(s.field("ts").type) == "timestamp[ns]",
      str(s.field("ts").type))
check("duration[ns] preserved", str(s.field("sp").type) == "duration[ns]",
      str(s.field("sp").type))

# ── the codec actually landed in the file ─────────────────────────────
# pyarrow prints codec 7 (LZ4_RAW) as plain "LZ4"; duckdb's
# parquet_metadata() names it LZ4_RAW, which is what is on disk.
WANT = {"none": {"UNCOMPRESSED"}, "zstd": {"ZSTD"}, "snappy": {"SNAPPY"},
        "lz4": {"LZ4", "LZ4_RAW"}, "gzip": {"GZIP"}}
for c in CODECS:
    m = pq.ParquetFile(f(f"codec_{c}")).metadata
    got = {m.row_group(0).column(j).compression
           for j in range(m.row_group(0).num_columns)}
    check(f"compression {c}", got <= WANT[c] and len(got) == 1, got)
m = pq.ParquetFile(f("codec_default")).metadata
got = {m.row_group(0).column(j).compression
       for j in range(m.row_group(0).num_columns)}
check("default compression is UNCOMPRESSED", got == {"UNCOMPRESSED"}, got)

# ── encoding policy, straight out of the file metadata ────────────────
def encodings(path):
    m = pq.ParquetFile(path).metadata
    out = {}
    for g in range(m.num_row_groups):
        rg = m.row_group(g)
        for j in range(rg.num_columns):
            col = rg.column(j)
            out.setdefault(col.path_in_schema, set()).update(col.encodings)
    return out


e = encodings(f("enc"))
check("sorted ints -> DELTA_BINARY_PACKED",
      "DELTA_BINARY_PACKED" in e["srt"] and "RLE_DICTIONARY" not in e["srt"],
      e["srt"])
check("low-cardinality ints -> RLE_DICTIONARY",
      "RLE_DICTIONARY" in e["lo"], e["lo"])
check("spread ints -> PLAIN",
      "PLAIN" in e["hi"] and "RLE_DICTIONARY" not in e["hi"]
      and "DELTA_BINARY_PACKED" not in e["hi"], e["hi"])
check("symbols -> RLE_DICTIONARY", "RLE_DICTIONARY" in e["sym"], e["sym"])
check("floats -> PLAIN",
      "PLAIN" in e["fl"] and "RLE_DICTIONARY" not in e["fl"], e["fl"])
check("booleans -> RLE", e["bo"] == {"RLE"}, e["bo"])

# floats stay PLAIN under every codec: BYTE_STREAM_SPLIT costs more
# than it saves once the codec is stronger than zstd-1 (see write.rs).
for c in CODECS:
    ef = encodings(f(f"codec_{c}"))["f"]
    check(f"floats -> PLAIN ({c})",
          "PLAIN" in ef and "BYTE_STREAM_SPLIT" not in ef, ef)

e = encodings(f("enc_nodict"))
check("dict=0b: no dictionary anywhere",
      not any("RLE_DICTIONARY" in v for v in e.values()), e)
check("dict=0b: symbols fall back to PLAIN", "PLAIN" in e["sym"], e["sym"])

# ── statistics + row groups ───────────────────────────────────────────
m = pq.ParquetFile(f("codec_none")).metadata
check("stats on by default",
      all(m.row_group(0).column(j).statistics is not None
          for j in range(m.row_group(0).num_columns)))
st = m.row_group(0).column(3).statistics          # `i`, has nulls
check("null_count present", st.null_count == 334, st.null_count)
m = pq.ParquetFile(f("nostats")).metadata
check("stats=0b turns them off",
      all(m.row_group(0).column(j).statistics is None
          for j in range(m.row_group(0).num_columns)))

check("rg=100 gives 10 row groups",
      pq.ParquetFile(f("rg100")).metadata.num_row_groups == 10,
      pq.ParquetFile(f("rg100")).metadata.num_row_groups)
check("1M rows is one row group",
      pq.ParquetFile(f("rows1m")).metadata.num_row_groups == 1)
check("2M+1 rows is three row groups",
      pq.ParquetFile(f("rows2m1")).metadata.num_row_groups == 3)
check("2M+1 rows @ rg=700000 is three row groups",
      pq.ParquetFile(f("rows2m1_rg")).metadata.num_row_groups == 3,
      pq.ParquetFile(f("rows2m1_rg")).metadata.num_row_groups)
check("empty table has no row groups",
      pq.ParquetFile(f("rows0")).metadata.num_row_groups == 0)
check("empty table keeps its column",
      pq.read_table(f("rows0")).num_columns == 1)
check("5000 columns survive",
      pq.read_table(f("wide5000")).num_columns == 5000)

# ── row-group boundaries hold the right rows, in order ────────────────
t = pq.read_table(f("rows2m1"))
a = t.column("a").to_pylist()
check("2M+1 values in order", a == list(range(2097153)))
t = pq.read_table(f("rows1m"))
check("1M values in order", t.column("a").to_pylist() == list(range(1048576)))

# ── duckdb: a completely independent reader, every codec ──────────────
con = duckdb.connect()
for c in CODECS + ["default"]:
    q = f"SELECT count(*) c, sum(j) sj, count(s) cs FROM " \
        f"read_parquet('{f('codec_' + c)}')"
    got = con.execute(q).fetchone()
    check(f"duckdb reads {c}", got[0] == N, got)
got = con.execute(
    f"SELECT sum(srt), count(distinct lo), count(distinct sym) "
    f"FROM read_parquet('{f('enc')}')").fetchone()
check("duckdb reads the encoding fixture",
      got == (99999 * 100000 // 2, 7, 3), got)
got = con.execute(
    f"SELECT count(*) FROM read_parquet('{f('rows2m1')}')").fetchone()
check("duckdb reads 2M+1 rows", got[0] == 2097153, got)
got = con.execute(
    f"SELECT count(*) FROM read_parquet('{f('wide5000')}')").fetchone()
check("duckdb reads 5000 columns", got[0] == 4, got)
# and the codec name as it is actually written to the file
got = con.execute(
    f"SELECT DISTINCT compression FROM "
    f"parquet_metadata('{f('codec_lz4')}')").fetchall()
check("lz4 is LZ4_RAW on disk", got == [("LZ4_RAW",)], got)

print(f"check_write: {'ALL PASSED' if not fail else str(fail) + ' FAILED'}")
sys.exit(1 if fail else 0)
